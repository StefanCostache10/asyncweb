#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <libaio.h>
#include <errno.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* Definim constanta local, deoarece lipseste din aws.h */
#define AWS_IO_MAX_EVENTS 128

static int listenfd;
static int epollfd;
static int async_eventfd;
static io_context_t ctx;

/* --- Helpers --- */

struct connection *connection_create(int sockfd)
{
	struct connection *conn = malloc(sizeof(*conn));
	DIE(conn == NULL, "malloc");

	conn->sockfd = sockfd;
	memset(conn->recv_buffer, 0, BUFSIZ);
	memset(conn->send_buffer, 0, BUFSIZ);

	conn->state = STATE_INITIAL;
	conn->fd = -1;
	conn->recv_len = 0;
	conn->send_len = 0;
	conn->send_pos = 0;
	conn->file_pos = 0;
	conn->file_size = 0;
	conn->have_path = 0;
	conn->async_read_len = 0;

	conn->ctx = ctx;
	conn->eventfd = async_eventfd;

	return conn;
}

void connection_remove(struct connection *conn)
{
	if (conn->fd != -1) {
		close(conn->fd);
		conn->fd = -1;
	}
	
	/* Scoatem din epoll inainte de a inchide */
	w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	close(conn->sockfd);
	conn->state = STATE_CONNECTION_CLOSED;
	
	free(conn);
}

int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

enum resource_type connection_get_resource_type(struct connection *conn)
{
	if (strstr(conn->request_path, AWS_REL_STATIC_FOLDER) == conn->request_path + 1)
		return RESOURCE_TYPE_STATIC;
	if (strstr(conn->request_path, AWS_REL_DYNAMIC_FOLDER) == conn->request_path + 1)
		return RESOURCE_TYPE_DYNAMIC;
	return RESOURCE_TYPE_NONE;
}

/* --- Logică Acceptare --- */

void handle_new_connection(void)
{
	static int sockfd;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	struct sockaddr_in addr;
	struct connection *conn;
	int rc;
	int flags;

	sockfd = accept(listenfd, (struct sockaddr *)&addr, &addrlen);
	if (sockfd < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) return;
		ERR("accept");
		return;
	}

	flags = fcntl(sockfd, F_GETFL, 0);
	rc = fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
	if (rc < 0) {
		close(sockfd);
		return;
	}

	conn = connection_create(sockfd);

	/* Adaugam doar pentru EPOLLIN initial */
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
	if (rc < 0) {
		connection_remove(conn);
		return;
	}
}

/* --- Receive & Parse --- */

void receive_data(struct connection *conn)
{
	ssize_t bytes_recv;

	bytes_recv = recv(conn->sockfd,
					  conn->recv_buffer + conn->recv_len,
					  BUFSIZ - conn->recv_len, 0);

	if (bytes_recv < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK) {
			return;
		}
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	if (bytes_recv == 0) {
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	conn->recv_len += bytes_recv;
	conn->state = STATE_RECEIVING_DATA;
}

int parse_header(struct connection *conn)
{
	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};
	
	size_t bytes_parsed;

	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;

	bytes_parsed = http_parser_execute(&conn->request_parser, 
									   &settings_on_path, 
									   conn->recv_buffer, 
									   conn->recv_len);
	(void)bytes_parsed;

	if (conn->have_path) {
		return 0;
	}

	return -1;
}

int connection_open_file(struct connection *conn)
{
	char filepath[BUFSIZ];
	struct stat st;
	int rc;

	sprintf(filepath, "%s%s", AWS_DOCUMENT_ROOT, conn->request_path + 1);

	conn->res_type = connection_get_resource_type(conn);
	
	if (conn->res_type == RESOURCE_TYPE_NONE) {
		conn->state = STATE_SENDING_404;
		return 0;
	}

	rc = stat(filepath, &st);
	if (rc < 0) {
		conn->state = STATE_SENDING_404;
		return 0;
	}
	
	conn->file_size = st.st_size;

	conn->fd = open(filepath, O_RDONLY);
	if (conn->fd < 0) {
		conn->state = STATE_SENDING_404;
		return 0;
	}

	conn->state = STATE_SENDING_HEADER;
	return 0;
}

/* --- Logică Trimitere Helper --- */

void connection_prepare_send_reply_header(struct connection *conn)
{
	sprintf(conn->send_buffer,
		"HTTP/1.0 200 OK\r\n"
		"Date: Sun, 01 Jan 2025 00:00:00 GMT\r\n"
		"Server: SO-Assignment-AWS\r\n"
		"Content-Length: %ld\r\n"
		"Connection: close\r\n"
		"\r\n",
		conn->file_size);
	
	conn->send_len = strlen(conn->send_buffer);
	conn->send_pos = 0;
}

void connection_prepare_send_404(struct connection *conn)
{
	sprintf(conn->send_buffer,
		"HTTP/1.0 404 Not Found\r\n"
		"Content-Length: 0\r\n"
		"Connection: close\r\n"
		"\r\n");
	
	conn->send_len = strlen(conn->send_buffer);
	conn->send_pos = 0;
}

enum connection_state connection_send_msg_buffer(struct connection *conn)
{
	ssize_t bytes_sent;

	while (conn->send_pos < conn->send_len) {
		bytes_sent = send(conn->sockfd, 
						  conn->send_buffer + conn->send_pos, 
						  conn->send_len - conn->send_pos, 
						  0);
		
		if (bytes_sent < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				return conn->state;
			}
			return STATE_CONNECTION_CLOSED;
		}

		if (bytes_sent == 0) {
			return STATE_CONNECTION_CLOSED;
		}

		conn->send_pos += bytes_sent;
	}

	conn->send_pos = 0;
	conn->send_len = 0;
	
	return STATE_NO_STATE;
}

/* --- Implementare Async --- */

void connection_start_async_io(struct connection *conn)
{
	if (conn->file_pos >= conn->file_size) {
		conn->state = STATE_DATA_SENT;
		return;
	}

	/* Calculam cat citim: maxim BUFSIZ sau cat a ramas */
	size_t len_to_read = BUFSIZ;
	if (conn->file_size - conn->file_pos < BUFSIZ) {
		len_to_read = conn->file_size - conn->file_pos;
	}

	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, len_to_read, conn->file_pos);
	conn->iocb.data = conn;
	io_set_eventfd(&conn->iocb, async_eventfd);
	
	conn->piocb[0] = &conn->iocb;

	if (io_submit(ctx, 1, conn->piocb) < 0) {
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	conn->state = STATE_ASYNC_ONGOING;
	
	/* CRUCIAL: Scoatem EPOLLOUT de pe socket cat asteptam discul!
	   Daca nu facem asta, epoll_wait va returna imediat (busy wait) 
	   pentru ca socketul e liber la scriere. */
	w_epoll_update_ptr_in(epollfd, conn->sockfd, conn);
}

void connection_complete_async_io(struct connection *conn)
{
	conn->file_pos += conn->async_read_len;
	conn->send_len = conn->async_read_len;
	conn->send_pos = 0;

	conn->state = STATE_SENDING_DATA;
	
	/* Avem date de trimis, reactivam EPOLLOUT */
	w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
}

int connection_send_dynamic(struct connection *conn)
{
	enum connection_state ret_state;

	/* Trimitem ce e in buffer */
	ret_state = connection_send_msg_buffer(conn);
	
	if (ret_state == STATE_CONNECTION_CLOSED) {
		conn->state = STATE_CONNECTION_CLOSED;
		return -1;
	}

	if (ret_state != STATE_NO_STATE) {
		/* EAGAIN sau partial send */
		return 0;
	}

	/* Buffer golit. Verificam daca am terminat tot fisierul */
	if (conn->file_pos == conn->file_size) {
		conn->state = STATE_DATA_SENT;
		return 0;
	}

	/* Mai avem date pe disc, pornim urmatoarea citire */
	connection_start_async_io(conn);
	return 0;
}

enum connection_state connection_send_static(struct connection *conn)
{
	ssize_t bytes_sent;
	
	while (conn->file_pos < conn->file_size) {
		off_t offset = conn->file_pos;
		bytes_sent = sendfile(conn->sockfd, conn->fd, &offset, 
							  conn->file_size - conn->file_pos);
		
		conn->file_pos = offset;

		if (bytes_sent < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				return STATE_SENDING_DATA;
			}
			return STATE_CONNECTION_CLOSED;
		}

		if (bytes_sent == 0) {
			/* EOF de la sendfile, dar noi n-am terminat? */
			if (conn->file_pos < conn->file_size)
				return STATE_CONNECTION_CLOSED;
			break;
		}
	}

	if (conn->file_pos == conn->file_size)
		return STATE_DATA_SENT;

	return STATE_SENDING_DATA;
}

void handle_async_events(void)
{
	uint64_t val;
	int rc, i;
	struct io_event events[8];

	/* Consumam notificarea din eventfd */
	rc = read(async_eventfd, &val, sizeof(val));
	if (rc < 0) return;

	/* Luam evenimentele terminate */
	rc = io_getevents(ctx, 1, 8, events, NULL);
	for (i = 0; i < rc; i++) {
		struct connection *conn = (struct connection *)events[i].data;
		
		if ((long)events[i].res < 0) {
			conn->state = STATE_CONNECTION_CLOSED;
			connection_remove(conn);
			continue;
		}

		conn->async_read_len = events[i].res;
		connection_complete_async_io(conn);
	}
}

/* --- Handlers Main --- */

void handle_input(struct connection *conn)
{
	int rc;
	
	receive_data(conn);
	if (conn->state == STATE_CONNECTION_CLOSED)
		goto remove;

	rc = parse_header(conn);
	if (rc < 0) return; 

	connection_open_file(conn);

	/* Trecem la monitorizare IN|OUT pentru a trimite raspunsul */
	rc = w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
	if (rc < 0) goto remove;
	return;

remove:
	connection_remove(conn);
}

void handle_output(struct connection *conn)
{
	enum connection_state s_ret;

	/* Trimitere 404 */
	if (conn->state == STATE_SENDING_404) {
		if (conn->send_len == 0)
			connection_prepare_send_404(conn);
		
		s_ret = connection_send_msg_buffer(conn);
		if (s_ret == STATE_CONNECTION_CLOSED) {
			connection_remove(conn);
			return;
		}
		if (s_ret == STATE_NO_STATE) 
			conn->state = STATE_404_SENT;
	}

	/* Trimitere Header 200 */
	else if (conn->state == STATE_SENDING_HEADER) {
		if (conn->send_len == 0)
			connection_prepare_send_reply_header(conn);
		
		s_ret = connection_send_msg_buffer(conn);
		if (s_ret == STATE_CONNECTION_CLOSED) {
			connection_remove(conn);
			return;
		}
		
		if (s_ret == STATE_NO_STATE) {
			/* Header trimis complet */
			if (conn->res_type == RESOURCE_TYPE_STATIC) {
				conn->state = STATE_SENDING_DATA;
			} 
			else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
				/* Start Async IO: trecem in ASYNC_ONGOING si scoatem EPOLLOUT */
				connection_start_async_io(conn);
				return; /* Iesim direct, asteptam eventfd */
			}
		}
	}

	/* Trimitere Date */
	else if (conn->state == STATE_SENDING_DATA) {
		if (conn->res_type == RESOURCE_TYPE_STATIC) {
			conn->state = connection_send_static(conn);
		}
		else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			connection_send_dynamic(conn);
		}
		
		if (conn->state == STATE_CONNECTION_CLOSED) {
			connection_remove(conn);
			return;
		}
	}

	if (conn->state == STATE_DATA_SENT || conn->state == STATE_404_SENT) {
		connection_remove(conn);
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* Tratam erorile mai intai */
	if ((event & EPOLLERR) || (event & EPOLLHUP)) {
		connection_remove(conn);
		return;
	}

	if (event & EPOLLIN) {
		handle_input(conn);
	}
	/* Verificam daca conexiunea mai e valida inainte de output */
	if (conn->state != STATE_CONNECTION_CLOSED && (event & EPOLLOUT)) {
		handle_output(conn);
	}
}

int main(void)
{
	int rc;

	ctx = 0;
	rc = io_setup(AWS_IO_MAX_EVENTS, &ctx);
	DIE(rc < 0, "io_setup");

	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	async_eventfd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
	DIE(async_eventfd < 0, "eventfd");

	rc = w_epoll_add_fd_in(epollfd, async_eventfd);
	DIE(rc < 0, "add eventfd");

	listenfd = tcp_create_listener(AWS_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "add listener");

	dlog(LOG_INFO, "Server running on port %d\n", AWS_LISTEN_PORT);

	while (1) {
		struct epoll_event rev;

		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");

		if (rev.data.fd == listenfd) {
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} 
		else if (rev.data.fd == async_eventfd) {
			if (rev.events & EPOLLIN)
				handle_async_events();
		} 
		else {
			handle_client(rev.events, (struct connection *)rev.data.ptr);
		}
	}

	return 0;
}