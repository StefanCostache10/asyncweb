// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

/* Callback: Append parsed path segment to connection's request_path */
static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;
	size_t current_len = strlen(conn->request_path);

	if (current_len + len < BUFSIZ) {
		memcpy(conn->request_path + current_len, buf, len);
		conn->request_path[current_len + len] = '\0';
	}
	return 0;
}

/* Callback: Mark request as fully received */
static int aws_on_message_complete_cb(http_parser *p)
{
	struct connection *conn = (struct connection *)p->data;

	conn->have_path = 1;
	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	size_t len;

	/* Prepare the connection buffer to send the reply header. */
	len = snprintf(conn->send_buffer, BUFSIZ,
		"HTTP/1.1 200 OK\r\n"
		"Date: Sun, 01 Jan 2024 00:00:00 GMT\r\n"
		"Server: AWS\r\n"
		"Content-Length: %zu\r\n"
		"Connection: close\r\n"
		"\r\n",
		conn->file_size);

	conn->send_len = len;
	conn->send_pos = 0;
}

static void connection_prepare_send_404(struct connection *conn)
{
	size_t len;

	/* Prepare the connection buffer to send the 404 header. */
	len = snprintf(conn->send_buffer, BUFSIZ,
		"HTTP/1.1 404 Not Found\r\n"
		"Date: Sun, 01 Jan 2024 00:00:00 GMT\r\n"
		"Server: AWS\r\n"
		"Content-Length: 0\r\n"
		"Connection: close\r\n"
		"\r\n");

	conn->send_len = len;
	conn->send_pos = 0;
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* Get resource type depending on request path/filename. */
	if (strstr(conn->request_path, AWS_REL_STATIC_FOLDER) == conn->request_path)
		return RESOURCE_TYPE_STATIC;
	if (strstr(conn->request_path, AWS_REL_DYNAMIC_FOLDER) == conn->request_path)
		return RESOURCE_TYPE_DYNAMIC;

	return RESOURCE_TYPE_NONE;
}

struct connection *connection_create(int sockfd)
{
	/* Initialize connection structure on given socket. */
	struct connection *conn = calloc(1, sizeof(*conn));

	DIE(conn == NULL, "calloc");

	conn->sockfd = sockfd;
	conn->fd = -1;
	
	/* Create eventfd for Async I/O notifications */
	conn->eventfd = eventfd(0, EFD_NONBLOCK);
	DIE(conn->eventfd < 0, "eventfd");

	/* Initialize per-connection AIO context */
	conn->ctx = 0;
	int rc = io_setup(1, &conn->ctx);
	DIE(rc < 0, "io_setup");

	/* Add eventfd to epoll to handle async IO completion */
	rc = w_epoll_add_ptr_in(epollfd, conn->eventfd, conn);
	DIE(rc < 0, "w_epoll_add_ptr_in eventfd");

	conn->state = STATE_INITIAL;
	
	/* Initialize HTTP parser */
	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;

	return conn;
}

void connection_start_async_io(struct connection *conn)
{
	/* Start asynchronous operation (read from file). */
	size_t bytes_to_read = BUFSIZ;

	if (conn->file_pos + bytes_to_read > conn->file_size)
		bytes_to_read = conn->file_size - conn->file_pos;

	if (bytes_to_read == 0) {
		conn->state = STATE_DATA_SENT;
		return;
	}

	/* Prepare AIO request */
	io_prep_pread(&conn->iocb, conn->fd, conn->send_buffer, bytes_to_read, conn->file_pos);
	/* Link eventfd to notify us when done */
	io_set_eventfd(&conn->iocb, conn->eventfd);
	conn->piocb[0] = &conn->iocb;

	int rc = io_submit(conn->ctx, 1, conn->piocb);

	if (rc < 0) {
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	conn->state = STATE_ASYNC_ONGOING;
	
	/* CRITICAL: Disable EPOLLOUT on socket while waiting for disk IO. 
	 * This prevents busy-looping in epoll. 
	 */
	w_epoll_update_ptr_in(epollfd, conn->sockfd, conn);
}

void connection_remove(struct connection *conn)
{
	/* Remove connection handler. */
	if (conn->fd >= 0)
		close(conn->fd);
	
	/* Clean up AIO context */
	io_destroy(conn->ctx);

	if (conn->eventfd >= 0) {
		w_epoll_remove_ptr(epollfd, conn->eventfd, conn);
		close(conn->eventfd);
	}
	
	w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
	tcp_close_connection(conn->sockfd);
	
	free(conn);
}

void handle_new_connection(void)
{
	/* Handle a new connection request on the server socket. */
	struct sockaddr_in addr;
	socklen_t addrlen = sizeof(struct sockaddr_in);
	int sockfd;

	/* Accept new connection. */
	sockfd = accept(listenfd, (SSA *)&addr, &addrlen);
	if (sockfd < 0)
		return;

	/* Set socket to be non-blocking. */
	int flags = fcntl(sockfd, F_GETFL, 0);

	fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);

	/* Instantiate new connection handler. */
	struct connection *conn = connection_create(sockfd);

	/* Add socket to epoll. */
	int rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);

	DIE(rc < 0, "w_epoll_add_ptr_in");
}

void receive_data(struct connection *conn)
{
	/* Receive message on socket. */
	ssize_t bytes_recv;

	while (1) {
		if (conn->recv_len >= BUFSIZ) break;
		
		bytes_recv = recv(conn->sockfd, conn->recv_buffer + conn->recv_len, BUFSIZ - conn->recv_len, 0);
		if (bytes_recv < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				break;
			conn->state = STATE_CONNECTION_CLOSED;
			return;
		}
		if (bytes_recv == 0) {
			conn->state = STATE_CONNECTION_CLOSED;
			return;
		}
		conn->recv_len += bytes_recv;
		/* Simplification: Assume request fits in one read or handle incrementally */
		break; 
	}
	
	conn->state = STATE_RECEIVING_DATA;
}

int connection_open_file(struct connection *conn)
{
	/* Open file and update connection fields. */
	char filepath[BUFSIZ];

	/* Use request_path as is (it starts with /) relative to ROOT */
	snprintf(filepath, BUFSIZ, "%s%s", AWS_DOCUMENT_ROOT, conn->request_path);

	conn->fd = open(filepath, O_RDONLY);
	if (conn->fd < 0)
		return -1;

	struct stat st;

	if (fstat(conn->fd, &st) < 0) {
		close(conn->fd);
		conn->fd = -1;
		return -1;
	}

	conn->file_size = st.st_size;
	return 0;
}

void connection_complete_async_io(struct connection *conn)
{
	/* Complete asynchronous operation; operation returns successfully. */
	struct io_event events[1];
	int rc = io_getevents(conn->ctx, 1, 1, events, NULL);

	if (rc < 0) {
		conn->state = STATE_CONNECTION_CLOSED;
		return;
	}

	/* Get the number of bytes actually read */
	conn->async_read_len = events[0].res;
	conn->send_len = conn->async_read_len;
	conn->send_pos = 0;
	conn->file_pos += conn->async_read_len;

	conn->state = STATE_SENDING_DATA;
	
	/* Re-enable EPOLLOUT on socket to start sending data */
	w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
}

int parse_header(struct connection *conn)
{
	/* Parse the HTTP header and extract the file path. */
	http_parser_settings settings = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = aws_on_message_complete_cb
	};

	/* Parse only new bytes.
	 * TRICK: Use conn->file_pos (unused in RECEIVE state) to track parse offset
	 */
	size_t bytes_to_parse = conn->recv_len - conn->file_pos;
	size_t parsed = http_parser_execute(&conn->request_parser, &settings, 
										conn->recv_buffer + conn->file_pos, bytes_to_parse);

	conn->file_pos += parsed;

	if (conn->have_path) {
		return 0;
	}
	return -1;
}

enum connection_state connection_send_static(struct connection *conn)
{
	/* Send static data using sendfile(2). */
	off_t offset = conn->file_pos;
	size_t count = conn->file_size - conn->file_pos;
	ssize_t sent = sendfile(conn->sockfd, conn->fd, &offset, count);

	if (sent < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return STATE_SENDING_DATA;
		return STATE_CONNECTION_CLOSED;
	}

	conn->file_pos = offset; /* sendfile updates offset variable */
	if (conn->file_pos == conn->file_size)
		return STATE_DATA_SENT;

	return STATE_SENDING_DATA;
}

int connection_send_data(struct connection *conn)
{
	/* Send as much data as possible from the connection send buffer. */
	ssize_t sent;
	size_t left = conn->send_len - conn->send_pos;

	sent = send(conn->sockfd, conn->send_buffer + conn->send_pos, left, 0);

	if (sent < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return 0; 
		return -1;
	}

	conn->send_pos += sent;
	return sent;
}


int connection_send_dynamic(struct connection *conn)
{
	/* Send data previously read async */
	int rc = connection_send_data(conn);

	if (rc < 0) return -1;

	if (conn->send_pos == conn->send_len) {
		/* Finished sending current buffer */
		if (conn->file_pos == conn->file_size)
			conn->state = STATE_DATA_SENT;
		else {
			/* Read more */
			connection_start_async_io(conn);
		}
	}
	return 0;
}


void handle_input(struct connection *conn)
{
	/* Handle input information */

	/* Check if it's the eventfd signaling async completion */
	if (conn->state == STATE_ASYNC_ONGOING) {
		uint64_t val;
		/* Check if eventfd is readable. If read succeeds, it's the AIO completion. */
		if (read(conn->eventfd, &val, sizeof(val)) == sizeof(val)) {
			connection_complete_async_io(conn);
			if (conn->state == STATE_SENDING_DATA) {
				handle_output(conn); /* Try sending immediately */
			}
			return;
		}
		/* If read failed with EAGAIN, it's not eventfd, so it must be sockfd.
		   Continue to receive_data logic (likely disconnect). */
	}

	receive_data(conn);
	
	/* Try to parse */
	if (conn->state != STATE_CONNECTION_CLOSED)
		parse_header(conn);

	if (conn->have_path) {
		conn->state = STATE_REQUEST_RECEIVED;
		
		/* Reset file_pos used for parse offset */
		conn->file_pos = 0; 
		
		conn->res_type = connection_get_resource_type(conn);
		
		if (conn->res_type == RESOURCE_TYPE_NONE) {
			conn->state = STATE_SENDING_404;
			connection_prepare_send_404(conn);
		} else {
			if (connection_open_file(conn) < 0) {
				conn->state = STATE_SENDING_404;
				connection_prepare_send_404(conn);
			} else {
				conn->state = STATE_SENDING_HEADER;
				connection_prepare_send_reply_header(conn);
			}
		}
		/* Enable EPOLLOUT to send header */
		w_epoll_update_ptr_inout(epollfd, conn->sockfd, conn);
	} 
	else if (conn->state == STATE_CONNECTION_CLOSED) {
		goto remove;
	}
	return;

remove:
	connection_remove(conn);
}

void handle_output(struct connection *conn)
{
	/* Handle output information */
	int rc;

	switch (conn->state) {
	case STATE_SENDING_HEADER:
	case STATE_SENDING_404:
		rc = connection_send_data(conn);
		if (rc < 0) goto remove;
		
		if (conn->send_pos == conn->send_len) {
			if (conn->state == STATE_SENDING_404) {
				conn->state = STATE_404_SENT;
				goto remove; 
			} else {
				/* Header sent, start sending body */
				if (conn->res_type == RESOURCE_TYPE_STATIC) {
					conn->state = STATE_SENDING_DATA;
					conn->state = connection_send_static(conn);
				} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
					/* Start async read. This will disable EPOLLOUT. */
					connection_start_async_io(conn);
					return; 
				}
			}
		}
		break;

	case STATE_SENDING_DATA:
		if (conn->res_type == RESOURCE_TYPE_STATIC) {
			conn->state = connection_send_static(conn);
		} else if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			connection_send_dynamic(conn);
		}
		break;

	default:
		break;
	}

	if (conn->state == STATE_DATA_SENT || conn->state == STATE_CONNECTION_CLOSED) {
		goto remove;
	}
	return;

remove:
	connection_remove(conn);
}

void handle_client(uint32_t event, struct connection *conn)
{
	/* Handle new client. */
	if (event & EPOLLIN) {
		handle_input(conn);
	}
	/* Don't handle EPOLLOUT if async is ongoing (should be disabled anyway) */
	if ((event & EPOLLOUT) && (conn->state != STATE_ASYNC_ONGOING)) {
		handle_output(conn);
	} 
	if ((event & EPOLLHUP) || (event & EPOLLERR)) {
		connection_remove(conn);
	}
}

int main(void)
{
	int rc;

	/* Initialize multiplexing. */
	epollfd = w_epoll_create();
	DIE(epollfd < 0, "w_epoll_create");

	/* Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	DIE(listenfd < 0, "tcp_create_listener");

	/* Add server socket to epoll object */
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	DIE(rc < 0, "w_epoll_add_fd_in");

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* Wait for events. */
		rc = w_epoll_wait_infinite(epollfd, &rev);
		DIE(rc < 0, "w_epoll_wait_infinite");

		/* Switch event types */
		if (rev.data.fd == listenfd) {
			if (rev.events & EPOLLIN)
				handle_new_connection();
		} else {
			handle_client(rev.events, rev.data.ptr);
		}
	}

	return 0;
}