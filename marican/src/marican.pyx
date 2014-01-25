from libc.stdlib cimport malloc, free
from libc.stdio cimport sprintf
from libc.string cimport strchr, strncpy

cdef class Marican(object):
    def __init__(self, match_index=None, words_map=None, files_index=None):
        self.match_index = match_index
        self.words_map = words_map
        self.files_index = files_index

import socket
from libc.stdint cimport uint32_t

cdef extern from "string.h" nogil:
    char *stpcpy(char *dst, const char *src)

cdef extern from "sys/socket.h" nogil:
    ssize_t recv(int s, void *buffer, size_t length, int flags)
    ssize_t send(int s, void *buffer, size_t length, int flags)

cdef ssize_t socksend(int f, Py_ssize_t n, char *data):
    cdef ssize_t s, i = 0
    while i < n:
        s = send(f, data + i, n - i, 0)
        if s == 0:
            break
        i += s
    return i

from cpython cimport PyObject_GetBuffer, PyBUF_SIMPLE
cimport cpython.array

cdef int query(app, char *body, int size, str q):
    "Print query results to body of size"
    cdef char* pos = body
    pos = stpcpy(pos, '{"success": true, "results": [  ')

    files_index = app.files_index
    cdef uint32_t[:] matches = app.match_index[app.words_map[q]]
    cdef uint32_t match
    for i in range(matches.shape[0]):
        match = matches[i]
        pos += sprintf(pos, '"%s:%d", ', <char *> files_index[match >> 16], match & 0xffff)

    pos = stpcpy(pos - 2, ']}\n')

    n_used = <int>(pos - body)
    if n_used > size:
        raise OverflowError('out of memory by %d bytes' % n_used)
    return n_used

cdef class HTTPServer(object):
    cdef object app
    cdef object socket
    cdef int buff_size
    cdef char* buff

    def __init__(self, app, addr):
        self.buff_size = 512 * 1024
        self.buff = <char*>malloc(self.buff_size)
        self.app = app
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(addr)
        self.socket.listen(255)

    def run(self):
        cdef ssize_t n, t
        cdef char* buff = self.buff
        cdef int buff_size = self.buff_size
        cdef char* data
        cdef char* space
        while True:
            (client, addr) = self.socket.accept()
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            f = client.fileno()
            n = recv(f, buff, 512, 0)
            if n > 8:
                n = self.response(buff, buff, buff_size)
                socksend(f, n, buff)
            client.close()

    cdef int response(self, char *req, char *resp, int size):
        # Assume GET
        cdef int preamble_size = 100
        cdef char* path = req + 4
        cdef char* body = resp + preamble_size
        strchr(path, 0x20)[0] = 0
        cdef int n = self.body(path, body, size - preamble_size)
        strncpy(resp, ('HTTP/1.1 200 OK\r\n'
                       'Server: marican-server/1.0.0\r\n'
                       'Content-Type: application/json\r\n'
                       'Connection: close\r\n'
                       '\r\n'), preamble_size)
        return preamble_size + n

    cdef int body(self, const char *path, char *body, int size):
        cdef char* resp_encoded
        cdef uint32_t first_four = (<uint32_t*> path)[0]
        app = self.app

        # Checks for /?q= with integer comparison
        if first_four == 0x2f3f713d or first_four == 0x3d713f2f:
            return query(app, body, size, str(path + 4))

        path_v = path.decode('ascii')
        path_v = path_v.split(' ', 1)[0]
        body_v = '{}\n'
        if path_v.startswith('/index?path='):
            body_v = app.index(path_v[12:])
        elif path_v == '/isIndexed':
            body_v = app.is_indexed()
        elif path_v == '/healthcheck':
            body_v = app.healthcheck()

        return <int>(stpcpy(body, body_v) - body)
