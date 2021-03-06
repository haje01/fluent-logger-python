# -*- coding: utf-8 -*-

from __future__ import print_function
import socket
import threading
import time
import logging

import msgpack

SEND_FAIL_SEC = 6
MAX_SEND_FAIL = 10


_global_sender = None


def setup(tag, **kwargs):
    host = kwargs.get('host', 'localhost')
    port = kwargs.get('port', 24224)
    max_send_fail = kwargs.get('max_send_fail', MAX_SEND_FAIL)

    global _global_sender
    _global_sender = FluentSender(tag, host=host, port=port,
                                  max_send_fail=max_send_fail)


def get_global_sender():
    return _global_sender


class FluentSender(object):
    def __init__(self,
                 tag,
                 host='localhost',
                 port=24224,
                 bufmax=1 * 1024 * 1024,
                 timeout=3.0,
                 verbose=False,
                 max_send_fail=MAX_SEND_FAIL):

        self.tag = tag
        self.host = host
        self.port = port
        self.bufmax = bufmax
        self.timeout = timeout
        self.verbose = verbose
        self.max_send_fail = max_send_fail

        self.socket = None
        self.pendings = None
        self.last_send_fail = None
        self.send_fail_cnt = 0
        self.lock = threading.Lock()

        try:
            self._reconnect()
        except Exception:
            # will be retried in emit()
            self._close()

    def emit(self, label, data):
        cur_time = int(time.time())
        self.emit_with_time(label, cur_time, data)

    def emit_with_time(self, label, timestamp, data):
        bytes_ = self._make_packet(label, timestamp, data)
        self._send(bytes_)

    def _make_packet(self, label, timestamp, data):
        if label:
            tag = '.'.join((self.tag, label))
        else:
            tag = self.tag
        packet = (tag, timestamp, data)
        if self.verbose:
            print(packet)
        return msgpack.packb(packet)

    def _send(self, bytes_):
        self.lock.acquire()
        try:
            self._send_internal(bytes_)
        finally:
            self.lock.release()

    def _send_internal(self, bytes_):
        # logging.debug("_send_internal")
        # buffering
        if self.pendings:
            self.pendings += bytes_
            bytes_ = self.pendings

        try:
            # reconnect if possible
            self._reconnect()

            # send message
            self.socket.sendall(bytes_)

            # send finished
            self.pendings = None
            self.last_send_fail = None
            self.send_fail_cnt = 0
        except Exception, e:
            logging.error("FluentSender::_send_internal - Exception"
                          " '{}'".format(e))
            # close socket
            self._close()
            self.send_fail_cnt += 1
            logging.debug("FluentSender::_send_internal - send_fail_cnt {},"
                          " max_send_fail {}".format(self.send_fail_cnt,
                                                     self.max_send_fail))
            # clear buffer if it exceeds max bufer size
            if self.send_fail_cnt > self.max_send_fail:
                logging.error("raising")
                self.send_fail_cnt = 0
                raise
            if self.last_send_fail and time.time() - self.last_send_fail >\
                    SEND_FAIL_SEC:
                logging.error("send fail too long")
                self.last_send_fail = None
                raise
            elif self.pendings and (len(self.pendings) > self.bufmax):
                # TODO: add callback handler here
                logging.error("fail pending {} > bufmax "
                              "{}".format(len(self.pendings), self.bufmax))
                self.pendings = None
                raise
            else:
                self.pendings = bytes_
                logging.warning("pending {} bytes".format(len(bytes_)))
                if self.last_send_fail is None:
                    self.last_send_fail = time.time()

    def _reconnect(self):
        if not self.socket:
            if self.host.startswith('unix://'):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                sock.connect(self.host[len('unix://'):])
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                sock.connect((self.host, self.port))
            self.socket = sock

    def _close(self):
        if self.socket:
            self.socket.close()
        self.socket = None
