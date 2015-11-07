#!/usr/bin/python3

import sys
import socket
import tornado.ioloop
from functools import partial

import logging
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
log = logging.getLogger(__name__)

class AckSender(object):
    def __init__(self, port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setblocking(False)
        self._sock.bind(("0.0.0.0", port))
        self._io_loop = tornado.ioloop.IOLoop.instance()
        self._io_loop.add_handler(self._sock.fileno(), partial(self._callback, self._sock), self._io_loop.READ)

    def _callback(self, sock, fd, events):
        if events & self._io_loop.READ:
            self._callback_read(sock)
        if events & self._io_loop.ERROR:
            log.critical("IOLoop error")
            sys.exit(1)

    def _callback_read(self, sock):
        (data, addr) = sock.recvfrom(65535)
        log.debug("got UDP datagram from %s", str(addr))
        if not isinstance(data, str):
            data = data.decode("UTF-8")
        self._processdata(addr, data)

    def _send(self, addr, data):
        log.info('send(%s, "%s")', str(addr), data)
        if isinstance(data, str):
            data = data.encode("UTF-8")
        try:
            self._sock.sendto(data, addr)
        except:
            log.error("sendto error")

    def _processdata(self, addr, data):
        self.id = None
        self.inn = False
        for line in data.splitlines():
            if not ':' in line:
                continue
            try:
                (key, val) = line.split(':', 1)
            except:
                log.warning('datagram line format error: %s', line)
                continue
            if key == 'id':
                self.id = val
            if key == 'in':
                if not self.id:
                    log.error('No id from controller')
                    return
                self._send(addr, "id:%s\nin:%s\n" % (self.id, val))
                self.inn = True
        if self.id and not self.inn:
            self._send(addr, "id:%s\n" % self.id)
                
if __name__ == '__main__':
    AckSender(44445)
    tornado.ioloop.IOLoop.instance().start()
