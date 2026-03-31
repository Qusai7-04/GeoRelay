from __future__ import annotations

import socket


def build_listen_socket(host: str, port: int, reuse_addr: bool, keepalive: bool, rcvbuf: int, tcp_nodelay: bool) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1 if reuse_addr else 0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1 if keepalive else 0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, int(rcvbuf))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1 if tcp_nodelay else 0)
    sock.bind((host, port))
    sock.listen(512)
    sock.setblocking(False)
    return sock
