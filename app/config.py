from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ServerConfig:
    host: str
    port: int
    db_file: str
    syslog_file: str
    tls_cert: str | None
    tls_key: str | None
    reuse_addr: bool
    keepalive: bool
    rcvbuf: int
    tcp_nodelay: bool
    ping_interval: float
    ping_timeout: float
    max_message_size: int


@dataclass(slots=True)
class SocketProfile:
    name: str
    keepalive: bool
    rcvbuf: int
    tcp_nodelay: bool
