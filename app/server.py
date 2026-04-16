from __future__ import annotations

import argparse
import asyncio
import json
import logging
import ssl
from datetime import datetime, timezone

from websockets.exceptions import ConnectionClosed
from websockets.asyncio.server import ServerConnection, serve

from app.config import ServerConfig
from app.connection_manager import ConnectionManager
from app.db import Database
from app.socket_utils import build_listen_socket


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def setup_logging(syslog_file: str) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(syslog_file), logging.StreamHandler()],
    )


def bool_arg(raw: str) -> bool:
    if raw.lower() in {"1", "true", "yes", "y", "on"}:
        return True
    if raw.lower() in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid bool value: {raw}")


class LocationServer:
    def __init__(self, cfg: ServerConfig) -> None:
        self.cfg = cfg
        self.db = Database(cfg.db_file)
        self.manager = ConnectionManager()

    async def run(self) -> None:
        self.db.init_schema()
        ssl_ctx = None
        if self.cfg.tls_cert and self.cfg.tls_key:
            ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ssl_ctx.load_cert_chain(self.cfg.tls_cert, self.cfg.tls_key)
            ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2

        listen_sock = build_listen_socket(
            self.cfg.host,
            self.cfg.port,
            self.cfg.reuse_addr,
            self.cfg.keepalive,
            self.cfg.rcvbuf,
            self.cfg.tcp_nodelay,
        )

        async with serve(
            self.handle_connection,
            sock=listen_sock,
            ssl=ssl_ctx,
            ping_interval=self.cfg.ping_interval,
            ping_timeout=self.cfg.ping_timeout,
            max_size=self.cfg.max_message_size,
        ):
            logging.info("Server started on %s:%s", self.cfg.host, self.cfg.port)
            await asyncio.Future()

    async def handle_connection(self, ws: ServerConnection) -> None:
        remote = getattr(ws, "remote_address", None)
        remote_addr = f"{remote[0]}:{remote[1]}" if remote else "unknown"
        state = await self.manager.register(ws)
        logging.info("connection_open remote=%s", remote_addr)

        try:
            async for raw in ws:
                await self.handle_message(ws, raw, remote_addr)
        except ConnectionClosed as exc:
            logging.info("connection_closed remote=%s code=%s reason=%s", remote_addr, exc.code, exc.reason)
        except Exception as exc:
            logging.exception("connection_error remote=%s err=%s", remote_addr, exc)
        finally:
            final_state = await self.manager.unregister(ws)
            if final_state and final_state.session_id is not None:
                await self.db.close_session(final_state.session_id, "disconnect")
            logging.info("connection_finalize remote=%s", remote_addr)

    async def handle_message(self, ws: ServerConnection, raw: str, remote_addr: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            await self.db.log_security_event(remote_addr, None, "malformed_json", raw[:250])
            await ws.send(json.dumps({"type": "error", "message": "Malformed JSON"}))
            return

        message_type = payload.get("type")
        if message_type == "auth":
            await self.handle_auth(ws, payload, remote_addr)
            return

        if message_type == "register":
            await self.handle_register(ws, payload, remote_addr)
            return

        state = await self.manager.state_for(ws)
        if state.username is None:
            await self.db.log_security_event(remote_addr, None, "unauthenticated_message", str(message_type))
            await ws.send(json.dumps({"type": "error", "message": "Authenticate first"}))
            return

        if message_type == "subscribe":
            users = payload.get("users", [])
            if not isinstance(users, list) or not all(isinstance(u, str) for u in users):
                await ws.send(json.dumps({"type": "error", "message": "Invalid subscribe payload"}))
                return
            await self.manager.set_subscriptions(ws, set(users))
            await ws.send(json.dumps({"type": "subscribe_ok", "users": users}))
            return

        if message_type == "location_update":
            await self.handle_location_update(ws, payload)
            return

        if message_type == "admin_dump":
            await self.handle_admin_dump(ws)
            return

        await ws.send(json.dumps({"type": "error", "message": f"Unknown message type: {message_type}"}))

    async def handle_admin_dump(self, ws: ServerConnection) -> None:
        try:
            tables = self.db.admin_dump()
            payload = {"type": "admin_dump_ok", "tables": tables}
            await ws.send(json.dumps(payload))
            logging.info("admin_dump sent to %s", ws.remote_address)
        except Exception as exc:
            await ws.send(json.dumps({"type": "error", "message": f"Admin dump failed: {exc}"}))

    async def handle_auth(self, ws: ServerConnection, payload: dict, remote_addr: str) -> None:
        username = payload.get("username")
        password = payload.get("password")
        if not isinstance(username, str) or not isinstance(password, str):
            await ws.send(json.dumps({"type": "auth_error", "message": "username and password required"}))
            return

        user = self.db.authenticate(username, password)
        if user is None:
            await self.db.log_security_event(remote_addr, username, "auth_failed", "invalid credentials")
            await ws.send(json.dumps({"type": "auth_error", "message": "invalid credentials"}))
            return

        session_id = await self.db.create_session(user.id, remote_addr)
        await self.manager.set_authenticated(ws, username=user.username, user_id=user.id, session_id=session_id)
        await ws.send(json.dumps({"type": "auth_ok", "username": user.username, "session_id": session_id}))
        logging.info("auth_ok user=%s remote=%s session=%s", user.username, remote_addr, session_id)

    async def handle_register(self, ws: ServerConnection, payload: dict, remote_addr: str) -> None:
        username = payload.get("username")
        password = payload.get("password")
        if not isinstance(username, str) or not isinstance(password, str):
            await ws.send(json.dumps({"type": "register_error", "message": "username and password required"}))
            return
        if len(username) < 8:
            await ws.send(json.dumps({"type": "register_error", "message": "Username must be at least 8 characters"}))
            return
        if len(password) < 8:
            await ws.send(json.dumps({"type": "register_error", "message": "Password must be at least 8 characters"}))
            return
        if not any(c.isupper() for c in password):
            await ws.send(json.dumps({"type": "register_error", "message": "Password must contain at least one uppercase letter"}))
            return
        if not any(c in "@#" for c in password):
            await ws.send(json.dumps({"type": "register_error", "message": "Password must contain at least one special character (@ or #)"}))
            return
        try:
            self.db.create_user(username, password)
        except Exception:
            await ws.send(json.dumps({"type": "register_error", "message": "Username already exists"}))
            return
        # Auto-login after registration
        user = self.db.authenticate(username, password)
        if user is None:
            await ws.send(json.dumps({"type": "register_error", "message": "Registration succeeded but auto-login failed"}))
            return
        session_id = await self.db.create_session(user.id, remote_addr)
        await self.manager.set_authenticated(ws, username=user.username, user_id=user.id, session_id=session_id)
        await ws.send(json.dumps({"type": "register_ok", "username": user.username, "session_id": session_id}))
        logging.info("register_ok user=%s remote=%s session=%s", user.username, remote_addr, session_id)

    async def handle_location_update(self, ws: ServerConnection, payload: dict) -> None:
        state = await self.manager.state_for(ws)
        lat = payload.get("lat")
        lon = payload.get("lon")
        accuracy = payload.get("accuracy")
        client_ts = payload.get("client_ts")
        seq = payload.get("seq")

        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            await ws.send(json.dumps({"type": "error", "message": "lat/lon must be numbers"}))
            return
        if not (-90 <= float(lat) <= 90 and -180 <= float(lon) <= 180):
            await ws.send(json.dumps({"type": "error", "message": "lat/lon out of range"}))
            return
        if accuracy is not None and not isinstance(accuracy, (int, float)):
            await ws.send(json.dumps({"type": "error", "message": "accuracy must be numeric"}))
            return
        if seq is not None and not isinstance(seq, int):
            await ws.send(json.dumps({"type": "error", "message": "seq must be integer"}))
            return

        await self.db.store_location(
            user_id=state.user_id,
            session_id=state.session_id,
            lat=float(lat),
            lon=float(lon),
            accuracy=float(accuracy) if accuracy is not None else None,
            client_ts=client_ts if isinstance(client_ts, str) else None,
            seq=seq,
        )

        message = {
            "type": "location_broadcast",
            "username": state.username,
            "lat": lat,
            "lon": lon,
            "accuracy": accuracy,
            "client_ts": client_ts,
            "server_ts": utc_now(),
            "seq": seq,
        }

        subscribers = await self.manager.authorized_subscribers_for(state.username)
        serialized = json.dumps(message)
        send_tasks = [sub.ws.send(serialized) for sub in subscribers]
        if send_tasks:
            await asyncio.gather(*send_tasks, return_exceptions=True)


def parse_args() -> ServerConfig:
    parser = argparse.ArgumentParser(description="Secure TCP/WebSocket location server")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--db-file", required=True)
    parser.add_argument("--syslog-file", required=True)
    parser.add_argument("--tls-cert")
    parser.add_argument("--tls-key")
    parser.add_argument("--reuse-addr", default="true")
    parser.add_argument("--keepalive", default="true")
    parser.add_argument("--rcvbuf", default=262144, type=int)
    parser.add_argument("--tcp-nodelay", default="true")
    parser.add_argument("--ping-interval", default=20.0, type=float)
    parser.add_argument("--ping-timeout", default=20.0, type=float)
    parser.add_argument("--max-message-size", default=1048576, type=int)
    args = parser.parse_args()

    return ServerConfig(
        host=args.host,
        port=args.port,
        db_file=args.db_file,
        syslog_file=args.syslog_file,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
        reuse_addr=bool_arg(args.reuse_addr),
        keepalive=bool_arg(args.keepalive),
        rcvbuf=args.rcvbuf,
        tcp_nodelay=bool_arg(args.tcp_nodelay),
        ping_interval=args.ping_interval,
        ping_timeout=args.ping_timeout,
        max_message_size=args.max_message_size,
    )


def main() -> None:
    cfg = parse_args()
    setup_logging(cfg.syslog_file)
    server = LocationServer(cfg)
    asyncio.run(server.run())


if __name__ == "__main__":
    main()
