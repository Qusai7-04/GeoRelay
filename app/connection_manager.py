from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from websockets.server import WebSocketServerProtocol


@dataclass(slots=True)
class ConnState:
    ws: WebSocketServerProtocol
    username: str | None = None
    user_id: int | None = None
    session_id: int | None = None
    subscriptions: set[str] = field(default_factory=set)


class ConnectionManager:
    def __init__(self) -> None:
        self._by_ws: dict[WebSocketServerProtocol, ConnState] = {}
        self._lock = asyncio.Lock()

    async def register(self, ws: WebSocketServerProtocol) -> ConnState:
        state = ConnState(ws=ws)
        async with self._lock:
            self._by_ws[ws] = state
        return state

    async def unregister(self, ws: WebSocketServerProtocol) -> ConnState | None:
        async with self._lock:
            return self._by_ws.pop(ws, None)

    async def set_authenticated(self, ws: WebSocketServerProtocol, username: str, user_id: int, session_id: int) -> None:
        async with self._lock:
            state = self._by_ws[ws]
            state.username = username
            state.user_id = user_id
            state.session_id = session_id

    async def set_subscriptions(self, ws: WebSocketServerProtocol, usernames: set[str]) -> None:
        async with self._lock:
            self._by_ws[ws].subscriptions = usernames

    async def authorized_subscribers_for(self, username: str) -> list[ConnState]:
        async with self._lock:
            return [
                s
                for s in self._by_ws.values()
                if s.username is not None and (username in s.subscriptions or s.username == username)
            ]

    async def state_for(self, ws: WebSocketServerProtocol) -> ConnState:
        async with self._lock:
            return self._by_ws[ws]
