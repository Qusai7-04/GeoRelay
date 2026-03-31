from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import tempfile
import time
from dataclasses import asdict

from websockets.client import connect

from app.db import Database
from app.config import ServerConfig, SocketProfile
from app.server import LocationServer


async def run_profile(db_file: str, profile: SocketProfile, updates: int, clients: int) -> dict:
    cfg = ServerConfig(
        host="127.0.0.1",
        port=9800,
        db_file=db_file,
        syslog_file=f"experiment-{profile.name}.log",
        tls_cert=None,
        tls_key=None,
        reuse_addr=True,
        keepalive=profile.keepalive,
        rcvbuf=profile.rcvbuf,
        tcp_nodelay=profile.tcp_nodelay,
        ping_interval=10,
        ping_timeout=10,
        max_message_size=1048576,
    )
    server = LocationServer(cfg)
    server_task = asyncio.create_task(server.run())
    await asyncio.sleep(0.6)

    db = Database(db_file)
    db.init_schema()
    for i in range(clients):
        try:
            db.create_user(f"u{i}", "pw")
        except Exception:
            pass

    latencies: list[float] = []

    async def one_client(ix: int) -> None:
        async with connect("ws://127.0.0.1:9800") as ws:
            await ws.send(json.dumps({"type": "auth", "username": f"u{ix}", "password": "pw"}))
            await ws.recv()
            await ws.send(json.dumps({"type": "subscribe", "users": [f"u{ix}"]}))
            await ws.recv()
            for seq in range(updates):
                t0 = time.perf_counter()
                await ws.send(
                    json.dumps(
                        {
                            "type": "location_update",
                            "lat": 1.0 + ix,
                            "lon": 2.0 + ix,
                            "accuracy": 1.0,
                            "seq": seq,
                            "client_ts": str(time.time()),
                        }
                    )
                )
                await ws.recv()
                latencies.append((time.perf_counter() - t0) * 1000)

    start = time.perf_counter()
    await asyncio.gather(*[one_client(i) for i in range(clients)])
    duration = time.perf_counter() - start

    server_task.cancel()
    try:
        await server_task
    except BaseException:
        pass

    total = updates * clients
    return {
        "profile": asdict(profile),
        "total_updates": total,
        "duration_s": round(duration, 3),
        "throughput_updates_s": round(total / duration, 2),
        "avg_latency_ms": round(statistics.mean(latencies), 2) if latencies else None,
        "p95_latency_ms": round(statistics.quantiles(latencies, n=20)[18], 2) if len(latencies) > 20 else None,
    }


async def main_async(db_file: str) -> None:
    profiles = [
        SocketProfile(name="baseline", keepalive=True, rcvbuf=262144, tcp_nodelay=True),
        SocketProfile(name="no_nodelay", keepalive=True, rcvbuf=262144, tcp_nodelay=False),
        SocketProfile(name="small_buf", keepalive=True, rcvbuf=16384, tcp_nodelay=True),
        SocketProfile(name="no_keepalive", keepalive=False, rcvbuf=262144, tcp_nodelay=True),
    ]

    for profile in profiles:
        with tempfile.NamedTemporaryFile(prefix=f"{profile.name}-", suffix=".db", delete=True) as tmp:
            target_db = tmp.name if db_file == ":temp:" else db_file
            result = await run_profile(target_db, profile, updates=60, clients=4)
        print(json.dumps(result, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Socket option experiment runner")
    parser.add_argument("--db-file", default=":temp:", help="Use :temp: to create per-profile temp DB")
    args = parser.parse_args()
    asyncio.run(main_async(args.db_file))


if __name__ == "__main__":
    main()
