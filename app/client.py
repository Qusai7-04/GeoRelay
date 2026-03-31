from __future__ import annotations

import argparse
import asyncio
import getpass
import json
from datetime import datetime, timezone

from websockets.client import connect


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def sender(ws, username: str) -> None:
    seq = 0
    while True:
        line = await asyncio.to_thread(input, "Enter lat,lon,accuracy or 'sub user1,user2' or 'quit': ")
        line = line.strip()
        if not line:
            continue
        if line.lower() == "quit":
            await ws.close(code=1000, reason="client_quit")
            return
        if line.startswith("sub "):
            users = [u.strip() for u in line[4:].split(",") if u.strip()]
            await ws.send(json.dumps({"type": "subscribe", "users": users}))
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 2:
            print("Invalid input")
            continue
        try:
            lat = float(parts[0])
            lon = float(parts[1])
            accuracy = float(parts[2]) if len(parts) > 2 else None
        except ValueError:
            print("Invalid number format")
            continue

        seq += 1
        payload = {
            "type": "location_update",
            "lat": lat,
            "lon": lon,
            "accuracy": accuracy,
            "seq": seq,
            "client_ts": utc_now(),
            "username": username,
        }
        await ws.send(json.dumps(payload))


async def receiver(ws) -> None:
    async for message in ws:
        print(f"[RECV] {message}")


async def run_client(url: str, username: str, insecure: bool) -> None:
    password = getpass.getpass("Password: ")
    ssl_ctx = None
    if url.startswith("wss://") and insecure:
        import ssl

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    async with connect(url, ssl=ssl_ctx, max_size=1048576) as ws:
        await ws.send(json.dumps({"type": "auth", "username": username, "password": password}))
        print(f"Connected as {username}.")
        await asyncio.gather(sender(ws, username), receiver(ws))


def main() -> None:
    parser = argparse.ArgumentParser(description="Interactive location client")
    parser.add_argument("--url", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--insecure", action="store_true", help="Disable certificate validation for local testing")
    args = parser.parse_args()
    asyncio.run(run_client(args.url, args.username, args.insecure))


if __name__ == "__main__":
    main()
