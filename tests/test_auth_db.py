from __future__ import annotations

import asyncio
import tempfile
import unittest

from app.db import Database


class AuthDbTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=True)
        self.db = Database(self.tmp.name)
        self.db.init_schema()

    def tearDown(self) -> None:
        self.tmp.close()

    def test_create_and_authenticate_user(self) -> None:
        self.db.create_user("alice", "secret")
        user = self.db.authenticate("alice", "secret")
        self.assertIsNotNone(user)
        self.assertEqual(user.username, "alice")

    def test_auth_fail_wrong_password(self) -> None:
        self.db.create_user("alice", "secret")
        user = self.db.authenticate("alice", "bad")
        self.assertIsNone(user)

    def test_store_location_and_close_session(self) -> None:
        self.db.create_user("alice", "secret")
        user = self.db.authenticate("alice", "secret")
        self.assertIsNotNone(user)

        async def ops() -> None:
            sid = await self.db.create_session(user.id, "127.0.0.1:1000")
            await self.db.store_location(
                user_id=user.id,
                session_id=sid,
                lat=1.0,
                lon=2.0,
                accuracy=5.0,
                client_ts="now",
                seq=1,
            )
            await self.db.close_session(sid, "done")

        asyncio.run(ops())


if __name__ == "__main__":
    unittest.main()
