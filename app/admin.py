from __future__ import annotations

import argparse
import getpass

from app.db import Database


def main() -> None:
    parser = argparse.ArgumentParser(description="Admin utilities")
    parser.add_argument("--db-file", required=True)
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create-user")
    create.add_argument("--username", required=True)

    args = parser.parse_args()
    db = Database(args.db_file)
    db.init_schema()

    if args.command == "create-user":
        password = getpass.getpass("Enter password: ")
        confirm = getpass.getpass("Confirm password: ")
        if password != confirm:
            raise SystemExit("Passwords do not match")
        db.create_user(args.username, password)
        print(f"User created: {args.username}")


if __name__ == "__main__":
    main()
