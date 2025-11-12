import argparse
from pathlib import Path
from typing import Optional

from colorama import Fore, Style

from time_keeper import db, auth


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="time-authority", description="Manage timezones and crossings")
    p.add_argument("--db", default="timekeeper.db", help="SQLite database file path")

    sub = p.add_subparsers(dest="cmd", required=True)

    # User commands
    sub.add_parser("view", help="View your current timezone and next deposit requirement")
    sub.add_parser("move-up", help="Move up one timezone (burns deposit)")
    sub.add_parser("move-down", help="Move down one timezone (no refund)")

    # Admin commands
    a = sub.add_parser("admin", help="Admin actions")
    a.add_argument("--username", required=True, help="Admin username")
    a_sub = a.add_subparsers(dest="admin_cmd", required=True)
    a_sub.add_parser("zones-list", help="List all timezones and settings")
    a_sub.add_parser("zones-defaults", help="Seed/reset default timezones")

    return p.parse_args(argv)


def prompt_passcode() -> str:
    import getpass
    pw = getpass.getpass("Passcode: ")
    if not pw:
        raise SystemExit("Passcode cannot be empty")
    return pw


def require_admin(db_path: Path, username: str) -> dict:
    u = db.find_user(db_path, username)
    if not u:
        raise SystemExit("User not found")
    if not u["is_admin"]:
        raise SystemExit("User is not an admin")
    pw = prompt_passcode()
    if not auth.verify_passcode(pw, u["passcode_hash"]):
        raise SystemExit("Authentication failed")
    return dict(u)


def cmd_view(db_path: Path, username: str) -> None:
    info = db.get_user_timezone_info(db_path, username)
    if not info.get("success"):
        print(Fore.RED + info.get("message", "Failed"))
        return
    z = int(info.get("zone", 12))
    earn = float(info.get("earn_multiplier", 1.0))
    store = float(info.get("store_multiplier", 1.0))
    nxt = info.get("next_deposit_seconds")
    print(Fore.CYAN + Style.BRIGHT + f"Timezone: TZ-{z}")
    print(f"Earner multiplier: x{earn:g}; Store multiplier: x{store:g}")
    if nxt is not None:
        from time_keeper import formatting
        print(f"Next deposit to move up: {formatting.format_duration(int(nxt), style='short')}")


def cmd_move_up(db_path: Path, username: str) -> None:
    res = db.move_up_timezone(db_path, username)
    if res.get("success"):
        from time_keeper import formatting
        dep = int(res.get("deposit", 0))
        bal = int(res.get("balance", 0))
        print(Fore.GREEN + f"Moved to TZ-{int(res.get('zone', 0))}. Deposit burned: {formatting.format_duration(dep, style='short')}. Balance: {formatting.format_duration(bal, style='short')}.")
    else:
        print(Fore.RED + res.get("message", "Move up failed"))


def cmd_move_down(db_path: Path, username: str) -> None:
    res = db.move_down_timezone(db_path, username)
    if res.get("success"):
        print(Fore.GREEN + f"Moved to TZ-{int(res.get('zone', 0))}.")
    else:
        print(Fore.RED + res.get("message", "Move down failed"))


def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)
    db_path = Path(args.db)
    if args.cmd in ("view", "move-up", "move-down"):
        # Determine user by asking
        import getpass
        username = input("Username: ").strip()
        user = db.find_user(db_path, username)
        if not user:
            raise SystemExit("User not found")
        pw = prompt_passcode()
        if not auth.verify_passcode(pw, user["passcode_hash"]):
            raise SystemExit("Authentication failed")
        if args.cmd == "view":
            cmd_view(db_path, username)
        elif args.cmd == "move-up":
            cmd_move_up(db_path, username)
        elif args.cmd == "move-down":
            cmd_move_down(db_path, username)
        return
    if args.cmd == "admin":
        require_admin(Path(args.db), args.username)
        if args.admin_cmd == "zones-list":
            rows = db.list_timezones(db_path)
            from time_keeper import formatting
            print(Fore.CYAN + Style.BRIGHT + "== Timezones ==")
            print("Zone  Deposit           Earn   Store")
            for r in rows:
                dep = formatting.format_duration(int(r["deposit_seconds"]), style="short") if int(r["zone"]) != 12 else "-"
                print(f"{int(r['zone']):>4}  {dep:<16}  x{float(r['earn_multiplier']):<4g}  x{float(r['store_multiplier']):<4g}")
        elif args.admin_cmd == "zones-defaults":
            db.set_timezones_defaults(db_path)
            print(Fore.GREEN + "Seeded default timezones.")
        return


if __name__ == "__main__":
    main()
