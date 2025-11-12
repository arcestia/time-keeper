import argparse
from pathlib import Path
from typing import Optional

from colorama import Fore, Style

from time_keeper import db, auth


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="time-authority", description="Manage timezones and crossings")
    p.add_argument("--db", default="timekeeper.db", help="SQLite database file path")

    sub = p.add_subparsers(dest="cmd", required=False)

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

    sub.add_parser("interactive", help="Run interactive menu")
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


def _print_table(headers, rows) -> None:
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    header_line = "  ".join((Fore.CYAN + Style.BRIGHT + str(h).ljust(widths[i]) + Style.RESET_ALL) for i, h in enumerate(headers))
    sep_line = "  ".join("-" * w for w in widths)
    print(header_line)
    print(sep_line)
    for row in rows:
        print("  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))

def interactive_menu(db_path: Path) -> None:
    current_user: Optional[dict] = None
    while True:
        print("")
        print(Fore.CYAN + Style.BRIGHT + "=== Time Authority ===")
        if current_user is None:
            print("Status: not logged in")
            print("1) Login")
            print("0) Quit")
            choice = input("Choose: ").strip()
            if choice == "0":
                print(Fore.GREEN + "Goodbye.")
                return
            elif choice == "1":
                username = input("Username: ").strip()
                u = db.find_user(db_path, username)
                if not u:
                    print(Fore.RED + "User not found")
                    continue
                pw = prompt_passcode()
                if not auth.verify_passcode(pw, u["passcode_hash"]):
                    print(Fore.RED + "Authentication failed")
                    continue
                current_user = dict(u)
                role = "admin" if current_user.get("is_admin") else "user"
                print(Fore.GREEN + f"Login success. User: {current_user['username']} ({role})")
            else:
                print(Fore.RED + "Invalid choice")
        else:
            uname = current_user.get("username")
            is_admin = bool(current_user.get("is_admin"))
            print(Fore.CYAN + Style.BRIGHT + f"Logged in as: {uname} ({'admin' if is_admin else 'user'})")
            print("1) View my timezone")
            print("2) Move up (burn deposit)")
            print("3) Move down (no refund)")
            if is_admin:
                print("4) Admin: zones-list")
                print("5) Admin: zones-defaults")
            print("6) Logout")
            print("0) Quit")
            choice = input("Choose: ").strip()
            if choice == "0":
                print(Fore.GREEN + "Goodbye.")
                return
            elif choice == "6":
                current_user = None
                print(Fore.YELLOW + "Logged out.")
            elif choice == "1":
                cmd_view(db_path, uname)
            elif choice == "2":
                cmd_move_up(db_path, uname)
            elif choice == "3":
                cmd_move_down(db_path, uname)
            elif is_admin and choice == "4":
                zones = db.list_timezones(db_path)
                from time_keeper import formatting
                headers = ["Zone", "Deposit", "Earn", "Store"]
                rows = []
                for z in zones:
                    dep = formatting.format_duration(int(z["deposit_seconds"]), style="short") if int(z["zone"]) != 12 else "-"
                    rows.append([
                        str(int(z["zone"])),
                        dep,
                        f"x{float(z['earn_multiplier']):g}",
                        f"x{float(z['store_multiplier']):g}",
                    ])
                _print_table(headers, rows)
            elif is_admin and choice == "5":
                db.set_timezones_defaults(db_path)
                print(Fore.GREEN + "Seeded default timezones.")
            else:
                print(Fore.RED + "Invalid choice")

def main(argv: Optional[list] = None) -> None:
    args = parse_args(argv)
    db_path = Path(args.db)
    if args.cmd is None or args.cmd == "interactive":
        interactive_menu(db_path)
        return
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
            zones = db.list_timezones(db_path)
            from time_keeper import formatting
            headers = ["Zone", "Deposit", "Earn", "Store"]
            rows = []
            for z in zones:
                dep = formatting.format_duration(int(z["deposit_seconds"]), style="short") if int(z["zone"]) != 12 else "-"
                rows.append([
                    str(int(z["zone"])),
                    dep,
                    f"x{float(z['earn_multiplier']):g}",
                    f"x{float(z['store_multiplier']):g}",
                ])
            _print_table(headers, rows)
        elif args.admin_cmd == "zones-defaults":
            db.set_timezones_defaults(db_path)
            print(Fore.GREEN + "Seeded default timezones.")
        return


if __name__ == "__main__":
    main()
