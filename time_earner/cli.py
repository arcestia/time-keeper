import argparse
import getpass
from pathlib import Path
from typing import Optional

from colorama import Fore, Style, init as colorama_init

from time_keeper import db, auth, formatting
import time
import sys


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="time-earner", description="Earn time by logging in and adding to your balance")
    p.add_argument("--db", default="timekeeper.db", help="SQLite database file path")

    sub = p.add_subparsers(dest="cmd", required=False)

    # Non-interactive earn command
    earn = sub.add_parser("earn", help="Earn time non-interactively")
    earn.add_argument("--username", required=True)
    earn.add_argument("--amount", required=True, help="Amount to add (e.g., '1h 30m' or seconds)")
    earn.add_argument("--require-active", action="store_true", help="Fail if account is deactivated")

    sub.add_parser("interactive", help="Run interactive menu")

    return p.parse_args(argv)


def prompt_passcode() -> str:
    pw = getpass.getpass("Passcode: ")
    if not pw:
        raise SystemExit("Passcode cannot be empty")
    return pw


def login_and_get_user(db_path: Path, username: str) -> Optional[dict]:
    user = db.find_user(db_path, username)
    if not user:
        print(Fore.RED + "User not found")
        return None
    pw = prompt_passcode()
    if not auth.verify_passcode(pw, user["passcode_hash"]):
        print(Fore.RED + "Authentication failed")
        return None
    return dict(user)


def earn_time(db_path: Path, username: str, seconds: int, require_active: bool = False) -> dict:
    seconds = int(seconds)
    if seconds <= 0:
        return {"success": False, "message": "Amount must be greater than zero"}
    user = db.find_user(db_path, username)
    if not user:
        return {"success": False, "message": "User not found"}
    if require_active and not user["active"]:
        return {"success": False, "message": "Account is deactivated"}
    with db.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute("SELECT active FROM users WHERE username = ?", (username,)).fetchone()
        if not cur:
            conn.rollback()
            return {"success": False, "message": "User not found"}
        if require_active and int(cur[0]) != 1:
            conn.rollback()
            return {"success": False, "message": "Account is deactivated"}
        conn.execute("UPDATE users SET balance_seconds = balance_seconds + ? WHERE username = ?", (seconds, username))
        bal = conn.execute("SELECT balance_seconds FROM users WHERE username = ?", (username,)).fetchone()[0]
        conn.commit()
    return {"success": True, "message": "Earned time added", "balance": int(bal)}


def start_earn_session(db_path: Path, username: str, stake_seconds: int) -> dict:
    """Deduct stake immediately and start a countdown.
    If countdown completes, reward double the stake. If interrupted, stake is lost.
    Returns dict with success, message, balance.
    """
    stake = int(stake_seconds)
    if stake <= 0:
        return {"success": False, "message": "Amount must be greater than zero"}
    with db.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT balance_seconds, active FROM users WHERE username = ?", (username,)).fetchone()
        if not row:
            conn.rollback()
            return {"success": False, "message": "User not found"}
        bal, active = int(row[0]), int(row[1])
        if active != 1:
            conn.rollback()
            return {"success": False, "message": "Account is deactivated"}
        if bal < stake:
            conn.rollback()
            return {"success": False, "message": "Insufficient balance"}
        conn.execute("UPDATE users SET balance_seconds = balance_seconds - ? WHERE username = ?", (stake, username))
        conn.commit()

    # Countdown loop (foreground)
    remaining = stake
    print(Fore.YELLOW + f"Session started. Staked {formatting.format_duration(stake, style='short')}.")
    print(Fore.YELLOW + "Do not exit. If you exit early, you lose the stake.")
    try:
        last_print = -1
        while remaining > 0:
            # print once per second
            if remaining != last_print:
                sys.stdout.write("\r" + f"Remaining: {formatting.format_duration(remaining, style='short', max_parts=2)}    ")
                sys.stdout.flush()
                last_print = remaining
            time.sleep(1)
            remaining -= 1
        sys.stdout.write("\r" + "Remaining: 0s" + " " * 20 + "\n")
    except KeyboardInterrupt:
        print("")
        print(Fore.RED + "Session interrupted. Stake forfeited.")
        # no refund; just show balance
        bal = db.get_balance_seconds(db_path, username) or 0
        return {"success": False, "message": "Forfeited", "balance": int(bal)}

    # Reward double
    reward = stake * 2
    with db.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE users SET balance_seconds = balance_seconds + ? WHERE username = ?", (reward, username))
        bal = conn.execute("SELECT balance_seconds FROM users WHERE username = ?", (username,)).fetchone()[0]
        conn.commit()
    return {"success": True, "message": "Session complete", "balance": int(bal), "reward": reward}


def interactive_menu(db_path: Path) -> None:
    current_db = db_path
    current_user: Optional[dict] = None
    while True:
        print("")
        print(Fore.CYAN + Style.BRIGHT + "=== Time Earner ===")
        if current_user is None:
            print("Status: not logged in")
            print(f"{Fore.YELLOW}1){Style.RESET_ALL} Login")
            print(f"{Fore.YELLOW}0){Style.RESET_ALL} Quit")
            choice = input("Choose: ").strip()
            if choice == "0":
                print(Fore.GREEN + "Goodbye.")
                return
            elif choice == "1":
                username = input("Username: ").strip()
                logged = login_and_get_user(current_db, username)
                if logged:
                    current_user = logged
                    bal = db.get_balance_seconds(current_db, current_user["username"]) or 0
                    human = formatting.format_duration(int(bal), style="short", max_parts=2)
                    print(Fore.GREEN + f"Login success. User: {current_user['username']}, Balance: {human}")
            else:
                print(Fore.RED + "Invalid choice")
        else:
            uname = current_user.get("username")
            bal = db.get_balance_seconds(current_db, uname) or 0
            human = formatting.format_duration(int(bal), style="short", max_parts=2)
            print(Fore.CYAN + Style.BRIGHT + f"Logged in as: {uname} | Balance: {human}")
            print(f"{Fore.YELLOW}1){Style.RESET_ALL} Start earning session (stake and countdown)")
            print(f"{Fore.YELLOW}2){Style.RESET_ALL} Refresh balance")
            print(f"{Fore.YELLOW}3){Style.RESET_ALL} Logout")
            print(f"{Fore.YELLOW}0){Style.RESET_ALL} Quit")
            choice = input("Choose: ").strip()
            if choice == "0":
                print(Fore.GREEN + "Goodbye.")
                return
            elif choice == "1":
                amt = input("Stake amount (e.g., 1h 30m): ").strip()
                try:
                    seconds = int(formatting.parse_duration(amt))
                except Exception:
                    print(Fore.RED + "Invalid amount.")
                    continue
                res = start_earn_session(current_db, uname, seconds)
                if res.get("success"):
                    reward = res.get("reward", 0)
                    nb = res.get("balance", 0)
                    print(Fore.GREEN + f"Success! Rewarded {formatting.format_duration(reward, style='short')}. New balance: {formatting.format_duration(nb, style='short')}.")
                else:
                    nb = res.get("balance", 0)
                    print(Fore.RED + f"{res.get('message', 'Failed')}. Balance: {formatting.format_duration(nb, style='short')}")
            elif choice == "2":
                bal = db.get_balance_seconds(current_db, uname) or 0
                print(f"Balance: {formatting.format_duration(int(bal), style='short')}")
            elif choice == "3":
                current_user = None
                print(Fore.YELLOW + "Logged out.")
            else:
                print(Fore.RED + "Invalid choice")


def main(argv: Optional[list] = None) -> None:
    colorama_init(autoreset=True)
    ns = parse_args(argv)
    db_path = Path(ns.db)

    if ns.cmd is None or ns.cmd == "interactive":
        interactive_menu(db_path)
    elif ns.cmd == "earn":
        seconds = formatting.parse_duration(ns.amount)
        res = earn_time(db_path, ns.username, int(seconds), require_active=ns.require_active)
        if res.get("success"):
            print(f"OK: new balance {formatting.format_duration(int(res['balance']), style='short')} ({res['balance']} seconds)")
        else:
            raise SystemExit(res.get("message", "Failed"))
    else:
        raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()
