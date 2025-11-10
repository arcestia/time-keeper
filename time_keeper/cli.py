import argparse
import getpass
import sys
import os
import subprocess
import platform
import signal
from pathlib import Path
from typing import Optional

from . import db
from . import auth
from .worker import run as run_worker
from . import formatting
from colorama import Fore, Style, init as colorama_init


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="time-keeper", description="Time Keeper CLI")
    p.add_argument("--db", default="timekeeper.db", help="SQLite database file path")

    sub = p.add_subparsers(dest="cmd", required=False)

    p_init = sub.add_parser("init-db", help="Initialize the database")
    p_init.add_argument("--db", help="SQLite database file path")

    p_create = sub.add_parser("create-account", help="Create a new account")
    p_create.add_argument("--username", required=True)
    p_create.add_argument("--initial-seconds", type=int, default=db.DEFAULT_INITIAL_SECONDS)
    p_create.add_argument("--admin", action="store_true", help="Create as admin account")
    p_create.add_argument("--db", help="SQLite database file path")

    p_login = sub.add_parser("login", help="Login to an account and show balance")
    p_login.add_argument("--username", required=True)
    p_login.add_argument("--db", help="SQLite database file path")

    p_admin = sub.add_parser("admin", help="Admin actions (requires admin authentication)")
    p_admin.add_argument("--username", required=True, help="Admin username")
    p_admin.add_argument("--list", action="store_true", help="List all accounts")
    p_admin.add_argument("--reserves", action="store_true", help="Show Time Reserves balance")
    p_admin.add_argument("--db", help="SQLite database file path")

    p_lead = sub.add_parser("leaderboard", help="Show top accounts by balance")
    p_lead.add_argument("--limit", type=int, default=10)
    p_lead.add_argument("--db", help="SQLite database file path")

    p_worker = sub.add_parser("run-worker", help="Run background worker to deduct time every second")
    p_worker.add_argument("--interval", type=float, default=1.0)
    p_worker.add_argument("--background", action="store_true", help="Run the worker in the background")
    p_worker.add_argument("--pid-file", type=str, help="Path to PID file (default next to DB)")
    p_worker.add_argument("--log-file", type=str, help="Path to log file (default next to DB)")
    p_worker.add_argument("--stop", action="store_true", help="Stop a background worker using the PID file")
    p_worker.add_argument("--status", action="store_true", help="Show background worker status using the PID file")
    p_worker.add_argument("--db", help="SQLite database file path")

    p_inter = sub.add_parser("interactive", help="Run interactive menu")
    p_inter.add_argument("--db", help="SQLite database file path")

    return p.parse_args(argv)


def prompt_passcode(confirm: bool = False) -> str:
    pw = getpass.getpass("Passcode: ")
    if confirm:
        pw2 = getpass.getpass("Confirm passcode: ")
        if pw != pw2:
            raise SystemExit("Passcodes do not match")
    if not pw:
        raise SystemExit("Passcode cannot be empty")
    return pw


def require_admin(db_path: Path, username: str) -> None:
    user = db.find_user(db_path, username)
    if not user:
        raise SystemExit("User not found")
    if not user["is_admin"]:
        raise SystemExit("User is not an admin")
    pw = prompt_passcode(confirm=False)
    if not auth.verify_passcode(pw, user["passcode_hash"]):
        raise SystemExit("Authentication failed")

def login_and_get_user(db_path: Path, username: str) -> Optional[dict]:
    """Attempt login and return a user dict on success, else None."""
    user = db.find_user(db_path, username)
    if not user:
        print(Fore.RED + "User not found")
        return None
    pw = prompt_passcode(confirm=False)
    if not auth.verify_passcode(pw, user["passcode_hash"]):
        print(Fore.RED + "Authentication failed")
        return None
    return dict(user)


def cmd_init_db(db_path: Path) -> None:
    db.init_db(db_path)
    print(f"Database initialized at {db_path}")


def cmd_create_account(db_path: Path, username: str, initial_seconds: int, is_admin: bool) -> None:
    if db.find_user(db_path, username):
        raise SystemExit("Username already exists")
    pw = prompt_passcode(confirm=True)
    ph = auth.hash_passcode(pw)
    uid = db.create_account(db_path, username=username, passcode_hash=ph, initial_seconds=initial_seconds, is_admin=is_admin)
    print(f"Created {'admin ' if is_admin else ''}account '{username}' (id={uid}) with {initial_seconds} seconds")


def cmd_login(db_path: Path, username: str) -> None:
    user = db.find_user(db_path, username)
    if not user:
        raise SystemExit("User not found")
    pw = prompt_passcode(confirm=False)
    if not auth.verify_passcode(pw, user["passcode_hash"]):
        raise SystemExit("Authentication failed")
    bal = db.get_balance_seconds(db_path, username)
    status = "active" if user["active"] else "deactivated"
    human = formatting.format_duration(int(bal) if bal is not None else 0, style="short")
    print(f"Login success. User: {username}, Balance: {human}, Status: {status}")


def cmd_admin(db_path: Path, username: str, do_list: bool, show_reserves: bool = False) -> None:
    require_admin(db_path, username)
    if show_reserves:
        total = db.get_time_reserves(db_path)
        human = formatting.format_duration(total, style="short")
        print(f"Time Reserves: {human} ({total} seconds)")
    elif do_list:
        print_admin_table(db_path)
    else:
        print("No admin action specified. Use --list.")


def cmd_leaderboard(db_path: Path, limit: int) -> None:
    rows = db.top_accounts(db_path, limit)
    table_rows = []
    for i, r in enumerate(rows, start=1):
        human = formatting.format_duration(int(r['balance_seconds']), style="short")
        status = "active" if r.get('active') else "deactivated"
        table_rows.append([str(i), r['username'], human, status])
    print_table(["Rank", "Username", "Balance", "Status"], table_rows)


def cmd_run_worker(db_path: Path, interval: float) -> None:
    run_worker(db_path, interval_seconds=interval)

def _default_pid_log(db_path: Path) -> tuple[Path, Path]:
    base = db_path.with_suffix("")
    pid_path = Path(str(base) + ".worker.pid")
    log_path = Path(str(base) + ".worker.log")
    return pid_path, log_path

def _is_process_running(pid: int) -> bool:
    try:
        if platform.system() == "Windows":
            # On Windows, os.kill with 0 is not reliable; fallback to tasklist
            out = subprocess.check_output(["tasklist", "/FI", f"PID eq {pid}"], creationflags=subprocess.CREATE_NO_WINDOW)
            return str(pid) in out.decode(errors="ignore")
        else:
            os.kill(pid, 0)
            return True
    except Exception:
        return False

def _stop_process(pid: int) -> bool:
    try:
        if platform.system() == "Windows":
            res = subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=subprocess.CREATE_NO_WINDOW)
            return res.returncode == 0
        else:
            os.kill(pid, signal.SIGTERM)
            return True
    except Exception:
        return False

def start_worker_background(db_path: Path, interval: float, pid_file: Path, log_file: Path) -> None:
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            if _is_process_running(pid):
                print(Fore.YELLOW + f"Worker already running (pid {pid}).")
                return
        except Exception:
            pass
        # stale pid file
        try:
            pid_file.unlink()
        except Exception:
            pass

    # Ensure log directory exists
    if log_file.parent:
        log_file.parent.mkdir(parents=True, exist_ok=True)

    creationflags = 0
    if platform.system() == "Windows":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

    with open(log_file, "ab", buffering=0) as lf:
        args = [sys.executable, "-m", "time_keeper.cli", "run-worker", "--db", str(db_path), "--interval", str(interval)]
        proc = subprocess.Popen(args, stdout=lf, stderr=lf, stdin=subprocess.DEVNULL, creationflags=creationflags, close_fds=(platform.system() != "Windows"))
        pid_file.write_text(str(proc.pid))
        print(Fore.GREEN + f"Worker started in background (pid {proc.pid}). Logs: {log_file}")

def stop_worker_background(pid_file: Path) -> None:
    if not pid_file.exists():
        print(Fore.RED + "PID file not found; is the worker running?")
        return
    try:
        pid = int(pid_file.read_text().strip())
    except Exception:
        print(Fore.RED + "Invalid PID file.")
        return
    if not _is_process_running(pid):
        print(Fore.YELLOW + "Worker not running. Cleaning up PID file.")
        try:
            pid_file.unlink()
        except Exception:
            pass
        return
    if _stop_process(pid):
        print(Fore.GREEN + f"Stopped worker (pid {pid}).")
        try:
            pid_file.unlink()
        except Exception:
            pass
    else:
        print(Fore.RED + "Failed to stop worker.")

def status_worker_background(pid_file: Path) -> None:
    if not pid_file.exists():
        print("Worker status: not running (no PID file)")
        return
    try:
        pid = int(pid_file.read_text().strip())
    except Exception:
        print("Worker status: unknown (invalid PID file)")
        return
    running = _is_process_running(pid)
    print(f"Worker status: {'running' if running else 'stopped'} (pid {pid})")

def print_admin_table(db_path: Path) -> None:
    rows = db.list_all_accounts(db_path)
    table_rows = []
    for r in rows:
        human = formatting.format_duration(int(r['balance_seconds']), style="short")
        status = "active" if r.get('active') else "deactivated"
        role = "admin" if r.get('is_admin') else "user"
        table_rows.append([r['username'], human, status, role])
    print_table(["Username", "Balance", "Status", "Role"], table_rows)

def print_table(headers, rows):
    # compute column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(str(cell)))
    # header
    header_line = "  ".join((Fore.CYAN + Style.BRIGHT + h.ljust(widths[i]) + Style.RESET_ALL) for i, h in enumerate(headers))
    sep_line = "  ".join("-" * w for w in widths)
    print(header_line)
    print(sep_line)
    # rows
    for row in rows:
        cells = []
        for i, cell in enumerate(row):
            text = str(cell).ljust(widths[i])
            if i == 2:  # Balance column
                text = Fore.GREEN + text + Style.RESET_ALL
            elif i == 3:  # Status
                color = Fore.GREEN if str(cell).lower().startswith("active") else Fore.RED
                text = color + str(cell).ljust(widths[i]) + Style.RESET_ALL
            cells.append(text)
        print("  ".join(cells))

def _input_with_default(prompt: str, default: str) -> str:
    s = input(f"{prompt} [{default}]: ").strip()
    return s if s else default

def _input_int_with_default(prompt: str, default: int) -> int:
    s = input(f"{prompt} [{default}]: ").strip()
    if not s:
        return default
    try:
        return int(s)
    except ValueError:
        print("Invalid number, using default.")
        return default

def _input_float_with_default(prompt: str, default: float) -> float:
    s = input(f"{prompt} [{default}]: ").strip()
    if not s:
        return default
    try:
        return float(s)
    except ValueError:
        print("Invalid number, using default.")
        return default

def _input_duration_with_default(prompt: str, default_seconds: int) -> int:
    default_disp = formatting.format_duration(int(default_seconds), style="short")
    s = input(f"{prompt} [{default_disp}]: ").strip()
    if not s:
        return int(default_seconds)
    try:
        return int(formatting.parse_duration(s))
    except Exception:
        print(Fore.RED + "Invalid duration; using default.")
        return int(default_seconds)

def interactive_menu(db_path: Path) -> None:
    current_db = db_path
    current_user: Optional[dict] = None
    while True:
        print("")
        print(Fore.CYAN + Style.BRIGHT + "=== Time Keeper ===")
        if current_user is None:
            print("Status: not logged in")
            print(f"{Fore.YELLOW}1){Style.RESET_ALL} Login")
            print(f"{Fore.YELLOW}2){Style.RESET_ALL} Create account")
            print(f"{Fore.YELLOW}3){Style.RESET_ALL} Leaderboard")
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
                    status = "active" if current_user.get("active") else "deactivated"
                    human = formatting.format_duration(int(bal), style="short")
                    print(Fore.GREEN + f"Login success. User: {current_user['username']}, Balance: {human}, Status: {status}")
            elif choice == "2":
                username = input("Username: ").strip()
                initial = _input_duration_with_default("Initial time", db.DEFAULT_INITIAL_SECONDS)
                is_admin = input("Create as admin? (y/N): ").strip().lower() == "y"
                try:
                    cmd_create_account(current_db, username, initial, is_admin)
                except SystemExit as e:
                    print(Fore.RED + str(e))
            elif choice == "3":
                limit = _input_int_with_default("Top N", 10)
                try:
                    cmd_leaderboard(current_db, limit)
                except SystemExit as e:
                    print(Fore.RED + str(e))
            else:
                print(Fore.RED + "Invalid choice")
        else:
            uname = current_user.get("username")
            is_admin = bool(current_user.get("is_admin"))
            bal = db.get_balance_seconds(current_db, uname) or 0
            status = "active" if current_user.get("active") else "deactivated"
            human = formatting.format_duration(int(bal), style="short", max_parts=2)
            print(Fore.CYAN + Style.BRIGHT + f"Logged in as: {uname} ({'admin' if is_admin else 'user'}) | Balance: {human} | Status: {status}")
            print(f"{Fore.YELLOW}1){Style.RESET_ALL} Refresh balance")
            if is_admin:
                print(f"{Fore.YELLOW}2){Style.RESET_ALL} Transfer time")
                print(f"{Fore.YELLOW}3){Style.RESET_ALL} Admin: list accounts")
                print(f"{Fore.YELLOW}4){Style.RESET_ALL} Show Time Reserves")
                print(f"{Fore.YELLOW}5){Style.RESET_ALL} Leaderboard")
                print(f"{Fore.YELLOW}6){Style.RESET_ALL} Run worker (background)")
                print(f"{Fore.YELLOW}7){Style.RESET_ALL} Init DB")
                print(f"{Fore.YELLOW}8){Style.RESET_ALL} Change DB path")
                print(f"{Fore.YELLOW}9){Style.RESET_ALL} Logout")
                print(f"{Fore.YELLOW}0){Style.RESET_ALL} Quit")
            else:
                print(f"{Fore.YELLOW}2){Style.RESET_ALL} Transfer time")
                print(f"{Fore.YELLOW}3){Style.RESET_ALL} Leaderboard")
                print(f"{Fore.YELLOW}4){Style.RESET_ALL} Logout")
                print(f"{Fore.YELLOW}0){Style.RESET_ALL} Quit")
            choice = input("Choose: ").strip()

            if choice == "0":
                print(Fore.GREEN + "Goodbye.")
                return
            elif choice == "1":
                bal = db.get_balance_seconds(current_db, uname) or 0
                status = "active" if db.find_user(current_db, uname)["active"] else "deactivated"
                human = formatting.format_duration(int(bal), style="short")
                print(f"Balance: {human} | Status: {status}")
            elif is_admin and choice == "2":
                # transfer time
                to_user = input("Send to username: ").strip()
                amt = input("Amount (e.g., 1h 30m): ").strip()
                try:
                    seconds = int(formatting.parse_duration(amt))
                except Exception:
                    print(Fore.RED + "Invalid amount.")
                    continue
                res = db.transfer_seconds(current_db, uname, to_user, seconds)
                if res.get("success"):
                    fb = res.get("from_balance")
                    tb = res.get("to_balance")
                    print(Fore.GREEN + f"Transfer completed. Your balance: {formatting.format_duration(int(fb), style='short')}. Recipient balance: {formatting.format_duration(int(tb), style='short')}.")
                else:
                    print(Fore.RED + res.get("message", "Transfer failed."))
            elif (is_admin and choice == "3"):
                # admin list
                try:
                    print_admin_table(current_db)
                except SystemExit as e:
                    print(Fore.RED + str(e))
            elif (is_admin and choice == "4"):
                total = db.get_time_reserves(current_db)
                print(Fore.CYAN + Style.BRIGHT + f"Time Reserves: {formatting.format_duration(total, style='short')} ({total} seconds)")
            elif (is_admin and choice == "5") or (not is_admin and choice == "3"):
                limit = _input_int_with_default("Top N", 10)
                try:
                    cmd_leaderboard(current_db, limit)
                except SystemExit as e:
                    print(Fore.RED + str(e))
            elif is_admin and choice == "6":
                interval = _input_float_with_default("Interval seconds", 1.0)
                pid_path, log_path = _default_pid_log(current_db)
                start_worker_background(current_db, interval, pid_path, log_path)
            elif is_admin and choice == "7":
                try:
                    cmd_init_db(current_db)
                except SystemExit as e:
                    print(Fore.RED + str(e))
            elif is_admin and choice == "8":
                new_db = _input_with_default("DB path", str(current_db))
                current_db = Path(new_db)
            elif (is_admin and choice == "9"):
                current_user = None
                print(Fore.YELLOW + "Logged out.")
            elif (not is_admin and choice == "2"):
                # transfer for normal user
                to_user = input("Send to username: ").strip()
                amt = input("Amount (e.g., 1h 30m): ").strip()
                try:
                    seconds = int(formatting.parse_duration(amt))
                except Exception:
                    print(Fore.RED + "Invalid amount.")
                    continue
                res = db.transfer_seconds(current_db, uname, to_user, seconds)
                if res.get("success"):
                    fb = res.get("from_balance")
                    tb = res.get("to_balance")
                    print(Fore.GREEN + f"Transfer completed. Your balance: {formatting.format_duration(int(fb), style='short')}. Recipient balance: {formatting.format_duration(int(tb), style='short')}.")
                else:
                    print(Fore.RED + res.get("message", "Transfer failed."))
            elif (not is_admin and choice == "4"):
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
    elif ns.cmd == "init-db":
        cmd_init_db(db_path)
    elif ns.cmd == "create-account":
        cmd_create_account(db_path, ns.username, ns.initial_seconds, ns.admin)
    elif ns.cmd == "login":
        cmd_login(db_path, ns.username)
    elif ns.cmd == "admin":
        cmd_admin(db_path, ns.username, ns.list, ns.reserves)
    elif ns.cmd == "leaderboard":
        cmd_leaderboard(db_path, ns.limit)
    elif ns.cmd == "run-worker":
        # background control
        pid_file = Path(ns.pid_file) if ns.pid_file else _default_pid_log(db_path)[0]
        log_file = Path(ns.log_file) if ns.log_file else _default_pid_log(db_path)[1]
        if ns.stop:
            stop_worker_background(pid_file)
        elif ns.status:
            status_worker_background(pid_file)
        elif ns.background:
            start_worker_background(db_path, ns.interval, pid_file, log_file)
        else:
            cmd_run_worker(db_path, ns.interval)
    else:
        raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()
