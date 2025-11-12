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

    p_bcreate = sub.add_parser("bulk-create", help="Create many accounts for simulation")
    p_bcreate.add_argument("--count", type=int, required=True, help="Number of accounts to create")
    p_bcreate.add_argument("--prefix", default="user", help="Username prefix, usernames will be prefix+index")
    p_bcreate.add_argument("--start-index", type=int, default=1, help="Starting index appended to prefix")
    p_bcreate.add_argument("--initial", default="1d", help="Initial time per account (e.g., '1d 2h')")
    p_bcreate.add_argument("--passcode", help="Passcode to use for all accounts (prompt once if omitted)")
    p_bcreate.add_argument("--admin-frequency", type=int, default=0, help="If >0, every Nth account is admin")
    p_bcreate.add_argument("--db", help="SQLite database file path")

    p_login = sub.add_parser("login", help="Login to an account and show balance")
    p_login.add_argument("--username", required=True)
    p_login.add_argument("--db", help="SQLite database file path")

    p_admin = sub.add_parser("admin", help="Admin actions (requires admin authentication)")
    p_admin.add_argument("--username", required=True, help="Admin username")
    p_admin.add_argument("--list", action="store_true", help="List all accounts")
    p_admin.add_argument("--reserves", action="store_true", help="Show Time Reserves balance")
    p_admin.add_argument("--stats", action="store_true", help="Show aggregate statistics and top accounts")
    # Time Reserves operations
    p_admin.add_argument("--reserves-transfer-to", help="Transfer from Time Reserves to this username")
    p_admin.add_argument("--reserves-transfer-amount", help="Amount to transfer from reserves (e.g., '30m', '2h')")
    p_admin.add_argument("--reserves-distribute", action="store_true", help="Distribute Time Reserves equally among active users")
    p_admin.add_argument("--reserves-distribute-amount", help="Optional amount to distribute equally; omit to use full reserves")
    # User stats operations
    p_admin.add_argument("--set-stats-full", help="Restore a user's energy/hunger/water to their max cap (username)")
    p_admin.add_argument("--set-stats-full-all", action="store_true", help="Restore all users' energy/hunger/water to their max cap")
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


def cmd_bulk_create(db_path: Path, count: int, prefix: str, start_index: int, initial_display: str, passcode: Optional[str], admin_frequency: int) -> None:
    try:
        initial_seconds = int(formatting.parse_duration(initial_display))
    except Exception:
        raise SystemExit("Invalid --initial duration")
    if count <= 0:
        raise SystemExit("--count must be > 0")
    if start_index < 0:
        raise SystemExit("--start-index must be >= 0")
    pw = passcode or prompt_passcode(confirm=False)
    ph = auth.hash_passcode(pw)
    created = 0
    skipped = 0
    admins = 0
    end = start_index + count
    for i in range(start_index, end):
        uname = f"{prefix}{i}"
        if db.find_user(db_path, uname):
            print(Fore.YELLOW + f"Skip existing: {uname}")
            skipped += 1
            continue
        is_admin = False
        if admin_frequency and admin_frequency > 0:
            # Make every Nth (relative to sequence) admin
            seq = (i - start_index + 1)
            is_admin = (seq % admin_frequency) == 0
        uid = db.create_account(db_path, username=uname, passcode_hash=ph, initial_seconds=initial_seconds, is_admin=is_admin)
        if is_admin:
            admins += 1
        created += 1
        if created <= 5:
            print(Fore.GREEN + f"Created {'admin ' if is_admin else ''}{uname} (id={uid})")
    print(Style.BRIGHT + f"Done. Created={created}, Skipped={skipped}, Admins={admins}.")


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


def cmd_admin(db_path: Path, username: str, do_list: bool, show_reserves: bool = False, show_stats: bool = False,
              reserves_transfer_to: Optional[str] = None, reserves_transfer_amount: Optional[str] = None,
              reserves_distribute: bool = False, reserves_distribute_amount: Optional[str] = None,
              set_stats_full: Optional[str] = None, set_stats_full_all: bool = False) -> None:
    require_admin(db_path, username)
    if show_reserves and not any([reserves_transfer_to, reserves_distribute, show_stats, do_list]):
        total = db.get_time_reserves(db_path)
        human = formatting.format_duration(total, style="short")
        print(f"Time Reserves: {human} ({total} seconds)")
    elif reserves_transfer_to and reserves_transfer_amount:
        try:
            amount = int(formatting.parse_duration(reserves_transfer_amount))
        except Exception:
            raise SystemExit("Invalid --reserves-transfer-amount")
        res = db.transfer_from_reserves(db_path, reserves_transfer_to, amount)
        if res.get("success"):
            tb = res.get("to_balance") or 0
            rem = res.get("reserves_remaining") or 0
            print(Fore.GREEN + f"Transferred {formatting.format_duration(amount, style='short')} from Reserves to {reserves_transfer_to}. Recipient balance: {formatting.format_duration(tb, style='short')}. Reserves remaining: {formatting.format_duration(rem, style='short')} ({rem} seconds)")
        else:
            raise SystemExit(res.get("message", "Transfer from reserves failed"))
    elif reserves_distribute:
        amt: Optional[int] = None
        if reserves_distribute_amount:
            try:
                amt = int(formatting.parse_duration(reserves_distribute_amount))
            except Exception:
                raise SystemExit("Invalid --reserves-distribute-amount")
        res = db.distribute_reserves_equal(db_path, amt)
        if res.get("success"):
            per = int(res.get("per_user", 0))
            rec = int(res.get("recipients", 0))
            rem = int(res.get("reserves_remaining", 0))
            print(Fore.GREEN + f"Distributed {formatting.format_duration(int(res.get('total_distributed', 0)), style='short')} equally to {rec} active users ({formatting.format_duration(per, style='short')} each). Reserves remaining: {formatting.format_duration(rem, style='short')} ({rem} seconds)")
        else:
            raise SystemExit(res.get("message", "Distribution failed"))
    elif set_stats_full_all:
        n = db.set_all_users_stats_full(db_path)
        print(Fore.GREEN + f"Restored stats to cap for {n} users")
    elif set_stats_full:
        ok = db.set_user_stats_full(db_path, set_stats_full)
        if ok:
            print(Fore.GREEN + f"Restored stats to cap for {set_stats_full}")
        else:
            raise SystemExit("Failed to set stats (user not found)")
    elif show_stats:
        print_admin_stats(db_path)
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

def print_user_stats(db_path: Path, username: str) -> None:
    s = db.get_user_stats(db_path, username)
    if not s:
        print(Fore.RED + "User not found or stats unavailable")
        return
    print(Fore.CYAN + Style.BRIGHT + f"== Users Stats for {username} ==")
    print(f"Energy: {s['energy']}%")
    print(f"Hunger: {s['hunger']}%")
    print(f"Water:  {s['water']}%")

def print_admin_stats(db_path: Path) -> None:
    stats = db.get_statistics(db_path)
    total_users = stats.get("total_users", 0)
    total_active = stats.get("total_active", 0)
    total_deactivated = stats.get("total_deactivated", 0)
    total_balance_seconds = stats.get("total_balance_seconds", 0)
    human_total = formatting.format_duration(int(total_balance_seconds), style="short")
    print(Fore.CYAN + Style.BRIGHT + "== Statistics ==")
    print(f"Total Users: {total_users}")
    print(f"Total Active Users: {total_active}")
    print(f"Total Deactivated: {total_deactivated}")
    print(f"Total Times Balances: {human_total} ({total_balance_seconds} seconds)")
    # Top accounts
    top = db.top_accounts(db_path, limit=10)
    rows = []
    for i, r in enumerate(top, start=1):
        rows.append([
            str(i),
            r["username"],
            formatting.format_duration(int(r["balance_seconds"]), style="short"),
            "active" if r.get("active") else "deactivated",
        ])
    print("")
    print_table(["Rank", "Username", "Balance", "Status"], rows)

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
            # Premium status and tier display
            prem = db.is_premium(current_db, uname)
            import time as _t
            now = int(_t.time())
            prem_badge = "premium" if prem.get("active") else "standard"
            # Tier: Roman numerals I-X
            try:
                tinfo = db.get_user_premium_tier(current_db, uname)
                tier_num = int(tinfo.get("tier", 0) or 0)
            except Exception:
                tier_num = 0
            romans = {1:"I",2:"II",3:"III",4:"IV",5:"V",6:"VI",7:"VII",8:"VIII",9:"IX",10:"X"}
            print(Fore.CYAN + Style.BRIGHT + f"Logged in as: {uname} ({'admin' if is_admin else 'user'}) | Balance: {human} | Status: {status}")
            # Separate concise Premium line
            if prem.get("active"):
                rem = max(0, int(prem.get("until", 0)) - now)
                if tier_num > 0:
                    print(Fore.CYAN + Style.BRIGHT + f"Premium {romans.get(tier_num)}: active ({formatting.format_duration(rem, style='short')})")
                else:
                    print(Fore.CYAN + Style.BRIGHT + f"Premium: active ({formatting.format_duration(rem, style='short')})")
            else:
                if tier_num > 0:
                    print(Fore.CYAN + Style.BRIGHT + f"Premium {romans.get(tier_num)}: inactive")
                else:
                    print(Fore.CYAN + Style.BRIGHT + "Premium: inactive")
            # Timezone brief
            try:
                tzinfo = db.get_user_timezone_info(current_db, uname)
                tz_zone = int(tzinfo.get("zone", 12)) if tzinfo.get("success") else 12
                tz_earn = float(tzinfo.get("earn_multiplier", 1.0)) if tzinfo.get("success") else 1.0
                tz_store = float(tzinfo.get("store_multiplier", 1.0)) if tzinfo.get("success") else 1.0
                print(Fore.CYAN + Style.BRIGHT + f"Timezone: TZ-{tz_zone} (earn x{tz_earn:g}; store x{tz_store:g})")
            except Exception:
                pass
            print(f"{Fore.YELLOW}1){Style.RESET_ALL} Refresh balance")
            if is_admin:
                print(f"{Fore.YELLOW}2){Style.RESET_ALL} Transfer time")
                print(f"{Fore.YELLOW}3){Style.RESET_ALL} Admin: list accounts")
                print(f"{Fore.YELLOW}4){Style.RESET_ALL} Show Time Reserves")
                print(f"{Fore.YELLOW}5){Style.RESET_ALL} Transfer from Reserves to user")
                print(f"{Fore.YELLOW}6){Style.RESET_ALL} Distribute Reserves equally")
                print(f"{Fore.YELLOW}7){Style.RESET_ALL} Restore a user's stats to cap")
                print(f"{Fore.YELLOW}8){Style.RESET_ALL} Restore ALL users' stats to cap")
                print(f"{Fore.YELLOW}9){Style.RESET_ALL} Show statistics")
                print(f"{Fore.YELLOW}10){Style.RESET_ALL} Leaderboard")
                print(f"{Fore.YELLOW}11){Style.RESET_ALL} Run worker (background)")
                print(f"{Fore.YELLOW}12){Style.RESET_ALL} Init DB")
                print(f"{Fore.YELLOW}13){Style.RESET_ALL} Change DB path")
                print(f"{Fore.YELLOW}14){Style.RESET_ALL} Premium...")
                print(f"{Fore.YELLOW}15){Style.RESET_ALL} Logout")
                print(f"{Fore.YELLOW}0){Style.RESET_ALL} Quit")
            else:
                print(f"{Fore.YELLOW}2){Style.RESET_ALL} Transfer time")
                print(f"{Fore.YELLOW}3){Style.RESET_ALL} Leaderboard")
                print(f"{Fore.YELLOW}4){Style.RESET_ALL} Users Stats")
                print(f"{Fore.YELLOW}5){Style.RESET_ALL} Premium...")
                print(f"{Fore.YELLOW}6){Style.RESET_ALL} Logout")
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
            elif (is_admin and choice == "5"):
                # transfer from reserves to a user
                to_user = input("Recipient username: ").strip()
                amt = input("Amount from Reserves (e.g., 1h 30m): ").strip()
                try:
                    seconds = int(formatting.parse_duration(amt))
                except Exception:
                    print(Fore.RED + "Invalid amount.")
                    continue
                res = db.transfer_from_reserves(current_db, to_user, seconds)
                if res.get("success"):
                    tb = int(res.get("to_balance", 0))
                    rem = int(res.get("reserves_remaining", 0))
                    print(Fore.GREEN + f"Transferred {formatting.format_duration(seconds, style='short')} to {to_user}. Recipient balance: {formatting.format_duration(tb, style='short')}. Reserves remaining: {formatting.format_duration(rem, style='short')} ({rem} seconds)")
                else:
                    print(Fore.RED + res.get("message", "Transfer from reserves failed."))
            elif (is_admin and choice == "6"):
                # distribute reserves equally
                amt = _input_with_default("Total amount to distribute (Enter to use full reserves)", "")
                amt_seconds = None
                if amt.strip():
                    try:
                        amt_seconds = int(formatting.parse_duration(amt))
                    except Exception:
                        print(Fore.RED + "Invalid amount.")
                        amt_seconds = None
                res = db.distribute_reserves_equal(current_db, amt_seconds)
                if res.get("success"):
                    per = int(res.get("per_user", 0))
                    rec = int(res.get("recipients", 0))
                    rem = int(res.get("reserves_remaining", 0))
                    total_dist = int(res.get("total_distributed", 0))
                    print(Fore.GREEN + f"Distributed {formatting.format_duration(total_dist, style='short')} equally to {rec} users ({formatting.format_duration(per, style='short')} each). Reserves remaining: {formatting.format_duration(rem, style='short')} ({rem} seconds)")
                else:
                    print(Fore.RED + res.get("message", "Distribution failed."))
            elif (is_admin and choice == "7"):
                # restore one user's stats to cap
                target = _input_with_default("Username to restore stats to cap", "")
                if not target:
                    print(Fore.RED + "Username required.")
                else:
                    ok = db.set_user_stats_full(current_db, target)
                    if ok:
                        print(Fore.GREEN + f"Stats restored to cap for {target}")
                    else:
                        print(Fore.RED + "Failed to set stats (user not found)")
            elif (is_admin and choice == "8"):
                # restore all users stats to cap
                n = db.set_all_users_stats_full(current_db)
                print(Fore.GREEN + f"Stats restored to cap for {n} users")
            elif (is_admin and choice == "9"):
                # admin statistics
                try:
                    print_admin_stats(current_db)
                except SystemExit as e:
                    print(Fore.RED + str(e))
            elif (is_admin and choice == "10") or (not is_admin and choice == "3"):
                limit = _input_int_with_default("Top N", 10)
                try:
                    cmd_leaderboard(current_db, limit)
                except SystemExit as e:
                    print(Fore.RED + str(e))
            elif is_admin and choice == "11":
                interval = _input_float_with_default("Interval seconds", 1.0)
                pid_path, log_path = _default_pid_log(current_db)
                start_worker_background(current_db, interval, pid_path, log_path)
            elif is_admin and choice == "12":
                try:
                    cmd_init_db(current_db)
                except SystemExit as e:
                    print(Fore.RED + str(e))
            elif is_admin and choice == "13":
                new_db = _input_with_default("DB path", str(current_db))
                current_db = Path(new_db)
            elif (is_admin and choice == "14") or ((not is_admin) and choice == "5"):
                # Premium submenu
                while True:
                    print("")
                    print(Fore.CYAN + Style.BRIGHT + "== Premium ==")
                    if is_admin:
                        print(f"{Fore.YELLOW}1){Style.RESET_ALL} Buy Premium (self)")
                        print(f"{Fore.YELLOW}2){Style.RESET_ALL} Gift Premium to user")
                        print(f"{Fore.YELLOW}3){Style.RESET_ALL} List tiers")
                        print(f"{Fore.YELLOW}4){Style.RESET_ALL} Set default tiers")
                        print(f"{Fore.YELLOW}5){Style.RESET_ALL} Add/replace a tier")
                        print(f"{Fore.YELLOW}6){Style.RESET_ALL} Remove a tier")
                        print(f"{Fore.YELLOW}7){Style.RESET_ALL} Set user's tier")
                        print(f"{Fore.YELLOW}8){Style.RESET_ALL} Reset user's progression")
                        print(f"{Fore.YELLOW}9){Style.RESET_ALL} Set user's lifetime seconds")
                        print(f"{Fore.YELLOW}10){Style.RESET_ALL} Toggle user's Lifetime")
                        print(f"{Fore.YELLOW}11){Style.RESET_ALL} Backfill lifetime from remaining (admin)")
                        print(f"{Fore.YELLOW}12){Style.RESET_ALL} View my progression")
                        print(f"{Fore.YELLOW}13){Style.RESET_ALL} View a user's progression")
                        print(f"{Fore.YELLOW}14){Style.RESET_ALL} Restore stats to cap (self, 24h cool-down)")
                        print(f"{Fore.YELLOW}15){Style.RESET_ALL} Restore stats to cap for a user (admin)")
                        print(f"{Fore.YELLOW}0){Style.RESET_ALL} Back")
                        sel = input("Choose: ").strip()
                        if sel == "0":
                            break
                        elif sel == "1":
                            dur = _input_with_default("Premium duration (e.g., 3h)", "3h")
                            try:
                                secs = int(formatting.parse_duration(dur))
                            except Exception:
                                print(Fore.RED + "Invalid duration.")
                                continue
                            cost = secs * 3
                            ans = _input_with_default(f"This will cost {formatting.format_duration(cost, style='short')}. Proceed? (y/N)", "n").strip().lower()
                            if ans in ("y", "yes"):
                                res = db.purchase_premium(current_db, uname, secs)
                                if res.get("success"):
                                    until = int(res.get("premium_until", 0))
                                    rem = max(0, until - int(__import__('time').time()))
                                    print(Fore.GREEN + f"Premium updated. Remaining: {formatting.format_duration(rem, style='short')}. Balance: {formatting.format_duration(int(res.get('balance',0)), style='short')}")
                                else:
                                    print(Fore.RED + res.get("message", "Failed to purchase premium"))
                            else:
                                print("Cancelled.")
                        
                        elif sel == "2":
                            to_user = _input_with_default("Recipient username", "").strip()
                            if not to_user:
                                print(Fore.RED + "Recipient username required.")
                            else:
                                dur = _input_with_default("Premium duration (e.g., 3h)", "3h")
                                try:
                                    secs = int(formatting.parse_duration(dur))
                                except Exception:
                                    print(Fore.RED + "Invalid duration.")
                                    secs = 0
                                if secs > 0:
                                    cost = secs * 3
                                    ans = _input_with_default(f"This will cost {formatting.format_duration(cost, style='short')} from your balance. Proceed? (y/N)", "n").strip().lower()
                                    if ans in ("y", "yes"):
                                        res = db.gift_premium(current_db, uname, to_user, secs)
                                        if res.get("success"):
                                            until = int(res.get("to_premium_until", 0))
                                            rem = max(0, until - int(__import__('time').time()))
                                            fb = int(res.get("from_balance", 0))
                                            print(Fore.GREEN + f"Premium gifted to {to_user}. Recipient remaining: {formatting.format_duration(rem, style='short')}. Your balance: {formatting.format_duration(fb, style='short')}")
                                        else:
                                            print(Fore.RED + res.get("message", "Failed to gift premium"))
                                    else:
                                        print("Cancelled.")
                        elif sel == "3":
                            tiers = db.list_premium_tiers(current_db)
                            if not tiers:
                                print(Fore.YELLOW + "No premium tiers defined.")
                            else:
                                print(Fore.CYAN + Style.BRIGHT + "== Premium Tiers ==")
                                print_table(["Tier", "Min", "Earn+%", "Store-%", "Cap%"], [[
                                    str(t["tier"]),
                                    formatting.format_duration(int(t["min_seconds"]), style='short'),
                                    f"{float(t['earn_bonus_percent'])*100:.0f}%",
                                    f"{float(t['store_discount_percent'])*100:.0f}%",
                                    str(int(t['stat_cap_percent']))
                                ] for t in tiers])
                        elif sel == "4":
                            db.set_premium_tiers_defaults(current_db)
                            print(Fore.GREEN + "Premium tiers set to defaults.")
                        elif sel == "5":
                            try:
                                tr = _input_int_with_default("Tier number (1-10)", 1)
                                mins = _input_duration_with_default("Min cumulative purchased time", 3600)
                                earnp = _input_float_with_default("Earn bonus percent (e.g., 0.10)", 0.10)
                                storep = _input_float_with_default("Store discount percent (e.g., 0.10)", 0.10)
                                cap = _input_int_with_default("Stat cap percent (e.g., 250)", 250)
                                db.add_or_replace_premium_tier(current_db, tr, mins, earnp, storep, cap)
                                print(Fore.GREEN + "Premium tier saved.")
                            except Exception as e:
                                print(Fore.RED + f"Failed to save tier: {e}")
                        elif sel == "6":
                            tr = _input_int_with_default("Tier number to remove", 1)
                            ok = db.remove_premium_tier(current_db, tr)
                            print(Fore.GREEN + ("Tier removed." if ok else "Tier not found."))
                        elif sel == "7":
                            target = _input_with_default("Username to set tier", "")
                            if not target:
                                print(Fore.RED + "Username required.")
                            else:
                                tr = _input_int_with_default("Tier number", 1)
                                res = db.set_user_premium_tier(current_db, target, tr)
                                if res.get("success"):
                                    print(Fore.GREEN + f"Set {target} to Tier {tr}.")
                                else:
                                    print(Fore.RED + res.get("message", "Failed"))
                        elif sel == "8":
                            target = _input_with_default("Username to reset progression", "")
                            if not target:
                                print(Fore.RED + "Username required.")
                            else:
                                res = db.reset_user_premium_progress(current_db, target)
                                if res.get("success"):
                                    print(Fore.GREEN + f"Reset progression for {target}.")
                                else:
                                    print(Fore.RED + res.get("message", "Failed"))
                        elif sel == "9":
                            target = _input_with_default("Username to set lifetime seconds", "")
                            if not target:
                                print(Fore.RED + "Username required.")
                            else:
                                try:
                                    secs = _input_duration_with_default("Lifetime seconds value", 0)
                                    res = db.set_user_premium_lifetime_seconds(current_db, target, int(secs))
                                    if res.get("success"):
                                        print(Fore.GREEN + f"Lifetime seconds updated for {target}.")
                                    else:
                                        print(Fore.RED + res.get("message", "Failed"))
                                except Exception as e:
                                    print(Fore.RED + f"Failed: {e}")
                        elif sel == "10":
                            target = _input_with_default("Username to toggle Lifetime", "")
                            if not target:
                                print(Fore.RED + "Username required.")
                            else:
                                on = _input_with_default("Turn Lifetime on? (y/N)", "n").strip().lower() in ("y","yes")
                                res = db.set_user_premium_lifetime(current_db, target, on)
                                if res.get("success"):
                                    print(Fore.GREEN + f"Lifetime {'enabled' if on else 'disabled'} for {target}.")
                                else:
                                    print(Fore.RED + res.get("message", "Failed"))
                        elif sel == "11":
                            scope = _input_with_default("Scope: all or one? (all/one)", "all").strip().lower()
                            mode = _input_with_default("Mode: add or set lifetime from remaining? (add/set)", "add").strip().lower()
                            if scope == "one":
                                target = _input_with_default("Username", "").strip()
                                if not target:
                                    print(Fore.RED + "Username required.")
                                else:
                                    res = db.backfill_lifetime_from_remaining(current_db, target, mode)
                                    if res.get("success"):
                                        print(Fore.GREEN + f"Backfill completed. Updated: {int(res.get('updated',0))}")
                                    else:
                                        print(Fore.RED + res.get("message", "Failed"))
                            else:
                                res = db.backfill_lifetime_from_remaining(current_db, None, mode)
                                if res.get("success"):
                                    print(Fore.GREEN + f"Backfill completed. Updated: {int(res.get('updated',0))}")
                                else:
                                    print(Fore.RED + res.get("message", "Failed"))
                        elif sel == "12":
                            info = db.get_user_premium_progress(current_db, uname)
                            if not info.get("success", True):
                                print(Fore.RED + info.get("message", "Failed"))
                            else:
                                life = int(info.get("lifetime_seconds", 0))
                                tier = int(info.get("current_tier", 0))
                                ntier = info.get("next_tier")
                                to_next = info.get("to_next_seconds")
                                pct = float(info.get("percent_to_next", 0.0))
                                bar_w = 30
                                filled = int(round((pct/100.0) * bar_w))
                                bar = ("#" * filled) + ("-" * (bar_w - filled))
                                print(Fore.CYAN + Style.BRIGHT + f"Tier {tier}  |  Lifetime: {formatting.format_duration(life, style='short')}")
                                if ntier is None:
                                    print(Fore.GREEN + "Max tier reached. Progress: [##############################] 100%")
                                else:
                                    print(f"Next: Tier {int(ntier)}  |  To next: {formatting.format_duration(int(to_next), style='short')}  |  {pct:.1f}%")
                                    print(f"[{bar}] {pct:.1f}%")
                        elif sel == "13":
                            target = _input_with_default("Username to view progression", "").strip()
                            if not target:
                                print(Fore.RED + "Username required.")
                            else:
                                info = db.get_user_premium_progress(current_db, target)
                                if not info.get("success", True):
                                    print(Fore.RED + info.get("message", "Failed"))
                                else:
                                    life = int(info.get("lifetime_seconds", 0))
                                    tier = int(info.get("current_tier", 0))
                                    ntier = info.get("next_tier")
                                    to_next = info.get("to_next_seconds")
                                    pct = float(info.get("percent_to_next", 0.0))
                                    bar_w = 30
                                    filled = int(round((pct/100.0) * bar_w))
                                    bar = ("#" * filled) + ("-" * (bar_w - filled))
                                    print(Fore.CYAN + Style.BRIGHT + f"{target} — Tier {tier}  |  Lifetime: {formatting.format_duration(life, style='short')}")
                                    if ntier is None:
                                        print(Fore.GREEN + "Max tier reached. Progress: [##############################] 100%")
                                    else:
                                        print(f"Next: Tier {int(ntier)}  |  To next: {formatting.format_duration(int(to_next), style='short')}  |  {pct:.1f}%")
                                        print(f"[{bar}] {pct:.1f}%")
                        elif sel == "14":
                            res = db.premium_daily_restore(current_db, uname)
                            if res.get("success"):
                                print(Fore.GREEN + f"Restored to cap. Energy {res['energy']}%  Hunger {res['hunger']}%  Water {res['water']}%")
                            else:
                                msg = res.get("message", "Failed")
                                nxt = res.get("next_available_seconds")
                                if nxt is not None:
                                    msg += f" (next in {formatting.format_duration(int(nxt), style='short')})"
                                print(Fore.RED + msg)
                        elif sel == "15" and is_admin:
                            target = _input_with_default("Username to restore", "").strip()
                            if not target:
                                print(Fore.RED + "Username required.")
                            else:
                                res = db.premium_daily_restore(current_db, target)
                                if res.get("success"):
                                    print(Fore.GREEN + f"Restored {target} to cap. Energy {res['energy']}%  Hunger {res['hunger']}%  Water {res['water']}%")
                                else:
                                    msg = res.get("message", "Failed")
                                    nxt = res.get("next_available_seconds")
                                    if nxt is not None:
                                        msg += f" (next in {formatting.format_duration(int(nxt), style='short')})"
                                    print(Fore.RED + msg)
                        
            elif is_admin and choice == "15":
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
                # Users Stats for current user
                print_user_stats(current_db, uname)
            elif (not is_admin and choice == "6"):
                # Logout
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
        cmd_admin(
            db_path,
            ns.username,
            ns.list,
            ns.reserves,
            ns.stats,
            ns.reserves_transfer_to,
            ns.reserves_transfer_amount,
            ns.reserves_distribute,
            ns.reserves_distribute_amount,
            ns.set_stats_full,
            ns.set_stats_full_all,
        )
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
    elif ns.cmd == "bulk-create":
        cmd_bulk_create(
            db_path,
            ns.count,
            ns.prefix,
            ns.start_index,
            ns.initial,
            ns.passcode,
            ns.admin_frequency,
        )
    else:
        raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()
