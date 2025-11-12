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

    # Open earning session (no stake; promo-config driven)
    openp = sub.add_parser("open-session", help="Start open earning session (no stake; promo-config driven)")
    openp.add_argument("--username", required=True)

    # Admin: set promo config
    promo = sub.add_parser("set-promo", help="Admin: set promo earning configuration")
    promo.add_argument("--admin", required=True, help="Admin username")
    promo.add_argument("--base", type=float, required=True, help="Base percent at first block, e.g. 0.10 for 10%")
    promo.add_argument("--per-block", type=float, dest="per_block", required=True, help="Additional percent per block, e.g. 0.0125 for +1.25% per block")
    promo.add_argument("--min-seconds", type=int, required=True, help="Minimum elapsed seconds to be eligible for reward")
    promo.add_argument("--block-seconds", type=int, required=True, help="Block length in seconds (e.g., 600)")
    promo.add_argument("--default-bonus", type=float, default=0.10, help="Flat bonus percent when promo is disabled (e.g., 0.10 for 10%)")
    g = promo.add_mutually_exclusive_group()
    g.add_argument("--enable", action="store_true", help="Enable progressive promo")
    g.add_argument("--disable", action="store_true", help="Disable progressive promo; use default bonus")

    # Admin: set default config (used when promo is disabled)
    dflt = sub.add_parser("set-default", help="Admin: set default earning configuration (used when promo disabled)")
    dflt.add_argument("--admin", required=True, help="Admin username")
    dflt.add_argument("--base", type=float, required=True, help="Base percent at first block, e.g. 0.10 for 10%")
    dflt.add_argument("--per-block", type=float, dest="per_block", required=True, help="Additional percent per block, e.g. 0.0125 for +1.25% per block")
    dflt.add_argument("--min-seconds", type=int, required=True, help="Minimum elapsed seconds to be eligible for reward")
    dflt.add_argument("--block-seconds", type=int, required=True, help="Block length in seconds (e.g., 600)")

    # Admin: set stake config (min stake, reward multiplier)
    stakep = sub.add_parser("set-stake-config", help="Admin: set staking session configuration")
    stakep.add_argument("--admin", required=True, help="Admin username")
    stakep.add_argument("--min-seconds", type=int, required=True, help="Minimum stake duration in seconds (e.g., 7200)")
    stakep.add_argument("--multiplier", type=float, required=True, help="Reward multiplier on successful completion (e.g., 2.0)")

    # Admin: manage stake tiers
    tiers = sub.add_parser("stake-tiers", help="Admin: manage staking tiers")
    tiers.add_argument("--admin", required=True, help="Admin username")
    tiers_sub = tiers.add_subparsers(dest="tiers_cmd", required=True)
    tiers_sub.add_parser("list", help="List staking tiers")
    tiers_sub.add_parser("set-defaults", help="Seed balanced defaults up to 10x")
    addp = tiers_sub.add_parser("add", help="Add or replace a tier")
    addp.add_argument("--min-seconds", type=int, required=True)
    addp.add_argument("--multiplier", type=float, required=True)
    remp = tiers_sub.add_parser("remove", help="Remove a tier")
    remp.add_argument("--min-seconds", type=int, required=True)
    tiers_sub.add_parser("clear", help="Clear all tiers")

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


def _premium_info(db_path: Path, username: Optional[str]) -> tuple[bool, int]:
    if not username:
        return (False, 0)
    try:
        p = db.is_premium(db_path, username)
        import time as _t
        active = bool(p.get("active")) and int(p.get("until", 0)) > int(_t.time())
        rem = 0
        if active:
            rem = max(0, int(p.get("until", 0)) - int(_t.time()))
        return (active, rem)
    except Exception:
        return (False, 0)


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
    cfg = db.get_earner_stake_config(db_path)
    reward_mult = float(cfg.get("reward_multiplier", 2.0))
    # Prefer tiered minimum if tiers exist
    tiers = db.list_earner_stake_tiers(db_path)
    if tiers:
        min_stake = int(tiers[0]["min_seconds"])  # list ordered ASC in DB helper
        if stake < min_stake:
            human_min = formatting.format_duration(min_stake, style='short')
            return {"success": False, "message": f"Minimum stake duration is {human_min}"}
        tier_mult = db.get_multiplier_for_stake(db_path, stake)
        if tier_mult is not None:
            reward_mult = float(tier_mult)
    else:
        min_stake = int(cfg.get("min_stake_seconds", 7200))
        if stake < min_stake:
            human_min = formatting.format_duration(min_stake, style='short')
            return {"success": False, "message": f"Minimum stake duration is {human_min}"}
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
    print(Fore.YELLOW + "Note: If any stat (Energy/Hunger/Water) reaches 0%, the session ends and you lose the stake.")
    try:
        last_print = -1
        last_deplete = int(time.time())
        deplete_tick = 0  # counts 10-minute ticks
        # track last warned levels to avoid repeat spam (values: 0, 50, 20)
        warned_energy = 0
        warned_hunger = 0
        warned_water = 0
        last_bal_check = 0
        while remaining > 0:
            # print once per second
            if remaining != last_print:
                try:
                    stats = db.get_user_stats(db_path, username) or {"energy": 100, "hunger": 100, "water": 100}
                    stat_line = f" | Energy {int(stats['energy'])}%  Hunger {int(stats['hunger'])}%  Water {int(stats['water'])}%"
                except Exception:
                    stat_line = ""
                sys.stdout.write("\r" + f"Remaining: {formatting.format_duration(remaining, style='short', max_parts=2)}{stat_line}    ")
                sys.stdout.flush()
                last_print = remaining
            time.sleep(1)
            remaining -= 1
            # Periodically check if user balance has hit zero (e.g., background deductions)
            now = int(time.time())
            if now - last_bal_check >= 5:
                try:
                    bal_now = int(db.get_balance_seconds(db_path, username) or 0)
                    if bal_now <= 0:
                        print("\n" + Fore.RED + Style.BRIGHT + "Balance reached 0. Session ended. Stake forfeited.")
                        return {"success": False, "message": "Forfeited (balance reached 0)", "balance": 0}
                except Exception:
                    pass
                last_bal_check = now
            # Every 10 minutes, deplete stats by 1%
            if now - last_deplete >= 600:
                # Advance ticks in case multiple intervals passed
                intervals = (now - last_deplete) // 600
                for _ in range(int(intervals)):
                    deplete_tick += 1
                    # Pattern per 10-min tick: energy 0.75% => -1 on 3/4 ticks; hunger 0.5% => -1 every other; water 1% => -1 every tick
                    e_drop = -1 if (deplete_tick % 4) in (1, 2, 3) else 0
                    h_drop = -1 if (deplete_tick % 2) == 1 else 0
                    w_drop = -1
                    try:
                        db.apply_stat_changes(db_path, username, e_drop, h_drop, w_drop)
                    except Exception:
                        pass
                    # Fetch stats and warn/abort if needed
                    try:
                        stats = db.get_user_stats(db_path, username) or {"energy": 100, "hunger": 100, "water": 100}
                        e, h, w = int(stats["energy"]), int(stats["hunger"]), int(stats["water"])
                        # warnings
                        if e <= 20 and warned_energy < 20:
                            print("\n" + Fore.RED + Style.BRIGHT + "Warning: Energy is at 20% or lower!")
                            warned_energy = 20
                        elif e <= 50 and warned_energy < 50:
                            print("\n" + Fore.YELLOW + "Notice: Energy is at 50% or lower.")
                            warned_energy = 50
                        if h <= 20 and warned_hunger < 20:
                            print("\n" + Fore.RED + Style.BRIGHT + "Warning: Hunger is at 20% or lower!")
                            warned_hunger = 20
                        elif h <= 50 and warned_hunger < 50:
                            print("\n" + Fore.YELLOW + "Notice: Hunger is at 50% or lower.")
                            warned_hunger = 50
                        if w <= 20 and warned_water < 20:
                            print("\n" + Fore.RED + Style.BRIGHT + "Warning: Water is at 20% or lower!")
                            warned_water = 20
                        elif w <= 50 and warned_water < 50:
                            print("\n" + Fore.YELLOW + "Notice: Water is at 50% or lower.")
                            warned_water = 50
                        # abort if any hits 0 -> stake forfeited
                        if e <= 0 or h <= 0 or w <= 0:
                            print("\n" + Fore.RED + Style.BRIGHT + "A stat reached 0%. Session ended. Stake forfeited.")
                            bal = db.get_balance_seconds(db_path, username) or 0
                            return {"success": False, "message": "Forfeited (stat reached 0)", "balance": int(bal)}
                    except Exception:
                        pass
                last_deplete += int(intervals) * 600
        sys.stdout.write("\r" + "Remaining: 0s" + " " * 20 + "\n")
    except KeyboardInterrupt:
        print("")
        print(Fore.RED + "Session interrupted. Stake forfeited.")
        # no refund; just show balance
        bal = db.get_balance_seconds(db_path, username) or 0
        return {"success": False, "message": "Forfeited", "balance": int(bal)}

    # Reward by multiplier (+10% if premium active at claim time), then apply timezone earn multiplier
    base_reward = int(round(stake * reward_mult))
    premium_applied = False
    premium_extra = 0
    prem = db.is_premium(db_path, username)
    import time as _t
    if bool(prem.get("active")):
        tier = db.get_user_premium_tier(db_path, username)
        bonus_pct = float(tier.get("earn_bonus_percent", 0.10))
        premium_applied = True
        premium_extra = int(round(base_reward * bonus_pct))
    # Timezone earn multiplier
    try:
        tz = db.get_timezone_multipliers(db_path, username)
        earn_mul = float(tz.get("earn_multiplier", 1.0))
    except Exception:
        earn_mul = 1.0
    reward = int(round((base_reward + premium_extra) * earn_mul))
    with db.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE users SET balance_seconds = balance_seconds + ? WHERE username = ?", (reward, username))
        bal = conn.execute("SELECT balance_seconds FROM users WHERE username = ?", (username,)).fetchone()[0]
        conn.commit()
    return {"success": True, "message": "Session complete", "balance": int(bal), "reward": reward, "premium_applied": premium_applied, "premium_extra": int(premium_extra), "base_reward": int(base_reward)}


def start_earn_session_to_progress(db_path: Path, username: str, stake_seconds: int) -> dict:
    """Stake countdown; on success add reward to premium lifetime progression (not balance)."""
    stake = int(stake_seconds)
    if stake <= 0:
        return {"success": False, "message": "Amount must be greater than zero"}
    # Reuse stake config/min checks
    cfg = db.get_earner_stake_config(db_path)
    reward_mult = float(cfg.get("reward_multiplier", 2.0))
    tiers = db.list_earner_stake_tiers(db_path)
    if tiers:
        min_stake = int(tiers[0]["min_seconds"])
        if stake < min_stake:
            human_min = formatting.format_duration(min_stake, style='short')
            return {"success": False, "message": f"Minimum stake duration is {human_min}"}
        tier_mult = db.get_multiplier_for_stake(db_path, stake)
        if tier_mult is not None:
            reward_mult = float(tier_mult)
    else:
        min_stake = int(cfg.get("min_stake_seconds", 7200))
        if stake < min_stake:
            human_min = formatting.format_duration(min_stake, style='short')
            return {"success": False, "message": f"Minimum stake duration is {human_min}"}
    # Deduct stake from balance as usual
    with db.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT balance_seconds, active FROM users WHERE username = ?", (username,)).fetchone()
        if not row:
            conn.rollback(); return {"success": False, "message": "User not found"}
        bal, active = int(row[0]), int(row[1])
        if active != 1:
            conn.rollback(); return {"success": False, "message": "Account is deactivated"}
        if bal < stake:
            conn.rollback(); return {"success": False, "message": "Insufficient balance"}
        conn.execute("UPDATE users SET balance_seconds = balance_seconds - ? WHERE username = ?", (stake, username))
        conn.commit()
    # Countdown (simplified; no duplicate logic for warnings vs original)
    remaining = stake
    print(Fore.YELLOW + f"Session to progression started. Staked {formatting.format_duration(stake, style='short')}.")
    print(Fore.YELLOW + "Do not exit. If you exit early, you lose the stake.")
    try:
        last_print = -1
        while remaining > 0:
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
        bal = db.get_balance_seconds(db_path, username) or 0
        return {"success": False, "message": "Forfeited", "balance": int(bal)}
    # Reward calculation with premium bonus
    base_reward = int(round(stake * reward_mult))
    premium_applied = False
    premium_extra = 0
    prem = db.is_premium(db_path, username)
    import time as _t
    if bool(prem.get("active")):
        tier = db.get_user_premium_tier(db_path, username)
        bonus_pct = float(tier.get("earn_bonus_percent", 0.10))
        premium_applied = True
        premium_extra = int(round(base_reward * bonus_pct))
    # Apply timezone earn multiplier
    try:
        tz = db.get_timezone_multipliers(db_path, username)
        earn_mul = float(tz.get("earn_multiplier", 1.0))
    except Exception:
        earn_mul = 1.0
    reward = int(round((base_reward + premium_extra) * earn_mul))
    # Apply to premium progression
    res = db.add_premium_lifetime_progress(db_path, username, reward)
    if not res.get("success"):
        return {"success": False, "message": res.get("message", "Failed to add progression")}
    return {"success": True, "message": "Session complete (progression)", "added_progress": int(reward), "premium_applied": premium_applied, "premium_extra": int(premium_extra), "base_reward": int(base_reward), "current_tier": int(res.get("current_tier", 0)), "lifetime_seconds": int(res.get("lifetime_seconds", 0))}

def start_open_earn_session(db_path: Path, username: str) -> dict:
    """Run a foreground open earning session (no stake), using promo config from DB.
    Returns dict with success, message, balance, reward, elapsed, bonus, rate.
    """
    promo_cfg = db.get_earner_promo_config(db_path)
    default_cfg = db.get_earner_default_config(db_path)
    promo_enabled = int(promo_cfg.get("promo_enabled", 1))
    if promo_enabled:
        base = float(promo_cfg.get("base_percent", 0.10))
        per_block = float(promo_cfg.get("per_block_percent", 0.0125))
        min_seconds = int(promo_cfg.get("min_seconds", 600))
        block_seconds = int(promo_cfg.get("block_seconds", 600))
    else:
        base = float(default_cfg.get("base_percent", 0.10))
        per_block = float(default_cfg.get("per_block_percent", 0.0125))
        min_seconds = int(default_cfg.get("min_seconds", 600))
        block_seconds = int(default_cfg.get("block_seconds", 600))
    print(Fore.YELLOW + "Open session started. No stake. Press Ctrl+C to claim anytime.")
    print(Fore.YELLOW + "Note: If any stat (Energy/Hunger/Water) reaches 0%, the session stops and you get no reward.")
    human_min = formatting.format_duration(min_seconds, style="short")
    human_block = formatting.format_duration(block_seconds, style="short")
    mode_label = "Promo" if promo_enabled else "Default"
    print(Fore.YELLOW + f"Minimum duration for rewards is {human_min}. {mode_label}: {base*100:.1f}% at first block, +{per_block*100:.2f}% per each {human_block}.")
    start_ts = int(time.time())
    last_print = -1
    next_deplete_at = start_ts + 600
    deplete_tick = 0
    warned_energy = 0
    warned_hunger = 0
    warned_water = 0
    while True:
        try:
            elapsed = int(time.time()) - start_ts
            if elapsed != last_print:
                try:
                    stats = db.get_user_stats(db_path, username) or {"energy": 100, "hunger": 100, "water": 100}
                    stat_line = f" | Energy {int(stats['energy'])}%  Hunger {int(stats['hunger'])}%  Water {int(stats['water'])}%"
                except Exception:
                    stat_line = ""
                # Also show user's current balance and premium remaining time
                try:
                    bal_live = int(db.get_balance_seconds(db_path, username) or 0)
                    bal_line = f" | Balance {formatting.format_duration(bal_live, style='short', max_parts=2)}"
                except Exception:
                    bal_line = ""
                try:
                    prem_active, prem_rem = _premium_info(db_path, username)
                    prem_line = f" | Premium {formatting.format_duration(prem_rem, style='short')}" if prem_active else ""
                except Exception:
                    prem_line = ""
                sys.stdout.write("\r" + f"Elapsed: {formatting.format_duration(elapsed, style='short', max_parts=2)}{stat_line}{bal_line}{prem_line}    ")
                sys.stdout.flush()
                last_print = elapsed
            # Check balance hit zero -> stop with 25% penalty path similar to stat-zero
            try:
                bal_now = int(db.get_balance_seconds(db_path, username) or 0)
                if bal_now <= 0:
                    stop_ts = int(time.time())
                    print("\n" + Fore.RED + Style.BRIGHT + "Balance reached 0. Session stopped (25% penalty applied).")
                    elapsed_stop = stop_ts - start_ts
                    blocks_stop = elapsed_stop // block_seconds
                    rate_stop = float(base + per_block * max(0, blocks_stop - 1))
                    bonus_stop = int(round(elapsed_stop * rate_stop))
                    total_base = int(elapsed_stop + bonus_stop)
                    penalized = int(round(total_base * 0.75))
                    prem = db.is_premium(db_path, username)
                    import time as _t
                    if bool(prem.get("active")):
                        tier = db.get_user_premium_tier(db_path, username)
                        bonus_pct = float(tier.get("earn_bonus_percent", 0.10))
                        premium_applied = True
                        premium_extra = int(round(penalized * bonus_pct))
                    else:
                        premium_applied = False
                        premium_extra = 0
                    try:
                        tz = db.get_timezone_multipliers(db_path, username)
                        earn_mul = float(tz.get("earn_multiplier", 1.0))
                    except Exception:
                        earn_mul = 1.0
                    final_add = int(round((penalized + premium_extra) * earn_mul))
                    with db.connect(db_path) as conn2:
                        conn2.execute("BEGIN IMMEDIATE")
                        conn2.execute("UPDATE users SET balance_seconds = balance_seconds + ? WHERE username = ?", (final_add, username))
                        bal2 = conn2.execute("SELECT balance_seconds FROM users WHERE username = ?", (username,)).fetchone()[0]
                        conn2.commit()
                    return {
                        "success": True,
                        "message": "Session ended (balance reached 0, penalty applied)",
                        "balance": int(bal2),
                        "reward": int(final_add),
                        "elapsed": int(elapsed_stop),
                        "bonus": int(bonus_stop),
                        "rate": rate_stop,
                        "penalty_applied": True,
                        "penalty_percent": 25,
                        "penalty_loss": int(max(0, total_base - penalized)),
                        "premium_applied": premium_applied,
                        "premium_extra": int(premium_extra),
                        "base_reward": int(total_base)
                    }
            except Exception:
                pass
            # Apply depletion when passing each 10-minute boundary
            now = int(time.time())
            if now >= next_deplete_at:
                # catch up through all passed ticks
                while next_deplete_at <= now:
                    deplete_tick += 1
                    e_drop = -1 if (deplete_tick % 4) in (1, 2, 3) else 0
                    h_drop = -1 if (deplete_tick % 2) == 1 else 0
                    w_drop = -1
                    try:
                        db.apply_stat_changes(db_path, username, e_drop, h_drop, w_drop)
                    except Exception:
                        pass
                    # Fetch stats and warn/stop if needed (open session stops; reward with 25% penalty)
                    try:
                        stats = db.get_user_stats(db_path, username) or {"energy": 100, "hunger": 100, "water": 100}
                        e, h, w = int(stats["energy"]), int(stats["hunger"]), int(stats["water"])
                        if e <= 20 and warned_energy < 20:
                            print("\n" + Fore.RED + Style.BRIGHT + "Warning: Energy is at 20% or lower!")
                            warned_energy = 20
                        elif e <= 50 and warned_energy < 50:
                            print("\n" + Fore.YELLOW + "Notice: Energy is at 50% or lower.")
                            warned_energy = 50
                        if h <= 20 and warned_hunger < 20:
                            print("\n" + Fore.RED + Style.BRIGHT + "Warning: Hunger is at 20% or lower!")
                            warned_hunger = 20
                        elif h <= 50 and warned_hunger < 50:
                            print("\n" + Fore.YELLOW + "Notice: Hunger is at 50% or lower.")
                            warned_hunger = 50
                        if w <= 20 and warned_water < 20:
                            print("\n" + Fore.RED + Style.BRIGHT + "Warning: Water is at 20% or lower!")
                            warned_water = 20
                        elif w <= 50 and warned_water < 50:
                            print("\n" + Fore.YELLOW + "Notice: Water is at 50% or lower.")
                            warned_water = 50
                        if e <= 0 or h <= 0 or w <= 0:
                            stop_ts = int(time.time())
                            print("\n" + Fore.RED + Style.BRIGHT + "A stat reached 0%. Session stopped (25% penalty applied).")
                            elapsed_stop = stop_ts - start_ts
                            # Compute reward components
                            blocks_stop = elapsed_stop // block_seconds
                            rate_stop = float(base + per_block * max(0, blocks_stop - 1))
                            bonus_stop = int(round(elapsed_stop * rate_stop))
                            total_base = int(elapsed_stop + bonus_stop)
                            # Apply 25% penalty
                            penalized = int(round(total_base * 0.75))
                            # Apply premium +10% if active at stop time
                            prem = db.is_premium(db_path, username)
                            import time as _t
                            if bool(prem.get("active")):
                                tier = db.get_user_premium_tier(db_path, username)
                                bonus_pct = float(tier.get("earn_bonus_percent", 0.10))
                                premium_applied = True
                                premium_extra = int(round(penalized * bonus_pct))
                            else:
                                premium_applied = False
                                premium_extra = 0
                            # Apply timezone earn multiplier to penalized progression
                            try:
                                tz = db.get_timezone_multipliers(db_path, username)
                                earn_mul = float(tz.get("earn_multiplier", 1.0))
                            except Exception:
                                earn_mul = 1.0
                            final_add = int(round((penalized + premium_extra) * earn_mul))
                            with db.connect(db_path) as conn2:
                                conn2.execute("BEGIN IMMEDIATE")
                                conn2.execute("UPDATE users SET balance_seconds = balance_seconds + ? WHERE username = ?", (final_add, username))
                                bal2 = conn2.execute("SELECT balance_seconds FROM users WHERE username = ?", (username,)).fetchone()[0]
                                conn2.commit()
                            return {
                                "success": True,
                                "message": "Session ended (stat reached 0, penalty applied)",
                                "balance": int(bal2),
                                "reward": int(final_add),
                                "elapsed": int(elapsed_stop),
                                "bonus": int(bonus_stop),
                                "rate": rate_stop,
                                "penalty_applied": True,
                                "penalty_percent": 25,
                                "penalty_loss": int(max(0, total_base - penalized)),
                                "premium_applied": premium_applied,
                                "premium_extra": int(premium_extra),
                                "base_reward": int(total_base)
                            }
                    except Exception:
                        pass
                    next_deplete_at += 600
            time.sleep(1)
        except KeyboardInterrupt:
            sys.stdout.write("\n")
            # Confirm claim or resume
            ans = input("Claim now and end session? (y/N to resume): ").strip().lower()
            if ans in ("y", "yes"):
                break
            else:
                # resume loop
                continue

    # Claimed: compute final elapsed and apply reward
    elapsed = int(time.time()) - start_ts
    if elapsed < min_seconds:
        # No reward; just report
        bal = db.get_balance_seconds(db_path, username) or 0
        print(Fore.YELLOW + f"Minimum {formatting.format_duration(min_seconds, style='short')} required for rewards; earned 0.")
        return {"success": True, "message": "Session ended", "balance": int(bal), "reward": 0, "elapsed": elapsed}
    # Rate based on selected config (promo or default)
    blocks = elapsed // block_seconds
    rate = float(base + per_block * max(0, blocks - 1))
    bonus = int(round(elapsed * rate))
    total_add_base = int(elapsed + bonus)
    # +10% premium bonus if active at claim time
    premium_applied = False
    premium_extra = 0
    prem = db.is_premium(db_path, username)
    import time as _t
    if bool(prem.get("active")):
        tier = db.get_user_premium_tier(db_path, username)
        bonus_pct = float(tier.get("earn_bonus_percent", 0.10))
        premium_applied = True
        premium_extra = int(round(total_add_base * bonus_pct))
    try:
        tz = db.get_timezone_multipliers(db_path, username)
        earn_mul = float(tz.get("earn_multiplier", 1.0))
    except Exception:
        earn_mul = 1.0
    total_add = int(round((total_add_base + premium_extra) * earn_mul))
    with db.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE users SET balance_seconds = balance_seconds + ? WHERE username = ?", (total_add, username))
        bal = conn.execute("SELECT balance_seconds FROM users WHERE username = ?", (username,)).fetchone()[0]
        conn.commit()
    return {"success": True, "message": "Open session claimed", "balance": int(bal), "reward": total_add, "elapsed": elapsed, "bonus": bonus, "rate": rate, "premium_applied": premium_applied, "premium_extra": int(premium_extra), "base_reward": int(total_add_base)}


def start_open_earn_session_to_progress(db_path: Path, username: str) -> dict:
    """Open earning; on claim add reward to premium lifetime progression (not balance)."""
    promo_cfg = db.get_earner_promo_config(db_path)
    default_cfg = db.get_earner_default_config(db_path)
    promo_enabled = int(promo_cfg.get("promo_enabled", 1))
    if promo_enabled:
        base = float(promo_cfg.get("base_percent", 0.10))
        per_block = float(promo_cfg.get("per_block_percent", 0.0125))
        min_seconds = int(promo_cfg.get("min_seconds", 600))
        block_seconds = int(promo_cfg.get("block_seconds", 600))
    else:
        base = float(default_cfg.get("base_percent", 0.10))
        per_block = float(default_cfg.get("per_block_percent", 0.0125))
        min_seconds = int(default_cfg.get("min_seconds", 600))
        block_seconds = int(default_cfg.get("block_seconds", 600))
    print(Fore.YELLOW + "Open session to progression started (no minimum). Press Ctrl+C to claim anytime.")
    start_ts = int(time.time())
    last_print = -1
    next_deplete_at = start_ts + 600
    deplete_tick = 0
    warned_energy = 0
    warned_hunger = 0
    warned_water = 0
    while True:
        try:
            elapsed = int(time.time()) - start_ts
            if elapsed != last_print:
                try:
                    stats = db.get_user_stats(db_path, username) or {"energy": 100, "hunger": 100, "water": 100}
                    stat_line = f" | Energy {int(stats['energy'])}%  Hunger {int(stats['hunger'])}%  Water {int(stats['water'])}%"
                except Exception:
                    stat_line = ""
                try:
                    bal_live = int(db.get_balance_seconds(db_path, username) or 0)
                    bal_line = f" | Balance {formatting.format_duration(bal_live, style='short', max_parts=2)}"
                except Exception:
                    bal_line = ""
                try:
                    prem_active, prem_rem = _premium_info(db_path, username)
                    prem_line = f" | Premium {formatting.format_duration(prem_rem, style='short')}" if prem_active else ""
                except Exception:
                    prem_line = ""
                sys.stdout.write("\r" + f"Elapsed: {formatting.format_duration(elapsed, style='short', max_parts=2)}{stat_line}{bal_line}{prem_line}    ")
                sys.stdout.flush()
                last_print = elapsed
            # Stop if balance reached 0: apply 25% penalty and add to progression
            try:
                bal_now = int(db.get_balance_seconds(db_path, username) or 0)
                if bal_now <= 0:
                    stop_ts = int(time.time())
                    print("\n" + Fore.RED + Style.BRIGHT + "Balance reached 0. Session stopped (25% penalty applied).")
                    elapsed_stop = stop_ts - start_ts
                    blocks_stop = max(0, elapsed_stop // block_seconds)
                    rate_stop = float(base + per_block * max(0, blocks_stop - 1))
                    bonus_stop = int(round(elapsed_stop * rate_stop))
                    total_base = int(elapsed_stop + bonus_stop)
                    penalized = int(round(total_base * 0.75))
                    prem = db.is_premium(db_path, username)
                    import time as _t
                    if bool(prem.get("active")):
                        tier = db.get_user_premium_tier(db_path, username)
                        bonus_pct = float(tier.get("earn_bonus_percent", 0.10))
                        premium_applied = True
                        premium_extra = int(round(penalized * bonus_pct))
                    else:
                        premium_applied = False
                        premium_extra = 0
                    final_add = int(penalized + premium_extra)
                    resp = db.add_premium_lifetime_progress(db_path, username, final_add)
                    if not resp.get("success"):
                        return {"success": False, "message": resp.get("message", "Failed to add progression")}
                    return {
                        "success": True,
                        "message": "Session ended (balance reached 0, penalty applied)",
                        "added_progress": int(final_add),
                        "elapsed": int(elapsed_stop),
                        "bonus": int(bonus_stop),
                        "rate": rate_stop,
                        "penalty_applied": True,
                        "penalty_percent": 25,
                        "penalty_loss": int(max(0, total_base - penalized)),
                        "premium_applied": premium_applied,
                        "premium_extra": int(premium_extra),
                        "base_reward": int(total_base),
                        "current_tier": int(resp.get("current_tier", 0)),
                        "lifetime_seconds": int(resp.get("lifetime_seconds", 0)),
                    }
            except Exception:
                pass
            # Apply depletion every 10 minutes and stop with penalty on stat==0
            now = int(time.time())
            if now >= next_deplete_at:
                while next_deplete_at <= now:
                    deplete_tick += 1
                    e_drop = -1 if (deplete_tick % 4) in (1, 2, 3) else 0
                    h_drop = -1 if (deplete_tick % 2) == 1 else 0
                    w_drop = -1
                    try:
                        db.apply_stat_changes(db_path, username, e_drop, h_drop, w_drop)
                    except Exception:
                        pass
                    try:
                        stats2 = db.get_user_stats(db_path, username) or {"energy": 100, "hunger": 100, "water": 100}
                        e, h, w = int(stats2["energy"]), int(stats2["hunger"]), int(stats2["water"])
                        if e <= 20 and warned_energy < 20:
                            print("\n" + Fore.RED + Style.BRIGHT + "Warning: Energy is at 20% or lower!")
                            warned_energy = 20
                        elif e <= 50 and warned_energy < 50:
                            print("\n" + Fore.YELLOW + "Notice: Energy is at 50% or lower.")
                            warned_energy = 50
                        if h <= 20 and warned_hunger < 20:
                            print("\n" + Fore.RED + Style.BRIGHT + "Warning: Hunger is at 20% or lower!")
                            warned_hunger = 20
                        elif h <= 50 and warned_hunger < 50:
                            print("\n" + Fore.YELLOW + "Notice: Hunger is at 50% or lower.")
                            warned_hunger = 50
                        if w <= 20 and warned_water < 20:
                            print("\n" + Fore.RED + Style.BRIGHT + "Warning: Water is at 20% or lower!")
                            warned_water = 20
                        elif w <= 50 and warned_water < 50:
                            print("\n" + Fore.YELLOW + "Notice: Water is at 50% or lower.")
                            warned_water = 50
                        if e <= 0 or h <= 0 or w <= 0:
                            stop_ts = int(time.time())
                            print("\n" + Fore.RED + Style.BRIGHT + "A stat reached 0%. Session stopped (25% penalty applied).")
                            elapsed_stop = stop_ts - start_ts
                            blocks_stop = max(0, elapsed_stop // block_seconds)
                            rate_stop = float(base + per_block * max(0, blocks_stop - 1))
                            bonus_stop = int(round(elapsed_stop * rate_stop))
                            total_base = int(elapsed_stop + bonus_stop)
                            penalized = int(round(total_base * 0.75))
                            prem = db.is_premium(db_path, username)
                            import time as _t
                            if bool(prem.get("active")):
                                tier = db.get_user_premium_tier(db_path, username)
                                bonus_pct = float(tier.get("earn_bonus_percent", 0.10))
                                premium_applied = True
                                premium_extra = int(round(penalized * bonus_pct))
                            else:
                                premium_applied = False
                                premium_extra = 0
                            final_add = int(penalized + premium_extra)
                            resp = db.add_premium_lifetime_progress(db_path, username, final_add)
                            if not resp.get("success"):
                                return {"success": False, "message": resp.get("message", "Failed to add progression")}
                            return {
                                "success": True,
                                "message": "Session ended (stat reached 0, penalty applied)",
                                "added_progress": int(final_add),
                                "elapsed": int(elapsed_stop),
                                "bonus": int(bonus_stop),
                                "rate": rate_stop,
                                "penalty_applied": True,
                                "penalty_percent": 25,
                                "penalty_loss": int(max(0, total_base - penalized)),
                                "premium_applied": premium_applied,
                                "premium_extra": int(premium_extra),
                                "base_reward": int(total_base),
                                "current_tier": int(resp.get("current_tier", 0)),
                                "lifetime_seconds": int(resp.get("lifetime_seconds", 0)),
                            }
                    except Exception:
                        pass
                    next_deplete_at += 600
            time.sleep(1)
        except KeyboardInterrupt:
            sys.stdout.write("\n")
            ans = input("Claim now and end session? (y/N to resume): ").strip().lower()
            if ans in ("y", "yes"):
                break
            else:
                continue
    elapsed = int(time.time()) - start_ts
    blocks = elapsed // block_seconds
    rate = float(base + per_block * max(0, blocks - 1))
    bonus = int(round(elapsed * rate))
    total_add_base = int(elapsed + bonus)
    # Premium bonus
    premium_applied = False
    premium_extra = 0
    prem = db.is_premium(db_path, username)
    import time as _t
    if bool(prem.get("active")):
        tier = db.get_user_premium_tier(db_path, username)
        bonus_pct = float(tier.get("earn_bonus_percent", 0.10))
        premium_applied = True
        premium_extra = int(round(total_add_base * bonus_pct))
    # Apply timezone earn multiplier
    try:
        tz = db.get_timezone_multipliers(db_path, username)
        earn_mul = float(tz.get("earn_multiplier", 1.0))
    except Exception:
        earn_mul = 1.0
    total_add = int(round((total_add_base + premium_extra) * earn_mul))
    res = db.add_premium_lifetime_progress(db_path, username, total_add)
    if not res.get("success"):
        return {"success": False, "message": res.get("message", "Failed to add progression")}
    return {"success": True, "message": "Open session claimed (progression)", "added_progress": int(total_add), "elapsed": elapsed, "bonus": bonus, "rate": rate, "premium_applied": premium_applied, "premium_extra": int(premium_extra), "base_reward": int(total_add_base), "current_tier": int(res.get("current_tier", 0)), "lifetime_seconds": int(res.get("lifetime_seconds", 0))}


def _format_promo_line(db_path: Path) -> str:
    pc = db.get_earner_promo_config(db_path)
    return "Promo: ongoing" if int(pc.get("promo_enabled", 1)) else "Promo: disabled"


def _format_bonus_brief(db_path: Path) -> str:
    pc = db.get_earner_promo_config(db_path)
    enabled = int(pc.get("promo_enabled", 1)) == 1
    if enabled:
        base = float(pc.get("base_percent", 0.10))
        perb = float(pc.get("per_block_percent", 0.0125))
        return f"Bonus: base {base*100:.0f}%, +{perb*100:.2f}%/block"
    else:
        dc = db.get_earner_default_config(db_path)
        base = float(dc.get("base_percent", 0.10))
        perb = float(dc.get("per_block_percent", 0.0125))
        return f"Bonus: base {base*100:.0f}%, +{perb*100:.2f}%/block"


def _format_stake_brief(db_path: Path) -> str:
    try:
        tiers = db.list_earner_stake_tiers(db_path)
        if tiers:
            # Show up to first 3 tiers
            parts = []
            for t in tiers[:3]:
                parts.append(f"{formatting.format_duration(int(t['min_seconds']), style='short')}→x{float(t['multiplier']):g}")
            more = "..." if len(tiers) > 3 else ""
            return "tiers: " + ", ".join(parts) + (f", {more}" if more else "")
        cfg = db.get_earner_stake_config(db_path)
        mins = int(cfg.get("min_stake_seconds", 7200))
        mult = float(cfg.get("reward_multiplier", 2.0))
        return f"min {formatting.format_duration(mins, style='short')}, x{mult:g} reward"
    except Exception:
        return "min 2h, x2 reward"


def _print_stake_tiers(db_path: Path) -> None:
    tiers = db.list_earner_stake_tiers(db_path)
    rows = []
    if tiers:
        for t in tiers:
            rows.append((formatting.format_duration(int(t['min_seconds']), style='short'), f"x{float(t['multiplier']):g}"))
    else:
        cfg = db.get_earner_stake_config(db_path)
        mins = int(cfg.get("min_stake_seconds", 7200))
        mult = float(cfg.get("reward_multiplier", 2.0))
        rows.append((f">= {formatting.format_duration(mins, style='short')}", f"x{mult:g}"))
    # Table formatting
    h1, h2 = "Min Stake", "Multiplier"
    w1 = max(len(h1), *(len(r[0]) for r in rows))
    w2 = max(len(h2), *(len(r[1]) for r in rows))
    sep = "+" + "-"*(w1+2) + "+" + "-"*(w2+2) + "+"
    def line(c1, c2):
        print("| " + c1.ljust(w1) + " | " + c2.ljust(w2) + " |")
    print(sep)
    line(h1, h2)
    print(sep)
    for r in rows:
        line(r[0], r[1])
    print(sep)


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
            prem_active, prem_rem = _premium_info(current_db, uname)
            # Determine tier for display
            tier_num = 0
            try:
                tinfo = db.get_user_premium_tier(current_db, uname)
                tier_num = int(tinfo.get("tier", 0))
            except Exception:
                tier_num = 0
            # Header and concise Premium line
            print(Fore.CYAN + Style.BRIGHT + f"Logged in as: {uname} | Balance: {human}")
            romans = {1:"I",2:"II",3:"III",4:"IV",5:"V",6:"VI",7:"VII",8:"VIII",9:"IX",10:"X"}
            if prem_active:
                if tier_num > 0:
                    print(Fore.CYAN + Style.BRIGHT + f"Premium {romans.get(tier_num)}: active ({formatting.format_duration(prem_rem, style='short')})")
                else:
                    print(Fore.CYAN + Style.BRIGHT + f"Premium: active ({formatting.format_duration(prem_rem, style='short')})")
                # Show benefits
                try:
                    tinfo = db.get_user_premium_tier(current_db, uname)
                    earn = int(round(float(tinfo.get("earn_bonus_percent", 0.0)) * 100))
                    cap = int(tinfo.get("stat_cap_percent", 100))
                    print(Fore.GREEN + f"Benefits: +{earn}% earn bonus; stat cap {cap}%")
                except Exception:
                    pass
            else:
                if tier_num > 0:
                    print(Fore.CYAN + Style.BRIGHT + f"Premium {romans.get(tier_num)}: inactive")
                else:
                    print(Fore.CYAN + Style.BRIGHT + "Premium: inactive")
            # Show promo status line immediately under header
            print(_format_promo_line(current_db))
            print(f"{Fore.YELLOW}1){Style.RESET_ALL} Start earning session (stake and countdown)")
            print(f"{Fore.YELLOW}2){Style.RESET_ALL} Start open earning (no stake)  [{_format_bonus_brief(current_db)}]")
            print(f"{Fore.YELLOW}3){Style.RESET_ALL} View stake tiers")
            print(f"{Fore.YELLOW}12){Style.RESET_ALL} Start open earning to Premium progression")
            # If admin, show promo config option
            if current_user.get("is_admin"):
                print(f"{Fore.YELLOW}4){Style.RESET_ALL} Set promo config (admin)")
                print(f"{Fore.YELLOW}5){Style.RESET_ALL} Set default config (admin)")
                print(f"{Fore.YELLOW}6){Style.RESET_ALL} Set stake config (admin)")
                print(f"{Fore.YELLOW}7){Style.RESET_ALL} Manage stake tiers (admin)")
                print(f"{Fore.YELLOW}8){Style.RESET_ALL} Enable promo")
                print(f"{Fore.YELLOW}9){Style.RESET_ALL} Disable promo")
                print(f"{Fore.YELLOW}10){Style.RESET_ALL} Refresh balance")
                print(f"{Fore.YELLOW}11){Style.RESET_ALL} Logout")
            else:
                print(f"{Fore.YELLOW}4){Style.RESET_ALL} Refresh balance")
                print(f"{Fore.YELLOW}5){Style.RESET_ALL} Logout")
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
                    line = f"Success! Rewarded {formatting.format_duration(reward, style='short')}"
                    if res.get("premium_applied") and int(res.get("premium_extra", 0)) > 0:
                        # show actual tier percent
                        try:
                            tinfo = db.get_user_premium_tier(current_db, uname)
                            pct = float(tinfo.get("earn_bonus_percent", 0.10)) * 100.0
                        except Exception:
                            pct = 10.0
                        line += f" (includes +{pct:.0f}% Premium: {formatting.format_duration(int(res['premium_extra']), style='short')})"
                    print(Fore.GREEN + line + f". New balance: {formatting.format_duration(nb, style='short')}.")
                else:
                    nb = res.get("balance", 0)
                    print(Fore.RED + f"{res.get('message', 'Failed')}. Balance: {formatting.format_duration(nb, style='short')}")
            elif choice == "2":
                res = start_open_earn_session(current_db, uname)
                if res.get("success"):
                    reward = res.get("reward", 0)
                    bonus = res.get("bonus", 0)
                    rate = float(res.get("rate", 0.0))
                    nb = res.get("balance", 0)
                    el = res.get("elapsed", 0)
                    pct = f"{rate*100:.1f}%" if rate > 0 else "0%"
                    msg = f"Claimed after {formatting.format_duration(el, style='short')}. Added: {formatting.format_duration(el, style='short')} + bonus {formatting.format_duration(bonus, style='short')} ({pct})"
                    if res.get("penalty_applied"):
                        loss = int(res.get("penalty_loss", 0))
                        msg += f" - penalty 25% ({formatting.format_duration(loss, style='short')})"
                    if res.get("premium_applied") and int(res.get("premium_extra", 0)) > 0:
                        try:
                            tinfo = db.get_user_premium_tier(current_db, uname)
                            pct = float(tinfo.get("earn_bonus_percent", 0.10)) * 100.0
                        except Exception:
                            pct = 10.0
                        msg += f" + Premium +{pct:.0f}% {formatting.format_duration(int(res['premium_extra']), style='short')}"
                    msg += f" = {formatting.format_duration(reward, style='short')}. New balance: {formatting.format_duration(nb, style='short')}."
                    print(Fore.GREEN + msg)
                else:
                    print(Fore.RED + res.get("message", "Failed"))
            elif choice == "3":
                _print_stake_tiers(current_db)
            elif choice == "12":
                res = start_open_earn_session_to_progress(current_db, uname)
                if res.get("success"):
                    print(Fore.GREEN + f"Added {formatting.format_duration(int(res.get('added_progress',0)), style='short')} to Premium progression. Now Tier {int(res.get('current_tier',0))}.")
                else:
                    print(Fore.RED + res.get("message", "Failed"))
            elif choice == "4" and current_user.get("is_admin"):
                # Admin-only: set promo config interactively
                print(Fore.CYAN + "Set promo config (percentages as decimals, e.g., 0.10 for 10%)")
                cfg = db.get_earner_promo_config(current_db)
                base_s = input(f"Base percent [{cfg['base_percent']:.4f}]: ").strip()
                per_s = input(f"Per-block percent [{cfg['per_block_percent']:.4f}]: ").strip()
                min_s = input(f"Minimum seconds [{cfg['min_seconds']}]: ").strip()
                blk_s = input(f"Block seconds [{cfg['block_seconds']}]: ").strip()
                en_s = input(f"Enable progressive promo? (Y/n) [{'Y' if int(cfg['promo_enabled']) else 'N'}]: ").strip().lower()
                def_s = input(f"Default bonus percent when disabled [{cfg['default_bonus_percent']:.4f}]: ").strip()
                try:
                    base_p = float(base_s) if base_s else float(cfg['base_percent'])
                    perb_p = float(per_s) if per_s else float(cfg['per_block_percent'])
                    mins = int(min_s) if min_s else int(cfg['min_seconds'])
                    blks = int(blk_s) if blk_s else int(cfg['block_seconds'])
                    en = 0 if en_s in ('n','no','0','false') else 1
                    defb = float(def_s) if def_s else float(cfg['default_bonus_percent'])
                    # authenticate admin
                    u = db.find_user(current_db, uname)
                    if not u or not u['is_admin']:
                        print(Fore.RED + 'Not an admin user')
                    else:
                        pw = prompt_passcode()
                        if not auth.verify_passcode(pw, u['passcode_hash']):
                            print(Fore.RED + 'Authentication failed')
                        else:
                            db.set_earner_promo_config(current_db, base_p, perb_p, mins, blks, en, defb)
                            print(Fore.GREEN + 'Promo config updated.')
                except Exception as e:
                    print(Fore.RED + f"Invalid input: {e}")
            elif choice == "5" and current_user.get("is_admin"):
                # Admin-only: set default config interactively
                print(Fore.CYAN + "Set default config (percentages as decimals, e.g., 0.10 for 10%)")
                cfg = db.get_earner_default_config(current_db)
                base_s = input(f"Base percent [{cfg['base_percent']:.4f}]: ").strip()
                per_s = input(f"Per-block percent [{cfg['per_block_percent']:.4f}]: ").strip()
                min_s = input(f"Minimum seconds [{cfg['min_seconds']}]: ").strip()
                blk_s = input(f"Block seconds [{cfg['block_seconds']}]: ").strip()
                try:
                    base_p = float(base_s) if base_s else float(cfg['base_percent'])
                    perb_p = float(per_s) if per_s else float(cfg['per_block_percent'])
                    mins = int(min_s) if min_s else int(cfg['min_seconds'])
                    blks = int(blk_s) if blk_s else int(cfg['block_seconds'])
                    # authenticate admin
                    u = db.find_user(current_db, uname)
                    if not u or not u['is_admin']:
                        print(Fore.RED + 'Not an admin user')
                    else:
                        pw = prompt_passcode()
                        if not auth.verify_passcode(pw, u['passcode_hash']):
                            print(Fore.RED + 'Authentication failed')
                        else:
                            db.set_earner_default_config(current_db, base_p, perb_p, mins, blks)
                            print(Fore.GREEN + 'Default config updated.')
                except Exception as e:
                    print(Fore.RED + f"Invalid input: {e}")
            elif choice == "6" and current_user.get("is_admin"):
                # Admin-only: set stake config interactively
                cfg = db.get_earner_stake_config(current_db)
                print(Fore.CYAN + "Set stake config")
                min_s = input(f"Minimum stake seconds [{cfg['min_stake_seconds']}]: ").strip()
                mult_s = input(f"Reward multiplier [{cfg['reward_multiplier']:.2f}]: ").strip()
                try:
                    mins = int(min_s) if min_s else int(cfg['min_stake_seconds'])
                    mult = float(mult_s) if mult_s else float(cfg['reward_multiplier'])
                    # authenticate admin
                    u = db.find_user(current_db, uname)
                    if not u or not u['is_admin']:
                        print(Fore.RED + 'Not an admin user')
                    else:
                        pw = prompt_passcode()
                        if not auth.verify_passcode(pw, u['passcode_hash']):
                            print(Fore.RED + 'Authentication failed')
                        else:
                            db.set_earner_stake_config(current_db, mins, mult)
                            print(Fore.GREEN + 'Stake config updated.')
                except Exception as e:
                    print(Fore.RED + f"Invalid input: {e}")
            elif choice == "7" and current_user.get("is_admin"):
                # Manage stake tiers (list/add/remove/clear/set-defaults)
                while True:
                    print(Fore.CYAN + "Stake tiers management")
                    tiers = db.list_earner_stake_tiers(current_db)
                    if tiers:
                        print("Current tiers:")
                        for t in tiers:
                            print(f" - {formatting.format_duration(int(t['min_seconds']), style='short')}: x{float(t['multiplier']):g}")
                    else:
                        print("No tiers defined (fallback to single stake config).")
                    print("1) Set balanced defaults  2) Add tier  3) Remove tier  4) Clear  0) Back")
                    sub = input("Choose: ").strip()
                    if sub == "0":
                        break
                    elif sub == "1":
                        u = db.find_user(current_db, uname)
                        if not u or not u['is_admin']:
                            print(Fore.RED + 'Not an admin user')
                            continue
                        pw = prompt_passcode()
                        if not auth.verify_passcode(pw, u['passcode_hash']):
                            print(Fore.RED + 'Authentication failed')
                            continue
                        db.set_earner_stake_tiers_defaults(current_db)
                        print(Fore.GREEN + 'Seeded balanced default tiers.')
                    elif sub == "2":
                        ms = input("Min seconds: ").strip()
                        ml = input("Multiplier (e.g., 2.5): ").strip()
                        try:
                            u = db.find_user(current_db, uname)
                            if not u or not u['is_admin']:
                                print(Fore.RED + 'Not an admin user')
                                continue
                            pw = prompt_passcode()
                            if not auth.verify_passcode(pw, u['passcode_hash']):
                                print(Fore.RED + 'Authentication failed')
                                continue
                            db.add_earner_stake_tier(current_db, int(ms), float(ml))
                            print(Fore.GREEN + 'Tier added/updated.')
                        except Exception as e:
                            print(Fore.RED + f"Invalid input: {e}")
                    elif sub == "3":
                        ms = input("Min seconds to remove: ").strip()
                        try:
                            u = db.find_user(current_db, uname)
                            if not u or not u['is_admin']:
                                print(Fore.RED + 'Not an admin user')
                                continue
                            pw = prompt_passcode()
                            if not auth.verify_passcode(pw, u['passcode_hash']):
                                print(Fore.RED + 'Authentication failed')
                                continue
                            ok = db.remove_earner_stake_tier(current_db, int(ms))
                            print(Fore.GREEN + ('Tier removed.' if ok else 'Tier not found.'))
                        except Exception as e:
                            print(Fore.RED + f"Invalid input: {e}")
                    elif sub == "4":
                        u = db.find_user(current_db, uname)
                        if not u or not u['is_admin']:
                            print(Fore.RED + 'Not an admin user')
                            continue
                        pw = prompt_passcode()
                        if not auth.verify_passcode(pw, u['passcode_hash']):
                            print(Fore.RED + 'Authentication failed')
                            continue
                        db.clear_earner_stake_tiers(current_db)
                        print(Fore.YELLOW + 'All tiers cleared.')
            elif choice == "8" and current_user.get("is_admin"):
                # Enable promo quickly with current values
                cfg = db.get_earner_promo_config(current_db)
                u = db.find_user(current_db, uname)
                if not u or not u['is_admin']:
                    print(Fore.RED + 'Not an admin user')
                else:
                    pw = prompt_passcode()
                    if not auth.verify_passcode(pw, u['passcode_hash']):
                        print(Fore.RED + 'Authentication failed')
                    else:
                        db.set_earner_promo_config(current_db, float(cfg['base_percent']), float(cfg['per_block_percent']), int(cfg['min_seconds']), int(cfg['block_seconds']), 1, float(cfg['default_bonus_percent']))
                        print(Fore.GREEN + 'Promo enabled.')
            elif choice == "9" and current_user.get("is_admin"):
                # Disable promo quickly with current values
                cfg = db.get_earner_promo_config(current_db)
                u = db.find_user(current_db, uname)
                if not u or not u['is_admin']:
                    print(Fore.RED + 'Not an admin user')
                else:
                    pw = prompt_passcode()
                    if not auth.verify_passcode(pw, u['passcode_hash']):
                        print(Fore.RED + 'Authentication failed')
                    else:
                        db.set_earner_promo_config(current_db, float(cfg['base_percent']), float(cfg['per_block_percent']), int(cfg['min_seconds']), int(cfg['block_seconds']), 0, float(cfg['default_bonus_percent']))
                        print(Fore.YELLOW + 'Promo disabled (using default bonus).')
            elif (choice == "10" and current_user.get("is_admin")) or (choice == "4" and not current_user.get("is_admin")):
                bal = db.get_balance_seconds(current_db, uname) or 0
                print(f"Balance: {formatting.format_duration(int(bal), style='short')}")
            elif (choice == "11" and current_user.get("is_admin")) or (choice == "5" and not current_user.get("is_admin")):
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
    elif ns.cmd == "open-session":
        # Run foreground open earning session until user interrupts to claim
        res = start_open_earn_session(db_path, ns.username)
        if res.get("success"):
            reward = res.get("reward", 0)
            bonus = res.get("bonus", 0)
            rate = float(res.get("rate", 0.0))
            el = res.get("elapsed", 0)
            pct = f"{rate*100:.1f}%" if rate > 0 else "0%"
            print(f"OK: claimed after {formatting.format_duration(el, style='short')} | added {formatting.format_duration(el, style='short')} + bonus {formatting.format_duration(bonus, style='short')} ({pct}) = {formatting.format_duration(reward, style='short')} | new balance {formatting.format_duration(int(res.get('balance',0)), style='short')}")
        else:
            raise SystemExit(res.get("message", "Failed"))
    elif ns.cmd == "set-promo":
        # Admin-only: set promo earning config
        admin_user = db.find_user(db_path, ns.admin)
        if not admin_user or not admin_user["is_admin"]:
            raise SystemExit("User is not an admin")
        pw = prompt_passcode()
        if not auth.verify_passcode(pw, admin_user["passcode_hash"]):
            raise SystemExit("Authentication failed")
        promo_enabled = 1 if ns.enable else (0 if ns.disable else 1)
        db.set_earner_promo_config(db_path, float(ns.base), float(ns.per_block), int(ns.min_seconds), int(ns.block_seconds), promo_enabled, float(ns.default_bonus))
        cfg = db.get_earner_promo_config(db_path)
        print(
            Fore.GREEN + "Promo config set: " +
            (
                f"enabled | base {cfg['base_percent']*100:.1f}% | per-block {cfg['per_block_percent']*100:.2f}% | "
                f"min {formatting.format_duration(cfg['min_seconds'], style='short')} | block {formatting.format_duration(cfg['block_seconds'], style='short')}"
                if int(cfg.get('promo_enabled',1)) else
                f"disabled | default bonus {cfg['default_bonus_percent']*100:.1f}% | min {formatting.format_duration(cfg['min_seconds'], style='short')}"
            )
        )
    elif ns.cmd == "set-default":
        # Admin-only: set default open-earning config
        admin_user = db.find_user(db_path, ns.admin)
        if not admin_user or not admin_user["is_admin"]:
            raise SystemExit("User is not an admin")
        pw = prompt_passcode()
        if not auth.verify_passcode(pw, admin_user["passcode_hash"]):
            raise SystemExit("Authentication failed")
        db.set_earner_default_config(db_path, float(ns.base), float(ns.per_block), int(ns.min_seconds), int(ns.block_seconds))
        cfg = db.get_earner_default_config(db_path)
        print(
            Fore.GREEN + "Default open-earning config set: " +
            f"base {cfg['base_percent']*100:.1f}% | per-block {cfg['per_block_percent']*100:.2f}% | min {formatting.format_duration(cfg['min_seconds'], style='short')} | block {formatting.format_duration(cfg['block_seconds'], style='short')}"
        )
    elif ns.cmd == "set-stake-config":
        # Admin-only: set stake session config
        admin_user = db.find_user(db_path, ns.admin)
        if not admin_user or not admin_user["is_admin"]:
            raise SystemExit("User is not an admin")
        pw = prompt_passcode()
        if not auth.verify_passcode(pw, admin_user["passcode_hash"]):
            raise SystemExit("Authentication failed")
        db.set_earner_stake_config(db_path, int(ns.min_seconds), float(ns.multiplier))
        cfg = db.get_earner_stake_config(db_path)
        print(
            Fore.GREEN + "Stake config set: " +
            f"min {formatting.format_duration(int(cfg['min_stake_seconds']), style='short')} | multiplier x{float(cfg['reward_multiplier']):g}"
        )
    else:
        raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()
