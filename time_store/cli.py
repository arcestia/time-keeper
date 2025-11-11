import argparse
import getpass
from pathlib import Path
from typing import Optional

from colorama import Fore, Style, init as colorama_init

from time_keeper import db as tkdb
from time_keeper import auth as tkauth
from time_keeper import formatting


def _require_user_login(db_path: Path, username: str) -> None:
    user = tkdb.find_user(db_path, username)
    if not user:
        raise SystemExit("User not found")
    pw = getpass.getpass("Passcode: ")
    if not tkauth.verify_passcode(pw, user["passcode_hash"]):
        raise SystemExit("Authentication failed")


def _require_admin_login(db_path: Path, username: str) -> None:
    user = tkdb.find_user(db_path, username)
    if not user:
        raise SystemExit("User not found")
    if not user["is_admin"]:
        raise SystemExit("User is not an admin")
    pw = getpass.getpass("Passcode: ")
    if not tkauth.verify_passcode(pw, user["passcode_hash"]):
        raise SystemExit("Authentication failed")


def _premium_info(db_path: Path, username: Optional[str]) -> tuple[bool, int]:
    if not username:
        return (False, 0)
    try:
        p = tkdb.is_premium(db_path, username)
        import time as _t
        active = bool(p.get("active"))
        rem = 0
        if active:
            rem = max(0, int(p.get("until", 0)) - int(_t.time()))
        return (active, rem)
    except Exception:
        return (False, 0)

def _premium_tier_discount(db_path: Path, username: Optional[str]) -> tuple[int, float]:
    """Return (tier_number, store_discount_percent as fraction)."""
    if not username:
        return (0, 0.0)
    try:
        t = tkdb.get_user_premium_tier(db_path, username)
        tier_num = int(t.get("tier", 0) or 0)
        disc = float(t.get("store_discount_percent", 0.0) or 0.0)
        return (tier_num, disc)
    except Exception:
        return (0, 0.0)


def cmd_list(db_path: Path, username: Optional[str] = None) -> None:
    items = tkdb.list_store_items(db_path)
    if not items:
        print(Fore.YELLOW + "No items in the store yet. Ask admin to add some.")
        return
    prem_active, _ = _premium_info(db_path, username)
    tier_num, disc_frac = _premium_tier_discount(db_path, username)
    headers = ["ID", "Key", "Name", "Kind", "Qty", "Restores", "Price (eff)"]
    if prem_active:
        headers.append(f"Your price (-{int(disc_frac*100)}%)")
    rows = []
    idx = tkdb.get_market_index_percent(db_path)
    for it in items:
        restores = []
        if it["restore_energy"]:
            restores.append(f"E+{it['restore_energy']}")
        if it["restore_hunger"]:
            restores.append(f"H+{it['restore_hunger']}")
        if it["restore_water"]:
            restores.append(f"W+{it['restore_water']}")
        restores_s = ", ".join(restores) if restores else "-"
        row = [
            str(it.get("id") or "-"),
            it["item"],
            it.get("name") or "-",
            it["kind"],
            str(it["qty"]),
            restores_s,
            formatting.format_duration(it["effective_price_seconds"], style="short") + f"  (idx {idx}%)",
        ]
        if prem_active:
            your = max(1, int(round(int(it["effective_price_seconds"]) * (1.0 - float(disc_frac)))))
            row.append(formatting.format_duration(your, style="short"))
        rows.append(row)
    _print_table(headers, rows)


def cmd_buy(db_path: Path, username: str, item: str, qty: int, apply_now: bool = True) -> None:
    _require_user_login(db_path, username)
    res = tkdb.purchase_store_item(db_path, username, item, qty, apply_now=apply_now)
    if not res.get("success"):
        raise SystemExit(res.get("message", "Purchase failed"))
    if res.get("stored"):
        print(Fore.GREEN + f"Purchased and stored in inventory. Unit: {formatting.format_duration(int(res['unit_price_seconds']), style='short')}  Total: {formatting.format_duration(int(res['total_cost_seconds']), style='short')}.")
        print(f"Balance: {formatting.format_duration(int(res['balance']), style='short')}")
    else:
        print(Fore.GREEN + f"Purchase completed. Unit price: {formatting.format_duration(int(res['unit_price_seconds']), style='short')}. Total: {formatting.format_duration(int(res['total_cost_seconds']), style='short')}.")
        print(
            f"Balance: {formatting.format_duration(int(res['balance']), style='short')}  |  Stats -> "
            f"Energy {res['energy']}%  Hunger {res['hunger']}%  Water {res['water']}%  |  Stock left: {res['qty_remaining']}"
        )


def cmd_prices(db_path: Path, username: Optional[str] = None) -> None:
    idx = tkdb.get_market_index_percent(db_path)
    prices = tkdb.get_store_prices(db_path)
    if not prices:
        print(Fore.YELLOW + "No price data yet. Seed items first.")
        return
    # We don't have names in the prices table; join comes in list view.
    prem_active, _ = _premium_info(db_path, username)
    _, disc_frac = _premium_tier_discount(db_path, username)
    headers = ["Item", "Base", "Current", "Effective", "Index"]
    if prem_active:
        headers.append(f"Your price (-{int(disc_frac*100)}%)")
    rows = []
    for p in prices:
        effective = max(1, int(round(p["current_price_seconds"] * (1.0 + float(idx)/100.0))))
        row = [
            p["item"],
            formatting.format_duration(p["base_price_seconds"], style="short"),
            formatting.format_duration(p["current_price_seconds"], style="short"),
            formatting.format_duration(effective, style="short"),
            f"{idx}%",
        ]
        if prem_active:
            your = max(1, int(round(effective * (1.0 - float(disc_frac)))))
            row.append(formatting.format_duration(your, style="short"))
        rows.append(row)
    _print_table(headers, rows)


def cmd_refresh_prices(db_path: Path, volatility: float) -> None:
    tkdb.refresh_store_prices(db_path, volatility)
    print(Fore.GREEN + f"Prices refreshed with volatility {volatility}")


def cmd_set_index(db_path: Path, admin_username: str, percent: int) -> None:
    _require_admin_login(db_path, admin_username)
    tkdb.set_market_index_percent(db_path, percent)
    print(Fore.GREEN + f"Market index set to {tkdb.get_market_index_percent(db_path)}%")


def cmd_upsert_item(db_path: Path, admin_username: str, item: str, kind: str, qty: int,
                    restore_energy: int, restore_hunger: int, restore_water: int,
                    base_price_seconds: int, name: Optional[str] = None) -> None:
    _require_admin_login(db_path, admin_username)
    tkdb.upsert_store_item(db_path, item, kind, qty, restore_energy, restore_hunger, restore_water, base_price_seconds, name=name)
    print(Fore.GREEN + f"Item '{item}' upserted.")


def cmd_set_qty(db_path: Path, admin_username: str, item: str, qty: int) -> None:
    _require_admin_login(db_path, admin_username)
    ok = tkdb.set_store_item_qty(db_path, item, qty)
    if ok:
        print(Fore.GREEN + f"Set qty of '{item}' to {qty}.")
    else:
        raise SystemExit("Item not found")


def cmd_inventory_list(db_path: Path, username: str) -> None:
    _require_user_login(db_path, username)
    items = tkdb.list_user_inventory(db_path, username)
    if not items:
        print(Fore.YELLOW + "Inventory empty.")
        return
    headers = ["ID", "Key", "Name", "Qty", "Restores"]
    rows = []
    for it in items:
        restores = []
        if it["restore_energy"]:
            restores.append(f"E+{it['restore_energy']}")
        if it["restore_hunger"]:
            restores.append(f"H+{it['restore_hunger']}")
        if it["restore_water"]:
            restores.append(f"W+{it['restore_water']}")
        restores_s = ", ".join(restores) if restores else "-"
        rows.append([
            str(it.get("id") or "-"),
            it["item"],
            it.get("name") or "-",
            str(it["qty"]),
            restores_s,
        ])
    _print_table(headers, rows)


def cmd_inventory_use(db_path: Path, username: str, item_or_id: str, qty: int) -> None:
    _require_user_login(db_path, username)
    key = item_or_id
    if item_or_id.isdigit():
        # resolve id -> key via catalog listing
        for it in tkdb.list_store_items(db_path):
            if it.get("id") == int(item_or_id):
                key = it["item"]
                break
    res = tkdb.use_inventory_item(db_path, username, key, qty)
    if not res.get("success"):
        raise SystemExit(res.get("message", "Use failed"))
    print(Fore.GREEN + "Used item.")
    print(f"Stats -> Energy {res['energy']}%  Hunger {res['hunger']}%  Water {res['water']}%")


def cmd_inventory_send(db_path: Path, from_username: str, to_username: str, item_or_id: str, qty: int) -> None:
    _require_user_login(db_path, from_username)
    key = item_or_id
    if item_or_id.isdigit():
        for it in tkdb.list_store_items(db_path):
            if it.get("id") == int(item_or_id):
                key = it["item"]
                break
    res = tkdb.transfer_inventory_item(db_path, from_username, to_username, key, qty)
    if not res.get("success"):
        raise SystemExit(res.get("message", "Transfer failed"))
    print(Fore.GREEN + "Transfer completed.")
    print(f"Sender now has: {res.get('sender_qty', 0)}  |  Recipient now has: {res.get('recipient_qty', 0)}")


def cmd_inventory_sell(db_path: Path, username: str, item_or_id: str, qty: int) -> None:
    _require_user_login(db_path, username)
    key = item_or_id
    if item_or_id.isdigit():
        for it in tkdb.list_store_items(db_path):
            if it.get("id") == int(item_or_id):
                key = it["item"]
                break
    res = tkdb.sell_inventory_item(db_path, username, key, qty)
    if not res.get("success"):
        raise SystemExit(res.get("message", "Sell failed"))
    print(Fore.GREEN + "Sold item(s).")
    print(
        "Unit effective: " + formatting.format_duration(int(res['unit_effective_price_seconds']), style='short') +
        f"  | Payout rate: {int(res['rate_percent'])}%" +
        "  | Unit payout: " + formatting.format_duration(int(res['unit_payout_seconds']), style='short') +
        "  | Total payout: " + formatting.format_duration(int(res['total_payout_seconds']), style='short')
    )
    print("Balance: " + formatting.format_duration(int(res['balance']), style='short') + f"  | Remaining qty: {int(res['remaining_qty'])}")


def _print_table(headers, rows):
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    header_line = "  ".join((Fore.CYAN + Style.BRIGHT + h.ljust(widths[i]) + Style.RESET_ALL) for i, h in enumerate(headers))
    sep_line = "  ".join("-" * w for w in widths)
    print(header_line)
    print(sep_line)
    for row in rows:
        print("  ".join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))


def _prompt_passcode() -> str:
    pw = getpass.getpass("Passcode: ")
    if not pw:
        raise SystemExit("Passcode cannot be empty")
    return pw


def login_and_get_user(db_path: Path, username: str) -> Optional[dict]:
    user = tkdb.find_user(db_path, username)
    if not user:
        print(Fore.RED + "User not found")
        return None
    pw = _prompt_passcode()
    if not tkauth.verify_passcode(pw, user["passcode_hash"]):
        print(Fore.RED + "Authentication failed")
        return None
    return dict(user)


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


def _slugify(name: str) -> str:
    s = (name or "").strip().lower()
    out = []
    prev_dash = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                out.append('-')
                prev_dash = True
    slug = ''.join(out).strip('-')
    return slug or ""


def interactive_menu(db_path: Path) -> None:
    current_user: Optional[dict] = None
    while True:
        print("")
        print(Fore.CYAN + Style.BRIGHT + "=== Time Store ===")
        if current_user is None:
            print("Status: not logged in")
            print(f"{Fore.YELLOW}1){Style.RESET_ALL} Login")
            print(f"{Fore.YELLOW}2){Style.RESET_ALL} List items")
            print(f"{Fore.YELLOW}3){Style.RESET_ALL} Show prices")
            print(f"{Fore.YELLOW}0){Style.RESET_ALL} Quit")
            choice = input("Choose: ").strip()
            if choice == "0":
                print(Fore.GREEN + "Goodbye.")
                return
            elif choice == "1":
                username = input("Username: ").strip()
                logged = login_and_get_user(db_path, username)
                if logged:
                    current_user = logged
                    role = "admin" if logged.get("is_admin") else "user"
                    print(Fore.GREEN + f"Login success. User: {logged['username']} ({role})")
            elif choice == "2":
                cmd_list(db_path)
            elif choice == "3":
                cmd_prices(db_path)
            else:
                print(Fore.RED + "Invalid choice")
        else:
            uname = current_user.get("username")
            is_admin = bool(current_user.get("is_admin"))
            prem_active, prem_rem = _premium_info(db_path, uname)
            tier_num, _disc = _premium_tier_discount(db_path, uname)
            romans = {1:"I",2:"II",3:"III",4:"IV",5:"V",6:"VI",7:"VII",8:"VIII",9:"IX",10:"X"}
            print(Fore.CYAN + Style.BRIGHT + f"Logged in as: {uname} ({'admin' if is_admin else 'user'})")
            if prem_active:
                if tier_num > 0:
                    print(Fore.CYAN + Style.BRIGHT + f"Premium {romans.get(tier_num)}: active ({formatting.format_duration(prem_rem, style='short')})")
                else:
                    print(Fore.CYAN + Style.BRIGHT + f"Premium: active ({formatting.format_duration(prem_rem, style='short')})")
                # Show benefits
                try:
                    tinfo = tkdb.get_user_premium_tier(db_path, uname)
                    disc = int(round(float(tinfo.get("store_discount_percent", 0.0)) * 100))
                    cap = int(tinfo.get("stat_cap_percent", 100))
                    print(Fore.GREEN + f"Benefits: -{disc}% store prices; stat cap {cap}%")
                except Exception:
                    pass
            else:
                if tier_num > 0:
                    print(Fore.CYAN + Style.BRIGHT + f"Premium {romans.get(tier_num)}: inactive")
                else:
                    print(Fore.CYAN + Style.BRIGHT + "Premium: inactive")
            if is_admin:
                print(f"{Fore.YELLOW}1){Style.RESET_ALL} List items")
                print(f"{Fore.YELLOW}2){Style.RESET_ALL} Show prices")
                print(f"{Fore.YELLOW}3){Style.RESET_ALL} Refresh prices")
                print(f"{Fore.YELLOW}4){Style.RESET_ALL} Set market index")
                print(f"{Fore.YELLOW}5){Style.RESET_ALL} Upsert item")
                print(f"{Fore.YELLOW}6){Style.RESET_ALL} Set item qty")
                print(f"{Fore.YELLOW}7){Style.RESET_ALL} Buy item")
                print(f"{Fore.YELLOW}8){Style.RESET_ALL} Inventory")
                print(f"{Fore.YELLOW}9){Style.RESET_ALL} Use inventory item")
                print(f"{Fore.YELLOW}10){Style.RESET_ALL} Send inventory item")
                print(f"{Fore.YELLOW}11){Style.RESET_ALL} Sell inventory item")
                print(f"{Fore.YELLOW}12){Style.RESET_ALL} Logout")
                print(f"{Fore.YELLOW}0){Style.RESET_ALL} Quit")
            else:
                print(f"{Fore.YELLOW}1){Style.RESET_ALL} List items")
                print(f"{Fore.YELLOW}2){Style.RESET_ALL} Show prices")
                print(f"{Fore.YELLOW}3){Style.RESET_ALL} Buy item")
                print(f"{Fore.YELLOW}4){Style.RESET_ALL} Inventory")
                print(f"{Fore.YELLOW}5){Style.RESET_ALL} Use inventory item")
                print(f"{Fore.YELLOW}6){Style.RESET_ALL} Send inventory item")
                print(f"{Fore.YELLOW}7){Style.RESET_ALL} Sell inventory item")
                print(f"{Fore.YELLOW}8){Style.RESET_ALL} Logout")
                print(f"{Fore.YELLOW}0){Style.RESET_ALL} Quit")
            choice = input("Choose: ").strip()
            if choice == "0":
                print(Fore.GREEN + "Goodbye.")
                return
            if is_admin:
                if choice == "1":
                    cmd_list(db_path, username=uname)
                elif choice == "2":
                    cmd_prices(db_path, username=uname)
                elif choice == "3":
                    vol = float(_input_with_default("Volatility", "0.2"))
                    cmd_refresh_prices(db_path, vol)
                elif choice == "4":
                    p = _input_int_with_default("Market index percent (-50 to 300)", tkdb.get_market_index_percent(db_path))
                    cmd_set_index(db_path, uname, p)
                elif choice == "5":
                    item = input("Item key (Enter to auto-generate): ").strip()
                    name = _input_with_default("Display name (optional)", "").strip()
                    if not item:
                        # Auto-generate key from name or next id
                        base = _slugify(name) if name else f"item{tkdb.get_next_store_item_id(db_path)}"
                        candidate = base if base else f"item{tkdb.get_next_store_item_id(db_path)}"
                        suffix = 1
                        while tkdb.store_item_exists(db_path, candidate):
                            candidate = f"{base}-{suffix}"
                            suffix += 1
                        item = candidate
                    kind = _input_with_default("Kind (food/water)", "food").strip().lower()
                    qty = _input_int_with_default("Qty", 0)
                    re = _input_int_with_default("Restore energy", 0)
                    rh = _input_int_with_default("Restore hunger", 0)
                    rw = _input_int_with_default("Restore water", 0)
                    base = _input_int_with_default("Base price (seconds)", 60)
                    cmd_upsert_item(db_path, uname, item, kind, qty, re, rh, rw, base, name if name else None)
                elif choice == "6":
                    item = input("Item key: ").strip()
                    qty = _input_int_with_default("Qty", 0)
                    cmd_set_qty(db_path, uname, item, qty)
                elif choice == "7":
                    item = input("Item to buy (ID or key): ").strip()
                    qty = _input_int_with_default("Qty", 1)
                    apply_ans = _input_with_default("Apply now? (Y/n)", "Y").strip().lower()
                    apply_now = not (apply_ans in ("n", "no"))
                    if item.isdigit():
                        _require_user_login(db_path, uname)
                        res = tkdb.purchase_store_item_by_id(db_path, uname, int(item), qty, apply_now=apply_now)
                        if not res.get("success"):
                            print(Fore.RED + (res.get("message") or "Purchase failed"))
                        else:
                            if res.get("stored"):
                                print(Fore.GREEN + f"Purchased and stored in inventory. Unit: {formatting.format_duration(int(res['unit_price_seconds']), style='short')}  Total: {formatting.format_duration(int(res['total_cost_seconds']), style='short')}.")
                                print(f"Balance: {formatting.format_duration(int(res['balance']), style='short')}")
                            else:
                                print(Fore.GREEN + f"Purchase completed. Unit price: {formatting.format_duration(int(res['unit_price_seconds']), style='short')}. Total: {formatting.format_duration(int(res['total_cost_seconds']), style='short')}.")
                                print(
                                    f"Balance: {formatting.format_duration(int(res['balance']), style='short')}  |  Stats -> "
                                    f"Energy {res['energy']}%  Hunger {res['hunger']}%  Water {res['water']}%  |  Stock left: {res['qty_remaining']}"
                                )
                    else:
                        cmd_buy(db_path, uname, item, qty, apply_now=apply_now)
                elif choice == "8":
                    cmd_inventory_list(db_path, uname)
                elif choice == "9":
                    it = input("Inventory item to use (ID or key): ").strip()
                    qty = _input_int_with_default("Qty", 1)
                    try:
                        cmd_inventory_use(db_path, uname, it, qty)
                    except SystemExit as e:
                        print(Fore.RED + str(e))
                elif choice == "10":
                    it = input("Inventory item to send (ID or key): ").strip()
                    qty = _input_int_with_default("Qty", 1)
                    to_user = input("Send to username: ").strip()
                    try:
                        cmd_inventory_send(db_path, uname, to_user, it, qty)
                    except SystemExit as e:
                        print(Fore.RED + str(e))
                elif choice == "11":
                    it = input("Inventory item to sell (ID or key): ").strip()
                    qty = _input_int_with_default("Qty", 1)
                    try:
                        cmd_inventory_sell(db_path, uname, it, qty)
                    except SystemExit as e:
                        print(Fore.RED + str(e))
                elif choice == "12":
                    current_user = None
                    print(Fore.YELLOW + "Logged out.")
                else:
                    print(Fore.RED + "Invalid choice")
            else:
                if choice == "1":
                    cmd_list(db_path, username=uname)
                elif choice == "2":
                    cmd_prices(db_path, username=uname)
                elif choice == "3":
                    item = input("Item to buy (ID or key): ").strip()
                    qty = _input_int_with_default("Qty", 1)
                    apply_ans = _input_with_default("Apply now? (Y/n)", "Y").strip().lower()
                    apply_now = not (apply_ans in ("n", "no"))
                    if item.isdigit():
                        _require_user_login(db_path, uname)
                        res = tkdb.purchase_store_item_by_id(db_path, uname, int(item), qty, apply_now=apply_now)
                        if not res.get("success"):
                            print(Fore.RED + (res.get("message") or "Purchase failed"))
                        else:
                            if res.get("stored"):
                                print(Fore.GREEN + f"Purchased and stored in inventory. Unit: {formatting.format_duration(int(res['unit_price_seconds']), style='short')}  Total: {formatting.format_duration(int(res['total_cost_seconds']), style='short')}.")
                                print(f"Balance: {formatting.format_duration(int(res['balance']), style='short')}")
                            else:
                                print(Fore.GREEN + f"Purchase completed. Unit price: {formatting.format_duration(int(res['unit_price_seconds']), style='short')}. Total: {formatting.format_duration(int(res['total_cost_seconds']), style='short')}.")
                                print(
                                    f"Balance: {formatting.format_duration(int(res['balance']), style='short')}  |  Stats -> "
                                    f"Energy {res['energy']}%  Hunger {res['hunger']}%  Water {res['water']}%  |  Stock left: {res['qty_remaining']}"
                                )
                    else:
                        cmd_buy(db_path, uname, item, qty, apply_now=apply_now)
                elif choice == "4":
                    cmd_inventory_list(db_path, uname)
                elif choice == "5":
                    it = input("Inventory item to use (ID or key): ").strip()
                    qty = _input_int_with_default("Qty", 1)
                    try:
                        cmd_inventory_use(db_path, uname, it, qty)
                    except SystemExit as e:
                        print(Fore.RED + str(e))
                elif choice == "6":
                    it = input("Inventory item to send (ID or key): ").strip()
                    qty = _input_int_with_default("Qty", 1)
                    to_user = input("Send to username: ").strip()
                    try:
                        cmd_inventory_send(db_path, uname, to_user, it, qty)
                    except SystemExit as e:
                        print(Fore.RED + str(e))
                elif choice == "7":
                    it = input("Inventory item to sell (ID or key): ").strip()
                    qty = _input_int_with_default("Qty", 1)
                    try:
                        cmd_inventory_sell(db_path, uname, it, qty)
                    except SystemExit as e:
                        print(Fore.RED + str(e))
                elif choice == "8":
                    current_user = None
                    print(Fore.YELLOW + "Logged out.")
                else:
                    print(Fore.RED + "Invalid choice")


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="time-store", description="Time Store CLI")
    p.add_argument("--db", default="timekeeper.db", help="SQLite database file path")

    sub = p.add_subparsers(dest="cmd", required=False)

    p_list = sub.add_parser("list", help="List items with qty, restores, and effective prices")
    p_list.add_argument("--db", help="SQLite database file path")

    p_buy = sub.add_parser("buy", help="Buy an item and apply restores")
    p_buy.add_argument("--username", required=True)
    grp = p_buy.add_mutually_exclusive_group(required=True)
    grp.add_argument("--item", help="Item key")
    grp.add_argument("--item-id", type=int, dest="item_id", help="Numeric item ID")
    p_buy.add_argument("--qty", type=int, default=1)
    p_buy.add_argument("--store", action="store_true", help="Do not apply now; store in inventory")
    p_buy.add_argument("--db", help="SQLite database file path")

    p_prices = sub.add_parser("prices", help="Show base/current/effective prices")
    p_prices.add_argument("--db", help="SQLite database file path")

    p_ref = sub.add_parser("refresh-prices", help="Refresh current prices using volatility (random)")
    p_ref.add_argument("--volatility", type=float, default=0.2)
    p_ref.add_argument("--db", help="SQLite database file path")

    p_idx = sub.add_parser("set-index", help="Set market index percent (-50 to 300)")
    p_idx.add_argument("--admin", required=True, help="Admin username")
    p_idx.add_argument("--percent", type=int, required=True)
    p_idx.add_argument("--db", help="SQLite database file path")

    p_up = sub.add_parser("upsert-item", help="Create/update a store item (admin)")
    p_up.add_argument("--admin", required=True, help="Admin username")
    p_up.add_argument("--item", required=True)
    p_up.add_argument("--name", help="Display name")
    p_up.add_argument("--kind", choices=["food", "water"], required=True)
    p_up.add_argument("--qty", type=int, required=True)
    p_up.add_argument("--restore-energy", type=int, default=0)
    p_up.add_argument("--restore-hunger", type=int, default=0)
    p_up.add_argument("--restore-water", type=int, default=0)
    p_up.add_argument("--base-price-seconds", type=int, required=True)
    p_up.add_argument("--db", help="SQLite database file path")

    p_setq = sub.add_parser("set-qty", help="Adjust stock qty (admin)")
    p_setq.add_argument("--admin", required=True, help="Admin username")
    p_setq.add_argument("--item", required=True)
    p_setq.add_argument("--qty", type=int, required=True)
    p_setq.add_argument("--db", help="SQLite database file path")

    p_inter = sub.add_parser("interactive", help="Run interactive menu")
    p_inter.add_argument("--db", help="SQLite database file path")

    p_inv_send = sub.add_parser("inventory-send", help="Send inventory item to another user")
    p_inv_send.add_argument("--from-username", required=True, dest="from_username")
    p_inv_send.add_argument("--to-username", required=True, dest="to_username")
    grp2 = p_inv_send.add_mutually_exclusive_group(required=True)
    grp2.add_argument("--item", help="Item key")
    grp2.add_argument("--item-id", type=int, dest="item_id", help="Numeric item ID")
    p_inv_send.add_argument("--qty", type=int, default=1)
    p_inv_send.add_argument("--db", help="SQLite database file path")

    p_inv_sell = sub.add_parser("inventory-sell", help="Sell inventory item for balance")
    p_inv_sell.add_argument("--username", required=True)
    grp3 = p_inv_sell.add_mutually_exclusive_group(required=True)
    grp3.add_argument("--item", help="Item key")
    grp3.add_argument("--item-id", type=int, dest="item_id", help="Numeric item ID")
    p_inv_sell.add_argument("--qty", type=int, default=1)
    p_inv_sell.add_argument("--db", help="SQLite database file path")

    return p.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    colorama_init(autoreset=True)
    ns = parse_args(argv)
    db_path = Path(ns.db)
    if ns.cmd is None or ns.cmd == "interactive":
        interactive_menu(db_path)
    elif ns.cmd == "list":
        cmd_list(db_path)
    elif ns.cmd == "buy":
        apply_now = not bool(getattr(ns, "store", False))
        if getattr(ns, "item_id", None) is not None:
            _require_user_login(db_path, ns.username)
            res = tkdb.purchase_store_item_by_id(db_path, ns.username, ns.item_id, ns.qty, apply_now=apply_now)
            if not res.get("success"):
                raise SystemExit(res.get("message", "Purchase failed"))
            if res.get("stored"):
                print(Fore.GREEN + f"Purchased and stored in inventory. Unit: {formatting.format_duration(int(res['unit_price_seconds']), style='short')}  Total: {formatting.format_duration(int(res['total_cost_seconds']), style='short')}.")
                print(f"Balance: {formatting.format_duration(int(res['balance']), style='short')}")
            else:
                print(Fore.GREEN + f"Purchase completed. Unit price: {formatting.format_duration(int(res['unit_price_seconds']), style='short')}. Total: {formatting.format_duration(int(res['total_cost_seconds']), style='short')}.")
                print(
                    f"Balance: {formatting.format_duration(int(res['balance']), style='short')}  |  Stats -> "
                    f"Energy {res['energy']}%  Hunger {res['hunger']}%  Water {res['water']}%  |  Stock left: {res['qty_remaining']}"
                )
        else:
            cmd_buy(db_path, ns.username, ns.item, ns.qty, apply_now=apply_now)
    elif ns.cmd == "inventory-list":
        cmd_inventory_list(db_path, ns.username)
    elif ns.cmd == "inventory-use":
        cmd_inventory_use(db_path, ns.username, ns.item, ns.qty)
    elif ns.cmd == "prices":
        cmd_prices(db_path)
    elif ns.cmd == "inventory-send":
        item_key = ns.item
        if getattr(ns, "item_id", None) is not None:
            # Resolve id to key
            for it in tkdb.list_store_items(db_path):
                if it.get("id") == int(ns.item_id):
                    item_key = it["item"]
                    break
        if not item_key:
            raise SystemExit("Item not found")
        cmd_inventory_send(db_path, ns.from_username, ns.to_username, item_key, ns.qty)
    elif ns.cmd == "inventory-sell":
        item_key = ns.item
        if getattr(ns, "item_id", None) is not None:
            for it in tkdb.list_store_items(db_path):
                if it.get("id") == int(ns.item_id):
                    item_key = it["item"]
                    break
        if not item_key:
            raise SystemExit("Item not found")
        cmd_inventory_sell(db_path, ns.username, item_key, ns.qty)
    elif ns.cmd == "refresh-prices":
        cmd_refresh_prices(db_path, ns.volatility)
    elif ns.cmd == "set-index":
        cmd_set_index(db_path, ns.admin, ns.percent)
    elif ns.cmd == "upsert-item":
        cmd_upsert_item(db_path, ns.admin, ns.item, ns.kind, ns.qty, ns.restore_energy, ns.restore_hunger, ns.restore_water, ns.base_price_seconds, ns.name)
    elif ns.cmd == "set-qty":
        cmd_set_qty(db_path, ns.admin, ns.item, ns.qty)
    else:
        raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()
