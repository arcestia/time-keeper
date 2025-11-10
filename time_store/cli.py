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


def cmd_list(db_path: Path) -> None:
    items = tkdb.list_store_items(db_path)
    if not items:
        print(Fore.YELLOW + "No items in the store yet. Ask admin to add some.")
        return
    headers = ["Item", "Kind", "Qty", "Restores", "Price (eff)"]
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
        rows.append([
            it["item"],
            it["kind"],
            str(it["qty"]),
            restores_s,
            formatting.format_duration(it["effective_price_seconds"], style="short") + f"  (idx {idx}%)",
        ])
    _print_table(headers, rows)


def cmd_buy(db_path: Path, username: str, item: str, qty: int) -> None:
    _require_user_login(db_path, username)
    res = tkdb.purchase_store_item(db_path, username, item, qty)
    if not res.get("success"):
        raise SystemExit(res.get("message", "Purchase failed"))
    print(Fore.GREEN + f"Purchase completed. Unit price: {formatting.format_duration(int(res['unit_price_seconds']), style='short')}. Total: {formatting.format_duration(int(res['total_cost_seconds']), style='short')}.")
    print(
        f"Balance: {formatting.format_duration(int(res['balance']), style='short')}  |  Stats -> "
        f"Energy {res['energy']}%  Hunger {res['hunger']}%  Water {res['water']}%  |  Stock left: {res['qty_remaining']}"
    )


def cmd_prices(db_path: Path) -> None:
    idx = tkdb.get_market_index_percent(db_path)
    prices = tkdb.get_store_prices(db_path)
    if not prices:
        print(Fore.YELLOW + "No price data yet. Seed items first.")
        return
    headers = ["Item", "Base", "Current", "Effective", "Index"]
    rows = []
    for p in prices:
        effective = max(1, int(round(p["current_price_seconds"] * (1.0 + float(idx)/100.0))))
        rows.append([
            p["item"],
            formatting.format_duration(p["base_price_seconds"], style="short"),
            formatting.format_duration(p["current_price_seconds"], style="short"),
            formatting.format_duration(effective, style="short"),
            f"{idx}%",
        ])
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
                    base_price_seconds: int) -> None:
    _require_admin_login(db_path, admin_username)
    tkdb.upsert_store_item(db_path, item, kind, qty, restore_energy, restore_hunger, restore_water, base_price_seconds)
    print(Fore.GREEN + f"Item '{item}' upserted.")


def cmd_set_qty(db_path: Path, admin_username: str, item: str, qty: int) -> None:
    _require_admin_login(db_path, admin_username)
    ok = tkdb.set_store_item_qty(db_path, item, qty)
    if ok:
        print(Fore.GREEN + f"Set qty of '{item}' to {qty}.")
    else:
        raise SystemExit("Item not found")


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


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="time-store", description="Time Store CLI")
    p.add_argument("--db", default="timekeeper.db", help="SQLite database file path")

    sub = p.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="List items with qty, restores, and effective prices")
    p_list.add_argument("--db", help="SQLite database file path")

    p_buy = sub.add_parser("buy", help="Buy an item and apply restores")
    p_buy.add_argument("--username", required=True)
    p_buy.add_argument("--item", required=True)
    p_buy.add_argument("--qty", type=int, default=1)
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

    return p.parse_args(argv)


def main(argv: Optional[list] = None) -> None:
    colorama_init(autoreset=True)
    ns = parse_args(argv)
    db_path = Path(ns.db)
    if ns.cmd == "list":
        cmd_list(db_path)
    elif ns.cmd == "buy":
        cmd_buy(db_path, ns.username, ns.item, ns.qty)
    elif ns.cmd == "prices":
        cmd_prices(db_path)
    elif ns.cmd == "refresh-prices":
        cmd_refresh_prices(db_path, ns.volatility)
    elif ns.cmd == "set-index":
        cmd_set_index(db_path, ns.admin, ns.percent)
    elif ns.cmd == "upsert-item":
        cmd_upsert_item(db_path, ns.admin, ns.item, ns.kind, ns.qty, ns.restore_energy, ns.restore_hunger, ns.restore_water, ns.base_price_seconds)
    elif ns.cmd == "set-qty":
        cmd_set_qty(db_path, ns.admin, ns.item, ns.qty)
    else:
        raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()
