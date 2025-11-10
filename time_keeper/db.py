import sqlite3
import time
import random
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

DEFAULT_INITIAL_SECONDS = 86400  # 1 day

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    passcode_hash TEXT NOT NULL,
    balance_seconds INTEGER NOT NULL DEFAULT 0,
    is_admin INTEGER NOT NULL DEFAULT 0,
    active INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    deactivated_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);
CREATE INDEX IF NOT EXISTS idx_users_balance ON users(balance_seconds);

CREATE TABLE IF NOT EXISTS time_reserves (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    total_seconds INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO time_reserves (id, total_seconds) VALUES (1, 0);
"""


def _ensure_parent(path: Path) -> None:
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def connect(db_path: Path):
    _ensure_parent(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        conn.close()


def init_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()

def _ensure_reserves(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS time_reserves (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            total_seconds INTEGER NOT NULL DEFAULT 0
        );
        INSERT OR IGNORE INTO time_reserves (id, total_seconds) VALUES (1, 0);
        """
    )

def _ensure_store_catalog(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS time_store_catalog (
            item TEXT PRIMARY KEY,
            kind TEXT NOT NULL CHECK(kind IN ('food','water')),
            qty INTEGER NOT NULL DEFAULT 0,
            restore_energy INTEGER NOT NULL DEFAULT 0,
            restore_hunger INTEGER NOT NULL DEFAULT 0,
            restore_water  INTEGER NOT NULL DEFAULT 0
        )
        """
    )

def _ensure_store_config(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS time_store_config (
            id INTEGER PRIMARY KEY CHECK(id=1),
            market_index_percent INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute("INSERT OR IGNORE INTO time_store_config(id, market_index_percent) VALUES (1, 0)")


def _ensure_stats(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    to_add = []
    if "energy" not in cols:
        to_add.append("ALTER TABLE users ADD COLUMN energy INTEGER NOT NULL DEFAULT 100")
    if "hunger" not in cols:
        to_add.append("ALTER TABLE users ADD COLUMN hunger INTEGER NOT NULL DEFAULT 100")
    if "water" not in cols:
        to_add.append("ALTER TABLE users ADD COLUMN water INTEGER NOT NULL DEFAULT 100")
    for sql in to_add:
        conn.execute(sql)

def _ensure_store_prices(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS time_store_prices (
            item TEXT PRIMARY KEY,
            base_price_seconds INTEGER NOT NULL,
            current_price_seconds INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )


def create_account(db_path: Path, username: str, passcode_hash: str, initial_seconds: int = DEFAULT_INITIAL_SECONDS, is_admin: bool = False) -> int:
    now = int(time.time())
    with connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO users (username, passcode_hash, balance_seconds, is_admin, active, created_at)
            VALUES (?, ?, ?, ?, 1, ?)
            """,
            (username, passcode_hash, max(0, int(initial_seconds)), 1 if is_admin else 0, now),
        )
        conn.commit()
        return int(cur.lastrowid)


def find_user(db_path: Path, username: str) -> Optional[sqlite3.Row]:
    with connect(db_path) as conn:
        _ensure_stats(conn)
        cur = conn.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        return row


def set_deactivated_if_zero(conn: sqlite3.Connection) -> None:
    now = int(time.time())
    conn.execute(
        "UPDATE users SET active = 0, deactivated_at = COALESCE(deactivated_at, ?) WHERE active = 1 AND balance_seconds <= 0",
        (now,),
    )


def deduct_one_second_all_active(db_path: Path) -> Tuple[int, int]:
    """Deduct one second from all active users with balance > 0.
    Returns (updated_rows, deactivated_rows).
    """
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_reserves(conn)
        cur = conn.execute(
            "UPDATE users SET balance_seconds = balance_seconds - 1 WHERE active = 1 AND balance_seconds > 0"
        )
        updated = cur.rowcount if cur.rowcount is not None else 0
        if updated > 0:
            # accumulate into time_reserves atomically
            conn.execute(
                "INSERT INTO time_reserves(id, total_seconds) VALUES (1, ?)\n"
                "ON CONFLICT(id) DO UPDATE SET total_seconds = total_seconds + excluded.total_seconds",
                (int(updated),),
            )
        set_deactivated_if_zero(conn)
        cur2 = conn.execute("SELECT changes()")
        deactivated = cur2.fetchone()[0]
        conn.commit()
        return updated, deactivated


def get_balance_seconds(db_path: Path, username: str) -> Optional[int]:
    with connect(db_path) as conn:
        cur = conn.execute("SELECT balance_seconds FROM users WHERE username = ?", (username,))
        row = cur.fetchone()
        return int(row[0]) if row else None


def list_all_accounts(db_path: Path) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        _ensure_stats(conn)
        cur = conn.execute(
            "SELECT username, balance_seconds, active, is_admin, created_at, deactivated_at FROM users ORDER BY username ASC"
        )
        return [dict(r) for r in cur.fetchall()]


def transfer_from_reserves(db_path: Path, to_username: str, amount_seconds: int) -> Dict[str, Any]:
    """Atomically transfer seconds from Time Reserves to a user's balance.
    Returns: {success, message, to_balance, reserves_remaining}
    """
    amount = int(max(0, amount_seconds))
    result: Dict[str, Any] = {"success": False, "message": "", "to_balance": None, "reserves_remaining": None}
    if amount <= 0:
        result["message"] = "Amount must be greater than zero"
        return result
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_reserves(conn)
            # Fetch reserves and recipient
            reserves_row = conn.execute("SELECT total_seconds FROM time_reserves WHERE id = 1").fetchone()
            reserves = int(reserves_row[0]) if reserves_row else 0
            if reserves < amount:
                result["message"] = "Insufficient Time Reserves"
                conn.rollback()
                return result
            u = conn.execute("SELECT id, active FROM users WHERE username = ?", (to_username,)).fetchone()
            if not u:
                result["message"] = "Recipient user not found"
                conn.rollback()
                return result
            if not int(u["active"]):
                result["message"] = "Recipient account is deactivated"
                conn.rollback()
                return result
            # Apply updates
            conn.execute("UPDATE users SET balance_seconds = balance_seconds + ? WHERE id = ?", (amount, int(u["id"])))
            conn.execute("UPDATE time_reserves SET total_seconds = total_seconds - ? WHERE id = 1", (amount,))
            # Read post state
            bal_row = conn.execute("SELECT balance_seconds FROM users WHERE id = ?", (int(u["id"]),)).fetchone()
            rem_row = conn.execute("SELECT total_seconds FROM time_reserves WHERE id = 1").fetchone()
            conn.commit()
            result["success"] = True
            result["message"] = "Transfer from reserves completed"
            result["to_balance"] = int(bal_row[0]) if bal_row else None
            result["reserves_remaining"] = int(rem_row[0]) if rem_row else None
            return result
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            result["message"] = f"Transfer failed: {e}"
            return result


def distribute_reserves_equal(db_path: Path, amount_seconds: Optional[int] = None) -> Dict[str, Any]:
    """Distribute Time Reserves equally to all active users.
    If amount_seconds is None, use the full available reserves.
    Remainder seconds remain in reserves.
    Returns: {success, message, recipients, per_user, total_distributed, reserves_remaining}
    """
    result: Dict[str, Any] = {
        "success": False,
        "message": "",
        "recipients": 0,
        "per_user": 0,
        "total_distributed": 0,
        "reserves_remaining": None,
    }
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_reserves(conn)
            # Get reserves and active user count
            reserves = int(conn.execute("SELECT total_seconds FROM time_reserves WHERE id = 1").fetchone()[0])
            cnt_row = conn.execute("SELECT COUNT(*) FROM users WHERE active = 1").fetchone()
            active_count = int(cnt_row[0]) if cnt_row else 0
            if active_count <= 0:
                result["message"] = "No active users to distribute to"
                conn.rollback()
                return result
            max_available = reserves
            if amount_seconds is None:
                to_use = max_available
            else:
                to_use = int(max(0, amount_seconds))
                to_use = min(to_use, max_available)
            if to_use <= 0:
                result["message"] = "Nothing to distribute"
                conn.rollback()
                return result
            per = to_use // active_count
            if per <= 0:
                result["message"] = "Requested amount too small for equal distribution"
                conn.rollback()
                return result
            total_dist = per * active_count
            # Credit all active users equally
            cur = conn.execute("UPDATE users SET balance_seconds = balance_seconds + ? WHERE active = 1", (per,))
            credited = cur.rowcount if cur.rowcount is not None else active_count
            # Deduct from reserves
            conn.execute("UPDATE time_reserves SET total_seconds = total_seconds - ? WHERE id = 1", (total_dist,))
            rem_row = conn.execute("SELECT total_seconds FROM time_reserves WHERE id = 1").fetchone()
            conn.commit()
            result["success"] = True
            result["message"] = "Distribution completed"
            result["recipients"] = credited
            result["per_user"] = per
            result["total_distributed"] = total_dist
            result["reserves_remaining"] = int(rem_row[0]) if rem_row else None
            return result
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            result["message"] = f"Distribution failed: {e}"
            return result


def get_time_reserves(db_path: Path) -> int:
    with connect(db_path) as conn:
        _ensure_reserves(conn)
        cur = conn.execute("SELECT total_seconds FROM time_reserves WHERE id = 1")
        row = cur.fetchone()
        return int(row[0]) if row else 0


def top_accounts(db_path: Path, limit: int = 10) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        cur = conn.execute(
            "SELECT username, balance_seconds, active FROM users ORDER BY balance_seconds DESC, username ASC LIMIT ?",
            (int(limit),),
        )
        return [dict(r) for r in cur.fetchall()]


def get_user_stats(db_path: Path, username: str) -> Optional[Dict[str, int]]:
    with connect(db_path) as conn:
        _ensure_stats(conn)
        r = conn.execute("SELECT energy, hunger, water FROM users WHERE username = ?", (username,)).fetchone()
        if not r:
            return None
        return {"energy": int(r[0]), "hunger": int(r[1]), "water": int(r[2])}


def set_user_stats_full(db_path: Path, username: str) -> bool:
    with connect(db_path) as conn:
        _ensure_stats(conn)
        cur = conn.execute("UPDATE users SET energy = 100, hunger = 100, water = 100 WHERE username = ?", (username,))
        conn.commit()
        return (cur.rowcount or 0) > 0


def set_all_users_stats_full(db_path: Path) -> int:
    with connect(db_path) as conn:
        _ensure_stats(conn)
        cur = conn.execute("UPDATE users SET energy = 100, hunger = 100, water = 100")
        conn.commit()
        return int(cur.rowcount or 0)


def apply_stat_changes_and_charge(db_path: Path, username: str, delta_energy: int, delta_hunger: int, delta_water: int, cost_seconds: int) -> Dict[str, Any]:
    """Atomically deduct cost_seconds from user's balance and apply stat deltas capped to [0,100].
    Returns: {success, message, balance, energy, hunger, water}
    """
    result: Dict[str, Any] = {"success": False, "message": ""}
    cost = int(max(0, cost_seconds))
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_stats(conn)
            u = conn.execute("SELECT id, active, balance_seconds, energy, hunger, water FROM users WHERE username = ?", (username,)).fetchone()
            if not u:
                result["message"] = "User not found"
                conn.rollback(); return result
            if not int(u["active"]):
                result["message"] = "Account is deactivated"
                conn.rollback(); return result
            bal = int(u["balance_seconds"])
            if bal < cost:
                result["message"] = "Insufficient balance"
                conn.rollback(); return result
            # Deduct cost
            conn.execute("UPDATE users SET balance_seconds = balance_seconds - ? WHERE id = ?", (cost, int(u["id"])) )
            # Apply capped stats
            def cap(v: int) -> int:
                return 0 if v < 0 else (100 if v > 100 else v)
            new_energy = cap(int(u["energy"]) + int(delta_energy))
            new_hunger = cap(int(u["hunger"]) + int(delta_hunger))
            new_water  = cap(int(u["water"])  + int(delta_water))
            conn.execute(
                "UPDATE users SET energy = ?, hunger = ?, water = ? WHERE id = ?",
                (new_energy, new_hunger, new_water, int(u["id"]))
            )
            row = conn.execute("SELECT balance_seconds, energy, hunger, water FROM users WHERE id = ?", (int(u["id"]),)).fetchone()
            conn.commit()
            result.update({
                "success": True,
                "message": "Purchase applied",
                "balance": int(row[0]),
                "energy": int(row[1]),
                "hunger": int(row[2]),
                "water": int(row[3]),
            })
            return result
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            result["message"] = f"Purchase failed: {e}"
            return result


def seed_or_update_store_prices(db_path: Path, catalog: Dict[str, Dict[str, int]]) -> None:
    """Ensure store prices table has entries for provided catalog items with base prices.
    catalog: {item_key: {"base_price_seconds": int}}
    """
    now = int(time.time())
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_store_prices(conn)
        for key, cfg in catalog.items():
            base = int(cfg.get("base_price_seconds", 60))
            # If exists, update base (but keep current if present); else insert with current=base
            row = conn.execute("SELECT item FROM time_store_prices WHERE item = ?", (key,)).fetchone()
            if row:
                conn.execute("UPDATE time_store_prices SET base_price_seconds = ?, updated_at = ? WHERE item = ?", (base, now, key))
            else:
                conn.execute(
                    "INSERT INTO time_store_prices(item, base_price_seconds, current_price_seconds, updated_at) VALUES (?, ?, ?, ?)",
                    (key, base, base, now)
                )
        conn.commit()


def refresh_store_prices(db_path: Path, volatility: float = 0.2) -> None:
    """Adjust current_price_seconds around base within +/- volatility randomly."""
    vol = max(0.0, float(volatility))
    now = int(time.time())
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_store_prices(conn)
        rows = conn.execute("SELECT item, base_price_seconds FROM time_store_prices").fetchall()
        for r in rows:
            base = int(r[1])
            factor = 1.0 + random.uniform(-vol, vol)
            new_price = max(1, int(round(base * factor)))
            conn.execute(
                "UPDATE time_store_prices SET current_price_seconds = ?, updated_at = ? WHERE item = ?",
                (new_price, now, r[0])
            )
        conn.commit()


def get_store_prices(db_path: Path) -> List[Dict[str, int]]:
    with connect(db_path) as conn:
        _ensure_store_prices(conn)
        cur = conn.execute("SELECT item, base_price_seconds, current_price_seconds, updated_at FROM time_store_prices ORDER BY item ASC")
        return [
            {
                "item": str(r[0]),
                "base_price_seconds": int(r[1]),
                "current_price_seconds": int(r[2]),
                "updated_at": int(r[3]),
            }
            for r in cur.fetchall()
        ]


def set_market_index_percent(db_path: Path, percent: int) -> None:
    p = int(percent)
    if p < -50: p = -50
    if p > 300: p = 300
    with connect(db_path) as conn:
        _ensure_store_config(conn)
        conn.execute("UPDATE time_store_config SET market_index_percent = ? WHERE id = 1", (p,))
        conn.commit()


def get_market_index_percent(db_path: Path) -> int:
    with connect(db_path) as conn:
        _ensure_store_config(conn)
        row = conn.execute("SELECT market_index_percent FROM time_store_config WHERE id = 1").fetchone()
        return int(row[0]) if row else 0


def upsert_store_item(db_path: Path, item: str, kind: str, qty: int, restore_energy: int, restore_hunger: int, restore_water: int, base_price_seconds: int) -> None:
    now = int(time.time())
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_store_catalog(conn)
        _ensure_store_prices(conn)
        conn.execute(
            "INSERT INTO time_store_catalog(item, kind, qty, restore_energy, restore_hunger, restore_water) VALUES(?,?,?,?,?,?)\n"
            "ON CONFLICT(item) DO UPDATE SET kind=excluded.kind, qty=excluded.qty, restore_energy=excluded.restore_energy, restore_hunger=excluded.restore_hunger, restore_water=excluded.restore_water",
            (item, kind, int(qty), int(restore_energy), int(restore_hunger), int(restore_water))
        )
        # seed or update price
        row = conn.execute("SELECT item FROM time_store_prices WHERE item = ?", (item,)).fetchone()
        base = int(base_price_seconds)
        if row:
            conn.execute("UPDATE time_store_prices SET base_price_seconds = ?, updated_at = ? WHERE item = ?", (base, now, item))
        else:
            conn.execute("INSERT INTO time_store_prices(item, base_price_seconds, current_price_seconds, updated_at) VALUES (?,?,?,?)", (item, base, base, now))
        conn.commit()


def set_store_item_qty(db_path: Path, item: str, qty: int) -> bool:
    with connect(db_path) as conn:
        _ensure_store_catalog(conn)
        cur = conn.execute("UPDATE time_store_catalog SET qty = ? WHERE item = ?", (int(qty), item))
        conn.commit()
        return (cur.rowcount or 0) > 0


def list_store_items(db_path: Path) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        _ensure_store_catalog(conn)
        _ensure_store_prices(conn)
        _ensure_store_config(conn)
        p = get_market_index_percent(db_path)
        rows = conn.execute(
            "SELECT c.item, c.kind, c.qty, c.restore_energy, c.restore_hunger, c.restore_water, p.base_price_seconds, p.current_price_seconds\n"
            "FROM time_store_catalog c JOIN time_store_prices p ON c.item = p.item ORDER BY c.item ASC"
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            base = int(r[6]); curr = int(r[7]); idx = float(p)/100.0
            effective = max(1, int(round(curr * (1.0 + idx))))
            out.append({
                "item": str(r[0]),
                "kind": str(r[1]),
                "qty": int(r[2]),
                "restore_energy": int(r[3]),
                "restore_hunger": int(r[4]),
                "restore_water": int(r[5]),
                "base_price_seconds": base,
                "current_price_seconds": curr,
                "effective_price_seconds": effective,
                "market_index_percent": int(p),
            })
        return out


def purchase_store_item(db_path: Path, username: str, item: str, quantity: int) -> Dict[str, Any]:
    """Atomically purchase quantity of item for username, applying stat restore and charging effective price with market index.
    Returns: {success, message, balance, energy, hunger, water, qty_remaining}
    """
    q = int(max(1, quantity))
    result: Dict[str, Any] = {"success": False, "message": ""}
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_stats(conn)
            _ensure_store_catalog(conn)
            _ensure_store_prices(conn)
            _ensure_store_config(conn)
            # Load user
            u = conn.execute("SELECT id, active, balance_seconds, energy, hunger, water FROM users WHERE username = ?", (username,)).fetchone()
            if not u:
                result["message"] = "User not found"; conn.rollback(); return result
            if not int(u["active"]):
                result["message"] = "Account is deactivated"; conn.rollback(); return result
            # Load item
            r = conn.execute(
                "SELECT c.qty, c.restore_energy, c.restore_hunger, c.restore_water, p.current_price_seconds FROM time_store_catalog c JOIN time_store_prices p ON c.item = p.item WHERE c.item = ?",
                (item,)
            ).fetchone()
            if not r:
                result["message"] = "Item not found"; conn.rollback(); return result
            qty_avail = int(r[0])
            if qty_avail < q:
                result["message"] = "Insufficient stock"; conn.rollback(); return result
            curr_price = int(r[4])
            idx_percent = int(conn.execute("SELECT market_index_percent FROM time_store_config WHERE id = 1").fetchone()[0])
            effective = max(1, int(round(curr_price * (1.0 + float(idx_percent)/100.0))))
            total_cost = effective * q
            bal = int(u["balance_seconds"])
            if bal < total_cost:
                result["message"] = "Insufficient balance"; conn.rollback(); return result
            # Deduct balance
            conn.execute("UPDATE users SET balance_seconds = balance_seconds - ? WHERE id = ?", (total_cost, int(u["id"])) )
            # Apply stats
            def cap(v: int) -> int: return 0 if v < 0 else (100 if v > 100 else v)
            new_energy = cap(int(u["energy"]) + int(r[1]) * q)
            new_hunger = cap(int(u["hunger"]) + int(r[2]) * q)
            new_water  = cap(int(u["water"])  + int(r[3]) * q)
            conn.execute("UPDATE users SET energy = ?, hunger = ?, water = ? WHERE id = ?", (new_energy, new_hunger, new_water, int(u["id"])) )
            # Decrement stock
            conn.execute("UPDATE time_store_catalog SET qty = qty - ? WHERE item = ?", (q, item))
            # Read post state
            post = conn.execute("SELECT balance_seconds, energy, hunger, water FROM users WHERE id = ?", (int(u["id"]),)).fetchone()
            rem = conn.execute("SELECT qty FROM time_store_catalog WHERE item = ?", (item,)).fetchone()
            conn.commit()
            return {
                "success": True,
                "message": "Purchase completed",
                "balance": int(post[0]),
                "energy": int(post[1]),
                "hunger": int(post[2]),
                "water": int(post[3]),
                "qty_remaining": int(rem[0]) if rem else 0,
                "unit_price_seconds": int(effective),
                "total_cost_seconds": int(total_cost),
            }
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Purchase failed: {e}"}


def transfer_seconds(db_path: Path, from_username: str, to_username: str, amount_seconds: int) -> Dict[str, Any]:
    """Atomically transfer amount_seconds from one user to another.
    Returns a dict with keys: success (bool), message (str), from_balance (int|None), to_balance (int|None).
    """
    amount = int(max(0, amount_seconds))
    result: Dict[str, Any] = {"success": False, "message": "", "from_balance": None, "to_balance": None}
    if amount <= 0:
        result["message"] = "Amount must be greater than zero"
        return result
    if from_username == to_username:
        result["message"] = "Cannot transfer to the same account"
        return result

    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            f = conn.execute("SELECT id, balance_seconds, active FROM users WHERE username = ?", (from_username,)).fetchone()
            t = conn.execute("SELECT id, balance_seconds, active FROM users WHERE username = ?", (to_username,)).fetchone()
            if not f or not t:
                result["message"] = "User not found"
                conn.rollback()
                return result
            if not f["active"]:
                result["message"] = "Sender account is deactivated"
                conn.rollback()
                return result
            if not t["active"]:
                result["message"] = "Recipient account is deactivated"
                conn.rollback()
                return result
            if int(f["balance_seconds"]) < amount:
                result["message"] = "Insufficient balance"
                conn.rollback()
                return result

            conn.execute(
                "UPDATE users SET balance_seconds = balance_seconds - ? WHERE id = ?",
                (amount, int(f["id"]))
            )
            conn.execute(
                "UPDATE users SET balance_seconds = balance_seconds + ? WHERE id = ?",
                (amount, int(t["id"]))
            )
            # read updated balances
            f2 = conn.execute("SELECT balance_seconds FROM users WHERE id = ?", (int(f["id"]),)).fetchone()
            t2 = conn.execute("SELECT balance_seconds FROM users WHERE id = ?", (int(t["id"]),)).fetchone()
            conn.commit()
            result["success"] = True
            result["message"] = "Transfer completed"
            result["from_balance"] = int(f2[0]) if f2 else None
            result["to_balance"] = int(t2[0]) if t2 else None
            return result
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            result["message"] = f"Transfer failed: {e}"
            return result


def get_statistics(db_path: Path) -> Dict[str, int]:
    """Return aggregate statistics for admin dashboards.
    Keys: total_users, total_active, total_deactivated, total_balance_seconds
    """
    with connect(db_path) as conn:
        total_users = int(conn.execute("SELECT COUNT(*) FROM users").fetchone()[0])
        total_active = int(conn.execute("SELECT COUNT(*) FROM users WHERE active = 1").fetchone()[0])
        total_deactivated = int(conn.execute("SELECT COUNT(*) FROM users WHERE active = 0").fetchone()[0])
        row = conn.execute("SELECT COALESCE(SUM(balance_seconds), 0) FROM users").fetchone()
        total_balance_seconds = int(row[0]) if row and row[0] is not None else 0
        return {
            "total_users": total_users,
            "total_active": total_active,
            "total_deactivated": total_deactivated,
            "total_balance_seconds": total_balance_seconds,
        }
