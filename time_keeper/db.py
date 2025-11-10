import sqlite3
import time
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
        cur = conn.execute(
            "SELECT username, balance_seconds, active, is_admin, created_at, deactivated_at FROM users ORDER BY username ASC"
        )
        return [dict(r) for r in cur.fetchall()]


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
