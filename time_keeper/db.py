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


# New separated configs
def _ensure_earner_promo_config(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS time_earner_promo_config (
            id INTEGER PRIMARY KEY CHECK(id=1),
            base_percent REAL,
            per_block_percent REAL,
            min_seconds INTEGER,
            block_seconds INTEGER,
            promo_enabled INTEGER
        )
        """
    )
    conn.execute("INSERT OR IGNORE INTO time_earner_promo_config(id) VALUES (1)")
    conn.execute("UPDATE time_earner_promo_config SET base_percent = COALESCE(base_percent, 0.10) WHERE id = 1")
    conn.execute("UPDATE time_earner_promo_config SET per_block_percent = COALESCE(per_block_percent, 0.0125) WHERE id = 1")
    conn.execute("UPDATE time_earner_promo_config SET min_seconds = COALESCE(min_seconds, 600) WHERE id = 1")
    conn.execute("UPDATE time_earner_promo_config SET block_seconds = COALESCE(block_seconds, 600) WHERE id = 1")
    conn.execute("UPDATE time_earner_promo_config SET promo_enabled = COALESCE(promo_enabled, 1) WHERE id = 1")


# ---- Time Earner staking tiers ----
def _ensure_earner_stake_tiers(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS time_earner_stake_tiers (
            min_seconds INTEGER PRIMARY KEY,
            multiplier REAL NOT NULL
        )
        """
    )


def seed_stake_tiers_balanced_defaults(conn: sqlite3.Connection) -> None:
    # Balanced progression up to 10x
    tiers = [
        (2*3600, 1.5),
        (6*3600, 2.0),
        (12*3600, 2.75),
        (24*3600, 4.0),
        (36*3600, 5.0),
        (48*3600, 6.0),
        (72*3600, 7.5),
        (96*3600, 8.5),
        (120*3600, 9.25),
        (144*3600, 10.0),
    ]
    conn.execute("DELETE FROM time_earner_stake_tiers")
    conn.executemany(
        "INSERT INTO time_earner_stake_tiers(min_seconds, multiplier) VALUES (?, ?)", tiers
    )


def list_earner_stake_tiers(db_path: Path) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        _ensure_earner_stake_tiers(conn)
        rows = conn.execute(
            "SELECT min_seconds, multiplier FROM time_earner_stake_tiers ORDER BY min_seconds ASC"
        ).fetchall()
        return [
            {"min_seconds": int(r[0]), "multiplier": float(r[1])}
            for r in rows
        ]

def get_user_premium_progress(db_path: Path, username: str) -> Dict[str, Any]:
    """Return user's premium progression info.
    Output: {lifetime_seconds, current_tier, next_tier, current_min_seconds, next_min_seconds, to_next_seconds, percent_to_next}
    If at max tier or lifetime, next_* will be None and percent_to_next=100.0
    """
    with connect(db_path) as conn:
        _ensure_premium(conn)
        _ensure_premium_tiers(conn)
        u = conn.execute("SELECT premium_lifetime_seconds, premium_is_lifetime FROM users WHERE username = ?", (username,)).fetchone()
        if not u:
            return {"success": False, "message": "User not found"}
        life = int(u[0] or 0)
        is_life = bool(int(u[1] or 0))
        rows = conn.execute("SELECT tier, min_seconds FROM premium_tiers ORDER BY tier ASC").fetchall()
        if not rows:
            return {"success": True, "lifetime_seconds": life, "current_tier": 0, "next_tier": None, "current_min_seconds": 0, "next_min_seconds": None, "to_next_seconds": None, "percent_to_next": 0.0, "is_lifetime": is_life}
        current_tier = 0
        current_min = 0
        next_tier = None
        next_min = None
        for idx, r in enumerate(rows):
            t = int(r[0]); m = int(r[1])
            if life >= m:
                current_tier = t
                current_min = m
                # continue to possibly higher tier
            else:
                next_tier = t
                next_min = m
                break
        if next_tier is None:
            # at or above highest tier
            return {"success": True, "lifetime_seconds": life, "current_tier": current_tier, "next_tier": None, "current_min_seconds": current_min, "next_min_seconds": None, "to_next_seconds": None, "percent_to_next": 100.0, "is_lifetime": is_life}
        denom = max(1, next_min - current_min)
        done = max(0, life - current_min)
        pct = max(0.0, min(100.0, (float(done) / float(denom)) * 100.0))
        to_next = max(0, next_min - life)
        return {"success": True, "lifetime_seconds": life, "current_tier": current_tier, "next_tier": next_tier, "current_min_seconds": current_min, "next_min_seconds": next_min, "to_next_seconds": to_next, "percent_to_next": pct, "is_lifetime": is_life}


def set_earner_stake_tiers_defaults(db_path: Path) -> None:
    with connect(db_path) as conn:
        _ensure_earner_stake_tiers(conn)
        seed_stake_tiers_balanced_defaults(conn)
        conn.commit()


def add_earner_stake_tier(db_path: Path, min_seconds: int, multiplier: float) -> None:
    with connect(db_path) as conn:
        _ensure_earner_stake_tiers(conn)
        conn.execute(
            "INSERT OR REPLACE INTO time_earner_stake_tiers(min_seconds, multiplier) VALUES (?, ?)",
            (int(min_seconds), float(multiplier)),
        )
        conn.commit()


def remove_earner_stake_tier(db_path: Path, min_seconds: int) -> bool:
    with connect(db_path) as conn:
        _ensure_earner_stake_tiers(conn)
        cur = conn.execute(
            "DELETE FROM time_earner_stake_tiers WHERE min_seconds = ?", (int(min_seconds),)
        )
        conn.commit()
        return (cur.rowcount or 0) > 0


def clear_earner_stake_tiers(db_path: Path) -> None:
    with connect(db_path) as conn:
        _ensure_earner_stake_tiers(conn)
        conn.execute("DELETE FROM time_earner_stake_tiers")
        conn.commit()


def get_multiplier_for_stake(db_path: Path, stake_seconds: int) -> Optional[float]:
    with connect(db_path) as conn:
        _ensure_earner_stake_tiers(conn)
        row = conn.execute(
            "SELECT multiplier FROM time_earner_stake_tiers WHERE min_seconds <= ? ORDER BY min_seconds DESC LIMIT 1",
            (int(stake_seconds),),
        ).fetchone()
        return float(row[0]) if row else None


def _ensure_earner_default_config(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS time_earner_default_config (
            id INTEGER PRIMARY KEY CHECK(id=1),
            base_percent REAL,
            per_block_percent REAL,
            min_seconds INTEGER,
            block_seconds INTEGER
        )
        """
    )
    conn.execute("INSERT OR IGNORE INTO time_earner_default_config(id) VALUES (1)")
    conn.execute("UPDATE time_earner_default_config SET base_percent = COALESCE(base_percent, 0.10) WHERE id = 1")
    conn.execute("UPDATE time_earner_default_config SET per_block_percent = COALESCE(per_block_percent, 0.0125) WHERE id = 1")
    conn.execute("UPDATE time_earner_default_config SET min_seconds = COALESCE(min_seconds, 600) WHERE id = 1")
    conn.execute("UPDATE time_earner_default_config SET block_seconds = COALESCE(block_seconds, 600) WHERE id = 1")


def get_earner_default_config(db_path: Path) -> Dict[str, float | int]:
    with connect(db_path) as conn:
        _ensure_earner_default_config(conn)
        row = conn.execute(
            "SELECT base_percent, per_block_percent, min_seconds, block_seconds FROM time_earner_default_config WHERE id = 1"
        ).fetchone()
        return {
            "base_percent": float(row[0]),
            "per_block_percent": float(row[1]),
            "min_seconds": int(row[2]),
            "block_seconds": int(row[3]),
        }


def set_earner_default_config(db_path: Path, base_percent: float, per_block_percent: float, min_seconds: int, block_seconds: int) -> None:
    b = float(base_percent); p = float(per_block_percent); mn = int(max(1, min_seconds)); bs = int(max(1, block_seconds))
    with connect(db_path) as conn:
        _ensure_earner_default_config(conn)
        conn.execute(
            "INSERT INTO time_earner_default_config(id, base_percent, per_block_percent, min_seconds, block_seconds) VALUES (1, ?, ?, ?, ?)\n"
            "ON CONFLICT(id) DO UPDATE SET base_percent=excluded.base_percent, per_block_percent=excluded.per_block_percent, min_seconds=excluded.min_seconds, block_seconds=excluded.block_seconds",
            (b, p, mn, bs),
        )
        conn.commit()


def get_earner_promo_config(db_path: Path) -> Dict[str, float | int]:
    """Override to prefer separated promo config if present; fallback to legacy table."""
    with connect(db_path) as conn:
        # Try new table
        try:
            cols = {row[1] for row in conn.execute("PRAGMA table_info(time_earner_promo_config)").fetchall()}
            if cols:
                _ensure_earner_promo_config(conn)
                row = conn.execute(
                    "SELECT base_percent, per_block_percent, min_seconds, block_seconds, promo_enabled FROM time_earner_promo_config WHERE id = 1"
                ).fetchone()
                return {
                    "base_percent": float(row[0]),
                    "per_block_percent": float(row[1]),
                    "min_seconds": int(row[2]),
                    "block_seconds": int(row[3]),
                    "promo_enabled": int(row[4]),
                    # Legacy compatible keys
                    "default_bonus_percent": 0.10,
                    "default_per_block_percent": 0.0,
                }
        except Exception:
            pass
    # Fallback to legacy combined table
    return get_earner_promo_config.__wrapped__(db_path)  # type: ignore


# ---- Time Earner stake config ----
def _ensure_earner_stake_config(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS time_earner_stake_config (
            id INTEGER PRIMARY KEY CHECK(id=1),
            min_stake_seconds INTEGER,
            reward_multiplier REAL
        )
        """
    )
    conn.execute("INSERT OR IGNORE INTO time_earner_stake_config(id) VALUES (1)")
    conn.execute("UPDATE time_earner_stake_config SET min_stake_seconds = COALESCE(min_stake_seconds, 7200) WHERE id = 1")
    conn.execute("UPDATE time_earner_stake_config SET reward_multiplier = COALESCE(reward_multiplier, 2.0) WHERE id = 1")


def get_earner_stake_config(db_path: Path) -> Dict[str, float | int]:
    with connect(db_path) as conn:
        _ensure_earner_stake_config(conn)
        row = conn.execute("SELECT min_stake_seconds, reward_multiplier FROM time_earner_stake_config WHERE id = 1").fetchone()
        return {"min_stake_seconds": int(row[0]), "reward_multiplier": float(row[1])}


def set_earner_stake_config(db_path: Path, min_stake_seconds: int, reward_multiplier: float) -> None:
    mn = int(max(1, min_stake_seconds))
    rm = float(reward_multiplier)
    with connect(db_path) as conn:
        _ensure_earner_stake_config(conn)
        conn.execute(
            "INSERT INTO time_earner_stake_config(id, min_stake_seconds, reward_multiplier) VALUES (1, ?, ?)\n"
            "ON CONFLICT(id) DO UPDATE SET min_stake_seconds = excluded.min_stake_seconds, reward_multiplier = excluded.reward_multiplier",
            (mn, rm),
        )
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
            name TEXT,
            kind TEXT NOT NULL CHECK(kind IN ('food','water')),
            qty INTEGER NOT NULL DEFAULT 0,
            restore_energy INTEGER NOT NULL DEFAULT 0,
            restore_hunger INTEGER NOT NULL DEFAULT 0,
            restore_water  INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    # Migration: ensure name column exists
    cols = {row[1] for row in conn.execute("PRAGMA table_info(time_store_catalog)").fetchall()}
    if "name" not in cols:
        conn.execute("ALTER TABLE time_store_catalog ADD COLUMN name TEXT")
    if "id" not in cols:
        # Add column without UNIQUE constraint (SQLite cannot add UNIQUE via ALTER)
        conn.execute("ALTER TABLE time_store_catalog ADD COLUMN id INTEGER")
        # Create a unique index to enforce uniqueness when ids are assigned
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_time_store_catalog_id ON time_store_catalog(id)")
        # Backfill ids for existing rows with NULL id
        rows = conn.execute("SELECT item FROM time_store_catalog WHERE id IS NULL ORDER BY item ASC").fetchall()
        if rows:
            max_row = conn.execute("SELECT COALESCE(MAX(id), 0) FROM time_store_catalog").fetchone()
            next_id = int(max_row[0]) if max_row and max_row[0] is not None else 0
            for r in rows:
                next_id += 1
                conn.execute("UPDATE time_store_catalog SET id = ? WHERE item = ?", (next_id, r[0]))
    else:
        # Ensure the unique index exists even if column already present
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_time_store_catalog_id ON time_store_catalog(id)")

def _ensure_store_config(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS time_store_config (
            id INTEGER PRIMARY KEY CHECK(id=1),
            market_index_percent INTEGER NOT NULL DEFAULT 0
        )
        """
    )


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

def _ensure_premium(conn: sqlite3.Connection) -> None:
    # Add premium_until epoch seconds to users if missing
    cols = {row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()}
    if "premium_until" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN premium_until INTEGER NOT NULL DEFAULT 0")
    # Premium tiers support: lifetime accumulation and lifetime flag
    if "premium_lifetime_seconds" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN premium_lifetime_seconds INTEGER NOT NULL DEFAULT 0")
    if "premium_is_lifetime" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN premium_is_lifetime INTEGER NOT NULL DEFAULT 0")

def _ensure_premium_tiers(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS premium_tiers (
            tier INTEGER PRIMARY KEY,
            min_seconds INTEGER NOT NULL,
            earn_bonus_percent REAL NOT NULL,
            store_discount_percent REAL NOT NULL,
            stat_cap_percent INTEGER NOT NULL
        )
        """
    )
    # Seed defaults if empty
    try:
        row = conn.execute("SELECT COUNT(*) FROM premium_tiers").fetchone()
        if not row or int(row[0]) == 0:
            seed_premium_tiers_defaults(conn)
    except Exception:
        # If the table is not accessible for some reason, ignore seeding
        pass

def seed_premium_tiers_defaults(conn: sqlite3.Connection) -> None:
    # Thresholds cumulative: 1h, 1d, 1w, 1mo, 3mo, 6mo, 1y, 2y, 3y, 5y
    H = 3600
    D = 86400
    W = 7 * D
    MO = 30 * D
    Y = 365 * D
    tiers = [
        (1, 1*H,    0.05, 0.05, 150),
        (2, 1*D,    0.08, 0.08, 175),
        (3, 1*W,    0.10, 0.10, 200),
        (4, 1*MO,   0.12, 0.12, 225),
        (5, 3*MO,   0.15, 0.15, 250),
        (6, 6*MO,   0.18, 0.18, 300),
        (7, 1*Y,    0.21, 0.21, 350),
        (8, 2*Y,    0.24, 0.24, 400),
        (9, 3*Y,    0.27, 0.27, 450),
        (10, 5*Y,   0.30, 0.30, 500),
    ]
    _ensure_premium_tiers(conn)
    conn.execute("DELETE FROM premium_tiers")
    conn.executemany(
        "INSERT INTO premium_tiers(tier, min_seconds, earn_bonus_percent, store_discount_percent, stat_cap_percent) VALUES (?,?,?,?,?)",
        tiers,
    )

def _get_premium_tier_row(conn: sqlite3.Connection, lifetime_seconds: int) -> Optional[sqlite3.Row]:
    _ensure_premium_tiers(conn)
    row = conn.execute(
        "SELECT tier, min_seconds, earn_bonus_percent, store_discount_percent, stat_cap_percent FROM premium_tiers WHERE min_seconds <= ? ORDER BY min_seconds DESC LIMIT 1",
        (int(lifetime_seconds),),
    ).fetchone()
    return row

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


def gift_premium(db_path: Path, from_username: str, to_username: str, seconds: int) -> Dict[str, Any]:
    """Buy Premium time for another user.
    Charges the giver at 1:3 pricing, applies duration to recipient's premium_until.
    Minimum 3h applies if the recipient is not currently premium; if active, any positive duration allowed.
    Returns: {success, message, from_balance, to_premium_until, cost}
    """
    secs = int(seconds)
    res: Dict[str, Any] = {"success": False, "message": ""}
    if secs <= 0:
        res["message"] = "Duration must be > 0"; return res
    if from_username == to_username:
        # Delegate to normal purchase for clarity
        return purchase_premium(db_path, from_username, secs)
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_premium(conn)
            g = conn.execute("SELECT id, active, balance_seconds FROM users WHERE username = ?", (from_username,)).fetchone()
            r = conn.execute("SELECT id, active, premium_until FROM users WHERE username = ?", (to_username,)).fetchone()
            if not g:
                res["message"] = "Giver not found"; conn.rollback(); return res
            if not r:
                res["message"] = "Recipient not found"; conn.rollback(); return res
            if not int(g[1]):
                res["message"] = "Giver account is deactivated"; conn.rollback(); return res
            if not int(r[1]):
                res["message"] = "Recipient account is deactivated"; conn.rollback(); return res
            import time as _t
            now = int(_t.time())
            r_active = int(r[2] or 0) > now
            if not r_active and secs < 10800:
                res["message"] = "Minimum 3h for first Premium for recipient"; conn.rollback(); return res
            cost = secs * 3
            if int(g[2]) < cost:
                res["message"] = "Insufficient balance"; conn.rollback(); return res
            # Deduct from giver
            conn.execute("UPDATE users SET balance_seconds = balance_seconds - ? WHERE id = ?", (cost, int(g[0])))
            # Extend recipient premium
            base = int(r[2] or 0)
            start = base if base > now else now
            new_until = start + secs
            conn.execute("UPDATE users SET premium_until = ? WHERE id = ?", (new_until, int(r[0])))
            # Increment recipient's lifetime accumulation per spec
            conn.execute(
                "UPDATE users SET premium_lifetime_seconds = premium_lifetime_seconds + ? WHERE id = ?",
                (secs, int(r[0]))
            )
            # Unlock lifetime for recipient if threshold met
            try:
                thr = conn.execute("SELECT min_seconds FROM premium_tiers WHERE tier = 10").fetchone()
                if thr:
                    min10 = int(thr[0])
                    cur_lt = conn.execute("SELECT premium_lifetime_seconds FROM users WHERE id = ?", (int(r[0]),)).fetchone()
                    if cur_lt and int(cur_lt[0] or 0) >= min10:
                        conn.execute("UPDATE users SET premium_is_lifetime = 1 WHERE id = ?", (int(r[0]),))
            except Exception:
                pass
            nb = conn.execute("SELECT balance_seconds FROM users WHERE id = ?", (int(g[0]),)).fetchone()[0]
            conn.commit()
            return {"success": True, "message": "Premium gifted", "from_balance": int(nb), "to_premium_until": int(new_until), "cost": int(cost)}
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Gift premium failed: {e}"}

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
        _ensure_premium(conn)
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


def apply_stat_changes(db_path: Path, username: str, delta_energy: int, delta_hunger: int, delta_water: int) -> Dict[str, Any]:
    """Apply stat deltas to a user, capping each stat to [0,100].
    Returns: {success, message, energy, hunger, water}
    """
    out: Dict[str, Any] = {"success": False, "message": ""}
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_stats(conn)
            _ensure_premium(conn)
            _ensure_premium_tiers(conn)
            u = conn.execute("SELECT id, energy, hunger, water, premium_until, premium_is_lifetime, premium_lifetime_seconds FROM users WHERE username = ?", (username,)).fetchone()
            if not u:
                conn.rollback(); out["message"] = "User not found"; return out
            import time as _t
            now_ts = int(_t.time())
            upper = 100
            try:
                is_life = int(u[5] or 0) == 1
                if is_life or int(u[4] or 0) > now_ts:
                    trow = _get_premium_tier_row(conn, int(u[6] or 0))
                    upper = int(trow[4]) if trow else 250
            except Exception:
                upper = 250 if int(u[4] or 0) > now_ts else 100
            def cap(v: int) -> int:
                return 0 if v < 0 else (upper if v > upper else v)
            new_energy = cap(int(u[1]) + int(delta_energy))
            new_hunger = cap(int(u[2]) + int(delta_hunger))
            new_water  = cap(int(u[3]) + int(delta_water))
            conn.execute(
                "UPDATE users SET energy = ?, hunger = ?, water = ? WHERE id = ?",
                (new_energy, new_hunger, new_water, int(u[0]))
            )
            conn.commit()
            out.update({
                "success": True,
                "message": "Stats updated",
                "energy": new_energy,
                "hunger": new_hunger,
                "water": new_water,
            })
            return out
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            out["message"] = f"Update failed: {e}"
            return out

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
        conn.execute(
            "INSERT INTO time_store_config(id, market_index_percent) VALUES (1, ?)\n"
            "ON CONFLICT(id) DO UPDATE SET market_index_percent = excluded.market_index_percent",
            (p,),
        )
        conn.commit()


def get_market_index_percent(db_path: Path) -> int:
    with connect(db_path) as conn:
        # Avoid writes on read path; if table/row missing, treat as 0
        try:
            _ensure_store_config(conn)
            row = conn.execute("SELECT market_index_percent FROM time_store_config WHERE id = 1").fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0


# ---- Time Earner promo config ----

def _ensure_earner_config(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS time_earner_config (
            id INTEGER PRIMARY KEY CHECK(id=1),
            base_percent REAL,
            per_block_percent REAL,
            min_seconds INTEGER,
            block_seconds INTEGER,
            promo_enabled INTEGER,
            default_bonus_percent REAL,
            default_per_block_percent REAL
        )
        """
    )
    # migrations: add missing columns if upgrading
    cols = {row[1] for row in conn.execute("PRAGMA table_info(time_earner_config)").fetchall()}
    if "base_percent" not in cols:
        conn.execute("ALTER TABLE time_earner_config ADD COLUMN base_percent REAL")
    if "per_block_percent" not in cols:
        conn.execute("ALTER TABLE time_earner_config ADD COLUMN per_block_percent REAL")
    if "min_seconds" not in cols:
        conn.execute("ALTER TABLE time_earner_config ADD COLUMN min_seconds INTEGER")
    if "block_seconds" not in cols:
        conn.execute("ALTER TABLE time_earner_config ADD COLUMN block_seconds INTEGER")
    if "promo_enabled" not in cols:
        conn.execute("ALTER TABLE time_earner_config ADD COLUMN promo_enabled INTEGER")
    if "default_bonus_percent" not in cols:
        conn.execute("ALTER TABLE time_earner_config ADD COLUMN default_bonus_percent REAL")
    if "default_per_block_percent" not in cols:
        conn.execute("ALTER TABLE time_earner_config ADD COLUMN default_per_block_percent REAL")
    # ensure row exists (id only), then set defaults where NULL
    conn.execute("INSERT OR IGNORE INTO time_earner_config(id) VALUES (1)")
    conn.execute("UPDATE time_earner_config SET base_percent = COALESCE(base_percent, 0.10) WHERE id = 1")
    conn.execute("UPDATE time_earner_config SET per_block_percent = COALESCE(per_block_percent, 0.0125) WHERE id = 1")
    conn.execute("UPDATE time_earner_config SET min_seconds = COALESCE(min_seconds, 600) WHERE id = 1")
    conn.execute("UPDATE time_earner_config SET block_seconds = COALESCE(block_seconds, 600) WHERE id = 1")
    conn.execute("UPDATE time_earner_config SET promo_enabled = COALESCE(promo_enabled, 1) WHERE id = 1")
    conn.execute("UPDATE time_earner_config SET default_bonus_percent = COALESCE(default_bonus_percent, 0.10) WHERE id = 1")
    conn.execute("UPDATE time_earner_config SET default_per_block_percent = COALESCE(default_per_block_percent, 0.00) WHERE id = 1")


def get_earner_promo_config(db_path: Path) -> Dict[str, float | int]:
    with connect(db_path) as conn:
        _ensure_earner_config(conn)
        row = conn.execute(
            "SELECT base_percent, per_block_percent, min_seconds, block_seconds, promo_enabled, default_bonus_percent, default_per_block_percent FROM time_earner_config WHERE id = 1"
        ).fetchone()
        if not row:
            return {"base_percent": 0.10, "per_block_percent": 0.0125, "min_seconds": 600, "block_seconds": 600, "promo_enabled": 1, "default_bonus_percent": 0.10, "default_per_block_percent": 0.00}
        return {
            "base_percent": float(row[0]),
            "per_block_percent": float(row[1]),
            "min_seconds": int(row[2]),
            "block_seconds": int(row[3]),
            "promo_enabled": int(row[4]),
            "default_bonus_percent": float(row[5]),
            "default_per_block_percent": float(row[6]) if row[6] is not None else 0.0,
        }


def set_earner_promo_config(db_path: Path, base_percent: float, per_block_percent: float, min_seconds: int, block_seconds: int, promo_enabled: int = 1, default_bonus_percent: float = 0.10, default_per_block_percent: float = 0.0) -> None:
    b = float(base_percent)
    p = float(per_block_percent)
    mn = int(max(1, min_seconds))
    bs = int(max(1, block_seconds))
    en = 1 if int(promo_enabled) != 0 else 0
    dbonus = float(default_bonus_percent)
    dper = float(default_per_block_percent)
    with connect(db_path) as conn:
        _ensure_earner_config(conn)
        conn.execute(
            "INSERT INTO time_earner_config(id, base_percent, per_block_percent, min_seconds, block_seconds, promo_enabled, default_bonus_percent, default_per_block_percent) VALUES (1, ?, ?, ?, ?, ?, ?, ?)\n"
            "ON CONFLICT(id) DO UPDATE SET base_percent=excluded.base_percent, per_block_percent=excluded.per_block_percent, min_seconds=excluded.min_seconds, block_seconds=excluded.block_seconds, promo_enabled=excluded.promo_enabled, default_bonus_percent=excluded.default_bonus_percent, default_per_block_percent=excluded.default_per_block_percent",
            (b, p, mn, bs, en, dbonus, dper),
        )
        conn.commit()


def upsert_store_item(db_path: Path, item: str, kind: str, qty: int, restore_energy: int, restore_hunger: int, restore_water: int, base_price_seconds: int, name: Optional[str] = None) -> None:
    now = int(time.time())
    with connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_store_catalog(conn)
        _ensure_store_prices(conn)
        # Assign sequence-like id only on first insert
        row = conn.execute("SELECT id FROM time_store_catalog WHERE item = ?", (item,)).fetchone()
        next_id = None
        if not row:
            rid_row = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM time_store_catalog").fetchone()
            next_id = int(rid_row[0]) if rid_row else 1
            conn.execute(
                "INSERT INTO time_store_catalog(item, id, name, kind, qty, restore_energy, restore_hunger, restore_water) VALUES(?,?,?,?,?,?,?,?)",
                (item, next_id, name, kind, int(qty), int(restore_energy), int(restore_hunger), int(restore_water))
            )
        else:
            conn.execute(
                "UPDATE time_store_catalog SET name=COALESCE(?, name), kind=?, qty=?, restore_energy=?, restore_hunger=?, restore_water=? WHERE item = ?",
                (name, kind, int(qty), int(restore_energy), int(restore_hunger), int(restore_water), item)
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
            "SELECT c.item, c.name, c.kind, c.qty, c.restore_energy, c.restore_hunger, c.restore_water, p.base_price_seconds, p.current_price_seconds, c.id\n"
            "FROM time_store_catalog c JOIN time_store_prices p ON c.item = p.item ORDER BY c.item ASC"
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            base = int(r[7]); curr = int(r[8]); idx = float(p)/100.0
            effective = max(1, int(round(curr * (1.0 + idx))))
            out.append({
                "item": str(r[0]),
                "name": (str(r[1]) if r[1] is not None else None),
                "kind": str(r[2]),
                "qty": int(r[3]),
                "restore_energy": int(r[4]),
                "restore_hunger": int(r[5]),
                "restore_water": int(r[6]),
                "base_price_seconds": base,
                "current_price_seconds": curr,
                "effective_price_seconds": effective,
                "market_index_percent": int(p),
                "id": int(r[9]) if r[9] is not None else None,
            })
        return out


def get_next_store_item_id(db_path: Path) -> int:
    with connect(db_path) as conn:
        _ensure_store_catalog(conn)
        row = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM time_store_catalog").fetchone()
        return int(row[0]) if row and row[0] is not None else 1


def store_item_exists(db_path: Path, item: str) -> bool:
    with connect(db_path) as conn:
        _ensure_store_catalog(conn)
        row = conn.execute("SELECT 1 FROM time_store_catalog WHERE item = ?", (item,)).fetchone()
        return row is not None


def _ensure_user_inventory(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_inventory (
            user_id INTEGER NOT NULL,
            item TEXT NOT NULL,
            qty INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (user_id, item)
        )
        """
    )


def purchase_store_item(db_path: Path, username: str, item: str, quantity: int, apply_now: bool = True) -> Dict[str, Any]:
    """Atomically purchase quantity of item for username.
    If apply_now is True, immediately apply stat restore; otherwise store into user inventory.
    Returns: {success, message, balance, energy, hunger, water, qty_remaining, stored}
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
            _ensure_user_inventory(conn)
            _ensure_premium(conn)
            # Load user
            u = conn.execute("SELECT id, active, balance_seconds, energy, hunger, water, premium_until FROM users WHERE username = ?", (username,)).fetchone()
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
            # Premium discount by tier if active (or lifetime)
            import time as _t
            now_ts = int(_t.time())
            is_lifetime = int(u.get("premium_is_lifetime", 0) if isinstance(u, sqlite3.Row) else 0) == 1
            if is_lifetime or int(u["premium_until"] or 0) > now_ts:
                # fetch tier
                try:
                    _ensure_premium_tiers(conn)
                    row = conn.execute("SELECT premium_lifetime_seconds FROM users WHERE id = ?", (int(u["id"]),)).fetchone()
                    lt = int(row[0]) if row and row[0] is not None else 0
                    trow = _get_premium_tier_row(conn, lt)
                    disc = float(trow[3]) if trow else 0.10
                except Exception:
                    disc = 0.10
                effective = max(1, int(round(effective * (1.0 - disc))))
            total_cost = effective * q
            bal = int(u["balance_seconds"])
            if bal < total_cost:
                result["message"] = "Insufficient balance"; conn.rollback(); return result
            # Deduct balance
            conn.execute("UPDATE users SET balance_seconds = balance_seconds - ? WHERE id = ?", (total_cost, int(u["id"])) )
            stored = False
            if apply_now:
                # Apply stats
                upper = 100
                try:
                    is_lifetime2 = int(u.get("premium_is_lifetime", 0) if isinstance(u, sqlite3.Row) else 0) == 1
                    if is_lifetime2 or int(u["premium_until"] or 0) > now_ts:
                        row2 = conn.execute("SELECT premium_lifetime_seconds FROM users WHERE id = ?", (int(u["id"]),)).fetchone()
                        lt2 = int(row2[0]) if row2 and row2[0] is not None else 0
                        trow2 = _get_premium_tier_row(conn, lt2)
                        upper = int(trow2[4]) if trow2 else 250
                except Exception:
                    upper = 250 if int(u["premium_until"] or 0) > now_ts else 100
                def cap(v: int) -> int: return 0 if v < 0 else (upper if v > upper else v)
                new_energy = cap(int(u["energy"]) + int(r[1]) * q)
                new_hunger = cap(int(u["hunger"]) + int(r[2]) * q)
                new_water  = cap(int(u["water"])  + int(r[3]) * q)
                conn.execute("UPDATE users SET energy = ?, hunger = ?, water = ? WHERE id = ?", (new_energy, new_hunger, new_water, int(u["id"])) )
            else:
                # Store into inventory
                stored = True
                conn.execute(
                    "INSERT INTO user_inventory(user_id, item, qty) VALUES(?,?,?)\n"
                    "ON CONFLICT(user_id, item) DO UPDATE SET qty = qty + excluded.qty",
                    (int(u["id"]), item, q)
                )
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
                "stored": stored,
            }
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Purchase failed: {e}"}


# Premium helpers
def is_premium(db_path: Path, username: str) -> Dict[str, Any]:
    with connect(db_path) as conn:
        _ensure_premium(conn)
        _ensure_premium_tiers(conn)
        row = conn.execute("SELECT premium_until, premium_is_lifetime, premium_lifetime_seconds FROM users WHERE username = ?", (username,)).fetchone()
        if not row:
            return {"active": False, "until": 0, "is_lifetime": False, "tier": None}
        import time as _t
        until = int(row[0] or 0)
        is_life = int(row[1] or 0) == 1
        lifetime = int(row[2] or 0)
        trow = _get_premium_tier_row(conn, lifetime)
        tier = int(trow[0]) if trow else 0
        return {"active": (is_life or until > int(_t.time())), "until": until, "is_lifetime": is_life, "tier": tier}

def get_user_premium_tier(db_path: Path, username: str) -> Dict[str, Any]:
    with connect(db_path) as conn:
        _ensure_premium(conn)
        _ensure_premium_tiers(conn)
        row = conn.execute("SELECT premium_lifetime_seconds FROM users WHERE username = ?", (username,)).fetchone()
        if not row:
            return {"tier": 0, "min_seconds": 0, "earn_bonus_percent": 0.0, "store_discount_percent": 0.0, "stat_cap_percent": 100}
        lt = int(row[0] or 0)
        trow = _get_premium_tier_row(conn, lt)
        if not trow:
            return {"tier": 0, "min_seconds": 0, "earn_bonus_percent": 0.0, "store_discount_percent": 0.0, "stat_cap_percent": 100}
        return {
            "tier": int(trow[0]),
            "min_seconds": int(trow[1]),
            "earn_bonus_percent": float(trow[2]),
            "store_discount_percent": float(trow[3]),
            "stat_cap_percent": int(trow[4]),
        }

# ---- Admin helpers: premium tiers management ----
def list_premium_tiers(db_path: Path) -> list[dict]:
    with connect(db_path) as conn:
        _ensure_premium_tiers(conn)
        rows = conn.execute(
            "SELECT tier, min_seconds, earn_bonus_percent, store_discount_percent, stat_cap_percent FROM premium_tiers ORDER BY tier ASC"
        ).fetchall()
        return [
            {
                "tier": int(r[0]),
                "min_seconds": int(r[1]),
                "earn_bonus_percent": float(r[2]),
                "store_discount_percent": float(r[3]),
                "stat_cap_percent": int(r[4]),
            }
            for r in rows
        ]

def set_premium_tiers_defaults(db_path: Path) -> None:
    with connect(db_path) as conn:
        _ensure_premium_tiers(conn)
        seed_premium_tiers_defaults(conn)
        conn.commit()

def add_or_replace_premium_tier(db_path: Path, tier: int, min_seconds: int, earn_bonus_percent: float, store_discount_percent: float, stat_cap_percent: int) -> None:
    with connect(db_path) as conn:
        _ensure_premium_tiers(conn)
        conn.execute(
            "INSERT INTO premium_tiers(tier, min_seconds, earn_bonus_percent, store_discount_percent, stat_cap_percent) VALUES (?,?,?,?,?)\n"
            "ON CONFLICT(tier) DO UPDATE SET min_seconds=excluded.min_seconds, earn_bonus_percent=excluded.earn_bonus_percent, store_discount_percent=excluded.store_discount_percent, stat_cap_percent=excluded.stat_cap_percent",
            (int(tier), int(min_seconds), float(earn_bonus_percent), float(store_discount_percent), int(stat_cap_percent)),
        )
        conn.commit()

def remove_premium_tier(db_path: Path, tier: int) -> bool:
    with connect(db_path) as conn:
        _ensure_premium_tiers(conn)
        cur = conn.execute("DELETE FROM premium_tiers WHERE tier = ?", (int(tier),))
        conn.commit()
        return (cur.rowcount or 0) > 0

# ---- Admin helpers: user premium progression controls ----
def set_user_premium_tier(db_path: Path, username: str, tier: int) -> Dict[str, Any]:
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_premium(conn)
            _ensure_premium_tiers(conn)
            u = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
            if not u:
                conn.rollback(); return {"success": False, "message": "User not found"}
            row = conn.execute("SELECT min_seconds FROM premium_tiers WHERE tier = ?", (int(tier),)).fetchone()
            if not row:
                conn.rollback(); return {"success": False, "message": "Tier not found"}
            min_secs = int(row[0])
            conn.execute("UPDATE users SET premium_lifetime_seconds = ?, premium_is_lifetime = CASE WHEN ? >= (SELECT min_seconds FROM premium_tiers WHERE tier = 10) THEN 1 ELSE premium_is_lifetime END WHERE id = ?", (min_secs, min_secs, int(u[0])))
            conn.commit()
            return {"success": True, "message": "User tier set", "tier": int(tier), "lifetime_seconds": int(min_secs)}
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Set tier failed: {e}"}

def reset_user_premium_progress(db_path: Path, username: str) -> Dict[str, Any]:
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_premium(conn)
            u = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
            if not u:
                conn.rollback(); return {"success": False, "message": "User not found"}
            conn.execute("UPDATE users SET premium_lifetime_seconds = 0, premium_is_lifetime = 0 WHERE id = ?", (int(u[0]),))
            conn.commit()
            return {"success": True, "message": "User premium progression reset"}
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Reset failed: {e}"}

def set_user_premium_lifetime_seconds(db_path: Path, username: str, seconds: int) -> Dict[str, Any]:
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_premium(conn)
            _ensure_premium_tiers(conn)
            u = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
            if not u:
                conn.rollback(); return {"success": False, "message": "User not found"}
            secs = int(max(0, seconds))
            conn.execute("UPDATE users SET premium_lifetime_seconds = ? WHERE id = ?", (secs, int(u[0])))
            # adjust lifetime flag according to tier 10 threshold
            try:
                thr = conn.execute("SELECT min_seconds FROM premium_tiers WHERE tier = 10").fetchone()
                if thr and secs >= int(thr[0]):
                    conn.execute("UPDATE users SET premium_is_lifetime = 1 WHERE id = ?", (int(u[0]),))
                else:
                    conn.execute("UPDATE users SET premium_is_lifetime = 0 WHERE id = ?", (int(u[0]),))
            except Exception:
                pass
            conn.commit()
            return {"success": True, "message": "Lifetime seconds set", "lifetime_seconds": secs}
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Set lifetime seconds failed: {e}"}

def set_user_premium_lifetime(db_path: Path, username: str, on: bool) -> Dict[str, Any]:
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_premium(conn)
            u = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
            if not u:
                conn.rollback(); return {"success": False, "message": "User not found"}
            conn.execute("UPDATE users SET premium_is_lifetime = ? WHERE id = ?", (1 if on else 0, int(u[0])))
            conn.commit()
            return {"success": True, "message": "Lifetime flag updated", "is_lifetime": bool(on)}
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Set lifetime flag failed: {e}"}

def backfill_lifetime_from_remaining(db_path: Path, username: Optional[str] = None, mode: str = "add") -> Dict[str, Any]:
    """Backfill premium_lifetime_seconds from remaining premium time.
    mode: 'add' to add remaining to existing lifetime; 'set' to replace with remaining.
    If username is None or empty, process all users with active premium.
    Returns: {success, message, updated}
    """
    import time as _t
    now = int(_t.time())
    mode_norm = (mode or "add").strip().lower()
    if mode_norm not in ("add", "set"):
        mode_norm = "add"
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_premium(conn)
            _ensure_premium_tiers(conn)
            thr = conn.execute("SELECT min_seconds FROM premium_tiers WHERE tier = 10").fetchone()
            tier10 = int(thr[0]) if thr else 0
            updated = 0
            if username:
                rows = conn.execute("SELECT id, premium_until, premium_lifetime_seconds FROM users WHERE username = ?", (username,)).fetchall()
            else:
                rows = conn.execute("SELECT id, premium_until, premium_lifetime_seconds FROM users WHERE premium_until IS NOT NULL AND premium_until > ?", (now,)).fetchall()
            for r in rows:
                uid = int(r[0])
                until = int(r[1] or 0)
                remaining = max(0, until - now)
                curr = int(r[2] or 0)
                if mode_norm == "set":
                    new_secs = remaining
                else:
                    new_secs = curr + remaining
                conn.execute("UPDATE users SET premium_lifetime_seconds = ? WHERE id = ?", (int(new_secs), uid))
                if tier10 > 0 and new_secs >= tier10:
                    conn.execute("UPDATE users SET premium_is_lifetime = 1 WHERE id = ?", (uid,))
                updated += 1
            conn.commit()
            return {"success": True, "message": "Backfill completed", "updated": int(updated)}
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Backfill failed: {e}"}


def purchase_premium(db_path: Path, username: str, seconds: int) -> Dict[str, Any]:
    """Purchase premium at 1:3 pricing. Min 3h if not currently premium; allow any positive extension if active.
    Returns: {success, message, balance, premium_until, cost}
    """
    secs = int(seconds)
    result: Dict[str, Any] = {"success": False, "message": ""}
    if secs <= 0:
        result["message"] = "Duration must be > 0"; return result
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_premium(conn)
            u = conn.execute("SELECT id, active, balance_seconds, premium_until FROM users WHERE username = ?", (username,)).fetchone()
            if not u:
                result["message"] = "User not found"; conn.rollback(); return result
            if not int(u[1]):
                result["message"] = "Account is deactivated"; conn.rollback(); return result
            import time as _t
            now = int(_t.time())
            active = int(u[3] or 0) > now
            if not active and secs < 10800:
                result["message"] = "Minimum 3h for first purchase"; conn.rollback(); return result
            cost = secs * 3
            bal = int(u[2])
            if bal < cost:
                result["message"] = "Insufficient balance"; conn.rollback(); return result
            # Deduct and extend
            conn.execute("UPDATE users SET balance_seconds = balance_seconds - ? WHERE id = ?", (cost, int(u[0])))
            base = int(u[3] or 0)
            start = base if base > now else now
            new_until = start + secs
            conn.execute("UPDATE users SET premium_until = ? WHERE id = ?", (new_until, int(u[0])))
            # Increment lifetime accumulation for buyer
            conn.execute(
                "UPDATE users SET premium_lifetime_seconds = premium_lifetime_seconds + ? WHERE id = ?",
                (secs, int(u[0]))
            )
            # Unlock lifetime if threshold reached
            try:
                thr = conn.execute("SELECT min_seconds FROM premium_tiers WHERE tier = 10").fetchone()
                if thr:
                    min10 = int(thr[0])
                    cur_lt = conn.execute("SELECT premium_lifetime_seconds FROM users WHERE id = ?", (int(u[0]),)).fetchone()
                    if cur_lt and int(cur_lt[0] or 0) >= min10:
                        conn.execute("UPDATE users SET premium_is_lifetime = 1 WHERE id = ?", (int(u[0]),))
            except Exception:
                pass
            nb = conn.execute("SELECT balance_seconds FROM users WHERE id = ?", (int(u[0]),)).fetchone()[0]
            conn.commit()
            return {"success": True, "message": "Premium purchased", "balance": int(nb), "premium_until": int(new_until), "cost": int(cost)}
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            result["message"] = f"Premium purchase failed: {e}"
            return result


def purchase_store_item_by_id(db_path: Path, username: str, item_id: int, quantity: int, apply_now: bool = True) -> Dict[str, Any]:
    # resolve id to item key then reuse implementation
    with connect(db_path) as conn:
        row = conn.execute("SELECT item FROM time_store_catalog WHERE id = ?", (int(item_id),)).fetchone()
        if not row:
            return {"success": False, "message": "Item not found"}
        key = str(row[0])
    return purchase_store_item(db_path, username, key, quantity, apply_now=apply_now)


def list_user_inventory(db_path: Path, username: str) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        _ensure_user_inventory(conn)
        _ensure_store_catalog(conn)
        u = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        if not u:
            return []
        rows = conn.execute(
            "SELECT ui.item, ui.qty, c.name, c.kind, c.restore_energy, c.restore_hunger, c.restore_water, c.id\n"
            "FROM user_inventory ui JOIN time_store_catalog c ON ui.item = c.item WHERE ui.user_id = ? ORDER BY c.item",
            (int(u[0]),)
        ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append({
                "item": str(r[0]),
                "qty": int(r[1]),
                "name": (str(r[2]) if r[2] is not None else None),
                "kind": str(r[3]),
                "restore_energy": int(r[4]),
                "restore_hunger": int(r[5]),
                "restore_water": int(r[6]),
                "id": int(r[7]) if r[7] is not None else None,
            })
        return out


def use_inventory_item(db_path: Path, username: str, item: str, quantity: int) -> Dict[str, Any]:
    q = int(max(1, quantity))
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_stats(conn)
            _ensure_premium(conn)
            _ensure_premium_tiers(conn)
            _ensure_user_inventory(conn)
            _ensure_store_catalog(conn)
            # Load user
            u = conn.execute("SELECT id, energy, hunger, water, premium_until, premium_is_lifetime, premium_lifetime_seconds FROM users WHERE username = ?", (username,)).fetchone()
            if not u:
                conn.rollback(); return {"success": False, "message": "User not found"}
            # Check inventory
            row = conn.execute("SELECT qty FROM user_inventory WHERE user_id = ? AND item = ?", (int(u[0]), item)).fetchone()
            if not row or int(row[0]) < q:
                conn.rollback(); return {"success": False, "message": "Not enough in inventory"}
            # Get item effects
            eff = conn.execute("SELECT restore_energy, restore_hunger, restore_water FROM time_store_catalog WHERE item = ?", (item,)).fetchone()
            if not eff:
                conn.rollback(); return {"success": False, "message": "Item not found"}
            import time as _t
            now_ts = int(_t.time())
            upper = 100
            try:
                is_life = int(u[5] or 0) == 1
                if is_life or int(u[4] or 0) > now_ts:
                    trow = _get_premium_tier_row(conn, int(u[6] or 0))
                    upper = int(trow[4]) if trow else 250
            except Exception:
                upper = 250 if int(u[4] or 0) > now_ts else 100
            def cap(v: int) -> int: return 0 if v < 0 else (upper if v > upper else v)
            new_energy = cap(int(u[1]) + int(eff[0]) * q)
            new_hunger = cap(int(u[2]) + int(eff[1]) * q)
            new_water  = cap(int(u[3]) + int(eff[2]) * q)
            conn.execute("UPDATE users SET energy = ?, hunger = ?, water = ? WHERE id = ?", (new_energy, new_hunger, new_water, int(u[0])))
            conn.execute("UPDATE user_inventory SET qty = qty - ? WHERE user_id = ? AND item = ?", (q, int(u[0]), item))
            # Clean zero rows
            conn.execute("DELETE FROM user_inventory WHERE user_id = ? AND item = ? AND qty <= 0", (int(u[0]), item))
            post = conn.execute("SELECT energy, hunger, water FROM users WHERE id = ?", (int(u[0]),)).fetchone()
            conn.commit()
            return {"success": True, "message": "Used item", "energy": int(post[0]), "hunger": int(post[1]), "water": int(post[2])}
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Use failed: {e}"}


def transfer_inventory_item(db_path: Path, from_username: str, to_username: str, item: str, quantity: int) -> Dict[str, Any]:
    """Transfer quantity of item from one user's inventory to another's, atomically."""
    q = int(max(1, quantity))
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_user_inventory(conn)
            # Load users
            u_from = conn.execute("SELECT id, active FROM users WHERE username = ?", (from_username,)).fetchone()
            u_to = conn.execute("SELECT id, active FROM users WHERE username = ?", (to_username,)).fetchone()
            if not u_from:
                conn.rollback(); return {"success": False, "message": "Sender not found"}
            if not u_to:
                conn.rollback(); return {"success": False, "message": "Recipient not found"}
            if not int(u_from[1]):
                conn.rollback(); return {"success": False, "message": "Sender account is deactivated"}
            if not int(u_to[1]):
                conn.rollback(); return {"success": False, "message": "Recipient account is deactivated"}
            # Check sender inventory
            row = conn.execute("SELECT qty FROM user_inventory WHERE user_id = ? AND item = ?", (int(u_from[0]), item)).fetchone()
            if not row or int(row[0]) < q:
                conn.rollback(); return {"success": False, "message": "Not enough in inventory"}
            # Move
            conn.execute("UPDATE user_inventory SET qty = qty - ? WHERE user_id = ? AND item = ?", (q, int(u_from[0]), item))
            conn.execute(
                "INSERT INTO user_inventory(user_id, item, qty) VALUES(?,?,?)\n"
                "ON CONFLICT(user_id, item) DO UPDATE SET qty = qty + excluded.qty",
                (int(u_to[0]), item, q)
            )
            # Clean zero rows for sender
            conn.execute("DELETE FROM user_inventory WHERE user_id = ? AND item = ? AND qty <= 0", (int(u_from[0]), item))
            # Read post
            post_from = conn.execute("SELECT qty FROM user_inventory WHERE user_id = ? AND item = ?", (int(u_from[0]), item)).fetchone()
            post_to = conn.execute("SELECT qty FROM user_inventory WHERE user_id = ? AND item = ?", (int(u_to[0]), item)).fetchone()
            conn.commit()
            return {
                "success": True,
                "message": "Transfer completed",
                "sender_qty": int(post_from[0]) if post_from else 0,
                "recipient_qty": int(post_to[0]) if post_to else q,
            }
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Transfer failed: {e}"}


def sell_inventory_item(db_path: Path, username: str, item: str, quantity: int) -> Dict[str, Any]:
    """Sell quantity of an inventory item back to the system at a percentage of the effective price.
    Non-premium: 75% of effective price. Premium: 85% of effective price.
    """
    q = int(max(1, quantity))
    with connect(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            _ensure_user_inventory(conn)
            _ensure_store_catalog(conn)
            _ensure_store_prices(conn)
            _ensure_premium(conn)
            # Load user
            u = conn.execute("SELECT id, active, balance_seconds, premium_until FROM users WHERE username = ?", (username,)).fetchone()
            if not u:
                conn.rollback(); return {"success": False, "message": "User not found"}
            if not int(u[1]):
                conn.rollback(); return {"success": False, "message": "Account is deactivated"}
            # Check inventory
            row = conn.execute("SELECT qty FROM user_inventory WHERE user_id = ? AND item = ?", (int(u[0]), item)).fetchone()
            if not row or int(row[0]) < q:
                conn.rollback(); return {"success": False, "message": "Not enough in inventory"}
            # Determine effective price
            r = conn.execute(
                "SELECT p.current_price_seconds FROM time_store_prices p WHERE p.item = ?",
                (item,)
            ).fetchone()
            if not r:
                conn.rollback(); return {"success": False, "message": "Item price not found"}
            curr_price = int(r[0])
            idx_percent = int(conn.execute("SELECT market_index_percent FROM time_store_config WHERE id = 1").fetchone()[0])
            effective = max(1, int(round(curr_price * (1.0 + float(idx_percent)/100.0))))
            # Premium rate
            import time as _t
            is_prem = int(u[3] or 0) > int(_t.time())
            rate = 0.85 if is_prem else 0.75
            unit_payout = max(1, int(round(effective * rate)))
            total_payout = unit_payout * q
            # Apply changes
            conn.execute("UPDATE users SET balance_seconds = balance_seconds + ? WHERE id = ?", (int(total_payout), int(u[0])))
            conn.execute("UPDATE user_inventory SET qty = qty - ? WHERE user_id = ? AND item = ?", (q, int(u[0]), item))
            conn.execute("DELETE FROM user_inventory WHERE user_id = ? AND item = ? AND qty <= 0", (int(u[0]), item))
            post_bal = conn.execute("SELECT balance_seconds FROM users WHERE id = ?", (int(u[0]),)).fetchone()
            post_qty = conn.execute("SELECT qty FROM user_inventory WHERE user_id = ? AND item = ?", (int(u[0]), item)).fetchone()
            conn.commit()
            return {
                "success": True,
                "message": "Sold item(s)",
                "balance": int(post_bal[0]) if post_bal else None,
                "unit_effective_price_seconds": int(effective),
                "unit_payout_seconds": int(unit_payout),
                "total_payout_seconds": int(total_payout),
                "remaining_qty": int(post_qty[0]) if post_qty else 0,
                "premium": bool(is_prem),
                "rate_percent": int(rate * 100),
            }
        except Exception as e:
            try: conn.rollback()
            except Exception: pass
            return {"success": False, "message": f"Sell failed: {e}"}


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
