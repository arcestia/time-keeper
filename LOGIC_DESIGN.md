# Logic Design: Time Keeper Suite

This document explains the core logic, data model, flows, and patterns so you can replicate or adapt the design to other projects.

## Architecture Overview
- **SQLite persistence** with WAL mode for safe concurrent reads and simple deployments.
- **Two CLIs** sharing the same database:
  - `time_keeper`: main app (accounts, auth, worker, transfers, reserves, leaderboard).
  - `time_earner`: earning mechanic (stake-and-countdown session).
- **Separation of concerns**:
  - `db.py` = data model and atomic operations (transaction boundaries live here).
  - `cli.py` = UX and orchestration (no direct SQL; calls `db.py`).
  - `formatting.py` = shared duration format/parse rules.
- **Background worker**: long-running process that decrements balances every second and deactivates empty accounts.

## Data Model
- `users`
  - `username TEXT PRIMARY KEY`
  - `passcode_hash TEXT` (PBKDF2-HMAC)
  - `balance_seconds INTEGER NOT NULL DEFAULT 0`
  - `is_admin INTEGER NOT NULL DEFAULT 0`
  - `active INTEGER NOT NULL DEFAULT 1`
  - `created_at INTEGER` (epoch seconds)
  - `deactivated_at INTEGER NULL`
- Indexes: `(active)`, `(balance_seconds)` for worker/leaderboard queries.
- `time_reserves` (singleton row `id=1`)
  - `total_seconds INTEGER NOT NULL DEFAULT 0`
  - Purpose: accumulate total seconds deducted by the worker (see Worker flow).

## Security & Auth
- Passcodes hashed with PBKDF2-HMAC; verification done on login only.
- Admin authorization: `is_admin=1` checked before privileged ops.

## Duration Standard
- Units and order: century, decade, year, month, week, day, hour, minute, second.
- Short style for UI (e.g., `9y, 11mo, 4w, 2d, 10h, 28m, 50s`).
- Parser accepts "1y 2mo 3d", "4w, 2d", "1h 30m", or integer seconds.

## Core Flows

### 1) Account Creation
- Input (interactive): human‑readable initial time -> parsed to seconds.
- Transaction: INSERT user with `balance_seconds = initial_seconds`.

### 2) Login
- Steps:
  1. Find user by username.
  2. Verify passcode.
  3. Present compact balance (short style, `max_parts=2`).

### 3) Transfers
- Preconditions: both users exist and are `active=1`; sender has enough balance; `from != to`.
- Transaction (atomic):
  - `UPDATE users SET balance_seconds = balance_seconds - amount WHERE id = from_id`
  - `UPDATE users SET balance_seconds = balance_seconds + amount WHERE id = to_id`
- Return updated balances for both.

### 4) Worker Deduction + Deactivation + Reserves
- Tick interval: default 1s.
- Transaction per tick:
  - Decrement `balance_seconds` by 1 for all `active=1 AND balance_seconds>0`.
  - Add the number of decremented rows to `time_reserves.total_seconds` (same transaction).
  - Deactivate users with `balance_seconds=0`.
- Background mode: detached process with PID/log files; CLI controls allow `--background`, `--status`, `--stop`.

### 5) Time Reserves Read
- `SELECT total_seconds FROM time_reserves WHERE id=1`.
- Lazily ensures `time_reserves` exists even if DB wasn’t reinitialized.

### 6) Earning Session (time_earner)
- Stake-and-countdown mechanic:
  - On start: deduct stake immediately in a transaction.
  - Foreground countdown equals staked seconds.
  - If interrupted (logout/exit/CTRL+C): stake is forfeited.
  - If countdown completes: credit `2 * stake` in a transaction.
- UX ensures the user understands the forfeit risk.

## Error Handling & Invariants
- All balance‑mutating operations run under `BEGIN IMMEDIATE` transactions to avoid lost updates.
- Validate inputs before SQL (e.g., amount > 0; non-empty username; not self‑transfer).
- Always commit or rollback explicitly on error.

## Extensibility Patterns
- Add new mechanics by placing logic in `db.py` as atomic functions; call from CLIs.
- Add new units by updating `formatting.py` (`_UNIT_DEFS`, aliases, tests).
- Swap SQLite for another DB by reimplementing `db.py` with same function signatures.
- Add APIs by wrapping `db.py` in a web layer; CLIs can remain as integration tests.

## Replication Checklist (to port into a new project)
1. Copy the structure: `db.py`, `formatting.py`, `cli.py`, worker module, tests.
2. Keep all balance changes within transactions and return post‑state balances.
3. Adopt the duration standard for consistent UX/API contracts.
4. Use a background worker or scheduler to apply periodic balance changes.
5. If you need an accumulation pool, mirror the `time_reserves` pattern (singleton row updated in the same transaction).
6. Expose admin read‑only views for system pools and aggregates.
7. Use short style in UI; cap `max_parts` for headers to keep them readable.

## Pseudocode Snippets

### Transfer (atomic)
```
BEGIN IMMEDIATE
f = SELECT id, balance, active FROM users WHERE username = from
t = SELECT id, balance, active FROM users WHERE username = to
assert f && t && f.active && t.active && f.balance >= amount && from != to
UPDATE users SET balance = balance - amount WHERE id = f.id
UPDATE users SET balance = balance + amount WHERE id = t.id
COMMIT
```

### Worker tick with reserves
```
BEGIN IMMEDIATE
updated = UPDATE users
    SET balance = balance - 1
    WHERE active = 1 AND balance > 0  -- rowcount = updated
INSERT INTO time_reserves(id,total)
    VALUES(1, updated)
    ON CONFLICT(id) DO UPDATE SET total = total + excluded.total
-- deactivate zero balances
UPDATE users SET active = 0, deactivated_at = now()
    WHERE active=1 AND balance=0
COMMIT
```

### Earning session (stake → countdown → reward)
```
-- stake
BEGIN IMMEDIATE
assert user.active && user.balance >= stake
UPDATE users SET balance = balance - stake WHERE username = ?
COMMIT

-- foreground loop (1s)
while remaining > 0: sleep(1); remaining -= 1

-- reward on success
BEGIN IMMEDIATE
UPDATE users SET balance = balance + 2*stake WHERE username = ?
COMMIT
```

## Testing Strategy (high-level)
- Unit-test `formatting` (format + parse roundtrips for many cases).
- Unit-test `db.transfer_seconds` (success, insufficient, inactive, self‑transfer).
- Integration-test worker tick + reserves increments.
- Integration-test earning session: stake deducts; completion rewards; interruption forfeits.

---
This logic design is implementation‑agnostic and intended to be portable. Preserve transactional boundaries and unit standards to ensure consistent behavior across platforms.
