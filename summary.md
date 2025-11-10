# Time Keeper Project Summary

## Overview
- Time-based accounting system: each user has a balance of seconds.
- Background worker deducts 1s from all active accounts every second and deactivates accounts at zero.
- Colorized interactive CLI with admin features, human‑readable time formatting, and a Time Reserves pool.
- Companion CLI app (time_earner) for earning time through a staking countdown session.

## Core Components
- time_keeper/db.py: SQLite schema, CRUD, worker utilities, transfers, time reserves.
- time_keeper/cli.py: Main CLI with interactive mode and subcommands.
- time_keeper/formatting.py: Standardized duration formatting and parsing.
- time_earner/cli.py: Separate CLI to earn time via a stake-and-countdown flow.

## Standard Duration Formatting
- Units: century, decade, year, month, week, day, hour, minute, second.
- Short style used in UI (e.g., 9y, 11mo, 4w, 2d, 10h, 28m, 50s).
- Parser accepts inputs like "1y 2mo 3d", "4w, 2d", "1h 30m", or plain seconds.

## Main CLI (time_keeper)
- Subcommands
  - init-db: Initialize the database.
  - create-account: Create accounts (supports admin flag). Non-interactive; interactive flow accepts human‑readable initial time.
  - login: Verify passcode and print balance.
  - admin: Admin utilities (list accounts, show time reserves).
  - leaderboard: Table of top balances with human‑readable time.
  - run-worker: Foreground or background (PID/log files; status/stop supported).
  - interactive: Colorized menu (default when no subcommand).
- Global/Per‑subcommand --db supported (before or after subcommand).

### Interactive Menu Highlights
- Not logged in: Login, Create account, Leaderboard, Quit.
- Logged in (user): Refresh balance, Transfer time, Leaderboard, Logout, Quit.
- Logged in (admin): Transfer time, List accounts, Show Time Reserves, Leaderboard, Run worker (background), Init DB, Change DB path, Logout, Quit.
- Menu headings are colorized; header shows compact balance with max_parts=2 in short style.

## Transfers
- Logged-in users can transfer time to another user.
- Atomic DB operation with checks: existence, activity, and sufficient balance.
- Results display sender and recipient balances in short style.

## Time Reserves
- Accumulates the total seconds deducted each tick by the worker in the same transaction.
- Admin can view reserves (interactive and CLI): human‑readable and raw seconds.
- Table ensured lazily even if DB wasn’t re‑initialized.

## Background Worker
- Deducts 1s from all active accounts each tick; deactivates zero-balance users.
- Background mode (Windows‑friendly detachment) with PID and log files adjacent to DB.
- CLI controls: --background, --status, --stop; interactive admin shortcut starts background worker.

## Companion CLI: time_earner
- Purpose: Let users earn time to their balance via a session mechanic.
- Interactive flow after login:
  - Start earning session (stake and countdown).
  - Stake is immediately deducted; countdown runs for staked duration.
  - If interrupted/logged out early: stake is forfeited.
  - On successful completion: double the stake is rewarded.
  - Refresh balance and Logout options available.
- Non‑interactive command `earn` adds time directly (not the session).

## Files Added
- time_keeper/formatting.py: formatting and parsing.
- time_keeper/cli.py: colorized UI, short style, admin features, background worker controls, transfer flow.
- time_keeper/db.py: schema and operations, transfer, reserves.
- time_earner/cli.py: earning session and direct earn.
- .gitignore: *.worker.pid, *.worker.log, *.db, Python artifacts.
- summary.md (this file).

## How to Run
- Install: `pip install -r requirements.txt`
- Main CLI interactive: `python -m time_keeper.cli`
- Background worker (background): `python -m time_keeper.cli run-worker --db timekeeper.db --background`
- Admin reserves: `python -m time_keeper.cli admin --username admin --reserves --db timekeeper.db`
- Earner interactive: `python -m time_earner.cli`
- Earner non-interactive: `python -m time_earner.cli earn --username alice --amount "1h 30m" --db timekeeper.db`
