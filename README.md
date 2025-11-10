# Time Keeper (Experimental)

A simple CLI that treats time like a bank balance. Accounts start with 1 day (86,400 seconds). A background worker deducts one second from all active accounts every second and deactivates accounts that reach zero.

## Requirements
- Python 3.9+
- colorama

## Install
```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
```

## Quickstart
Initialize a new database:
```bash
python -m time_keeper.cli init-db --db timekeeper.db
```

Interactive menu:
```bash
python -m time_keeper.cli
```

Run worker (foreground):
```bash
python -m time_keeper.cli run-worker --db timekeeper.db --interval 1.0
```

Run worker (background, with status/stop):
```bash
python -m time_keeper.cli run-worker --db timekeeper.db --background
python -m time_keeper.cli run-worker --db timekeeper.db --status
python -m time_keeper.cli run-worker --db timekeeper.db --stop
```

Leaderboard:
```bash
python -m time_keeper.cli leaderboard --db timekeeper.db --limit 10
```

Initialize database:
```bash
python -m time_keeper.cli init-db --db timekeeper.db
```

Create account (prompts for passcode):
```bash
python -m time_keeper.cli create-account --username alice --db timekeeper.db
```

Create admin account:
```bash
python -m time_keeper.cli create-account --username admin --admin --db timekeeper.db
```

Login (verifies passcode and shows balance):
```bash
python -m time_keeper.cli login --username alice --db timekeeper.db
```

Admin: list all balances:
```bash
python -m time_keeper.cli admin --username admin --db timekeeper.db --list
```

Leaderboard (top N):
```bash
python -m time_keeper.cli leaderboard --limit 10 --db timekeeper.db
```

Run background worker (Ctrl+C to stop):
```bash
python -m time_keeper.cli run-worker --db timekeeper.db
```

## Features
- Human‑readable durations (short style) across the UI.
- Interactive menu with color and session‑aware options.
- Admin‑only options (list accounts, Time Reserves, run worker, init DB, change DB path).
- Transfers between users (atomic, validated).
- Background worker (foreground or detached background with PID/log files).
- Time Reserves pool accumulates seconds deducted by worker.

## Commands
```bash
python -m time_keeper.cli [--db timekeeper.db] {init-db,create-account,login,admin,leaderboard,run-worker,interactive}
```
- `--db` can be placed before or after subcommands.

### create-account (interactive defaults)
Interactive flow accepts human‑readable initial time (e.g., `1d 2h`).

### admin
- `--list` shows all accounts in a table.
- `--reserves` shows Time Reserves in short style and raw seconds.

### run-worker
- `--interval` seconds between ticks (default 1.0).
- `--background` starts a detached process and writes PID/log files next to the DB.
- `--status`/`--stop` use the PID file to manage the background worker.

## Interactive Menus
- Not logged in: Login, Create account, Leaderboard, Quit.
- Logged in (user): Refresh balance, Transfer time, Leaderboard, Logout.
- Logged in (admin): Transfer, List accounts, Show Time Reserves, Leaderboard, Run worker (background), Init DB, Change DB path, Logout.

## Time Earner (companion CLI)
Interactive staking session that deducts stake up front and runs a countdown. If completed, double the stake is rewarded; if interrupted, stake is forfeited.

```bash
python -m time_earner.cli                   # interactive
python -m time_earner.cli earn --username alice --amount "30m" --db timekeeper.db
```

## Notes
- New accounts receive 86,400 seconds by default (1d).
- Accounts reaching 0 seconds are deactivated automatically by the worker.
- Admin is identified by the `is_admin` flag. Admin operations require admin authentication.

## Contributing
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
