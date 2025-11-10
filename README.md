# Time Keeper (Experimental)

A simple CLI that treats time like a bank balance. Accounts start with 1 day (86,400 seconds). A background worker deducts one second from all active accounts every second and deactivates accounts that reach zero.

## Requirements
- Python 3.9+
- No external dependencies

## Install
```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
```

## Usage
Interactive menu (single command):
```bash
python -m time_keeper.cli
```

Run via module:
```bash
python -m time_keeper.cli --help
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

## Notes
- New accounts receive 86,400 seconds by default.
- When an account reaches 0 seconds, it is automatically deactivated.
- Admin is identified by the `is_admin` flag. Admin operations require admin authentication.
