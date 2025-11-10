# Contributing to Time Keeper

Thanks for your interest in contributing! This document outlines how to set up your environment, coding standards, and the workflow for proposing changes.

## Getting Started
- Requirements: Python 3.9+
- Create a virtual environment and install deps:
  ```bash
  python -m venv .venv
  . .venv/Scripts/activate
  pip install -r requirements.txt
  ```

## Project Layout
- `time_keeper/` main CLI library and app
  - `db.py` database access and transactions
  - `auth.py` passcode hashing/verification
  - `worker.py` background tick logic
  - `cli.py` command-line interface and interactive menu
  - `formatting.py` duration formatting/parsing
- `time_earner/` companion CLI for earning sessions
- `README.md` usage guide
- `summary.md` high-level overview
- `LOGIC_DESIGN.md` replication-focused design doc

## Changelog and Versioning
- We follow Keep a Changelog format and Semantic Versioning 2.0.0.
- Update `CHANGELOG.md` under the Unreleased section for every change you make.
- If a change is made but not released, and you make additional changes to it, append another line in Unreleased describing the new change.

## Code Style
- Python code should be clear and readable; prefer explicit over implicit.
- Keep imports at the top of files.
- Add helpful, concise log/print messages for CLI feedback; avoid noisy output.
- UI strings: keep them short and consistent; use `colorama` for emphasis where helpful.

## Database and Transactions
- All balance mutations (deduct, transfer, earn) must happen inside a single transaction (`BEGIN IMMEDIATE`) and either commit or rollback.
- Never split a logical update across multiple transactions.
- Return post-state data (e.g., new balances) from DB functions when appropriate.

## Duration Utilities
- Use `time_keeper/formatting.py` for all duration formatting/parsing.
- Short style is the default for user-facing output (e.g., `1y, 2mo`).

## Background Worker
- The worker must update Time Reserves in the same transaction as deductions.
- When changing worker behavior, verify background mode PID/log handling on Windows.

## Adding Features
- Put core logic inside `db.py` as atomic helpers; call them from CLI code.
- For new units or parsing rules, extend `formatting.py` and add tests/notes.
- Keep interactive menus session-aware and minimal.

## Submitting Changes
1. Create a feature branch.
2. Make changes with small, focused commits.
3. Update `CHANGELOG.md` (Unreleased) and docs where applicable.
4. Open a pull request describing the motivation, approach, and testing.
5. Respond to review feedback promptly.

## Reporting Issues
- Include environment info, steps to reproduce, expected vs actual, and logs/screenshots.

Thanks for helping improve Time Keeper!
