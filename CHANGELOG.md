# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning 2.0.0.

## [Unreleased]
- Added initial project scaffolding for Time Keeper CLI.
- Implemented SQLite schema and DB utilities.
- Implemented PBKDF2-HMAC passcode hashing and verification.
- Added CLI with commands: `init-db`, `create-account`, `login`, `admin`, `leaderboard`, `run-worker`.
- Implemented background worker to deduct time every second and deactivate accounts at zero.
- Added README with setup and usage.
 - Added interactive menu mode (default when no subcommand is provided).
 - Interactive menu is session-aware: options change after login; admin options appear only for admin users.
 - Interactive UI: removed DB path from header; added colorized output via colorama.
 - Leaderboard now shown as a clean table with human-readable time (e.g., "1 day 2 hours and 3 seconds").
 - Standardized duration formatting with a single utility (`time_keeper/formatting.py`) and refactored CLI to use it.
 - Added .gitignore entries for worker PID/log files and local SQLite DB.
 - Balances in login messages, interactive header, and refresh are now human-readable time.
 - Interactive create-account uses human-readable duration for initial time (e.g., "1d 2h 30m"); added duration parser.
 - New CLI app `time_earner`: users can login and earn time to their balance (interactive and non-interactive modes).
 - Time Reserves: deducted seconds are accumulated in a pool; admins can view reserves via CLI and interactive.
 - time_earner interactive: earning is a timed session that stakes the chosen amount, counts down, forfeits on early exit, and pays double on successful completion.
 - Docs: Added `summary.md` (project summary) and `LOGIC_DESIGN.md` (replication-focused logic design).

## [0.1.0] - 2025-11-10
- Initial pre-release planning.
