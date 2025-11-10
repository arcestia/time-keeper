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
- Added `bulk-create` CLI subcommand to create many accounts for simulations with options for `--count`, `--prefix`, `--start-index`, `--initial` (human‑readable), `--passcode`, and `--admin-frequency`.
- Admin: Added statistics view in interactive menu and `admin --stats` to show totals and top accounts.
- Admin: Time Reserves operations — transfer from reserves to a user (`admin --reserves-transfer-to/--reserves-transfer-amount`) and distribute reserves equally across active users (`admin --reserves-distribute [--reserves-distribute-amount]`). Interactive admin menu includes both actions.
- Tools Dashboard: Users can view personal stats (energy, hunger, water) from the interactive menu.
- Admin: Stats controls — set a single user's stats to 100% or all users' stats to 100% via CLI flags (`admin --set-stats-full <username>`, `admin --set-stats-full-all`) and interactive admin menu.
- New app `time_store`:
  - CLI to list items with qty and effective prices, buy items to restore stats (energy/hunger/water), and manage store as admin.
  - Market index supported in range -50%..+300% (default 0%). Effective price = current_price * (1 + index%).
  - Admin commands to set market index, upsert items (kind, qty, restore values, base price), set qty, and refresh volatile prices.
  - Item display name supported. `upsert-item` accepts `--name`, interactive upsert prompts for name, and list view shows Name.
  - Numeric item IDs added. List shows `ID`; purchases support `--item-id` and interactive buy by ID.
  - Interactive upsert: Item key can be left blank to auto-generate from name (slug) or next numeric id; uniqueness ensured.
  - User inventory: purchases can be applied immediately or stored for later use.
    - CLI `buy` supports `--store` to save to inventory instead of applying.
    - Interactive buy prompts: "Apply now? (Y/n)".
    - New commands: `inventory-list`, `inventory-use` (accept key or ID).
    - Interactive menu: Inventory viewing and using items.

## [0.1.0] - 2025-11-10
- Initial pre-release planning.
