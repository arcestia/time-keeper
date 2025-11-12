# Time Keeper (Experimental)

Time Keeper is a Python CLI game inspired by the film "In Time". It treats time as a currency: users have balances measured in seconds, and a background worker deducts one second from active accounts every second. When a balance hits zero, the account deactivates.

This repository is a pure experiment and vibe coding sandbox. A more complete version of this idea also exists in Laravel: https://github.com/arcestia/time-keeper-laravel

## What it is
- A core Time Keeper app to manage accounts, balances, admin tools, and a global Time Reserves pool.
- A Time Earner companion that lets users run earning sessions (stake or open), with stat depletion and premium bonuses.
- A Time Store companion for buying consumables that restore stats, with dynamic pricing and personalized premium discounts.

## Premium tiers
- Lifetime progression accumulates as users buy or receive Premium time.
- Active or Lifetime Premium grants tiered benefits:
  - Earn bonus (applies to Time Earner sessions)
  - Store discount (personalized prices)
  - Stat cap increases (affects restores and daily premium restore)

## Time Authority (timezones)
- 12 zones: TZ-12 (lowest) → TZ-1 (richest). Moving up burns a deposit of time, increasing non-linearly.
- Benefits vs cons:
  - Earning multiplier increases with wealth (up to x3 at TZ-1).
  - Store prices get more expensive with wealth (up to x10 at TZ-1).
- Your current timezone and multipliers are shown in app headers. Use the Time Authority menu to view/move.

## Experiment vibe
This is a vibe coding experiment—iterative, exploratory, and fun. Expect rapid evolution and occasional sharp edges.

## Contributing
Contributions are welcome. Open an issue to discuss ideas or send a PR.
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
