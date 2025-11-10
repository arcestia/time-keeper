from typing import Iterable, List, Tuple
import re

# Base unit sizes (average-based)
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 60 * SECONDS_PER_MINUTE
SECONDS_PER_DAY = 24 * SECONDS_PER_HOUR
SECONDS_PER_YEAR = 31557600  # 365.25 days (astronomical year)
SECONDS_PER_MONTH = SECONDS_PER_YEAR // 12  # average month length
SECONDS_PER_WEEK = 7 * SECONDS_PER_DAY
SECONDS_PER_DECADE = 10 * SECONDS_PER_YEAR
SECONDS_PER_CENTURY = 100 * SECONDS_PER_YEAR

_UNIT_DEFS = [
    ("century", SECONDS_PER_CENTURY, "c"),
    ("decade", SECONDS_PER_DECADE, "dec"),
    ("year", SECONDS_PER_YEAR, "y"),
    ("month", SECONDS_PER_MONTH, "mo"),
    ("week", SECONDS_PER_WEEK, "w"),
    ("day", SECONDS_PER_DAY, "d"),
    ("hour", SECONDS_PER_HOUR, "h"),
    ("minute", SECONDS_PER_MINUTE, "m"),
    ("second", 1, "s"),
]

_DEF_UNIT_ORDER = tuple(name for name, _, _ in _UNIT_DEFS)
_SIZE_BY_NAME = {name: size for name, size, _ in _UNIT_DEFS}
_ABBR_BY_NAME = {name: abbr for name, _, abbr in _UNIT_DEFS}


def _pluralize(name: str, qty: int, style: str) -> str:
    if style == "short":
        return _ABBR_BY_NAME.get(name, name)
    # long style
    return name if qty == 1 else f"{name}s"


def format_duration(
    seconds: int,
    units: Iterable[str] = _DEF_UNIT_ORDER,
    max_parts: int | None = None,
    style: str = "long",  # "long" => "2 hours", "short" => "2h"
    conjunction: str = " and ",
    separator: str = ", ",
    include_zero: bool = False,
) -> str:
    """Format seconds into a human-readable duration string.

    Parameters:
    - seconds: total seconds (non-negative)
    - units: which units to include and in which order
    - max_parts: limit number of non-zero parts in the result
    - style: "long" or "short"
    - conjunction: joining word for the last two parts (only used if style is long and more than 1 part)
    - separator: separator between parts ("," recommended for long style)
    - include_zero: include zero-valued intermediate parts
    """
    total = max(0, int(seconds))

    # Filter to known units and build ordered list
    order: List[Tuple[str, int]] = [
        (name, _SIZE_BY_NAME[name]) for name in units if name in _SIZE_BY_NAME
    ]

    parts: List[Tuple[str, int]] = []
    for name, size in order:
        if size <= 0:
            continue
        qty, total = divmod(total, size)
        if qty != 0 or include_zero:
            parts.append((name, qty))

    # If everything zero, return 0 in smallest unit
    if not parts:
        smallest = order[-1][0] if order else "second"
        label = _pluralize(smallest, 0, style)
        return f"0{label if style == 'short' else ' ' + label}"

    # Trim to max_parts if provided (only non-zero unless include_zero)
    if max_parts is not None and max_parts > 0:
        if include_zero:
            parts = parts[:max_parts]
        else:
            nz = [(n, q) for (n, q) in parts if q != 0]
            parts = nz[:max_parts] if nz else parts[:1]

    # Build string
    rendered: List[str] = []
    for name, qty in parts:
        label = _pluralize(name, qty, style)
        if style == "short":
            rendered.append(f"{qty}{label}")
        else:
            rendered.append(f"{qty} {label}")

    if style == "short" or len(rendered) <= 1:
        return separator.join(rendered)
    # long style with conjunction for last two parts
    return separator.join(rendered[:-1]) + conjunction + rendered[-1]


_PARSE_UNIT_ALIASES = {
    "s": "second", "sec": "second", "secs": "second", "second": "second", "seconds": "second",
    "m": "minute", "min": "minute", "mins": "minute", "minute": "minute", "minutes": "minute",
    "h": "hour", "hr": "hour", "hrs": "hour", "hour": "hour", "hours": "hour",
    "d": "day", "day": "day", "days": "day",
    "w": "week", "wk": "week", "wks": "week", "week": "week", "weeks": "week",
    "mo": "month", "mon": "month", "mons": "month", "month": "month", "months": "month",
    "y": "year", "yr": "year", "yrs": "year", "year": "year", "years": "year",
    "dec": "decade", "decade": "decade", "decades": "decade",
    "c": "century", "cent": "century", "century": "century", "centuries": "century",
}

def parse_duration(text: str) -> int:
    """Parse a human-readable duration string into total seconds.

    Examples:
        '90' -> 90 seconds
        '1h 30m' -> 5400
        '1y 2mo 3d 4h 5m 6s' -> seconds
        '2w, 3d' -> seconds
        '1dec 5y' -> seconds
    """
    if text is None:
        raise ValueError("duration text is None")
    s = text.strip().lower().replace(",", " ")
    if not s:
        raise ValueError("empty duration")
    # If it's a pure integer, treat as seconds
    if re.fullmatch(r"\d+", s):
        return int(s)

    total = 0
    # Match sequences like '12h', '12 h', '3days', '5 mo'
    for qty, unit in re.findall(r"(\d+)\s*([a-zA-Z]+)?", s):
        q = int(qty)
        uname = _PARSE_UNIT_ALIASES.get(unit, None) if unit else "second"
        if not uname or uname not in _SIZE_BY_NAME:
            raise ValueError(f"unknown unit: {unit}")
        total += q * _SIZE_BY_NAME[uname]
    return total
