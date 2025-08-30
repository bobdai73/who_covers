"""Create a per-week summary by joining basic, advanced, and lines datasets.

Usage: python scripts/weekly_summary.py --year 2025 --start-week 1 --end-week 15
"""
import argparse
from pathlib import Path
import pandas as pd

from who_covers.io import raw_path, processed_path


def load_if_exists(path: Path):
    if path.exists():
        return pd.read_parquet(path)
    return None


def summarize_week(year: int, week: int, season: str):
    basic_p = raw_path(f"basic_{year}_week{week}_{season}.parquet")
    adv_p = raw_path(f"advanced_{year}_week{week}_{season}.parquet")
    lines_p = raw_path(f"lines_{year}_week{week}_{season}.parquet")

    basic = load_if_exists(basic_p)
    adv = load_if_exists(adv_p)
    lines = load_if_exists(lines_p)

    if basic is None and adv is None:
        print(f"  nothing to summarize for {year} week {week}")
        return False

    # Merge basic and advanced on game_id/team
    if basic is None:
        merged = adv.copy()
    elif adv is None:
        merged = basic.copy()
    else:
        # prefer values from basic when conflicts exist; use outer to preserve any mismatched rows
        merged = pd.merge(basic, adv, on=["game_id", "team"], how="outer", suffixes=("", "_adv"))

    # Attach lines (game-level) to each team row by merging on game_id
    if lines is not None and not lines.empty:
        merged = merged.merge(lines, on="game_id", how="left", suffixes=(None, "_line"))

    # Normalize column order: game_id, team, then other columns
    cols = list(merged.columns)
    ordered = [c for c in ["game_id", "team"] if c in cols] + [c for c in cols if c not in ("game_id", "team")]
    merged = merged[ordered]

    out = processed_path(f"weekly_{year}_week{week}_{season}.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(out, index=False)
    print(f"  saved weekly summary -> {out} ({len(merged)})")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, nargs='+', required=True)
    ap.add_argument("--start-week", type=int, default=1)
    ap.add_argument("--end-week", type=int, default=15)
    ap.add_argument("--season", default="regular", choices=["regular", "postseason", "both"])
    args = ap.parse_args()

    for yr in args.year:
        print(f"Processing year {yr} {args.season} weeks {args.start_week}-{args.end_week}")
        for wk in range(args.start_week, args.end_week + 1):
            summarize_week(yr, wk, args.season)


if __name__ == '__main__':
    main()
