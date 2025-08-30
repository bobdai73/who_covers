"""Merge games/basic/advanced/lines into a per-year dataset and save to data/processed.

Saves files as: data/processed/{year}_stats.parquet

Usage: python scripts/yearly_summary.py --year 2017 2018 --season regular
"""
import argparse
from pathlib import Path
import pandas as pd

from who_covers.io import raw_path, processed_path


def load_parquet_if(path: Path):
    if path.exists():
        return pd.read_parquet(path)
    return None


def merge_year(year: int, season: str):
    games_p = raw_path(f"games_{year}_{season}.parquet")
    basic_p = raw_path(f"basic_{year}_{season}.parquet")
    adv_p = raw_path(f"advanced_{year}_{season}.parquet")
    lines_p = raw_path(f"lines_{year}_{season}.parquet")

    games = load_parquet_if(games_p)
    basic = load_parquet_if(basic_p)
    adv = load_parquet_if(adv_p)
    lines = load_parquet_if(lines_p)

    if games is None and basic is None and adv is None and lines is None:
        print(f"No source files for {year} {season}, skipping")
        return False

    # Merge team-level basic and advanced on (game_id, team)
    if basic is None and adv is None:
        team = None
    elif basic is None:
        team = adv.copy()
    elif adv is None:
        team = basic.copy()
    else:
        team = pd.merge(basic, adv, on=["game_id", "team"], how="outer", suffixes=("", "_adv"))

    # If we have team rows, left-join games/meta and lines to each team row
    if team is not None:
        merged = team.copy()
        if games is not None:
            merged = merged.merge(games, on="game_id", how="left", suffixes=(None, "_game"))
        if lines is not None:
            keep = [c for c in ['game_id', 'spread', 'total', 'num_providers', 'providers_list', 'last_updated', 'provider'] if c in lines.columns]
            lines_sel = lines[keep].copy() if keep else lines.copy()
            merged = merged.merge(lines_sel, on="game_id", how="left", suffixes=(None, "_line"))
    else:
        # No team-level data: create a game-level merged dataset
        merged = pd.DataFrame()
        if games is not None:
            merged = games.copy()
        if lines is not None:
            keep = [c for c in ['game_id', 'spread', 'total', 'num_providers', 'providers_list', 'last_updated', 'provider'] if c in lines.columns]
            lines_sel = lines[keep].copy() if keep else lines.copy()
            if merged.empty:
                merged = lines_sel.copy()
            else:
                merged = merged.merge(lines_sel, on="game_id", how="left")

    # Reorder columns to put game_id, team, home/away first when present
    cols = list(merged.columns)
    front = []
    for c in ("game_id", "team", "home_team", "away_team"):
        if c in cols:
            front.append(c)
    remaining = [c for c in cols if c not in front]
    merged = merged[front + remaining]

    # Save to processed_path as {year}_stats.parquet
    out = processed_path(f"{year}_stats.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(out, index=False)
    print(f"Saved {year} merged stats -> {out} ({len(merged)} rows)")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, nargs='+', required=True)
    ap.add_argument("--season", default="regular", choices=["regular", "postseason", "both"])
    args = ap.parse_args()

    for yr in args.year:
        merge_year(yr, args.season)


if __name__ == '__main__':
    main()
