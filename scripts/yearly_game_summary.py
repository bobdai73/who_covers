"""Produce per-year game-level datasets: one row per game with home/away team stats.

Usage: python scripts/yearly_game_summary.py --year 2017 2018 --season regular
"""
import argparse
from pathlib import Path
import pandas as pd

from who_covers.io import raw_path, processed_path


def load_if(path: Path):
    return pd.read_parquet(path) if path.exists() else None


def make_game_level(year: int, season: str):
    games_p = raw_path(f"games_{year}_{season}.parquet")
    basic_p = raw_path(f"basic_{year}_{season}.parquet")
    adv_p = raw_path(f"advanced_{year}_{season}.parquet")
    lines_p = raw_path(f"lines_{year}_{season}.parquet")

    games = load_if(games_p)
    if games is None:
        print(f"games file missing for {year} {season}, skipping")
        return False

    basic = load_if(basic_p)
    adv = load_if(adv_p)
    lines = load_if(lines_p)

    # build team-level combined DF
    if basic is None and adv is None:
        team = pd.DataFrame()
    elif basic is None:
        team = adv.copy()
    elif adv is None:
        team = basic.copy()
    else:
        team = pd.merge(basic, adv, on=["game_id", "team"], how="outer", suffixes=("", "_adv"))

    # Merge home team stats
    g = games.copy()
    # ensure game_id type consistency
    g['game_id'] = g['game_id'].astype(int)

    if not team.empty:
        # Prepare home-prefixed team dataframe
        home_df = team.copy()
        home_cols = [c for c in home_df.columns if c not in ('game_id', 'team')]
        home_rename = {c: f'home_{c}' for c in home_cols}
        home_df = home_df.rename(columns=home_rename)
        # merge home stats on game_id + home_team
        merged = g.merge(home_df, left_on=['game_id', 'home_team'], right_on=['game_id', 'team'], how='left')
        # drop the duplicate 'team' column that came from home_df
        if 'team' in merged.columns:
            merged = merged.drop(columns=['team'])

        # Prepare away-prefixed team dataframe
        away_df = team.copy()
        away_cols = [c for c in away_df.columns if c not in ('game_id', 'team')]
        away_rename = {c: f'away_{c}' for c in away_cols}
        away_df = away_df.rename(columns=away_rename)
        # merge away stats on game_id + away_team
        merged = merged.merge(away_df, left_on=['game_id', 'away_team'], right_on=['game_id', 'team'], how='left')
        # drop the duplicate 'team' column that came from away_df
        if 'team' in merged.columns:
            merged = merged.drop(columns=['team'])
    else:
        merged = g

    # attach lines (game-level)
    if lines is not None and not lines.empty:
        # prefer only the consensus fields from lines to avoid pulling unexpected shapes
        keep = [c for c in ['game_id', 'spread', 'total', 'num_providers', 'providers_list', 'last_updated', 'provider'] if c in lines.columns]
        lines_sel = lines[keep].copy() if keep else lines.copy()
        merged = merged.merge(lines_sel, on='game_id', how='left', suffixes=(None, '_line'))

    # Reorder: game_id, home_team, away_team, then rest
    cols = list(merged.columns)
    front = [c for c in ['game_id', 'home_team', 'away_team'] if c in cols]
    rest = [c for c in cols if c not in front]
    merged = merged[front + rest]

    out = processed_path(f"{year}_game_stats.parquet")
    out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(out, index=False)
    print(f"Saved game-level {year} -> {out} ({len(merged)} rows)")
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--year', type=int, nargs='+', required=True)
    ap.add_argument('--season', default='regular', choices=['regular','postseason','both'])
    args = ap.parse_args()
    for yr in args.year:
        make_game_level(yr, args.season)


if __name__ == '__main__':
    main()
