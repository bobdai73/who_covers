import argparse
from who_covers.cfbd_client import get_apis
from who_covers.io import raw_path, save_parquet
import pandas as pd
from who_covers.flatten_basic import flatten_basic_team_game_stats, pivot_basic

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, nargs='+', required=True,
                    help="One or more season years (e.g. --year 2016 2017)")
    ap.add_argument("--season", default="regular", choices=["regular","postseason","both"])
    args = ap.parse_args()
    apis = get_apis()
    # Accept multiple years and process each one
    for yr in args.year:
        # Fetch game list (FBS only) and then fetch per-game team stats by game id to satisfy API validation
        games = apis["games"].get_games(year=yr, season_type=args.season, classification="fbs")
        # Prefer fetching by week to reduce number of API calls (one call per week instead of per game)
        weeks = sorted({g.week for g in games if getattr(g, 'week', None) is not None})
        all_recs = []
        import time
        # Precompute set of FBS game ids for this year/season so we can filter
        games_ids = {g.id for g in games}
        for wk in weeks:
            try:
                recs = apis["games"].get_game_team_stats(year=yr, week=wk)
                # Filter to only FBS games (some week calls return other classifications)
                recs = [r for r in recs if getattr(r, 'id', None) in games_ids]
                all_recs.extend(recs)
            except Exception as e:
                print(f"Warning: failed to fetch team stats for year={yr} week={wk}: {e}")
            # small pause to avoid hitting burst rate limits
            time.sleep(0.2)
        long_df = flatten_basic_team_game_stats(all_recs)
        wide_df = pivot_basic(long_df)

        # Ensure there's at most one row per (game_id, team)
        if not wide_df.empty and {'game_id', 'team'}.issubset(set(wide_df.columns)):
            wide_df = wide_df.drop_duplicates(subset=['game_id', 'team'], keep='last')

        # If a games parquet exists for this year/season, restrict to its game_ids to ensure parity
        games_path = raw_path(f"games_{yr}_{args.season}.parquet")
        if games_path.exists():
            games_df = pd.read_parquet(games_path)
            games_ids = set(games_df['game_id'].astype(int).unique())
            wide_df = wide_df[wide_df['game_id'].astype(int).isin(games_ids)]
        else:
            print(f"Warning: games file not found at {games_path}; not filtering basic output")

        out = raw_path(f"basic_{yr}_{args.season}.parquet")
        save_parquet(wide_df, out)
        print(f"Saved basic team-game stats ({len(wide_df)}) -> {out}")

if __name__ == "__main__":
    main()
