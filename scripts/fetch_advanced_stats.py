import argparse
from who_covers.cfbd_client import get_apis
from who_covers.io import raw_path, save_parquet
from who_covers.flatten_advanced import flatten_advanced_team_game_stats
import pandas as pd



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, nargs='+', required=True,
                    help="One or more season years (e.g. --year 2016 2017)")
    ap.add_argument("--season", default="regular", choices=["regular","postseason","both"])
    args = ap.parse_args()

    apis = get_apis()
    # The CFBD StatsApi provides advanced game stats via get_advanced_game_stats
    # (previously attempted to call a non-existent get_advanced_team_game_stats)
    # Accept multiple years and process each year in turn
    for yr in args.year:
        # Fetch FBS games for this year/season and use their ids to filter advanced records
        games = apis["games"].get_games(year=yr, season_type=args.season, classification="fbs")
        games_ids = {g.id for g in games}

        recs = apis["stats"].get_advanced_game_stats(year=yr, season_type=args.season)

        def _rec_game_id(r):
            """Return a record's game id using common attribute names or nested objects."""
            # common possible attribute names
            for attr in ("game_id", "gameId", "id"):
                v = getattr(r, attr, None)
                if v is not None:
                    return v
            # try nested object
            g = getattr(r, "game", None)
            if g is not None:
                return getattr(g, "id", None)
            return None

        # keep only records that match known game ids
        recs = [r for r in recs if _rec_game_id(r) in games_ids]
        df = flatten_advanced_team_game_stats(recs)

        # Ensure at most one row per (game_id, team)
        if not df.empty and {'game_id', 'team'}.issubset(set(df.columns)):
            df = df.drop_duplicates(subset=['game_id', 'team'], keep='last')

        # If a games parquet exists for this year/season, restrict to its game_ids to ensure parity
        games_path = raw_path(f"games_{yr}_{args.season}.parquet")
        if games_path.exists():
            games_df = pd.read_parquet(games_path)
            games_ids = set(games_df['game_id'].astype(int).unique())
            if 'game_id' in df.columns:
                df = df[df['game_id'].astype(int).isin(games_ids)]
            else:
                print(f"Advanced stats dataframe has no 'game_id' column; skipping game_id filter for {yr} {args.season}")
        else:
            print(f"Warning: games file not found at {games_path}; not filtering advanced output")

        out = raw_path(f"advanced_{yr}_{args.season}.parquet")
        save_parquet(df, out)
        print(f"Saved advanced team-game stats ({len(df)}) -> {out}")

if __name__ == "__main__":
    main()
