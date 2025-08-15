import argparse
from who_covers.cfbd_client import get_apis
from who_covers.io import raw_path, save_parquet
from who_covers.flatten_basic import flatten_basic_team_game_stats, pivot_basic

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--season", default="regular", choices=["regular","postseason","both"])
    args = ap.parse_args()

    apis = get_apis()
    recs = apis["stats"].get_team_game_stats(year=args.year, season_type=args.season)
    long_df = flatten_basic_team_game_stats(recs)
    wide_df = pivot_basic(long_df)

    out = raw_path(f"basic_{args.year}_{args.season}.parquet")
    save_parquet(wide_df, out)
    print(f"Saved basic team-game stats ({len(wide_df)}) -> {out}")

if __name__ == "__main__":
    main()
