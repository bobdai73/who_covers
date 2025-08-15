import argparse
from who_covers.cfbd_client import get_apis
from who_covers.io import raw_path, save_parquet
from who_covers.flatten_advanced import flatten_advanced_team_game_stats

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--season", default="regular", choices=["regular","postseason","both"])
    args = ap.parse_args()

    apis = get_apis()
    recs = apis["stats"].get_advanced_team_game_stats(year=args.year, season_type=args.season)
    df = flatten_advanced_team_game_stats(recs)

    out = raw_path(f"advanced_{args.year}_{args.season}.parquet")
    save_parquet(df, out)
    print(f"Saved advanced team-game stats ({len(df)}) -> {out}")

if __name__ == "__main__":
    main()
