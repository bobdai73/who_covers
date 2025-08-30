import argparse
import pandas as pd
from who_covers.cfbd_client import get_apis
from who_covers.io import raw_path, save_parquet

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, nargs='+', required=True,
                    help="One or more years (e.g. --year 2016 2017)")
    ap.add_argument("--season", default="regular", choices=["regular","postseason","both"])
    args = ap.parse_args()

    apis = get_apis()
    for yr in args.year:
        # Only fetch FBS regular/postseason games to keep datasets consistent
        games = apis["games"].get_games(year=yr, season_type=args.season, classification="fbs")

        rows = []
        for g in games:
            rows.append({
                "game_id": g.id,
                "season": g.season,
                "week": g.week,
                "season_type": g.season_type,
                "conference_game": getattr(g, "conference_game", None),
                "neutral_site": getattr(g, "neutral_site", None),
                "venue": getattr(g, "venue", None),
                "home_team": getattr(g, "home_team", None),
                "home_points": getattr(g, "home_points", None),
                "away_team": getattr(g, "away_team", None),
                "away_points": getattr(g, "away_points", None),
                "home_conference": getattr(g, "home_conference", None),
                "away_conference": getattr(g, "away_conference", None),
                "start_date": pd.to_datetime(getattr(g, "start_date", None), errors="coerce"),
            })
        df = pd.DataFrame(rows).drop_duplicates(subset=["game_id"]) if rows else pd.DataFrame()
        out = raw_path(f"games_{yr}_{args.season}.parquet")
        save_parquet(df, out)
        print(f"Saved {len(df)} games -> {out}")

if __name__ == "__main__":
    main()
