import argparse
import pandas as pd
from who_covers.cfbd_client import get_apis
from who_covers.io import raw_path, save_parquet

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--season", default="regular", choices=["regular","postseason","both"])
    args = ap.parse_args()

    apis = get_apis()
    games = apis["games"].get_games(year=args.year, season_type=args.season)

    rows = []
    for g in games:
        rows.append({
            "game_id": g.id,
            "season": g.season,
            "week": g.week,
            "season_type": g.season_type,
            "conference_game": g.conference_game,
            "neutral_site": g.neutral_site,
            "venue": getattr(g, "venue", None),
            "home_team": g.home_team,
            "home_points": g.home_points,
            "away_team": g.away_team,
            "away_points": g.away_points,
            "home_conference": getattr(g, "home_conference", None),
            "away_conference": getattr(g, "away_conference", None),
            "start_date": pd.to_datetime(getattr(g, "start_date", None)),
        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["game_id"])
    out = raw_path(f"games_{args.year}_{args.season}.parquet")
    save_parquet(df, out)
    print(f"Saved {len(df)} games -> {out}")

if __name__ == "__main__":
    main()
