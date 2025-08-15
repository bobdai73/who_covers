import argparse
import pandas as pd
import numpy as np
from who_covers.io import raw_path, processed_path, save_parquet, save_csv

def _side_map(g):
    m = {}
    for _, r in g[["game_id", "home_team", "away_team"]].iterrows():
        m[(r.game_id, r.home_team)] = "home"
        m[(r.game_id, r.away_team)] = "away"
    return m

def _prefix_side(df_teamwide: pd.DataFrame, which: str) -> pd.DataFrame:
    sub = df_teamwide[df_teamwide["side"] == which].drop(columns=["side"]).copy()
    sub = sub.rename(columns={"team": f"{which}_team"})
    value_cols = [c for c in sub.columns if c not in ("game_id", f"{which}_team")]
    return sub.rename(columns={c: f"{which}_{c}" for c in value_cols})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--season", default="regular", choices=["regular","postseason","both"])
    ap.add_argument("--with-lines", action="store_true")
    args = ap.parse_args()

    games = pd.read_parquet(raw_path(f"games_{args.year}_{args.season}.parquet"))
    basic = pd.read_parquet(raw_path(f"basic_{args.year}_{args.season}.parquet"))
    adv   = pd.read_parquet(raw_path(f"advanced_{args.year}_{args.season}.parquet"))

    side_map = _side_map(games)
    basic["side"] = basic.apply(lambda r: side_map.get((r["game_id"], r["team"])), axis=1)
    adv["side"]   = adv.apply(lambda r: side_map.get((r["game_id"], r["team"])), axis=1)

    basic_home = _prefix_side(basic, "home")
    basic_away = _prefix_side(basic, "away")
    adv_home   = _prefix_side(adv, "home")
    adv_away   = _prefix_side(adv, "away")

    basic_merged = basic_home.merge(basic_away, on="game_id", how="outer")
    adv_merged   = adv_home.merge(adv_away, on="game_id", how="outer")

    base_cols = [
        "game_id","season","season_type","week","start_date",
        "home_team","away_team","home_points","away_points",
        "home_conference","away_conference","conference_game","neutral_site","venue"
    ]
    base = games[base_cols].copy()

    df = (base
          .merge(basic_merged, on="game_id", how="left")
          .merge(adv_merged,   on="game_id", how="left"))

    if args.with_lines:
        try:
            lines = pd.read_parquet(raw_path(f"lines_{args.year}_{args.season}.parquet"))
            df = df.merge(lines[["game_id","spread","total"]], on="game_id", how="left")
        except FileNotFoundError:
            pass

    for c in df.columns:
        if c.startswith(("home_", "away_", "spread", "total")):
            df[c] = pd.to_numeric(df[c], errors="ignore")

    df["point_diff"] = df["home_points"] - df["away_points"]
    df["favorite"] = np.where(df.get("spread").notna(),
                              np.where(df["spread"] < 0, "home", "away"),
                              pd.NA)

    out_parq = processed_path(f"games_wide_{args.year}_{args.season}.parquet")
    save_parquet(df, out_parq)

    if args.csv.gz:
        out_csv_gz = processed_path(f"games_wide_{args.year}_{args.season}
        df.to_csv(out_csv_gz, index =False, compression="gzip")
        print(f"Saved Dataset -> {out_parq\nSaved CSV.GZ -> {out_csv_gz}\nRows: {len(df)}, Cols: {df.shape[1]")
    else: 
        out_csv = processed_path(f"games_wide_{args.year}_{args.season}.csv")
        save_csv(df, out_csv)
        print(f"Saved dataset -> {out_parq}\nSaved CSV -> {out_csv}\nRows: {len(df)}, Cols: {df.shape[1]}")

if __name__ == "__main__":
    main()
