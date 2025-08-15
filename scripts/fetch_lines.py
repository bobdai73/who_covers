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
    lines = apis["betting"].get_lines(year=args.year, season_type=args.season)

    rows = []
    for l in lines:
        if not l.lines:
            continue
        spread_val, total_val, provider, updated = None, None, None, None
        for bk in l.lines:
            provider = bk.provider or provider
            for ln in (bk.lines or []):
                if getattr(ln, "spread", None) is not None:
                    spread_val = ln.spread
                    updated = getattr(ln, "last_updated", updated)
                if getattr(ln, "over_under", None) is not None:
                    total_val = ln.over_under
                    updated = getattr(ln, "last_updated", updated)
        rows.append({"game_id": l.game_id, "spread": spread_val, "total": total_val,
                     "provider": provider, "last_updated": updated})

    df = (pd.DataFrame(rows)
          .drop_duplicates(subset=["game_id"], keep="last"))
    out = raw_path(f"lines_{args.year}_{args.season}.parquet")
    save_parquet(df, out)
    print(f"Saved lines ({len(df)}) -> {out}")

if __name__ == "__main__":
    main()
