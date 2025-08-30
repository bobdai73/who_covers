import argparse
import pandas as pd
from who_covers.cfbd_client import get_apis
from who_covers.io import raw_path, save_parquet

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, nargs='+', required=True,
                    help="One or more season years (e.g. --year 2016 2017)")
    ap.add_argument("--season", default="regular", choices=["regular","postseason","both"])
    args = ap.parse_args()

    apis = get_apis()

    for yr in args.year:
        # fetch FBS games for this year/season and use ids to filter lines
        games = apis["games"].get_games(year=yr, season_type=args.season, classification="fbs")
        games_ids = {int(g.id) for g in games}

        lines = apis["betting"].get_lines(year=yr, season_type=args.season)

        rows = []
        def to_number(x):
            if x is None:
                return None
            try:
                # strip percent or plus signs and unicode minus
                s = str(x).strip()
                s = s.replace('\u2212', '-')
                s = s.replace('+', '')
                return float(s)
            except Exception:
                return None

        for l in lines:
            # normalize game id and skip non-FBS entries
            gid_raw = getattr(l, 'game_id', getattr(l, 'id', None))
            try:
                gid = int(gid_raw) if gid_raw is not None else None
            except Exception:
                gid = None
            if gid is None or gid not in games_ids:
                continue

            # some providers may not set lines; skip if none
            if not getattr(l, 'lines', None):
                continue

            spread_val, total_val, provider, updated = None, None, None, None
            for bk in (l.lines or []):
                bk_provider = getattr(bk, 'provider', None) or (bk.get('provider') if isinstance(bk, dict) else None)
                # iterate inner lines; some providers expose a 'lines' list, others a flat 'line' or dict
                # If bk itself contains spread/overUnder, treat it as the line
                bk_dict = None
                try:
                    if hasattr(bk, 'to_dict'):
                        bk_dict = bk.to_dict()
                    elif isinstance(bk, dict):
                        bk_dict = bk
                except Exception:
                    bk_dict = None

                # first try extracting directly from bk_dict (common keys)
                cand_spread = None
                cand_total = None
                cand_updated = None
                if bk_dict:
                    for k in ('spread', 'point_spread', 'line', 'handicap'):
                        if k in bk_dict and bk_dict[k] is not None:
                            cand_spread = bk_dict[k]
                            break
                    for k in ('overUnder', 'over_under', 'total', 'ou'):
                        if k in bk_dict and bk_dict[k] is not None:
                            cand_total = bk_dict[k]
                            break
                    cand_updated = bk_dict.get('last_updated') or bk_dict.get('updated')

                # fallback to attribute access on bk
                if cand_spread is None:
                    cand_spread = getattr(bk, 'spread', None) or getattr(bk, 'point_spread', None) or getattr(bk, 'line', None)
                if cand_total is None:
                    cand_total = getattr(bk, 'overUnder', None) or getattr(bk, 'over_under', None) or getattr(bk, 'total', None)
                if cand_updated is None:
                    cand_updated = getattr(bk, 'last_updated', None) or getattr(bk, 'updated', None)

                # coerce to numeric where possible
                s = to_number(cand_spread) if cand_spread is not None else None
                t = to_number(cand_total) if cand_total is not None else None
                if s is not None and spread_val is None:
                    spread_val = s
                    provider = bk_provider or provider
                if t is not None and total_val is None:
                    total_val = t
                    provider = bk_provider or provider
                updated = updated or cand_updated

                # if bk had an inner list (unlikely here), also inspect those
                inner = getattr(bk, 'lines', None) or (bk_dict.get('lines') if isinstance(bk_dict, dict) else None)
                if inner:
                    for ln in inner:
                        if ln is None:
                            continue
                        try:
                            ln_d = ln.to_dict() if hasattr(ln, 'to_dict') else (ln if isinstance(ln, dict) else None)
                        except Exception:
                            ln_d = None
                        if ln_d:
                            s2 = to_number(ln_d.get('spread') or ln_d.get('point_spread') or ln_d.get('line') or ln_d.get('handicap'))
                            t2 = to_number(ln_d.get('overUnder') or ln_d.get('over_under') or ln_d.get('total') or ln_d.get('ou'))
                            if s2 is not None and spread_val is None:
                                spread_val = s2
                                provider = bk_provider or provider
                            if t2 is not None and total_val is None:
                                total_val = t2
                                provider = bk_provider or provider

                rows.append({"game_id": gid, "spread": spread_val, "total": total_val,
                             "provider": provider, "last_updated": updated})

            df = pd.DataFrame(rows)
            if df.empty:
                out = raw_path(f"lines_{yr}_{args.season}.parquet")
                save_parquet(df, out)
                print(f"Saved lines (0) -> {out}")
                continue

            # compute consensus per game: median spread and median total across providers
            def med_or_none(s):
                s2 = s.dropna()
                return float(s2.median()) if not s2.empty else None

            grouped = df.groupby('game_id').agg(
                spread_consensus=('spread', med_or_none),
                total_consensus=('total', med_or_none),
                num_providers=('provider', lambda x: int(sum(1 for v in x if v))),
                providers_list=('provider', lambda x: ','.join(sorted(set([v for v in x if v])))),
                last_updated=('last_updated', lambda x: max([v for v in x if v is not None]) if any(v is not None for v in x) else None)
            ).reset_index()

            # normalize column names to match previous API
            grouped = grouped.rename(columns={'spread_consensus': 'spread', 'total_consensus': 'total'})
            grouped['provider'] = 'consensus'

            out = raw_path(f"lines_{yr}_{args.season}.parquet")
            save_parquet(grouped, out)
            print(f"Saved lines ({len(grouped)}) -> {out}")

if __name__ == "__main__":
    main()
