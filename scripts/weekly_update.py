"""Fetch weekly basic and advanced team stats and save per-week parquet files.

Usage: python scripts/weekly_update.py --year 2025 --start-week 1 --end-week 15
"""
import argparse
import time
import sys
import pandas as pd

from who_covers.cfbd_client import get_apis
from who_covers.io import raw_path, save_parquet
from who_covers.flatten_basic import flatten_basic_team_game_stats, pivot_basic
from who_covers.flatten_advanced import flatten_advanced_team_game_stats


def sh_sleep(s):
    try:
        time.sleep(s)
    except KeyboardInterrupt:
        print('Interrupted')
        sys.exit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, nargs='+', default=[2025],
                    help="One or more years to fetch (e.g. --year 2024 2025)")
    ap.add_argument("--season", default="regular", choices=["regular", "postseason", "both"])
    ap.add_argument("--start-week", type=int, default=1)
    ap.add_argument("--end-week", type=int, default=15)
    ap.add_argument("--sleep", type=float, default=0.2, help="Seconds to sleep between API calls")
    args = ap.parse_args()

    apis = get_apis()

    for yr in args.year:
        print(f"Starting week fetch for {yr} {args.season}")
        # derive weeks from FBS games for the year unless user provided explicit range
        games = apis["games"].get_games(year=yr, season_type=args.season, classification="fbs")
        weeks = sorted({g.week for g in games if getattr(g, 'week', None) is not None})
        start = args.start_week or (weeks[0] if weeks else 1)
        end = args.end_week or (weeks[-1] if weeks else start)
    games_ids = {g.id for g in games}
    for wk in range(start, end + 1):
            print(f"  Fetching week {wk} basic stats for {yr} {args.season}")
            try:
                basic_recs = apis["games"].get_game_team_stats(year=yr, week=wk)
                # filter to only FBS games
                basic_recs = [r for r in basic_recs if getattr(r, 'id', None) in games_ids]
            except Exception as e:
                print(f"Failed to fetch basic stats for week {wk}: {e}")
                basic_recs = []

            basic_df = flatten_basic_team_game_stats(basic_recs)
            basic_wide = pivot_basic(basic_df) if not basic_df.empty else basic_df
            if not basic_wide.empty and {'game_id', 'team'}.issubset(set(basic_wide.columns)):
                basic_wide = basic_wide.drop_duplicates(subset=['game_id', 'team'], keep='last')
            basic_out = raw_path(f"basic_{yr}_week{wk}_{args.season}.parquet")
            save_parquet(basic_wide, basic_out)
            print(f"  Saved basic week {wk} -> {basic_out} ({len(basic_wide)})")
            sh_sleep(args.sleep)

            print(f"  Fetching week {wk} advanced stats for {yr} {args.season}")
            try:
                adv_recs = apis["stats"].get_advanced_game_stats(year=yr, week=wk, season_type=args.season)
                adv_recs = [r for r in adv_recs if getattr(r, 'id', None) in games_ids]
            except Exception as e:
                print(f"Failed to fetch advanced stats for week {wk}: {e}")
                adv_recs = []

            adv_df = flatten_advanced_team_game_stats(adv_recs) if adv_recs else None
            if adv_df is not None and not adv_df.empty:
                if {'game_id', 'team'}.issubset(set(adv_df.columns)):
                    adv_df = adv_df.drop_duplicates(subset=['game_id', 'team'], keep='last')
                adv_out = raw_path(f"advanced_{yr}_week{wk}_{args.season}.parquet")
                save_parquet(adv_df, adv_out)
                print(f"  Saved advanced week {wk} -> {adv_out} ({len(adv_df)})")
            else:
                print(f"  No advanced stats for week {wk}")

            sh_sleep(args.sleep)

            # Fetch betting lines for this week (restricted to FBS games for the week)
            print(f"  Fetching week {wk} betting lines for {yr} {args.season}")
            try:
                # prefer an API call that accepts week if available
                try:
                    lines_recs = apis["betting"].get_lines(year=yr, week=wk, season_type=args.season)
                except TypeError:
                    # fallback to year-level call and filter by week
                    lines_recs = apis["betting"].get_lines(year=yr, season_type=args.season)
            except Exception as e:
                print(f"Failed to fetch betting lines for week {wk}: {e}")
                lines_recs = []

            # filter to only the FBS games for this week
            week_game_ids = {g.id for g in games if getattr(g, 'week', None) == wk}
            filtered = []
            for l in (lines_recs or []):
                gid_raw = getattr(l, 'game_id', getattr(l, 'id', None))
                try:
                    gid = int(gid_raw) if gid_raw is not None else None
                except Exception:
                    gid = None
                if gid in week_game_ids:
                    filtered.append((gid, l))

            # Aggregate provider-level lines into a consensus per game (median spread/total)
            rows = []
            def _get_attr(o, *keys):
                # try attribute access then dict-like access for a sequence of possible keys
                for k in keys:
                    try:
                        v = getattr(o, k)
                    except Exception:
                        try:
                            v = o.get(k)
                        except Exception:
                            v = None
                    if v is not None:
                        return v
                return None

            for gid, l in filtered:
                provider_names = []
                spreads = []
                totals = []
                last_updated = None

                # each 'l' may have an outer lines list (books) or be a single line object
                books = _get_attr(l, 'lines') or []
                # if outer books list is empty, attempt to treat l itself as a book-like object
                if not books:
                    books = [l]

                for bk in (books or []):
                    prov = _get_attr(bk, 'provider', 'book', 'source')
                    if prov is not None:
                        provider_names.append(str(prov))

                    # some providers embed an inner 'lines' list, others are direct line objects
                    inner = _get_attr(bk, 'lines') or []
                    if not inner:
                        inner = [bk]

                    for ln in (inner or []):
                        # possible keys for spread/total across different shapes
                        s = _get_attr(ln, 'spread', 'point_spread', 'pointSpread')
                        t = _get_attr(ln, 'overUnder', 'over_under', 'total')
                        u = _get_attr(ln, 'last_updated', 'lastUpdated', 'updated')
                        try:
                            if s is not None:
                                spreads.append(float(s))
                        except Exception:
                            pass
                        try:
                            if t is not None:
                                totals.append(float(t))
                        except Exception:
                            pass
                        if u is not None:
                            last_updated = u

                # compute consensus (median) if we have numeric values
                if spreads or totals or provider_names:
                    spread_med = float(pd.Series(spreads).median()) if spreads else None
                    total_med = float(pd.Series(totals).median()) if totals else None
                    providers_list = ','.join(sorted(set(provider_names))) if provider_names else None
                    num_providers = len(set(provider_names)) if provider_names else 0
                    rows.append({
                        'game_id': gid,
                        'spread': spread_med,
                        'total': total_med,
                        'num_providers': num_providers,
                        'providers_list': providers_list,
                        'last_updated': last_updated,
                        'provider': 'consensus',
                    })

            if rows:
                lines_df = pd.DataFrame(rows).drop_duplicates(subset=['game_id'], keep='last')
                lines_out = raw_path(f"lines_{yr}_week{wk}_{args.season}.parquet")
                save_parquet(lines_df, lines_out)
                print(f"  Saved lines week {wk} -> {lines_out} ({len(lines_df)})")
            else:
                print(f"  No betting lines for week {wk}")


if __name__ == "__main__":
    main()
