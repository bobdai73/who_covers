import argparse
from pathlib import Path
import pandas as pd

from who_covers.io import raw_path


def ensure_pairs(games_df, df, kind):
    # games_df has columns game_id, home_team, away_team, home_conference, away_conference
    expected = []
    for _, r in games_df.iterrows():
        gid = int(r['game_id'])
        expected.append((gid, r.get('home_team')))
        expected.append((gid, r.get('away_team')))

    existing = set()
    if not df.empty:
        existing = set((int(x), y) for x, y in zip(df['game_id'], df['team']))

    missing = [pair for pair in expected if pair not in existing]
    if not missing:
        return df, 0

    # Prepare template row keys (use existing columns if any)
    cols = list(df.columns) if not df.empty else ['game_id', 'team', 'team_conference']
    rows = []
    games_map = {(int(r['game_id']), r.get('home_team')): ('home_conference',)
                 for _, r in games_df.iterrows()}
    # build conference mapping per (gid, team)
    conf_map = {}
    for _, r in games_df.iterrows():
        gid = int(r['game_id'])
        conf_map[(gid, r.get('home_team'))] = r.get('home_conference')
        conf_map[(gid, r.get('away_team'))] = r.get('away_conference')

    for gid, team in missing:
        row = {c: None for c in cols}
        row['game_id'] = gid
        row['team'] = team
        # if team_conference column exists, populate from games mapping
        if 'team_conference' in cols:
            row['team_conference'] = conf_map.get((gid, team))
        rows.append(row)

    add_df = pd.DataFrame(rows, columns=cols)
    if df.empty:
        result = add_df
    else:
        result = pd.concat([df, add_df], ignore_index=True, sort=False)
    return result, len(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--year', type=int, nargs='+', required=True)
    ap.add_argument('--season', default='regular', choices=['regular', 'postseason', 'both'])
    args = ap.parse_args()

    for yr in args.year:
        games_path = raw_path(f'games_{yr}_{args.season}.parquet')
        if not games_path.exists():
            print(f'skipping {yr}: games file not found at {games_path}')
            continue
        games_df = pd.read_parquet(games_path)

        for kind in ('basic', 'advanced'):
            path = raw_path(f'{kind}_{yr}_{args.season}.parquet')
            if path.exists():
                df = pd.read_parquet(path)
            else:
                df = pd.DataFrame()

            df2, added = ensure_pairs(games_df, df, kind)
            if added:
                df2.to_parquet(path, index=False)
            print(f'{yr} {kind}: added {added} placeholder rows')


if __name__ == '__main__':
    main()
