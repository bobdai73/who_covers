import pandas as pd

def flatten_basic_team_game_stats(records) -> pd.DataFrame:
    """Return long DF: (game_id, team, team_conference, stat_name, value)"""
    out = []
    for row in records:
        game_id = row.game_id
        team = row.team
        conf = getattr(row, "conference", None)
        for cat in (row.categories or []):
            for stat in getattr(cat, "types", []) or []:
                name = getattr(stat, "stat", None)
                val = getattr(stat, "stat_value", None)
                if name is None:
                    continue
                out.append({
                    "game_id": game_id,
                    "team": team,
                    "team_conference": conf,
                    "stat_name": name,
                    "value": pd.to_numeric(val, errors="coerce"),
                })
    return pd.DataFrame(out)

def pivot_basic(long_df: pd.DataFrame) -> pd.DataFrame:
    return (long_df
            .pivot_table(index=["game_id", "team"], columns="stat_name",
                         values="value", aggfunc="first")
            .reset_index())
