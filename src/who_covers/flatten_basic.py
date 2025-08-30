import pandas as pd

def flatten_basic_team_game_stats(records) -> pd.DataFrame:
    """Return long DF: (game_id, team, team_conference, stat_name, value)"""
    out = []
    for row in records:
        # Handle per-game payloads from GamesApi.get_game_team_stats
        if hasattr(row, "id") and (hasattr(row, "teams") or (hasattr(row, "to_dict") and "teams" in row.to_dict())):
            game_id = getattr(row, "id")
            teams = row.to_dict().get("teams") if hasattr(row, "to_dict") else getattr(row, "teams", [])
            for t in teams:
                # t is a dict-like structure
                team = t.get("team") or t.get("teamName")
                conf = t.get("conference")
                for stat in t.get("stats", []) or []:
                    # stats entries look like {"category": <name>, "stat": <value>}
                    name = stat.get("category") or stat.get("stat_name") or stat.get("stat")
                    val = stat.get("stat") or stat.get("stat_value")
                    if name is None:
                        continue
                    out.append({
                        "game_id": game_id,
                        "team": team,
                        "team_conference": conf,
                        "stat_name": name,
                        "value": pd.to_numeric(val, errors="coerce"),
                    })
            continue

        # Fallback: handle TeamStat-like objects (season/team aggregates)
        game_id = getattr(row, "game_id", None)
        team = getattr(row, "team", None)
        conf = getattr(row, "conference", None)
        for cat in (getattr(row, "categories", None) or []):
            for stat in (getattr(cat, "types", None) or []):
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
    # Auto-detect metadata columns: anything except the pivot keys and stat/value
    reserved = {"game_id", "team", "stat_name", "value"}
    meta_cols = [c for c in long_df.columns if c not in reserved]

    meta = None
    if meta_cols:
        meta = (long_df[["game_id", "team"] + meta_cols]
                .drop_duplicates(subset=["game_id", "team"]))

    stats = (long_df
             .pivot_table(index=["game_id", "team"], columns="stat_name",
                          values="value", aggfunc="first")
             .reset_index())

    if meta is not None and not meta.empty:
        # Merge metadata back on game_id/team
        stats = stats.merge(meta, on=["game_id", "team"], how="left")

    return stats
