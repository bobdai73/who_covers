import pandas as pd

def _flatten_ns(obj, ns: str, row: dict):
    if not obj:
        return
    d = obj.to_dict() if hasattr(obj, "to_dict") else {}
    for k, v in d.items():
        if isinstance(v, dict):
            for kk, vv in v.items():
                row[f"{ns}_{k}_{kk}"] = vv
        else:
            row[f"{ns}_{k}"] = v

def flatten_advanced_team_game_stats(records) -> pd.DataFrame:
    rows = []
    for a in records:
        row = {"game_id": a.game_id, "team": a.team}
        _flatten_ns(getattr(a, "offense", None), "off", row)
        _flatten_ns(getattr(a, "defense", None), "def", row)
        _flatten_ns(getattr(a, "special_teams", None), "st", row)
        rows.append(row)
    df = pd.DataFrame(rows).drop_duplicates(subset=["game_id", "team"])
    for c in df.columns:
        if c in ("game_id", "team"):
            continue
        # Attempt to convert to numeric; if conversion fails, keep original values.
        try:
            df[c] = pd.to_numeric(df[c])
        except Exception:
            # Leave column as-is (non-numeric)
            pass
    return df
