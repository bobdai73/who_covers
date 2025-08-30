import pandas as pd
from types import SimpleNamespace

from who_covers.flatten_basic import flatten_basic_team_game_stats, pivot_basic


def make_stat(name, val):
    return SimpleNamespace(stat=name, stat_value=val)


def make_cat(types):
    return SimpleNamespace(types=types)


def make_rec(game_id, team, conf, cats):
    return SimpleNamespace(game_id=game_id, team=team, conference=conf, categories=cats)


def test_flatten_and_pivot_preserves_metadata_and_rows():
    # two teams in same game
    recs = [
        make_rec(1, "TeamA", "ConfA", [make_cat([make_stat("yards", 120), make_stat("turnovers", 1)])]),
        make_rec(1, "TeamB", "ConfB", [make_cat([make_stat("yards", 95), make_stat("turnovers", 2)])]),
    ]

    long_df = flatten_basic_team_game_stats(recs)
    assert not long_df.empty

    wide_df = pivot_basic(long_df)

    # Expect exactly two rows (one per team)
    assert len(wide_df) == 2

    # Both stat columns should be present
    assert "yards" in wide_df.columns
    assert "turnovers" in wide_df.columns

    # Metadata column should be preserved and correct for TeamA
    row_a = wide_df[wide_df["team"] == "TeamA"].iloc[0]
    assert row_a["team_conference"] == "ConfA"
    # Numeric conversion should have taken place
    assert int(row_a["yards"]) == 120
