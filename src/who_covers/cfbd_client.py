import os
import cfbd
from cfbd import Configuration, ApiClient

def make_client():
    api_key = os.getenv("CFBD_API_KEY")
    if not api_key:
        raise RuntimeError("Set CFBD_API_KEY in your environment.")
    cfg = Configuration(
      access_token = api_key
    )
    return ApiClient(cfg)

def get_apis(client=None):
    client = client or make_client()
    return dict(
        games=cfbd.GamesApi(client),
        stats=cfbd.StatsApi(client),
        betting=cfbd.BettingApi(client),
        teams=cfbd.TeamsApi(client),
    )
