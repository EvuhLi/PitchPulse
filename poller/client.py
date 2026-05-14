import os
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://v3.football.api-sports.io"

headers = {
    "x-apisports-key": os.getenv("API_SPORTS_KEY")
}


def _get(url, params):
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 429:
        print("Rate limit hit — stopping. Try again tomorrow or reduce polling frequency.")
        raise SystemExit(1)
    response.raise_for_status()
    return response.json()["response"]


def get_live_fixtures():
    return _get(f"{BASE_URL}/fixtures", {"live": "all"})


def get_fixture_events(fixture_id):
    return _get(f"{BASE_URL}/fixtures/events", {"fixture": fixture_id})


def get_fixture_statistics(fixture_id):
    return _get(f"{BASE_URL}/fixtures/statistics", {"fixture": fixture_id})
