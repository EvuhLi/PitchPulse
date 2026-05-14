import time
import json
import redis
import os
from dotenv import load_dotenv
from client import get_live_fixtures, get_fixture_events, get_fixture_statistics

load_dotenv()

POLL_INTERVAL = 60
STREAM_NAME = "pitchpulse:events"

r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))

def pick_fixture(fixtures):
    print("\nLive matches:")
    for i, f in enumerate(fixtures):
        home = f["teams"]["home"]["name"]
        away = f["teams"]["away"]["name"]
        score = f["goals"]
        minute = f["fixture"]["status"]["elapsed"]
        print(f"  [{i+1}] {minute}' | {home} {score['home']} - {score['away']} {away}")

    while True:
        try:
            choice = int(input("\nSelect a match (number): ")) - 1
            if 0 <= choice < len(fixtures):
                return fixtures[choice]
            print(f"Enter a number between 1 and {len(fixtures)}")
        except ValueError:
            print("Enter a valid number")


def publish(fixture_id, data_type, payload):
    r.xadd(STREAM_NAME, {
        "fixture_id": fixture_id,
        "type": data_type,
        "data": json.dumps(payload),
    })
    print(f"  → published {data_type} to stream")

def poll(fixture):
    fid = fixture["fixture"]["id"]
    home = fixture["teams"]["home"]["name"]
    away = fixture["teams"]["away"]["name"]
    print(f"\nTracking: {home} vs {away}")
    print(f"Publishing to Redis stream '{STREAM_NAME}'")
    print("Press Ctrl+C to stop.\n")

    publish(fid, "fixture", {"home": home, "away": away})

    seen_event_ids = set()
    tick = 0
    while True:
        if tick % 2 == 0:
            events = get_fixture_events(fid)
            new_events = [e for e in events if (e["time"]["elapsed"], e["player"]["name"]) not in seen_event_ids]
            if new_events:
                for e in new_events:
                    seen_event_ids.add((e["time"]["elapsed"], e["player"]["name"]))
                publish(fid, "events", new_events)
        else:
            stats = get_fixture_statistics(fid)
            publish(fid, "statistics", stats)

        tick += 1
        time.sleep(POLL_INTERVAL)

def main():
    print("Fetching live matches...")
    fixtures = get_live_fixtures()

    if not fixtures:
        print("No live matches right now. Try again later.")
        return

    fixture = pick_fixture(fixtures)
    poll(fixture)


if __name__ == "__main__":
    main()
