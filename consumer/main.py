import json
import redis
import os
from dotenv import load_dotenv
from db import upsert_fixture, insert_event, insert_statistics, get_goal_count
import traceback

load_dotenv()

STREAM_NAME = "pitchpulse:events"
CONSUMER_GROUP = "pitchpulse-consumers"
CONSUMER_NAME = "consumer-1"

r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"))

try:
    r.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
    print(f"Created consumer group '{CONSUMER_GROUP}'")
except redis.exceptions.ResponseError:
    print(f"Consumer group '{CONSUMER_GROUP}' already exists")

def handle_fixture(fixture_id, data):
    upsert_fixture(fixture_id, data["home"], data["away"])
    print(f"\n[fixture {fixture_id}] Tracking: {data['home']} vs {data['away']}")

def calculate_score(events, home_team, away_team):
    home_score = 0
    away_score = 0
    for event in events:
        if event["type"] == "Goal":
            if event["team"]["name"] == home_team:
                home_score += 1
            else:
                away_score += 1
    return home_score, away_score


def calculate_momentum(stats):
    result = {}
    for team_stats in stats:
        team = team_stats["team"]["name"]
        values = {s["type"]: s["value"] for s in team_stats["statistics"]}
        shots = int(values.get("Total Shots") or 0)
        corners = int(values.get("Corner Kicks") or 0)
        fouls = int(values.get("Fouls") or 0)
        # weighted score: shots matter most, corners second, fouls negative
        momentum = (shots * 3) + (corners * 2) - (fouls * 0.5)
        result[team] = momentum
    return result


def handle_events(fixture_id, events):
    if not events:
        return

    teams = list({e["team"]["name"] for e in events})
    if len(teams) == 2:
        home_score, away_score = calculate_score(events, teams[0], teams[1])
        print(f"\n[fixture {fixture_id}] Score: {teams[0]} {home_score} - {away_score} {teams[1]}")

    print("  Recent events:")
    for event in events[-5:]:
        minute = event["time"]["elapsed"]
        etype = event["type"]
        player = event["player"]["name"]
        team = event["team"]["name"]
        print(f"  {minute}' [{team}] {etype} - {player}")
        insert_event(fixture_id, minute, team, etype, player)


def handle_statistics(fixture_id, stats):
    if not stats:
        return

    momentum = calculate_momentum(stats)

    print(f"\n[fixture {fixture_id}] Statistics:")
    for team_stats in stats:
        team = team_stats["team"]["name"]
        values = {s["type"]: s["value"] for s in team_stats["statistics"]}
        possession = values.get("Ball Possession", "N/A")
        shots = int(values.get("Total Shots") or 0)
        corners = int(values.get("Corner Kicks") or 0)
        fouls = int(values.get("Fouls") or 0)
        m = momentum[team]
        goals = get_goal_count(fixture_id, team)
        print(f"  {team}: possession={possession}  shots={shots}  corners={corners}  fouls={fouls}  goals={goals}  momentum={m}")
        insert_statistics(fixture_id, team, possession, shots, corners, fouls, m, goals)


def main():
    print(f"Consumer listening on stream '{STREAM_NAME}'...\n")
    
    while True:
        messages = r.xreadgroup(CONSUMER_GROUP, CONSUMER_NAME, {STREAM_NAME: ">"}, count=10, block=5000)
        if not messages:
            continue

        for stream, entries in messages:
            for entry_id, fields in entries:
                try:
                    fixture_id = int(fields[b"fixture_id"])
                    data_type = fields[b"type"].decode()
                    payload = json.loads(fields[b"data"])
                    if data_type == "fixture":
                        handle_fixture(fixture_id, payload)
                    elif data_type == "events":
                        handle_events(fixture_id, payload)
                    elif data_type == "statistics":
                        handle_statistics(fixture_id, payload)
                except Exception as e:
                    print(f"Error processing message: {e}")
                    traceback.print_exc()
                finally:
                    r.xack(STREAM_NAME, CONSUMER_GROUP, entry_id)


if __name__ == "__main__":
    main()