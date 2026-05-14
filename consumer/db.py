import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/pitchpulse"))
conn.autocommit = True
cursor = conn.cursor()

def get_goal_count(fixture_id, team):
    cursor.execute("""
        SELECT COUNT(*) FROM events
        WHERE fixture_id = %s AND team = %s AND event_type = 'Goal'
    """, (fixture_id, team))
    return cursor.fetchone()[0]

def upsert_fixture(fixture_id, home_team, away_team):
    cursor.execute("""
        INSERT INTO fixtures (id, home_team, away_team)
        VALUES (%s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """, (fixture_id, home_team, away_team))


def insert_event(fixture_id, minute, team, event_type, player):
    cursor.execute("""
        INSERT INTO events (fixture_id, minute, team, event_type, player)
        VALUES (%s, %s, %s, %s, %s)
    """, (fixture_id, minute, team, event_type, player))


def insert_statistics(fixture_id, team, possession, shots, corners, fouls, momentum, goals):
    cursor.execute("""
        INSERT INTO statistics (fixture_id, team, possession, shots, corners, fouls, momentum, goals)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (fixture_id, team, possession, shots, corners, fouls, momentum, goals))