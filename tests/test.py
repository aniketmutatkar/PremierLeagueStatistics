import duckdb

# Connect to database
conn = duckdb.connect('data/premierleague_analytics.duckdb')

print("🔍 DETAILED ANALYTICS DATA INVESTIGATION")
print("=" * 60)

# 1. Position distribution - how are positions stored?
print("\n⚽ POSITION DISTRIBUTION:")
positions = conn.execute("""
    SELECT position, COUNT(*) as count
    FROM analytics_players 
    WHERE is_current = true
    GROUP BY position 
    ORDER BY count DESC
""").fetchall()

for pos, count in positions:
    print(f"  {pos}: {count} players")

# 2. Sample current players with key stats
print("\n👤 CURRENT PLAYER SAMPLE (Top 5 goalscorers):")
sample = conn.execute("""
    SELECT player_name, squad, position, minutes_played, goals, assists, expected_goals
    FROM analytics_players 
    WHERE is_current = true AND minutes_played > 90 
    ORDER BY goals DESC 
    LIMIT 5
""").fetchall()

for row in sample:
    print(f"  {row[0]} ({row[1]}) - {row[2]} | {row[3]}min | {row[4]}g {row[5]}a | xG:{row[6]}")

# 3. Data availability - how many players have substantial data?
print("\n📊 DATA AVAILABILITY:")
stats = conn.execute("""
    SELECT 
        COUNT(*) as total_current,
        SUM(CASE WHEN minutes_played >= 90 THEN 1 ELSE 0 END) as substantial_minutes,
        SUM(CASE WHEN goals IS NOT NULL THEN 1 ELSE 0 END) as has_goals,
        SUM(CASE WHEN expected_goals IS NOT NULL THEN 1 ELSE 0 END) as has_xg
    FROM analytics_players 
    WHERE is_current = true
""").fetchone()

print(f"  Total current players: {stats[0]}")
print(f"  Players with ≥90 minutes: {stats[1]} ({stats[1]/stats[0]*100:.1f}%)")
print(f"  Players with goals data: {stats[2]} ({stats[2]/stats[0]*100:.1f}%)")
print(f"  Players with xG data: {stats[3]} ({stats[3]/stats[0]*100:.1f}%)")

# 4. Historical data depth
print("\n📅 HISTORICAL DATA:")
gameweeks = conn.execute("""
    SELECT DISTINCT gameweek 
    FROM analytics_players 
    ORDER BY gameweek
""").fetchall()

historical_total = conn.execute("""
    SELECT COUNT(*) FROM analytics_players WHERE is_current = false
""").fetchone()[0]

print(f"  Available gameweeks: {[gw[0] for gw in gameweeks]}")
print(f"  Historical records: {historical_total}")

# 5. Squad data availability
print("\n🏟️ SQUAD DATA:")
squad_count = conn.execute("""
    SELECT COUNT(*) FROM analytics_squads WHERE is_current = true
""").fetchone()[0]

print(f"  Current squads: {squad_count}")

# 6. Key column availability for analysis
print("\n🎯 KEY ANALYSIS COLUMNS:")
key_columns = [
    'shots', 'shots_on_target', 'passes_completed', 'passes_attempted',
    'tackles', 'interceptions', 'progressive_carries', 'key_passes',
    'yellow_cards', 'touches', 'successful_dribbles'
]

# Get all column names
all_cols = conn.execute("PRAGMA table_info(analytics_players)").fetchall()
available_cols = [col[1] for col in all_cols]

for col in key_columns:
    status = "✅" if col in available_cols else "❌"
    print(f"  {status} {col}")

# 7. Sample a specific player's historical data
print("\n📈 HISTORICAL TRACKING SAMPLE:")
sample_player = conn.execute("""
    SELECT player_name, gameweek, goals, minutes_played, is_current
    FROM analytics_players 
    WHERE player_name = (
        SELECT player_name FROM analytics_players 
        WHERE is_current = true AND goals > 0 
        LIMIT 1
    )
    ORDER BY gameweek
""").fetchall()

if sample_player:
    player_name = sample_player[0][0]
    print(f"  Player: {player_name}")
    for record in sample_player:
        status = "CURRENT" if record[4] else "HISTORICAL"
        print(f"    GW{record[1]}: {record[2]} goals, {record[3]} minutes ({status})")

conn.close()
print("\n✅ Investigation complete!")