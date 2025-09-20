#!/usr/bin/env python3
"""
Analytics System Validation Script v2.0
Comprehensive validation for the rebuilt analytics system with proper schema checks
"""

import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

class AnalyticsValidator:
    """Validates the complete analytics system with current schema"""
    
    def __init__(self, db_path: str = "data/premierleague_analytics.duckdb"):
        self.db_path = db_path
        self.conn = None
        
    def __enter__(self):
        self.conn = duckdb.connect(self.db_path)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def validate_schema_integrity(self) -> bool:
        """Validate database schema matches expected structure"""
        print("🏗️ VALIDATING SCHEMA INTEGRITY")
        print("=" * 50)
        
        try:
            # Check if both tables exist
            tables = self.conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]
            
            expected_tables = ['analytics_players', 'analytics_keepers']
            missing_tables = [t for t in expected_tables if t not in table_names]
            
            if missing_tables:
                print(f"❌ Missing tables: {missing_tables}")
                return False
            
            print(f"✅ Found required tables: {expected_tables}")
            
            # Check analytics_players structure
            players_info = self.conn.execute("PRAGMA table_info(analytics_players)").fetchall()
            players_columns = [col[1] for col in players_info]
            players_count = len(players_columns)
            
            # Check analytics_keepers structure  
            keepers_info = self.conn.execute("PRAGMA table_info(analytics_keepers)").fetchall()
            keepers_columns = [col[1] for col in keepers_info]
            keepers_count = len(keepers_columns)
            
            print(f"📊 Table Structure:")
            print(f"   analytics_players: {players_count} columns")
            print(f"   analytics_keepers: {keepers_count} columns")
            
            # Validate key columns exist
            required_player_cols = [
                'player_key', 'player_name', 'squad', 'position', 'gameweek',
                'is_current', 'valid_from', 'valid_to', 'minutes_played', 'touches'
            ]
            
            missing_cols = [col for col in required_player_cols if col not in players_columns]
            if missing_cols:
                print(f"❌ Missing required columns in analytics_players: {missing_cols}")
                return False
            
            print(f"✅ All required columns present")
            
            # Check for SCD Type 2 columns
            scd_columns = ['gameweek', 'is_current', 'valid_from', 'valid_to']
            scd_present = all(col in players_columns for col in scd_columns)
            print(f"✅ SCD Type 2 columns present: {scd_present}")
            
            return True
            
        except Exception as e:
            print(f"❌ Schema validation failed: {e}")
            return False
    
    def validate_scd_type_2(self) -> bool:
        """Test SCD Type 2 implementation"""
        print("\n🔍 VALIDATING SCD TYPE 2 IMPLEMENTATION")
        print("=" * 50)
        
        try:
            # Basic counts
            current_count = self.conn.execute("SELECT COUNT(*) FROM analytics_players WHERE is_current = true").fetchone()[0]
            historical_count = self.conn.execute("SELECT COUNT(*) FROM analytics_players WHERE is_current = false").fetchone()[0]
            total_count = self.conn.execute("SELECT COUNT(*) FROM analytics_players").fetchone()[0]
            
            print(f"📊 Record Counts:")
            print(f"   Current records: {current_count:,}")
            print(f"   Historical records: {historical_count:,}")
            print(f"   Total records: {total_count:,}")
            print(f"   ✅ Total = Current + Historical: {total_count == current_count + historical_count}")
            
            # Gameweek distribution
            gameweeks = self.conn.execute("SELECT DISTINCT gameweek FROM analytics_players ORDER BY gameweek").fetchall()
            gameweeks = [gw[0] for gw in gameweeks]
            print(f"\n🗓️ Gameweeks Present: {gameweeks}")
            
            # Records per gameweek
            gw_distribution = self.conn.execute("""
                SELECT gameweek, is_current, COUNT(*) as records
                FROM analytics_players 
                GROUP BY gameweek, is_current 
                ORDER BY gameweek, is_current DESC
            """).fetchall()
            
            print(f"\n📈 Records by Gameweek:")
            for gw, is_current, count in gw_distribution:
                status = "Current" if is_current else "Historical"
                print(f"   GW{gw}: {count:,} {status} records")
            
            # Validate only current gameweek has is_current=true
            current_gameweeks = self.conn.execute("SELECT DISTINCT gameweek FROM analytics_players WHERE is_current = true").fetchall()
            if len(current_gameweeks) == 1:
                current_gw = current_gameweeks[0][0]
                print(f"   ✅ Only GW{current_gw} marked as current")
            else:
                print(f"   ❌ Multiple gameweeks marked as current: {current_gameweeks}")
                return False
            
            # Check for overlapping valid periods (SCD integrity)
            overlaps = self.conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT player_id, COUNT(*) as versions
                    FROM analytics_players 
                    WHERE is_current = true
                    GROUP BY player_id
                    HAVING COUNT(*) > 1
                )
            """).fetchone()[0]
            
            if overlaps > 0:
                print(f"   ❌ {overlaps} players have multiple current records")
                return False
            else:
                print(f"   ✅ No overlapping current records")
            
            return True
            
        except Exception as e:
            print(f"❌ SCD Type 2 validation failed: {e}")
            return False
    
    def validate_player_tracking(self) -> bool:
        """Test individual player tracking across gameweeks"""
        print("\n🏃 VALIDATING PLAYER TRACKING")
        print("=" * 50)
        
        try:
            # Find players who appear in multiple gameweeks
            multi_gw_players = self.conn.execute("""
                SELECT player_name, COUNT(DISTINCT gameweek) as gameweeks,
                       COUNT(DISTINCT squad) as teams
                FROM analytics_players 
                GROUP BY player_name 
                HAVING COUNT(DISTINCT gameweek) > 1
                ORDER BY gameweeks DESC, player_name
                LIMIT 10
            """).fetchall()
            
            print(f"🔄 Players Tracked Across Multiple Gameweeks:")
            transfers_detected = 0
            for player_name, gameweeks, teams in multi_gw_players:
                status = f"({teams} teams)" if teams > 1 else ""
                print(f"   {player_name}: {gameweeks} gameweeks {status}")
                if teams > 1:
                    transfers_detected += 1
            
            if transfers_detected > 0:
                print(f"   🔄 Transfer detection: {transfers_detected} players changed teams")
            
            # Test a specific player's progression (take first multi-gameweek player)
            if multi_gw_players:
                test_player = multi_gw_players[0][0]
                print(f"\n🎯 Detailed Tracking Example - {test_player}:")
                
                player_history = self.conn.execute("""
                    SELECT gameweek, is_current, squad, position, 
                           minutes_played, touches, goals, assists
                    FROM analytics_players 
                    WHERE player_name = ?
                    ORDER BY gameweek
                """, [test_player]).fetchall()
                
                for gw, is_current, squad, pos, mins, touches, goals, assists in player_history:
                    status = "Current" if is_current else "Historical"
                    print(f"   GW{gw}: {squad} ({pos}) - {mins}min, {touches}touches, {goals}G/{assists}A [{status}]")
            
            return True
            
        except Exception as e:
            print(f"❌ Player tracking validation failed: {e}")
            return False
    
    def validate_data_quality(self) -> bool:
        """Test data quality and consistency"""
        print("\n✅ VALIDATING DATA QUALITY")
        print("=" * 50)
        
        try:
            issues = []
            
            # 1. Check for duplicate current records
            duplicates = self.conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT player_name, squad, gameweek 
                    FROM analytics_players 
                    WHERE is_current = true
                    GROUP BY player_name, squad, gameweek 
                    HAVING COUNT(*) > 1
                )
            """).fetchone()[0]
            
            if duplicates > 0:
                issues.append(f"{duplicates} duplicate current records")
            
            # 2. Check for missing player keys
            missing_keys = self.conn.execute("SELECT COUNT(*) FROM analytics_players WHERE player_key IS NULL").fetchone()[0]
            if missing_keys > 0:
                issues.append(f"{missing_keys} records with missing player_key")
            
            # 3. Check for invalid gameweeks
            invalid_gw = self.conn.execute("SELECT COUNT(*) FROM analytics_players WHERE gameweek < 1 OR gameweek > 38").fetchone()[0]
            if invalid_gw > 0:
                issues.append(f"{invalid_gw} records with invalid gameweeks")
            
            # 4. Check for negative minutes
            negative_minutes = self.conn.execute("SELECT COUNT(*) FROM analytics_players WHERE minutes_played < 0").fetchone()[0]
            if negative_minutes > 0:
                issues.append(f"{negative_minutes} records with negative minutes")
            
            # 5. Check for logical inconsistencies (goals > shots, if shots column exists)
            columns = [col[1] for col in self.conn.execute("PRAGMA table_info(analytics_players)").fetchall()]
            if 'shots' in columns:
                illogical_stats = self.conn.execute("""
                    SELECT COUNT(*) FROM analytics_players 
                    WHERE goals > shots AND shots > 0
                """).fetchone()[0]
                if illogical_stats > 0:
                    issues.append(f"{illogical_stats} records where goals > shots")
            
            # 6. Check for players with zero touches but significant minutes (data quality flag)
            zero_touches = self.conn.execute("""
                SELECT COUNT(*) FROM analytics_players 
                WHERE touches = 0 AND minutes_played > 90 AND is_current = true
            """).fetchone()[0]
            
            print(f"🔍 Data Quality Checks:")
            if not issues:
                print("   ✅ All data quality checks passed")
            else:
                print("   ❌ Issues found:")
                for issue in issues:
                    print(f"      - {issue}")
            
            # Data quality insights
            quality_stats = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_current,
                    SUM(CASE WHEN touches > 0 THEN 1 ELSE 0 END) as with_touches,
                    SUM(CASE WHEN minutes_played > 0 THEN 1 ELSE 0 END) as with_minutes,
                    AVG(minutes_played) as avg_minutes,
                    MAX(minutes_played) as max_minutes
                FROM analytics_players 
                WHERE is_current = true
            """).fetchall()[0]
            
            total, with_touches, with_mins, avg_mins, max_mins = quality_stats
            touch_pct = (with_touches / total * 100) if total > 0 else 0
            
            print(f"\n📊 Data Quality Summary:")
            print(f"   Current players: {total:,}")
            print(f"   Players with touches: {with_touches:,} ({touch_pct:.1f}%)")
            print(f"   Players with minutes: {with_mins:,}")
            print(f"   Average minutes: {avg_mins:.1f}")
            print(f"   Maximum minutes: {max_mins}")
            
            if zero_touches > 0:
                print(f"   ⚠️  {zero_touches} players with 0 touches but >90 minutes (possible data issue)")
            
            return len(issues) == 0
            
        except Exception as e:
            print(f"❌ Data quality validation failed: {e}")
            return False
    
    def validate_column_mapping(self) -> bool:
        """Validate that column mapping worked correctly"""
        print("\n🗂️ VALIDATING COLUMN MAPPING")
        print("=" * 50)
        
        try:
            # Check that we have the expected core columns with realistic data
            core_stats = self.conn.execute("""
                SELECT 
                    COUNT(*) as total_players,
                    SUM(CASE WHEN touches > 0 THEN 1 ELSE 0 END) as has_touches,
                    SUM(CASE WHEN minutes_played > 0 THEN 1 ELSE 0 END) as has_minutes,
                    SUM(CASE WHEN goals >= 0 THEN 1 ELSE 0 END) as has_goals,
                    SUM(CASE WHEN assists >= 0 THEN 1 ELSE 0 END) as has_assists
                FROM analytics_players 
                WHERE is_current = true
            """).fetchall()[0]
            
            total, touches, minutes, goals, assists = core_stats
            
            print(f"📈 Core Statistics Coverage:")
            print(f"   Total current players: {total:,}")
            print(f"   Players with touches: {touches:,} ({touches/total*100:.1f}%)")
            print(f"   Players with minutes: {minutes:,} ({minutes/total*100:.1f}%)")
            print(f"   Players with goals data: {goals:,} ({goals/total*100:.1f}%)")
            print(f"   Players with assists data: {assists:,} ({assists/total*100:.1f}%)")
            
            # Check that our explicit column mapping worked
            success_threshold = 0.95  # 95% of players should have realistic data
            
            mapping_success = True
            if touches / total < success_threshold:
                print(f"   ❌ Touch data coverage below threshold ({touches/total*100:.1f}% < {success_threshold*100}%)")
                mapping_success = False
            
            if mapping_success:
                print(f"   ✅ Column mapping successful - realistic data distribution")
            
            # Sample some actual values to verify mapping worked
            sample_data = self.conn.execute("""
                SELECT player_name, squad, minutes_played, touches, goals, assists, position
                FROM analytics_players 
                WHERE is_current = true AND minutes_played > 0
                ORDER BY touches DESC
                LIMIT 5
            """).fetchall()
            
            print(f"\n🎯 Sample Player Data (Top by Touches):")
            for name, squad, mins, touches, goals, assists, pos in sample_data:
                print(f"   {name} ({squad}, {pos}): {mins}min, {touches}touches, {goals}G/{assists}A")
            
            return mapping_success
            
        except Exception as e:
            print(f"❌ Column mapping validation failed: {e}")
            return False
    
    def validate_goalkeepers(self) -> bool:
        """Validate goalkeeper data separately"""
        print("\n🥅 VALIDATING GOALKEEPER DATA")
        print("=" * 50)
        
        try:
            # Check keeper counts
            total_keepers = self.conn.execute("SELECT COUNT(*) FROM analytics_keepers WHERE is_current = true").fetchone()[0]
            print(f"📊 Current Goalkeepers: {total_keepers}")
            
            if total_keepers == 0:
                print("   ⚠️  No current goalkeeper data found")
                return False
            
            # Check keeper-specific stats
            keeper_stats = self.conn.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN saves >= 0 THEN 1 ELSE 0 END) as has_saves,
                    SUM(CASE WHEN goals_against >= 0 THEN 1 ELSE 0 END) as has_ga,
                    AVG(saves) as avg_saves,
                    AVG(goals_against) as avg_ga
                FROM analytics_keepers 
                WHERE is_current = true
            """).fetchall()[0]
            
            total, saves, ga, avg_saves, avg_ga = keeper_stats
            
            print(f"📈 Goalkeeper Statistics:")
            print(f"   Keepers with saves data: {saves}/{total} ({saves/total*100:.1f}%)")
            print(f"   Keepers with GA data: {ga}/{total} ({ga/total*100:.1f}%)")
            print(f"   Average saves: {avg_saves:.1f}")
            print(f"   Average goals against: {avg_ga:.1f}")
            
            # Sample keeper data
            sample_keepers = self.conn.execute("""
                SELECT player_name, squad, minutes_played, saves, goals_against, clean_sheets
                FROM analytics_keepers 
                WHERE is_current = true AND minutes_played > 0
                ORDER BY saves DESC
                LIMIT 3
            """).fetchall()
            
            print(f"\n🌟 Top Goalkeepers by Saves:")
            for name, squad, mins, saves, ga, cs in sample_keepers:
                print(f"   {name} ({squad}): {mins}min, {saves}saves, {ga}GA, {cs}CS")
            
            return True
            
        except Exception as e:
            print(f"❌ Goalkeeper validation failed: {e}")
            return False
    
    def generate_system_insights(self) -> bool:
        """Generate insights about the analytics system"""
        print("\n🔮 ANALYTICS SYSTEM INSIGHTS")
        print("=" * 50)
        
        try:
            # Overall system summary
            summary = self.conn.execute("""
                SELECT 
                    COUNT(DISTINCT player_name) as unique_players,
                    COUNT(DISTINCT squad) as unique_teams,
                    MIN(gameweek) as min_gameweek,
                    MAX(gameweek) as max_gameweek,
                    COUNT(*) as total_records
                FROM analytics_players
            """).fetchall()[0]
            
            unique_players, teams, min_gw, max_gw, total_records = summary
            
            print(f"📊 System Overview:")
            print(f"   Unique players tracked: {unique_players:,}")
            print(f"   Teams in database: {teams}")
            print(f"   Gameweek range: {min_gw} - {max_gw}")
            print(f"   Total player records: {total_records:,}")
            print(f"   Historical depth: {max_gw - min_gw + 1} gameweeks")
            
            # Top performing teams by current stats
            team_stats = self.conn.execute("""
                SELECT squad,
                       COUNT(*) as squad_size,
                       SUM(goals) as total_goals,
                       SUM(assists) as total_assists,
                       AVG(minutes_played) as avg_minutes
                FROM analytics_players 
                WHERE is_current = true
                GROUP BY squad
                ORDER BY total_goals + total_assists DESC
                LIMIT 5
            """).fetchall()
            
            print(f"\n🏆 Top Attacking Teams (Current GW):")
            for squad, size, goals, assists, avg_mins in team_stats:
                print(f"   {squad}: {goals}G/{assists}A ({size} players, {avg_mins:.0f}min avg)")
            
            print(f"\n✅ SYSTEM READY FOR:")
            print(f"   🔬 Advanced player analysis")
            print(f"   📈 Performance tracking over time")
            print(f"   🔄 Transfer impact analysis")
            print(f"   🤖 Machine learning model training")
            print(f"   📊 Team formation analysis")
            print(f"   🎯 Tactical pattern recognition")
            
            return True
            
        except Exception as e:
            print(f"❌ System insights failed: {e}")
            return False


def main():
    """Run complete analytics system validation"""
    print("🏆 PREMIER LEAGUE ANALYTICS SYSTEM VALIDATION v2.0")
    print("=" * 70)
    print(f"📅 Validation run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    db_path = "data/premierleague_analytics.duckdb"
    if not Path(db_path).exists():
        print(f"❌ Analytics database not found: {db_path}")
        return False
    
    try:
        with AnalyticsValidator(db_path) as validator:
            # Run all validation tests
            tests = [
                ("Schema Integrity", validator.validate_schema_integrity()),
                ("SCD Type 2", validator.validate_scd_type_2()),
                ("Player Tracking", validator.validate_player_tracking()),
                ("Data Quality", validator.validate_data_quality()),
                ("Column Mapping", validator.validate_column_mapping()),
                ("Goalkeeper Data", validator.validate_goalkeepers()),
                ("System Insights", validator.generate_system_insights())
            ]
            
            # Summary
            print(f"\n📋 VALIDATION SUMMARY")
            print("=" * 50)
            
            passed = 0
            for test_name, result in tests:
                status = "✅ PASS" if result else "❌ FAIL"
                print(f"   {test_name}: {status}")
                if result:
                    passed += 1
            
            print(f"\n🎯 Overall Result: {passed}/{len(tests)} tests passed")
            
            if passed == len(tests):
                print(f"\n🎉 ALL VALIDATION TESTS PASSED!")
                print(f"✅ Your analytics system is production-ready")
                print(f"🚀 Ready for advanced analytics and machine learning")
                return True
            else:
                print(f"\n⚠️  {len(tests) - passed} validation test(s) failed")
                print(f"🔧 Review failed tests and fix issues before proceeding")
                return False
                
    except Exception as e:
        print(f"❌ Validation failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)