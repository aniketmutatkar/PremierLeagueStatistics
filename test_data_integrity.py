#!/usr/bin/env python3
"""
Holistic Data Integrity Test Suite
Run after EVERY pipeline execution to validate complete system health
Tests: raw data, fixtures, analytics, SCD Type 2, historical data, cross-table consistency
"""

import sys
import duckdb
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

class DataIntegrityValidator:
    """Comprehensive validation of entire data pipeline"""
    
    def __init__(self):
        self.raw_db = "data/premierleague_raw.duckdb"
        self.analytics_db = "data/premierleague_analytics.duckdb"
        self.errors = []
        self.warnings = []
        self.passed = 0
        self.failed = 0
        
    def run_all_tests(self) -> bool:
        """Execute complete test suite"""
        print("=" * 80)
        print("DATA INTEGRITY VALIDATION SUITE")
        print("=" * 80)
        print(f"Timestamp: {datetime.now()}")
        print(f"Raw DB: {self.raw_db}")
        print(f"Analytics DB: {self.analytics_db}")
        print("=" * 80)
        
        tests = [
            ("Database Connectivity", self._test_databases_exist),
            ("Raw Data Structure", self._test_raw_structure),
            ("Fixtures Table Integrity", self._test_fixtures),
            ("Team Gameweek Calculations", self._test_team_gameweeks),
            ("Analytics Table Structure", self._test_analytics_structure),
            ("Record Counts", self._test_record_counts),
            ("Season Coverage", self._test_season_coverage),
            ("Business Key Integrity", self._test_business_keys),
            ("SCD Type 2 Implementation", self._test_scd_type2),
            ("Gameweek Assignment Accuracy", self._test_gameweek_assignment),
            ("Cross-Table Consistency", self._test_cross_table),
            ("Data Quality", self._test_data_quality),
            ("Historical Data Integrity", self._test_historical_data),
        ]
        
        all_passed = True
        for test_name, test_func in tests:
            print(f"\n{'=' * 80}")
            print(f"TEST: {test_name}")
            print("-" * 80)
            if not test_func():
                all_passed = False
        
        self._print_summary(all_passed)
        return all_passed
    
    def _test_databases_exist(self) -> bool:
        """Verify both databases are accessible"""
        if not Path(self.raw_db).exists():
            self._fail(f"Raw database not found: {self.raw_db}")
            return False
        
        if not Path(self.analytics_db).exists():
            self._fail(f"Analytics database not found: {self.analytics_db}")
            return False
        
        try:
            conn = duckdb.connect(self.raw_db, read_only=True)
            conn.close()
            self._pass("Raw database accessible")
        except Exception as e:
            self._fail(f"Cannot connect to raw database: {e}")
            return False
        
        try:
            conn = duckdb.connect(self.analytics_db, read_only=True)
            conn.close()
            self._pass("Analytics database accessible")
        except Exception as e:
            self._fail(f"Cannot connect to analytics database: {e}")
            return False
        
        return True
    
    def _test_raw_structure(self) -> bool:
        """Validate raw database structure"""
        conn = duckdb.connect(self.raw_db, read_only=True)
        passed = True
        
        # Essential tables
        required = ['raw_fixtures', 'player_standard', 'squad_standard', 'opponent_standard']
        tables = {t[0] for t in conn.execute("SHOW TABLES").fetchall()}
        
        for table in required:
            if table not in tables:
                self._fail(f"Missing required table: {table}")
                passed = False
            else:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if count == 0:
                    self._fail(f"Table {table} is empty")
                    passed = False
                else:
                    self._pass(f"{table}: {count} records")
        
        # Verify no gameweek metadata in stat tables (fixture-based system)
        for table in ['player_standard', 'squad_standard', 'opponent_standard']:
            if table in tables:
                cols = {c[0] for c in conn.execute(f"DESCRIBE {table}").fetchall()}
                if 'current_through_gameweek' in cols:
                    self._fail(f"{table} has old gameweek metadata column")
                    passed = False
        
        if passed:
            self._pass("Raw database structure correct (fixture-based)")
        
        conn.close()
        return passed
    
    def _test_fixtures(self) -> bool:
        """Validate fixtures table"""
        conn = duckdb.connect(self.raw_db, read_only=True)
        passed = True
        
        # Check structure
        required_cols = {'gameweek', 'home_team', 'away_team', 'is_completed'}
        cols = {c[0] for c in conn.execute("DESCRIBE raw_fixtures").fetchall()}
        
        missing = required_cols - cols
        if missing:
            self._fail(f"Missing columns in raw_fixtures: {missing}")
            passed = False
        else:
            self._pass("Fixtures table has required columns")
        
        # Check data quality
        total = conn.execute("SELECT COUNT(*) FROM raw_fixtures").fetchone()[0]
        completed = conn.execute("SELECT COUNT(*) FROM raw_fixtures WHERE is_completed = true").fetchone()[0]
        
        if total == 0:
            self._fail("No fixtures in database")
            passed = False
        else:
            self._pass(f"Fixtures: {completed}/{total} completed")
        
        # Check for nulls
        nulls = conn.execute("""
            SELECT 
                SUM(CASE WHEN gameweek IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN home_team IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN away_team IS NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN is_completed IS NULL THEN 1 ELSE 0 END)
            FROM raw_fixtures
        """).fetchone()
        
        if any(nulls):
            self._fail(f"NULL values in fixtures: gw={nulls[0]}, home={nulls[1]}, away={nulls[2]}, completed={nulls[3]}")
            passed = False
        else:
            self._pass("No NULL values in critical fixture columns")
        
        conn.close()
        return passed
    
    def _test_team_gameweeks(self) -> bool:
        """Verify team-specific gameweek calculations"""
        conn = duckdb.connect(self.raw_db, read_only=True)
        
        team_gws = conn.execute("""
            SELECT team, MAX(gameweek) as max_gw
            FROM (
                SELECT home_team as team, gameweek FROM raw_fixtures WHERE is_completed = true
                UNION ALL
                SELECT away_team as team, gameweek FROM raw_fixtures WHERE is_completed = true
            )
            GROUP BY team
            ORDER BY max_gw DESC
        """).fetchall()
        
        if not team_gws:
            self._fail("No team gameweeks calculated from fixtures")
            conn.close()
            return False
        
        max_gw = max(gw for _, gw in team_gws)
        min_gw = min(gw for _, gw in team_gws)
        teams_at_max = sum(1 for _, gw in team_gws if gw == max_gw)
        
        self._pass(f"{len(team_gws)} teams tracked, GW range: {min_gw}-{max_gw}")
        
        if max_gw - min_gw > 3:
            self._warn(f"Large gameweek spread: {max_gw - min_gw} gameweeks")
        
        teams_behind = [(team, gw) for team, gw in team_gws if gw < max_gw]
        if teams_behind:
            team_list = ', '.join(f"{team}(GW{gw})" for team, gw in teams_behind[:5])
            self._warn(f"{len(teams_behind)} teams behind: {team_list}")
        else:
            self._pass(f"All {teams_at_max} teams aligned at GW{max_gw}")
        
        conn.close()
        return True
    
    def _test_analytics_structure(self) -> bool:
        """Validate analytics database structure"""
        conn = duckdb.connect(self.analytics_db, read_only=True)
        passed = True
        
        required_tables = ['analytics_players', 'analytics_keepers', 'analytics_squads', 'analytics_opponents']
        tables = {t[0] for t in conn.execute("SHOW TABLES").fetchall()}
        
        for table in required_tables:
            if table not in tables:
                self._fail(f"Missing analytics table: {table}")
                passed = False
            else:
                # Check SCD Type 2 columns
                cols = {c[0] for c in conn.execute(f"DESCRIBE {table}").fetchall()}
                required_scd = {'season', 'gameweek', 'valid_from', 'valid_to', 'is_current'}
                missing = required_scd - cols
                
                if missing:
                    self._fail(f"{table} missing SCD columns: {missing}")
                    passed = False
                else:
                    self._pass(f"{table} has complete SCD Type 2 structure")
        
        conn.close()
        return passed
    
    def _test_record_counts(self) -> bool:
        """Validate expected record counts"""
        conn = duckdb.connect(self.analytics_db, read_only=True)
        passed = True
        
        tables = {
            'analytics_players': (300, 10000, "players"),
            'analytics_keepers': (15, 1000, "goalkeepers"),
            'analytics_squads': (15, 500, "squads"),
            'analytics_opponents': (15, 500, "opponents")
        }
        
        for table, (min_expected, max_expected, entity) in tables.items():
            current = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE is_current = true").fetchone()[0]
            total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            historical = total - current
            
            if current < min_expected:
                self._fail(f"{table}: Only {current} current {entity} (expected >{min_expected})")
                passed = False
            elif current > max_expected:
                self._warn(f"{table}: {current} current {entity} (expected <{max_expected})")
            else:
                self._pass(f"{table}: {current} current, {historical} historical")
        
        conn.close()
        return passed
    
    def _test_season_coverage(self) -> bool:
        """Validate season coverage and format"""
        conn = duckdb.connect(self.analytics_db, read_only=True)
        passed = True
        
        seasons = conn.execute("""
            SELECT DISTINCT season FROM analytics_players ORDER BY season
        """).fetchall()
        
        if not seasons:
            self._fail("No seasons found in analytics_players")
            conn.close()
            return False
        
        season_list = [s[0] for s in seasons]
        self._pass(f"{len(season_list)} seasons: {season_list[0]} to {season_list[-1]}")
        
        # Validate format
        for season, in seasons:
            if len(season) != 9 or season[4] != '-':
                self._fail(f"Invalid season format: {season}")
                passed = False
        
        # Check current season has is_current=true records
        current_season = season_list[-1]
        current_count = conn.execute(f"""
            SELECT COUNT(*) FROM analytics_players 
            WHERE season = '{current_season}' AND is_current = true
        """).fetchone()[0]
        
        if current_count == 0:
            self._fail(f"Current season {current_season} has no is_current=true records")
            passed = False
        else:
            self._pass(f"Current season {current_season}: {current_count} active records")
        
        conn.close()
        return passed
    
    def _test_business_keys(self) -> bool:
        """Validate business key integrity"""
        conn = duckdb.connect(self.analytics_db, read_only=True)
        passed = True
        
        # Check player_id format includes season
        sample = conn.execute("""
            SELECT player_id, season, player_name 
            FROM analytics_players 
            WHERE is_current = true 
            LIMIT 5
        """).fetchall()
        
        for player_id, season, name in sample:
            if season not in player_id:
                self._fail(f"player_id missing season: {player_id}")
                passed = False
        
        if passed:
            self._pass("Business keys include season information")
        
        # Check for TRUE duplicates (same player+season+gameweek)
        duplicates = conn.execute("""
            SELECT player_id, season, gameweek, COUNT(*) 
            FROM analytics_players
            GROUP BY player_id, season, gameweek
            HAVING COUNT(*) > 1
        """).fetchall()
        
        if duplicates:
            self._fail(f"Found {len(duplicates)} TRUE duplicate records (same player+season+gameweek)")
            for player_id, season, gw, count in duplicates[:3]:
                self._fail(f"  {player_id} | {season} | GW{gw} | {count}x")
            passed = False
        else:
            self._pass("No duplicate player+season+gameweek combinations")
        
        # Verify SCD Type 2 tracking (multiple gameweeks per player-season)
        multi_gw = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT player_id, season 
                FROM analytics_players
                GROUP BY player_id, season
                HAVING COUNT(DISTINCT gameweek) > 1
            )
        """).fetchone()[0]
        
        if multi_gw > 0:
            self._pass(f"SCD Type 2 active: {multi_gw} player-seasons with multiple gameweeks")
        
        conn.close()
        return passed
    
    def _test_scd_type2(self) -> bool:
        """Validate SCD Type 2 implementation"""
        conn = duckdb.connect(self.analytics_db, read_only=True)
        passed = True
        
        # No duplicate current records per entity
        for table in ['analytics_players', 'analytics_keepers']:
            id_col = 'player_id'
            duplicates = conn.execute(f"""
                SELECT {id_col}, season, gameweek, COUNT(*) 
                FROM {table}
                WHERE is_current = true
                GROUP BY {id_col}, season, gameweek
                HAVING COUNT(*) > 1
            """).fetchall()
            
            if duplicates:
                self._fail(f"{table}: {len(duplicates)} duplicate current records")
                passed = False
        
        # Historical records have valid_to
        missing_valid_to = conn.execute("""
            SELECT COUNT(*) FROM analytics_players
            WHERE is_current = false AND valid_to IS NULL
        """).fetchone()[0]
        
        if missing_valid_to > 0:
            self._fail(f"{missing_valid_to} historical records missing valid_to")
            passed = False
        
        # Current records have NULL valid_to
        bad_valid_to = conn.execute("""
            SELECT COUNT(*) FROM analytics_players
            WHERE is_current = true AND valid_to IS NOT NULL
        """).fetchone()[0]
        
        if bad_valid_to > 0:
            self._fail(f"{bad_valid_to} current records have valid_to set")
            passed = False
        
        if passed:
            total_hist = conn.execute("SELECT COUNT(*) FROM analytics_players WHERE is_current = false").fetchone()[0]
            self._pass(f"SCD Type 2 integrity valid ({total_hist} historical records)")
        
        conn.close()
        return passed
    
    def _test_gameweek_assignment(self) -> bool:
        """Verify analytics gameweeks match fixture calculations"""
        raw_conn = duckdb.connect(self.raw_db, read_only=True)
        analytics_conn = duckdb.connect(self.analytics_db, read_only=True)
        passed = True
        
        # Calculate expected from fixtures
        expected = {}
        fixtures = raw_conn.execute("""
            SELECT team, MAX(gameweek) as gw FROM (
                SELECT home_team as team, gameweek FROM raw_fixtures WHERE is_completed = true
                UNION ALL
                SELECT away_team as team, gameweek FROM raw_fixtures WHERE is_completed = true
            ) GROUP BY team
        """).fetchall()
        
        for team, gw in fixtures:
            expected[team] = gw
        
        # Get actual from analytics
        actual = {}
        analytics_gws = analytics_conn.execute("""
            SELECT squad, MAX(gameweek) 
            FROM analytics_players 
            WHERE is_current = true 
            GROUP BY squad
        """).fetchall()
        
        for squad, gw in analytics_gws:
            actual[squad] = gw
        
        # Compare
        mismatches = []
        for squad in actual:
            if squad in expected and actual[squad] != expected[squad]:
                mismatches.append(f"{squad}: expected GW{expected[squad]}, got GW{actual[squad]}")
                passed = False
        
        if mismatches:
            self._fail(f"{len(mismatches)} gameweek mismatches:")
            for m in mismatches[:5]:
                self._fail(f"  {m}")
        else:
            self._pass(f"All {len(actual)} teams have correct gameweeks from fixtures")
        
        raw_conn.close()
        analytics_conn.close()
        return passed
    
    def _test_cross_table(self) -> bool:
        """Validate consistency across analytics tables"""
        conn = duckdb.connect(self.analytics_db, read_only=True)
        passed = True
        
        # Same teams across tables
        player_teams = {t[0] for t in conn.execute("""
            SELECT DISTINCT squad FROM analytics_players WHERE is_current = true
        """).fetchall()}
        
        squad_teams = {t[0] for t in conn.execute("""
            SELECT DISTINCT squad_name FROM analytics_squads WHERE is_current = true
        """).fetchall()}
        
        if player_teams != squad_teams:
            missing = player_teams - squad_teams
            extra = squad_teams - player_teams
            if missing:
                self._fail(f"Teams in players but not squads: {missing}")
                passed = False
            if extra:
                self._fail(f"Teams in squads but not players: {extra}")
                passed = False
        else:
            self._pass(f"Consistent {len(player_teams)} teams across entity tables")
        
        # Same gameweeks per team
        player_gws = dict(conn.execute("""
            SELECT squad, MAX(gameweek) FROM analytics_players 
            WHERE is_current = true GROUP BY squad
        """).fetchall())
        
        squad_gws = dict(conn.execute("""
            SELECT squad_name, MAX(gameweek) FROM analytics_squads 
            WHERE is_current = true GROUP BY squad_name
        """).fetchall())
        
        gw_mismatches = []
        for squad in player_gws:
            if squad in squad_gws and player_gws[squad] != squad_gws[squad]:
                gw_mismatches.append(f"{squad}: players GW{player_gws[squad]} vs squads GW{squad_gws[squad]}")
                passed = False
        
        if gw_mismatches:
            self._fail(f"Gameweek mismatches between tables:")
            for m in gw_mismatches:
                self._fail(f"  {m}")
        else:
            self._pass("Consistent gameweeks across all entity tables")
        
        conn.close()
        return passed
    
    def _test_data_quality(self) -> bool:
        """Validate data quality"""
        conn = duckdb.connect(self.analytics_db, read_only=True)
        passed = True
        
        # No NULLs in critical columns
        null_checks = [
            ('player_id', 'analytics_players'),
            ('player_name', 'analytics_players'),
            ('season', 'analytics_players'),
            ('gameweek', 'analytics_players'),
            ('squad_id', 'analytics_squads'),
            ('squad_name', 'analytics_squads'),
        ]
        
        for col, table in null_checks:
            nulls = conn.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE {col} IS NULL
            """).fetchone()[0]
            
            if nulls > 0:
                self._fail(f"{table}.{col}: {nulls} NULL values")
                passed = False
        
        # Reasonable player ages
        bad_ages = conn.execute("""
            SELECT COUNT(*) FROM analytics_players 
            WHERE is_current = true AND (age < 16 OR age > 45)
        """).fetchone()[0]
        
        if bad_ages > 0:
            self._warn(f"{bad_ages} players with unusual ages (<16 or >45)")
        
        # No negative stats
        negatives = conn.execute("""
            SELECT COUNT(*) FROM analytics_players
            WHERE is_current = true AND (matches_played < 0 OR goals < 0)
        """).fetchone()[0]
        
        if negatives > 0:
            self._fail(f"{negatives} records with negative stats")
            passed = False
        
        if passed:
            self._pass("Data quality checks passed")
        
        conn.close()
        return passed
    
    def _test_historical_data(self) -> bool:
        """Validate historical data integrity"""
        conn = duckdb.connect(self.analytics_db, read_only=True)
        
        # Check season distribution
        season_counts = conn.execute("""
            SELECT season, COUNT(*) as records
            FROM analytics_players
            GROUP BY season
            ORDER BY season
        """).fetchall()
        
        if len(season_counts) == 1:
            self._pass(f"Single season data: {season_counts[0][0]}")
            conn.close()
            return True
        
        self._pass(f"Historical data: {len(season_counts)} seasons")
        
        # Show distribution
        for season, count in season_counts[:3]:
            print(f"    {season}: {count:,} records")
        if len(season_counts) > 3:
            print(f"    ... and {len(season_counts) - 3} more seasons")
        
        # Verify no season overlaps in current data
        current_seasons = conn.execute("""
            SELECT DISTINCT season FROM analytics_players WHERE is_current = true
        """).fetchall()
        
        if len(current_seasons) > 1:
            self._warn(f"Multiple seasons marked as current: {[s[0] for s in current_seasons]}")
        else:
            self._pass(f"Single current season: {current_seasons[0][0]}")
        
        # Check for player career tracking
        multi_season = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT player_name 
                FROM analytics_players
                GROUP BY player_name
                HAVING COUNT(DISTINCT season) > 5
            )
        """).fetchone()[0]
        
        if multi_season > 0:
            self._pass(f"Career tracking: {multi_season} players with 5+ seasons")
        
        conn.close()
        return True
    
    def _pass(self, msg: str):
        print(f"  ✅ {msg}")
        self.passed += 1
    
    def _fail(self, msg: str):
        print(f"  ❌ {msg}")
        self.errors.append(msg)
        self.failed += 1
    
    def _warn(self, msg: str):
        print(f"  ⚠️  {msg}")
        self.warnings.append(msg)
    
    def _print_summary(self, all_passed: bool):
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)
        print(f"Passed: {self.passed}")
        print(f"Failed: {self.failed}")
        print(f"Warnings: {len(self.warnings)}")
        
        if self.errors:
            print("\n❌ CRITICAL ERRORS:")
            for error in self.errors:
                print(f"  • {error}")
        
        if self.warnings:
            print("\n⚠️  WARNINGS:")
            for warning in self.warnings:
                print(f"  • {warning}")
        
        print("\n" + "=" * 80)
        if all_passed and not self.errors:
            print("✅ ALL VALIDATION CHECKS PASSED")
            print("Database is healthy and ready for use.")
        else:
            print("❌ VALIDATION FAILED")
            print("Review errors above and investigate issues.")
        print("=" * 80)

def main():
    validator = DataIntegrityValidator()
    success = validator.run_all_tests()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()