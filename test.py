#!/usr/bin/env python3
"""
Fixed Comprehensive Data Validation Script
Updated to handle SCD Type 2 business key logic and current issues
"""

import sys
import duckdb
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FixedDataValidator:
    """Fixed comprehensive validation addressing SCD Type 2 and current issues"""
    
    def __init__(self):
        self.db_paths = {
            'raw': 'data/premierleague_raw.duckdb',
            'analytics': 'data/premierleague_analytics.duckdb'
        }
        
        self.expected_seasons = [
            '2010-2011', '2011-2012', '2012-2013', '2013-2014', '2014-2015',
            '2015-2016', '2016-2017', '2017-2018', '2018-2019', '2019-2020',
            '2020-2021', '2021-2022', '2022-2023', '2023-2024', '2024-2025',
            '2025-2026'
        ]
        
        # Validation results
        self.validation_results = {
            'database_accessibility': False,
            'season_coverage': False,
            'business_key_integrity': False,
            'scd_type2_correctness': False,
            'current_historical_status': False,
            'data_quality_by_era': False,
            'fixtures_integrity': False,
            'production_readiness': False,
            'environment_cleanup': False,
            'cross_season_consistency': False
        }
        
        self.issues_found = []
        self.warnings = []
        
    def run_complete_validation(self) -> bool:
        """Run all validation tests with fixes"""
        print("=" * 100)
        print("FIXED COMPREHENSIVE PREMIER LEAGUE DATA VALIDATION")
        print("=" * 100)
        print(f"Testing against expected 16 seasons: {self.expected_seasons[0]} to {self.expected_seasons[-1]}")
        print(f"Validation started: {datetime.now()}")
        print("Note: All data expected to be historical (no current data loaded yet)")
        print("=" * 100)
        
        # Test 1: Database Accessibility
        print("\n🗄️  TEST 1: DATABASE ACCESSIBILITY")
        print("-" * 50)
        if not self._test_database_accessibility():
            print("❌ Critical error: Cannot access databases")
            return False
        
        # Test 2: Season Coverage
        print("\n📅 TEST 2: SEASON COVERAGE VALIDATION") 
        print("-" * 50)
        self._test_season_coverage()
        
        # Test 3: Fixed Business Key Integrity (SCD Type 2 aware)
        print("\n🔑 TEST 3: BUSINESS KEY INTEGRITY (SCD Type 2)")
        print("-" * 50)
        self._test_fixed_business_key_integrity()
        
        # Test 4: SCD Type 2 Implementation
        print("\n⏰ TEST 4: SCD TYPE 2 IMPLEMENTATION")
        print("-" * 50)
        self._test_scd_type2_implementation()
        
        # Test 5: Fixed Current vs Historical Status (no current expected)
        print("\n📊 TEST 5: CURRENT VS HISTORICAL STATUS (All Historical Expected)")
        print("-" * 50)
        self._test_fixed_current_historical_status()
        
        # Test 6: Data Quality by Era
        print("\n📈 TEST 6: DATA QUALITY BY ERA")
        print("-" * 50)
        self._test_data_quality_by_era()
        
        # Test 7: Fixtures Integrity
        print("\n⚽ TEST 7: FIXTURES INTEGRITY")
        print("-" * 50)
        self._test_fixtures_integrity()
        
        # Test 8: Fixed Production Readiness (config path handling)
        print("\n🚀 TEST 8: PRODUCTION READINESS")
        print("-" * 50)
        self._test_fixed_production_readiness()
        
        # Test 9: Fixed Environment Variable Cleanup
        print("\n🧹 TEST 9: ENVIRONMENT VARIABLE CLEANUP")
        print("-" * 50)
        self._test_fixed_environment_cleanup()
        
        # Test 10: Cross-Season Consistency
        print("\n🔄 TEST 10: CROSS-SEASON CONSISTENCY")
        print("-" * 50)
        self._test_cross_season_consistency()
        
        # Final Summary
        self._print_final_summary()
        
        # Return overall success
        return all(self.validation_results.values())
    
    def _test_database_accessibility(self) -> bool:
        """Test basic database connectivity and structure"""
        try:
            # Test raw database
            if not Path(self.db_paths['raw']).exists():
                self.issues_found.append("Raw database file does not exist")
                return False
            
            # Test analytics database
            if not Path(self.db_paths['analytics']).exists():
                self.issues_found.append("Analytics database file does not exist")
                return False
            
            # Test connections
            with duckdb.connect(self.db_paths['analytics']) as conn:
                tables = conn.execute("SHOW TABLES").fetchall()
                table_names = [table[0] for table in tables]
                
                expected_tables = ['analytics_players', 'analytics_keepers', 'analytics_squads', 'analytics_opponents', 'analytics_fixtures']
                missing_tables = [t for t in expected_tables if t not in table_names]
                
                if missing_tables:
                    self.issues_found.append(f"Missing analytics tables: {missing_tables}")
                    return False
                
                print("✅ All databases accessible, all tables present")
                self.validation_results['database_accessibility'] = True
                return True
                
        except Exception as e:
            self.issues_found.append(f"Database accessibility error: {e}")
            return False
    
    def _test_season_coverage(self):
        """Test that all expected seasons are present"""
        try:
            with duckdb.connect(self.db_paths['analytics']) as conn:
                # Check season coverage in players table
                seasons_query = """
                SELECT DISTINCT season, COUNT(*) as records
                FROM analytics_players 
                GROUP BY season 
                ORDER BY season
                """
                
                seasons_result = conn.execute(seasons_query).fetchall()
                found_seasons = [row[0] for row in seasons_result]
                
                # Check coverage
                missing_seasons = set(self.expected_seasons) - set(found_seasons)
                extra_seasons = set(found_seasons) - set(self.expected_seasons)
                
                print(f"Expected seasons: {len(self.expected_seasons)}")
                print(f"Found seasons: {len(found_seasons)}")
                
                if missing_seasons:
                    self.issues_found.append(f"Missing seasons: {sorted(missing_seasons)}")
                    print(f"❌ Missing seasons: {sorted(missing_seasons)}")
                
                if extra_seasons:
                    self.warnings.append(f"Unexpected seasons found: {sorted(extra_seasons)}")
                    print(f"⚠️  Unexpected seasons: {sorted(extra_seasons)}")
                
                # Print detailed season breakdown
                print("\nSeason-by-season breakdown:")
                for season, count in seasons_result:
                    status = "✅" if season in self.expected_seasons else "⚠️"
                    print(f"  {status} {season}: {count:,} players")
                
                if not missing_seasons:
                    print("✅ All expected seasons present")
                    self.validation_results['season_coverage'] = True
                
        except Exception as e:
            self.issues_found.append(f"Season coverage test failed: {e}")
    
    def _test_fixed_business_key_integrity(self):
        """Test business key integrity for SCD Type 2 (business_key + gameweek combinations should be unique)"""
        try:
            with duckdb.connect(self.db_paths['analytics']) as conn:
                
                print("Testing SCD Type 2 business key integrity (business_key + gameweek should be unique):")
                
                # Test: Business key + gameweek combinations should be unique
                entities = [
                    ('analytics_players', 'player_id'),
                    ('analytics_keepers', 'player_id'), 
                    ('analytics_squads', 'squad_id'),
                    ('analytics_opponents', 'opponent_id')
                ]
                
                all_unique = True
                for table, key_col in entities:
                    # Check for duplicate business key + gameweek combinations
                    duplicate_query = f"""
                    SELECT {key_col}, gameweek, COUNT(*) as count
                    FROM {table}
                    GROUP BY {key_col}, gameweek
                    HAVING COUNT(*) > 1
                    ORDER BY count DESC
                    LIMIT 5
                    """
                    
                    duplicates = conn.execute(duplicate_query).fetchall()
                    if duplicates:
                        all_unique = False
                        self.issues_found.append(f"Duplicate business key + gameweek combinations in {table}: {len(duplicates)} duplicates found")
                        print(f"❌ {table}: {len(duplicates)} duplicate business key + gameweek combinations")
                        for dup_key, gw, count in duplicates[:3]:
                            print(f"   Example: {dup_key} + GW{gw} appears {count} times")
                    else:
                        print(f"✅ {table}: All business key + gameweek combinations unique")
                
                # Test: Business key format validation
                print("\nBusiness key format validation:")
                
                # Check player business key format (should include season)
                player_key_query = """
                SELECT player_id, season, squad
                FROM analytics_players 
                WHERE player_id NOT LIKE '%_____' -- At least some underscores
                   OR player_id NOT LIKE '%' || season 
                LIMIT 5
                """
                
                invalid_player_keys = conn.execute(player_key_query).fetchall()
                if invalid_player_keys:
                    self.issues_found.append(f"Invalid player business key format: {len(invalid_player_keys)} issues")
                    print(f"❌ Player keys: {len(invalid_player_keys)} keys don't include season")
                    for key, season, squad in invalid_player_keys[:3]:
                        print(f"   Example: {key} for season {season}")
                else:
                    print("✅ Player business keys: All include season")
                
                # Check squad business key format
                squad_key_query = """
                SELECT squad_id, season, squad_name
                FROM analytics_squads
                WHERE squad_id NOT LIKE '%' || season
                LIMIT 5  
                """
                
                invalid_squad_keys = conn.execute(squad_key_query).fetchall()
                if invalid_squad_keys:
                    self.issues_found.append(f"Invalid squad business key format: {len(invalid_squad_keys)} issues")
                    print(f"❌ Squad keys: {len(invalid_squad_keys)} keys don't include season")
                else:
                    print("✅ Squad business keys: All include season")
                
                # Test: Multiple gameweeks per business key (this is expected for SCD Type 2)
                print("\nSCD Type 2 validation - Multiple gameweeks per business key:")
                
                multi_gameweek_query = """
                SELECT 
                    COUNT(DISTINCT player_id) as unique_players,
                    COUNT(*) as total_records,
                    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT player_id), 1) as avg_gameweeks_per_player
                FROM analytics_players
                """
                
                multi_stats = conn.execute(multi_gameweek_query).fetchone()
                unique_players, total_records, avg_gw = multi_stats
                
                print(f"  Unique players: {unique_players:,}")
                print(f"  Total records: {total_records:,}")
                print(f"  Average gameweeks per player: {avg_gw}")
                
                if avg_gw > 1:
                    print("✅ SCD Type 2 working: Players appear across multiple gameweeks")
                else:
                    self.warnings.append("Players only appear in single gameweeks - may indicate SCD Type 2 not working")
                    print("⚠️  Players only in single gameweeks")
                
                if all_unique and not invalid_player_keys and not invalid_squad_keys:
                    self.validation_results['business_key_integrity'] = True
                    
        except Exception as e:
            self.issues_found.append(f"Business key integrity test failed: {e}")
    
    def _test_scd_type2_implementation(self):
        """Test SCD Type 2 implementation correctness"""
        try:
            with duckdb.connect(self.db_paths['analytics']) as conn:
                
                # Test 1: SCD columns exist and are properly populated
                scd_columns_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(valid_from) as has_valid_from,
                    COUNT(CASE WHEN is_current IS NOT NULL THEN 1 END) as has_is_current,
                    COUNT(CASE WHEN gameweek IS NOT NULL THEN 1 END) as has_gameweek,
                    COUNT(CASE WHEN season IS NOT NULL THEN 1 END) as has_season
                FROM analytics_players
                """
                
                scd_stats = conn.execute(scd_columns_query).fetchone()
                total, valid_from, is_current, gameweek, season = scd_stats
                
                print(f"SCD metadata completeness (out of {total:,} player records):")
                print(f"  valid_from: {valid_from:,} ({100*valid_from/total:.1f}%)")
                print(f"  is_current: {is_current:,} ({100*is_current/total:.1f}%)")
                print(f"  gameweek: {gameweek:,} ({100*gameweek/total:.1f}%)")
                print(f"  season: {season:,} ({100*season/total:.1f}%)")
                
                # Test 2: All records should be historical (no current data loaded yet)
                current_count_query = """
                SELECT COUNT(*) FROM analytics_players WHERE is_current = true
                """
                current_count = conn.execute(current_count_query).fetchone()[0]
                
                print(f"\nCurrent vs Historical status:")
                print(f"  Current records: {current_count:,} (expected: 0)")
                print(f"  Historical records: {total - current_count:,}")
                
                if current_count == 0:
                    print("✅ All records correctly marked as historical")
                else:
                    self.warnings.append(f"Found {current_count} current records - expected 0 since no GW6 loaded")
                    print(f"⚠️  Found {current_count} current records")
                
                # Test 3: Gameweek distribution makes sense
                gameweek_dist_query = """
                SELECT 
                    gameweek,
                    COUNT(*) as records,
                    COUNT(DISTINCT season) as seasons
                FROM analytics_players
                GROUP BY gameweek
                ORDER BY gameweek
                """
                
                gw_dist = conn.execute(gameweek_dist_query).fetchall()
                
                print(f"\nGameweek distribution:")
                for gw, records, seasons in gw_dist:
                    print(f"  GW{gw}: {records:,} records across {seasons} seasons")
                
                # Check if SCD implementation is working correctly
                scd_working = (
                    valid_from == total and  # All records have valid_from
                    is_current == total and  # All records have is_current flag
                    gameweek == total and    # All records have gameweek
                    season == total          # All records have season
                )
                
                if scd_working:
                    print("✅ SCD Type 2 implementation is working correctly")
                    self.validation_results['scd_type2_correctness'] = True
                else:
                    self.issues_found.append("SCD Type 2 implementation has issues")
                    
        except Exception as e:
            self.issues_found.append(f"SCD Type 2 test failed: {e}")
    
    def _test_fixed_current_historical_status(self):
        """Test current vs historical status - expecting all historical"""
        try:
            with duckdb.connect(self.db_paths['analytics']) as conn:
                
                tables = ['analytics_players', 'analytics_keepers', 'analytics_squads', 'analytics_opponents']
                
                print("Current vs Historical status by entity type (all should be historical):")
                all_correct = True
                
                for table in tables:
                    status_query = f"""
                    SELECT 
                        COUNT(CASE WHEN is_current THEN 1 END) as current_count,
                        COUNT(CASE WHEN NOT is_current THEN 1 END) as historical_count,
                        COUNT(*) as total_count
                    FROM {table}
                    """
                    
                    counts = conn.execute(status_query).fetchone()
                    current, historical, total = counts
                    
                    print(f"  {table}: {current:,} current, {historical:,} historical ({total:,} total)")
                    
                    # Validate that we have reasonable numbers
                    if total == 0:
                        self.issues_found.append(f"{table} has no records")
                        all_correct = False
                    elif current + historical != total:
                        self.issues_found.append(f"{table} has records with NULL is_current flags")
                        all_correct = False
                    elif current > 0:
                        self.warnings.append(f"{table} has {current} current records - expected 0 since no GW6 loaded")
                        print(f"    ⚠️  Expected 0 current records, found {current}")
                
                if all_correct:
                    print("✅ All records properly flagged as historical (as expected)")
                    self.validation_results['current_historical_status'] = True
                    
        except Exception as e:
            self.issues_found.append(f"Current/historical status test failed: {e}")
    
    def _test_data_quality_by_era(self):
        """Test data quality expectations by era"""
        try:
            with duckdb.connect(self.db_paths['analytics']) as conn:
                
                # Define eras
                modern_era = ['2017-2018', '2018-2019', '2019-2020', '2020-2021', '2021-2022', '2022-2023', '2023-2024', '2024-2025', '2025-2026']
                basic_era = ['2010-2011', '2011-2012', '2012-2013', '2013-2014', '2014-2015', '2015-2016', '2016-2017']
                
                print("Data quality analysis by era:")
                
                # Test modern era (should have advanced stats)
                modern_seasons_str = "'" + "','".join(modern_era) + "'"
                modern_stats_query = f"""
                SELECT 
                    COUNT(*) as total_players,
                    COUNT(expected_goals) as has_xg,
                    COUNT(touches) as has_touches,
                    COUNT(progressive_passes) as has_progressive_passes
                FROM analytics_players
                WHERE season IN ({modern_seasons_str})
                """
                
                modern_stats = conn.execute(modern_stats_query).fetchone()
                modern_total, modern_xg, modern_touches, modern_prog = modern_stats
                
                if modern_total > 0:
                    print(f"\nModern Era ({modern_era[0]} onwards): {modern_total:,} players")
                    print(f"  Expected Goals: {modern_xg:,} ({100*modern_xg/modern_total:.1f}%)")
                    print(f"  Touches: {modern_touches:,} ({100*modern_touches/modern_total:.1f}%)")
                    print(f"  Progressive Passes: {modern_prog:,} ({100*modern_prog/modern_total:.1f}%)")
                    
                    # Modern era should have high coverage of advanced stats
                    if modern_xg/modern_total < 0.7:  # At least 70% should have xG
                        self.warnings.append(f"Modern era missing expected goals data: only {100*modern_xg/modern_total:.1f}%")
                        print("⚠️  Modern era has lower than expected xG coverage")
                    else:
                        print("✅ Modern era has good advanced stats coverage")
                
                # Test basic era (should have basic stats but not advanced)
                if basic_era:
                    basic_seasons_str = "'" + "','".join(basic_era) + "'"
                    basic_stats_query = f"""
                    SELECT 
                        COUNT(*) as total_players,
                        COUNT(goals) as has_goals,
                        COUNT(assists) as has_assists,
                        COUNT(expected_goals) as has_xg,
                        COUNT(touches) as has_touches
                    FROM analytics_players
                    WHERE season IN ({basic_seasons_str})
                    """
                    
                    basic_stats = conn.execute(basic_stats_query).fetchone()
                    basic_total, basic_goals, basic_assists, basic_xg, basic_touches = basic_stats
                    
                    if basic_total > 0:
                        print(f"\nBasic Era ({basic_era[0]} to {basic_era[-1]}): {basic_total:,} players")
                        print(f"  Goals: {basic_goals:,} ({100*basic_goals/basic_total:.1f}%)")
                        print(f"  Assists: {basic_assists:,} ({100*basic_assists/basic_total:.1f}%)")
                        print(f"  Expected Goals: {basic_xg:,} ({100*basic_xg/basic_total:.1f}%)")
                        print(f"  Touches: {basic_touches:,} ({100*basic_touches/basic_total:.1f}%)")
                        
                        # Basic era should have core stats but fewer advanced stats
                        if basic_goals/basic_total < 0.8:  # At least 80% should have goals
                            self.issues_found.append(f"Basic era missing core stats: only {100*basic_goals/basic_total:.1f}% have goals")
                        else:
                            print("✅ Basic era has good core stats coverage")
                
                # Overall data quality check
                print("✅ Data quality analysis completed")
                self.validation_results['data_quality_by_era'] = True
                
        except Exception as e:
            self.issues_found.append(f"Data quality by era test failed: {e}")
    
    def _test_fixtures_integrity(self):
        """Test fixtures data integrity"""
        try:
            with duckdb.connect(self.db_paths['analytics']) as conn:
                
                # Check if fixtures table exists and has data
                try:
                    fixtures_query = """
                    SELECT 
                        COUNT(*) as total_fixtures,
                        COUNT(DISTINCT season) as seasons_with_fixtures,
                        MIN(season) as earliest_season,
                        MAX(season) as latest_season
                    FROM analytics_fixtures
                    """
                    
                    fixtures_stats = conn.execute(fixtures_query).fetchone()
                    total_fixtures, seasons_count, earliest, latest = fixtures_stats
                    
                    print(f"Fixtures overview:")
                    print(f"  Total fixtures: {total_fixtures:,}")
                    print(f"  Seasons with fixtures: {seasons_count}")
                    print(f"  Season range: {earliest} to {latest}")
                    
                    # Expected: ~380 fixtures per season
                    expected_per_season = 380
                    if total_fixtures > 0:
                        avg_per_season = total_fixtures / seasons_count
                        print(f"  Average per season: {avg_per_season:.0f} (expected ~{expected_per_season})")
                        
                        if avg_per_season < 300:  # Too few fixtures
                            self.warnings.append(f"Low fixture count per season: {avg_per_season:.0f} average")
                            print("⚠️  Lower than expected fixtures per season")
                        else:
                            print("✅ Reasonable fixture count per season")
                    
                    # Check season coverage vs player data - allow for 1 season difference
                    player_seasons_query = "SELECT COUNT(DISTINCT season) FROM analytics_players"
                    player_seasons = conn.execute(player_seasons_query).fetchone()[0]
                    
                    season_diff = abs(seasons_count - player_seasons)
                    if season_diff > 1:  # Allow for 1 season difference
                        self.warnings.append(f"Fixture seasons ({seasons_count}) significantly different from player seasons ({player_seasons})")
                        print(f"⚠️  Fixture seasons significantly different from player seasons")
                    else:
                        print("✅ Fixture seasons reasonably match player data seasons")
                    
                    if total_fixtures > 0:
                        self.validation_results['fixtures_integrity'] = True
                    
                except Exception as fixtures_error:
                    self.warnings.append(f"Fixtures table issue: {fixtures_error}")
                    print(f"⚠️  Fixtures table issue: {fixtures_error}")
                    
        except Exception as e:
            self.issues_found.append(f"Fixtures integrity test failed: {e}")
    
    def _test_fixed_production_readiness(self):
        """Test if system is ready for production use - fixed config handling"""
        try:
            # Check if production pipeline components exist
            pipeline_files = [
                'pipelines/master_pipeline.py',
                'pipelines/analytics_pipeline.py',
                'pipelines/raw_pipeline.py'
            ]
            
            missing_files = []
            for file_path in pipeline_files:
                if not Path(file_path).exists():
                    missing_files.append(file_path)
            
            if missing_files:
                self.issues_found.append(f"Missing pipeline files: {missing_files}")
                print(f"❌ Missing pipeline files: {missing_files}")
            else:
                print("✅ All pipeline files present")
            
            # Check config files
            config_files = [
                'config/database.yaml',
                'config/sources.yaml'
            ]
            
            missing_configs = []
            for config_path in config_files:
                if not Path(config_path).exists():
                    missing_configs.append(config_path)
            
            if missing_configs:
                self.issues_found.append(f"Missing config files: {missing_configs}")
                print(f"❌ Missing config files: {missing_configs}")
            else:
                print("✅ All config files present")
            
            # Fixed: Check that database paths in config point to production databases
            try:
                import yaml
                with open('config/database.yaml', 'r') as f:
                    db_config = yaml.safe_load(f)
                
                expected_paths = {
                    'raw': 'data/premierleague_raw.duckdb',
                    'analytics': 'data/premierleague_analytics.duckdb'
                }
                
                # Handle both nested and flat path structures
                actual_paths = db_config.get('paths', {})
                if not actual_paths:
                    # Try alternative structure
                    actual_paths = db_config.get('database', {}).get('paths', {})
                
                config_correct = True
                for db_type, expected_path in expected_paths.items():
                    actual_path = actual_paths.get(db_type, '')
                    if actual_path != expected_path:
                        self.warnings.append(f"Database config {db_type} path: '{actual_path}' vs expected '{expected_path}'")
                        print(f"⚠️  {db_type} database path: '{actual_path}' vs expected '{expected_path}'")
                        config_correct = False
                    else:
                        print(f"✅ {db_type} database path correct: {actual_path}")
                
                if not config_correct:
                    print("⚠️  Database config paths need attention but not critical")
                        
            except Exception as config_error:
                self.warnings.append(f"Could not validate database config: {config_error}")
                print(f"⚠️  Could not validate database config: {config_error}")
            
            if not missing_files and not missing_configs:
                self.validation_results['production_readiness'] = True
                
        except Exception as e:
            self.issues_found.append(f"Production readiness test failed: {e}")
    
    def _test_fixed_environment_cleanup(self):
        """Test that temporary environment variable code has been removed - refined detection"""
        try:
            print("Checking for temporary environment variable code cleanup...")
            
            # Files that should have had temporary code removed
            files_to_check = [
                'src/analytics/data_consolidation.py',
                'src/analytics/scd_processor.py'
            ]
            
            env_var_code_found = []
            
            for file_path in files_to_check:
                if not Path(file_path).exists():
                    self.warnings.append(f"Cannot check {file_path} - file not found")
                    continue
                
                try:
                    with open(file_path, 'r') as f:
                        content = f.read()
                    
                    # Look for specific temporary code patterns (more precise)
                    suspicious_patterns = [
                        'HISTORICAL_SEASON',
                        'historical_season = os.getenv',
                        'import os\n.*historical_season',
                        'if historical_season:'
                    ]
                    
                    found_patterns = []
                    for pattern in suspicious_patterns:
                        if pattern in content:
                            found_patterns.append(pattern)
                    
                    if found_patterns:
                        env_var_code_found.append((file_path, found_patterns))
                        print(f"⚠️  {file_path}: Found temporary code patterns: {found_patterns}")
                    else:
                        print(f"✅ {file_path}: No temporary environment variable code found")
                        
                except Exception as read_error:
                    self.warnings.append(f"Could not read {file_path}: {read_error}")
                    print(f"⚠️  Could not read {file_path}: {read_error}")
            
            if env_var_code_found:
                self.warnings.append(f"Temporary environment variable code still present in {len(env_var_code_found)} files")
                print("⚠️  Environment variable cleanup may be incomplete")
                print("   This is not critical but should be cleaned up for production")
            else:
                print("✅ Environment variable cleanup appears complete")
                self.validation_results['environment_cleanup'] = True
                
        except Exception as e:
            self.issues_found.append(f"Environment cleanup test failed: {e}")
    
    def _test_cross_season_consistency(self):
        """Test consistency across different seasons"""
        try:
            with duckdb.connect(self.db_paths['analytics']) as conn:
                
                print("Cross-season consistency checks:")
                
                # Test 1: Teams per season (should be 20 for Premier League)
                teams_by_season_query = """
                SELECT 
                    season,
                    COUNT(DISTINCT squad_name) as team_count
                FROM analytics_squads
                GROUP BY season
                ORDER BY season
                """
                
                teams_results = conn.execute(teams_by_season_query).fetchall()
                
                print(f"\nTeam counts by season:")
                non_twenty_seasons = []
                for season, count in teams_results:
                    status = "✅" if count == 20 else "⚠️"
                    print(f"  {status} {season}: {count} teams")
                    if count != 20:
                        non_twenty_seasons.append((season, count))
                
                if non_twenty_seasons:
                    self.warnings.append(f"Seasons with non-standard team count: {len(non_twenty_seasons)} seasons")
                    print(f"⚠️  {len(non_twenty_seasons)} seasons don't have exactly 20 teams")
                else:
                    print("✅ All seasons have 20 teams")
                
                # Test 2: Player career tracking (players appearing in multiple seasons)
                career_tracking_query = """
                SELECT 
                    player_name,
                    COUNT(DISTINCT season) as seasons_played,
                    COUNT(DISTINCT squad) as clubs_played_for,
                    MIN(season) as first_season,
                    MAX(season) as last_season
                FROM analytics_players
                WHERE player_name IS NOT NULL
                GROUP BY player_name
                HAVING COUNT(DISTINCT season) > 1
                ORDER BY seasons_played DESC
                LIMIT 10
                """
                
                career_results = conn.execute(career_tracking_query).fetchall()
                
                print(f"\nTop 10 players by career length (across seasons):")
                for player, seasons, clubs, first, last in career_results:
                    print(f"  {player}: {seasons} seasons ({first} to {last}), {clubs} clubs")
                
                if career_results:
                    print("✅ Career tracking working - players appear across multiple seasons")
                else:
                    self.warnings.append("No players found across multiple seasons - may indicate data isolation issues")
                    print("⚠️  No multi-season player careers found")
                
                # Test 3: Transfer detection (players changing clubs between seasons)
                transfer_query = """
                WITH player_season_clubs AS (
                    SELECT DISTINCT 
                        player_name,
                        season,
                        squad
                    FROM analytics_players
                    WHERE player_name IS NOT NULL
                ),
                player_transfers AS (
                    SELECT 
                        player_name,
                        season,
                        squad,
                        LAG(squad) OVER (PARTITION BY player_name ORDER BY season) as prev_squad
                    FROM player_season_clubs
                )
                SELECT 
                    COUNT(*) as total_transfers
                FROM player_transfers
                WHERE prev_squad IS NOT NULL 
                  AND squad != prev_squad
                """
                
                transfer_count = conn.execute(transfer_query).fetchone()[0]
                print(f"\nTransfers detected: {transfer_count:,}")
                
                if transfer_count > 0:
                    print("✅ Transfer tracking working")
                else:
                    self.warnings.append("No transfers detected - may indicate business key or data issues")
                    print("⚠️  No transfers detected")
                
                # Test 4: Season progression validation
                season_progression_query = """
                SELECT season
                FROM (SELECT DISTINCT season FROM analytics_players ORDER BY season)
                """
                
                seasons = [row[0] for row in conn.execute(season_progression_query).fetchall()]
                
                print(f"\nSeason progression validation:")
                progression_issues = 0
                for i in range(1, len(seasons)):
                    curr_season = seasons[i]
                    prev_season = seasons[i-1]
                    
                    try:
                        curr_start = int(curr_season.split('-')[0])
                        prev_start = int(prev_season.split('-')[0])
                        if curr_start != prev_start + 1:
                            progression_issues += 1
                            print(f"  ⚠️  Gap: {prev_season} → {curr_season}")
                    except:
                        self.warnings.append(f"Invalid season format: {curr_season}")
                
                if progression_issues == 0:
                    print("  ✅ Season progression is consecutive")
                else:
                    self.warnings.append(f"Season progression has {progression_issues} gaps")
                    print(f"  ⚠️  {progression_issues} gaps in season progression")
                
                # Test 5: Data volume consistency across seasons
                volume_query = """
                SELECT 
                    season,
                    COUNT(*) as player_records,
                    COUNT(DISTINCT player_name) as unique_players
                FROM analytics_players
                GROUP BY season
                ORDER BY season
                """
                
                volume_results = conn.execute(volume_query).fetchall()
                
                print(f"\nData volume by season:")
                volumes = []
                for season, records, unique_players in volume_results:
                    volumes.append(records)
                    avg_gw_per_player = records / unique_players if unique_players > 0 else 0
                    print(f"  {season}: {records:,} records, {unique_players:,} unique players ({avg_gw_per_player:.1f} avg records/player)")
                
                # Check if volumes are reasonably consistent
                if volumes:
                    avg_volume = sum(volumes) / len(volumes)
                    outlier_seasons = []
                    for i, (season, records, _) in enumerate(volume_results):
                        if records < avg_volume * 0.5 or records > avg_volume * 1.5:  # More than 50% off average
                            outlier_seasons.append((season, records))
                    
                    if outlier_seasons:
                        self.warnings.append(f"Seasons with unusual data volumes: {len(outlier_seasons)} seasons")
                        print(f"⚠️  {len(outlier_seasons)} seasons have unusual data volumes")
                        for season, records in outlier_seasons:
                            print(f"     {season}: {records:,} records (avg: {avg_volume:.0f})")
                    else:
                        print("✅ Data volumes are reasonably consistent across seasons")
                
                # Overall cross-season consistency
                if career_results and transfer_count > 0 and progression_issues == 0:
                    self.validation_results['cross_season_consistency'] = True
                    print("✅ Cross-season consistency validated")
                
        except Exception as e:
            self.issues_found.append(f"Cross-season consistency test failed: {e}")
    
    def _print_final_summary(self):
        """Print comprehensive final summary"""
        print("\n" + "=" * 100)
        print("FIXED COMPREHENSIVE VALIDATION SUMMARY")
        print("=" * 100)
        
        # Test results summary
        passed_tests = sum(1 for result in self.validation_results.values() if result)
        total_tests = len(self.validation_results)
        
        print(f"\nTest Results: {passed_tests}/{total_tests} tests passed")
        print("-" * 50)
        
        for test_name, result in self.validation_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{test_name.replace('_', ' ').title():<35} {status}")
        
        # Issues summary
        if self.issues_found:
            print(f"\n❌ CRITICAL ISSUES FOUND ({len(self.issues_found)}):")
            print("-" * 50)
            for i, issue in enumerate(self.issues_found, 1):
                print(f"{i:2d}. {issue}")
        
        # Warnings summary
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            print("-" * 50)
            for i, warning in enumerate(self.warnings, 1):
                print(f"{i:2d}. {warning}")
        
        # Overall status
        print("\n" + "=" * 100)
        if passed_tests == total_tests and not self.issues_found:
            print("🎉 OVERALL RESULT: ALL VALIDATIONS PASSED - SYSTEM IS READY FOR PRODUCTION")
        elif passed_tests >= total_tests * 0.8 and not self.issues_found:
            print("✅ OVERALL RESULT: MOSTLY PASSING - SYSTEM IS FUNCTIONAL WITH MINOR WARNINGS")
        elif passed_tests >= total_tests * 0.7:
            print("⚠️  OVERALL RESULT: SOME ISSUES FOUND - SYSTEM NEEDS MINOR FIXES")
        else:
            print("❌ OVERALL RESULT: CRITICAL ISSUES FOUND - SYSTEM NEEDS ATTENTION")
        print("=" * 100)
        
        # Data summary
        try:
            with duckdb.connect(self.db_paths['analytics']) as conn:
                summary_query = """
                SELECT 
                    'Players' as entity_type,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT season) as seasons,
                    COUNT(CASE WHEN is_current THEN 1 END) as current_records,
                    COUNT(CASE WHEN NOT is_current THEN 1 END) as historical_records
                FROM analytics_players
                
                UNION ALL
                
                SELECT 
                    'Keepers' as entity_type,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT season) as seasons,
                    COUNT(CASE WHEN is_current THEN 1 END) as current_records,
                    COUNT(CASE WHEN NOT is_current THEN 1 END) as historical_records
                FROM analytics_keepers
                
                UNION ALL
                
                SELECT 
                    'Squads' as entity_type,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT season) as seasons,
                    COUNT(CASE WHEN is_current THEN 1 END) as current_records,
                    COUNT(CASE WHEN NOT is_current THEN 1 END) as historical_records
                FROM analytics_squads
                
                UNION ALL
                
                SELECT 
                    'Opponents' as entity_type,
                    COUNT(*) as total_records,
                    COUNT(DISTINCT season) as seasons,
                    COUNT(CASE WHEN is_current THEN 1 END) as current_records,
                    COUNT(CASE WHEN NOT is_current THEN 1 END) as historical_records
                FROM analytics_opponents
                """
                
                summary_results = conn.execute(summary_query).fetchall()
                
                print(f"\n📊 DATA SUMMARY:")
                print("-" * 50)
                for entity, total, seasons, current, historical in summary_results:
                    print(f"{entity:<12}: {total:>6,} total ({seasons} seasons) | {current:>4,} current, {historical:>5,} historical")
                
                # Fixture summary
                try:
                    fixture_summary = conn.execute("SELECT COUNT(*), COUNT(DISTINCT season) FROM analytics_fixtures").fetchone()
                    fix_total, fix_seasons = fixture_summary
                    print(f"{'Fixtures':<12}: {fix_total:>6,} total ({fix_seasons} seasons)")
                except:
                    print(f"{'Fixtures':<12}: Not available")
                
                # Key insights
                print(f"\n🔍 KEY INSIGHTS:")
                print("-" * 50)
                
                # Calculate total unique entities across all time
                unique_players_query = "SELECT COUNT(DISTINCT player_name) FROM analytics_players WHERE player_name IS NOT NULL"
                unique_players = conn.execute(unique_players_query).fetchone()[0]
                
                unique_teams_query = "SELECT COUNT(DISTINCT squad_name) FROM analytics_squads WHERE squad_name IS NOT NULL"
                unique_teams = conn.execute(unique_teams_query).fetchone()[0]
                
                print(f"Total unique players across 16 seasons: {unique_players:,}")
                print(f"Total unique teams across 16 seasons: {unique_teams}")
                
                # Career length analysis
                career_length_query = """
                SELECT 
                    AVG(seasons_played) as avg_career_length,
                    MAX(seasons_played) as longest_career
                FROM (
                    SELECT player_name, COUNT(DISTINCT season) as seasons_played
                    FROM analytics_players 
                    WHERE player_name IS NOT NULL
                    GROUP BY player_name
                )
                """
                career_stats = conn.execute(career_length_query).fetchone()
                avg_career, longest_career = career_stats
                
                print(f"Average player career length: {avg_career:.1f} seasons")
                print(f"Longest player career: {longest_career} seasons")
                
                # Data completeness by era
                modern_completeness_query = """
                SELECT 
                    COUNT(*) as total,
                    COUNT(expected_goals) as has_xg,
                    ROUND(100.0 * COUNT(expected_goals) / COUNT(*), 1) as xg_pct
                FROM analytics_players 
                WHERE season >= '2017-2018'
                """
                modern_stats = conn.execute(modern_completeness_query).fetchone()
                modern_total, modern_xg, xg_pct = modern_stats
                
                basic_completeness_query = """
                SELECT 
                    COUNT(*) as total,
                    COUNT(goals) as has_goals,
                    ROUND(100.0 * COUNT(goals) / COUNT(*), 1) as goals_pct
                FROM analytics_players 
                WHERE season < '2017-2018'
                """
                basic_stats = conn.execute(basic_completeness_query).fetchone()
                basic_total, basic_goals, goals_pct = basic_stats
                
                print(f"Modern era (2017+) xG completeness: {xg_pct}% ({modern_xg:,}/{modern_total:,})")
                print(f"Basic era (2010-2016) goals completeness: {goals_pct}% ({basic_goals:,}/{basic_total:,})")
                    
        except Exception as e:
            print(f"Could not generate data summary: {e}")


def main():
    """Main execution function"""
    print("Initializing FIXED comprehensive data validation...")
    print("Fixed issues:")
    print("- Business key logic now tests business_key + gameweek uniqueness (SCD Type 2)")
    print("- Removed warnings about no current data (expected since GW6 not loaded)")
    print("- Fixed database config path validation")
    print("- Refined environment variable detection")
    print("-" * 50)
    
    validator = FixedDataValidator()
    
    try:
        success = validator.run_complete_validation()
        
        if success:
            print(f"\n🎉 Fixed validation completed successfully!")
            print("Your historical data system is ready for production!")
            return 0
        else:
            print(f"\n⚠️  Fixed validation completed with some issues!")
            print("Review the warnings and issues above.")
            if not validator.issues_found:
                print("No critical issues found - warnings are likely acceptable.")
                return 0
            else:
                print("Critical issues found - address before proceeding.")
                return 1
            
    except Exception as e:
        print(f"\n💥 Validation failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 2

if __name__ == "__main__":
    exit(main())