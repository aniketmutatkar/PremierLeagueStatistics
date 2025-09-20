#!/usr/bin/env python3
"""
Unified Analytics System Validation - Built from Scratch
Comprehensive validation for the complete analytics system with all entity types:
players, keepers, squads, and opponents
"""

import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys
from typing import List, Dict, Any, Tuple

class AnalyticsValidator:
    """Validates the complete analytics system with all entity types"""
    
    def __init__(self, db_path: str = "data/premierleague_analytics.duckdb"):
        self.db_path = db_path
        self.conn = None
        
        # Define all expected tables and their entity types
        self.entity_tables = {
            'analytics_players': {
                'entity_type': 'player',
                'id_column': 'player_id',
                'key_column': 'player_key',
                'name_column': 'player_name',
                'expected_min': 300,
                'expected_max': 500
            },
            'analytics_keepers': {
                'entity_type': 'player',
                'id_column': 'player_id', 
                'key_column': 'player_key',
                'name_column': 'player_name',
                'expected_min': 15,
                'expected_max': 30
            },
            'analytics_squads': {
                'entity_type': 'squad',
                'id_column': 'squad_id',
                'key_column': 'squad_key', 
                'name_column': 'squad_name',
                'expected_min': 15,
                'expected_max': 25
            },
            'analytics_opponents': {
                'entity_type': 'opponent',
                'id_column': 'opponent_id',
                'key_column': 'opponent_key',
                'name_column': 'squad_name',
                'expected_min': 15,
                'expected_max': 25
            }
        }
        
        # Required SCD Type 2 columns for all tables
        self.required_scd_columns = [
            'gameweek', 'season', 'valid_from', 'valid_to', 'is_current'
        ]
        
    def __enter__(self):
        self.conn = duckdb.connect(self.db_path)
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
    
    def run_complete_validation(self) -> bool:
        """Run comprehensive validation for all entity types"""
        print("=" * 80)
        print("UNIFIED ANALYTICS SYSTEM VALIDATION")
        print("=" * 80)
        print(f"Database: {self.db_path}")
        print(f"Validation time: {datetime.now()}")
        print("=" * 80)
        
        validation_results = []
        
        # 1. Schema validation
        schema_valid = self.validate_complete_schema()
        validation_results.append(("Schema Validation", schema_valid))
        
        # 2. SCD Type 2 validation
        scd_valid = self.validate_complete_scd_integrity()
        validation_results.append(("SCD Type 2 Validation", scd_valid))
        
        # 3. Data quality validation
        quality_valid = self.validate_complete_data_quality()
        validation_results.append(("Data Quality Validation", quality_valid))
        
        # 4. Cross-entity validation
        cross_valid = self.validate_cross_entity_relationships()
        validation_results.append(("Cross-Entity Validation", cross_valid))
        
        # 5. Business logic validation
        business_valid = self.validate_business_logic()
        validation_results.append(("Business Logic Validation", business_valid))
        
        # Summary
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)
        
        all_passed = True
        for test_name, passed in validation_results:
            status = "PASS" if passed else "FAIL"
            print(f"{test_name:.<60} {status}")
            if not passed:
                all_passed = False
        
        print("=" * 80)
        overall_status = "ALL VALIDATIONS PASSED" if all_passed else "VALIDATION FAILURES DETECTED"
        print(f"OVERALL RESULT: {overall_status}")
        print("=" * 80)
        
        return all_passed
    
    def validate_complete_schema(self) -> bool:
        """Validate database schema for all entity types"""
        print("\n🔍 VALIDATING COMPLETE SCHEMA")
        print("-" * 60)
        
        try:
            # Check if all expected tables exist
            existing_tables = self.conn.execute("SHOW TABLES").fetchall()
            existing_table_names = [table[0] for table in existing_tables]
            
            expected_tables = list(self.entity_tables.keys())
            missing_tables = [t for t in expected_tables if t not in existing_table_names]
            
            if missing_tables:
                print(f"❌ Missing tables: {missing_tables}")
                return False
            
            print(f"✅ All expected tables present: {expected_tables}")
            
            # Validate each table structure
            for table_name, table_info in self.entity_tables.items():
                if not self._validate_table_schema(table_name, table_info):
                    return False
            
            return True
            
        except Exception as e:
            print(f"❌ Schema validation failed: {e}")
            return False
    
    def _validate_table_schema(self, table_name: str, table_info: Dict) -> bool:
        """Validate individual table schema"""
        try:
            # Get table columns
            columns_info = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            column_names = [col[1] for col in columns_info]
            column_count = len(column_names)
            
            print(f"  {table_name}: {column_count} columns")
            
            # Check for required SCD columns
            missing_scd = [col for col in self.required_scd_columns if col not in column_names]
            if missing_scd:
                print(f"    ❌ Missing SCD columns: {missing_scd}")
                return False
            
            # Check for entity-specific key columns
            required_keys = [table_info['key_column'], table_info['name_column']]
            missing_keys = [col for col in required_keys if col not in column_names]
            if missing_keys:
                print(f"    ❌ Missing key columns: {missing_keys}")
                return False
            
            print(f"    ✅ Schema valid")
            return True
            
        except Exception as e:
            print(f"    ❌ Schema validation failed for {table_name}: {e}")
            return False
    
    def validate_complete_scd_integrity(self) -> bool:
        """Validate SCD Type 2 integrity for all entity types"""
        print("\n⏰ VALIDATING SCD TYPE 2 INTEGRITY")
        print("-" * 60)
        
        try:
            all_valid = True
            
            for table_name, table_info in self.entity_tables.items():
                # Check if table has data
                count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                if count == 0:
                    print(f"  {table_name}: No data (skipping SCD validation)")
                    continue
                
                table_valid = self._validate_table_scd_integrity(table_name, table_info)
                if not table_valid:
                    all_valid = False
            
            if all_valid:
                print("✅ SCD Type 2 integrity validated for all tables")
            
            return all_valid
            
        except Exception as e:
            print(f"❌ SCD validation failed: {e}")
            return False
    
    def _validate_table_scd_integrity(self, table_name: str, table_info: Dict) -> bool:
        """Validate SCD Type 2 integrity for a specific table"""
        try:
            id_column = table_info['id_column']
            entity_type = table_info['entity_type']
            
            # Check 1: Only one gameweek should be current
            current_gameweeks = self.conn.execute(f"""
                SELECT DISTINCT gameweek FROM {table_name} WHERE is_current = true
            """).fetchall()
            
            if len(current_gameweeks) != 1:
                print(f"  {table_name}: ❌ Multiple current gameweeks: {[gw[0] for gw in current_gameweeks]}")
                return False
            
            current_gw = current_gameweeks[0][0]
            
            # Check 2: No duplicate current records per entity
            duplicates = self.conn.execute(f"""
                SELECT {id_column}, COUNT(*) as count
                FROM {table_name} 
                WHERE is_current = true
                GROUP BY {id_column}
                HAVING COUNT(*) > 1
            """).fetchall()
            
            if duplicates:
                print(f"  {table_name}: ❌ {len(duplicates)} entities with duplicate current records")
                return False
            
            # Check 3: Current record counts
            current_entities = self.conn.execute(f"""
                SELECT COUNT(DISTINCT {id_column}) FROM {table_name} WHERE is_current = true
            """).fetchone()[0]
            
            current_records = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE is_current = true
            """).fetchone()[0]
            
            if current_records != current_entities:
                print(f"  {table_name}: ❌ Record count mismatch (records: {current_records}, entities: {current_entities})")
                return False
            
            print(f"  {table_name}: ✅ SCD integrity valid (GW {current_gw}, {current_entities} entities)")
            return True
            
        except Exception as e:
            print(f"  {table_name}: ❌ SCD validation error: {e}")
            return False
    
    def validate_complete_data_quality(self) -> bool:
        """Validate data quality across all entity types"""
        print("\n📊 VALIDATING DATA QUALITY")
        print("-" * 60)
        
        try:
            all_valid = True
            
            for table_name, table_info in self.entity_tables.items():
                table_valid = self._validate_table_data_quality(table_name, table_info)
                if not table_valid:
                    all_valid = False
            
            return all_valid
            
        except Exception as e:
            print(f"❌ Data quality validation failed: {e}")
            return False
    
    def _validate_table_data_quality(self, table_name: str, table_info: Dict) -> bool:
        """Validate data quality for a specific table"""
        try:
            # Get current record count
            current_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE is_current = true
            """).fetchone()[0]
            
            expected_min = table_info['expected_min']
            expected_max = table_info['expected_max']
            name_column = table_info['name_column']
            
            print(f"  {table_name}: {current_count} current records")
            
            # Check if count is in expected range
            if current_count < expected_min or current_count > expected_max:
                print(f"    ❌ Count outside expected range ({expected_min}-{expected_max})")
                return False
            
            # Check for null names
            null_names = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE is_current = true AND ({name_column} IS NULL OR {name_column} = '')
            """).fetchone()[0]
            
            if null_names > 0:
                print(f"    ❌ {null_names} records with null/empty names")
                return False
            
            # Entity-specific validation
            if 'player' in table_name:
                if not self._validate_player_data_quality(table_name):
                    return False
            elif 'squad' in table_name or 'opponent' in table_name:
                if not self._validate_team_data_quality(table_name):
                    return False
            
            print(f"    ✅ Data quality valid")
            return True
            
        except Exception as e:
            print(f"    ❌ Data quality validation failed for {table_name}: {e}")
            return False
    
    def _validate_player_data_quality(self, table_name: str) -> bool:
        """Player-specific data quality checks"""
        try:
            # Check for players with reasonable touches (if outfield players)
            if table_name == 'analytics_players':
                zero_touches = self.conn.execute(f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE is_current = true AND touches = 0 AND minutes_played > 90
                """).fetchone()[0]
                
                if zero_touches > 0:
                    print(f"    ⚠️  {zero_touches} players with 0 touches but >90 minutes")
            
            # Check for goalkeepers with saves (if keeper table)
            if table_name == 'analytics_keepers':
                keepers_with_saves = self.conn.execute(f"""
                    SELECT COUNT(*) FROM {table_name} 
                    WHERE is_current = true AND saves > 0
                """).fetchone()[0]
                
                if keepers_with_saves == 0:
                    print(f"    ❌ No goalkeepers have saves recorded")
                    return False
            
            return True
            
        except Exception as e:
            print(f"    ❌ Player validation error: {e}")
            return False
    
    def _validate_team_data_quality(self, table_name: str) -> bool:
        """Team-specific data quality checks"""
        try:
            # Check for teams with reasonable stats
            teams_with_goals = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE is_current = true AND goals > 0
            """).fetchone()[0]
            
            total_teams = self.conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE is_current = true
            """).fetchone()[0]
            
            if teams_with_goals == 0 and total_teams > 0:
                print(f"    ❌ No teams have goals recorded")
                return False
            
            return True
            
        except Exception as e:
            print(f"    ❌ Team validation error: {e}")
            return False
    
    def validate_cross_entity_relationships(self) -> bool:
        """Validate relationships between different entity types"""
        print("\n🔗 VALIDATING CROSS-ENTITY RELATIONSHIPS")
        print("-" * 60)
        
        try:
            # Check 1: Squad count consistency
            squad_count = self.conn.execute("""
                SELECT COUNT(*) FROM analytics_squads WHERE is_current = true
            """).fetchone()[0]
            
            opponent_count = self.conn.execute("""
                SELECT COUNT(*) FROM analytics_opponents WHERE is_current = true  
            """).fetchone()[0]
            
            print(f"  Squad count: {squad_count}")
            print(f"  Opponent count: {opponent_count}")
            
            if squad_count != opponent_count:
                print(f"  ❌ Squad count ({squad_count}) doesn't match opponent count ({opponent_count})")
                return False
            
            # Check 2: Player squads exist in squad table
            player_squads = set(self.conn.execute("""
                SELECT DISTINCT squad FROM analytics_players WHERE is_current = true
            """).fetchall())
            
            available_squads = set(self.conn.execute("""
                SELECT DISTINCT squad_name FROM analytics_squads WHERE is_current = true
            """).fetchall())
            
            missing_squads = player_squads - available_squads
            if missing_squads:
                print(f"  ❌ Players belong to squads not in squad table: {missing_squads}")
                return False
            
            print(f"  ✅ Cross-entity relationships valid")
            return True
            
        except Exception as e:
            print(f"❌ Cross-entity validation failed: {e}")
            return False
    
    def validate_business_logic(self) -> bool:
        """Validate football-specific business logic"""
        print("\n⚽ VALIDATING BUSINESS LOGIC")
        print("-" * 60)
        
        try:
            # Check 1: Goals should not exceed shots (for players with shots data)
            illogical_players = self.conn.execute("""
                SELECT COUNT(*) FROM analytics_players 
                WHERE is_current = true AND goals > shots AND shots > 0
            """).fetchone()[0]
            
            if illogical_players > 0:
                print(f"  ❌ {illogical_players} players have more goals than shots")
                return False
            
            # Check 2: Squad goals should be sum of player goals (approximately)
            # This is a rough check as squad stats may include different time periods
            
            # Check 3: Goalkeepers should have reasonable save percentages
            unrealistic_saves = self.conn.execute("""
                SELECT COUNT(*) FROM analytics_keepers 
                WHERE is_current = true AND save_percentage > 100
            """).fetchone()[0]
            
            if unrealistic_saves > 0:
                print(f"  ❌ {unrealistic_saves} goalkeepers have save percentage > 100%")
                return False
            
            print(f"  ✅ Business logic validation passed")
            return True
            
        except Exception as e:
            print(f"❌ Business logic validation failed: {e}")
            return False
    
    def get_system_summary(self) -> Dict[str, Any]:
        """Get comprehensive system summary"""
        try:
            summary = {
                'database_path': self.db_path,
                'validation_time': datetime.now().isoformat(),
                'tables': {}
            }
            
            for table_name, table_info in self.entity_tables.items():
                try:
                    # Get counts
                    total_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    current_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE is_current = true").fetchone()[0]
                    historical_count = total_count - current_count
                    
                    # Get latest gameweek
                    latest_gw = self.conn.execute(f"""
                        SELECT MAX(gameweek) FROM {table_name} WHERE is_current = true
                    """).fetchone()[0]
                    
                    summary['tables'][table_name] = {
                        'entity_type': table_info['entity_type'],
                        'total_records': total_count,
                        'current_records': current_count,
                        'historical_records': historical_count,
                        'latest_gameweek': latest_gw
                    }
                    
                except Exception as e:
                    summary['tables'][table_name] = {'error': str(e)}
            
            return summary
            
        except Exception as e:
            return {'error': str(e)}


if __name__ == "__main__":
    db_path = "data/premierleague_analytics.duckdb"
    
    if not Path(db_path).exists():
        print(f"❌ Analytics database not found: {db_path}")
        sys.exit(1)
    
    with AnalyticsValidator(db_path) as validator:
        success = validator.run_complete_validation()
        
        if success:
            print("\n🎉 All validations passed! Your unified analytics system is working correctly.")
            
            # Print system summary
            summary = validator.get_system_summary()
            print(f"\n📋 SYSTEM SUMMARY:")
            for table_name, info in summary.get('tables', {}).items():
                if 'error' not in info:
                    print(f"  {table_name}: {info['current_records']} current, {info['historical_records']} historical (GW {info['latest_gameweek']})")
        else:
            print("\n❌ Validation failures detected. Please review the issues above.")
            sys.exit(1)