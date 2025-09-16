#!/usr/bin/env python3
"""
Analytics ETL Pipeline - Production pipeline for analytics layer
Orchestrates the complete ETL process from raw to analytics
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple, Dict, Any
import sys
from pathlib import Path

# Add src to path if running as script
if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent / 'src'))

from database.analytics_db import AnalyticsDBConnection, AnalyticsDBOperations
from analytics.etl.player_consolidation import PlayerDataConsolidator
from analytics.etl.derived_metrics import DerivedMetricsCalculator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AnalyticsETLPipeline:
    """Production ETL pipeline for analytics layer"""
    
    def __init__(self):
        self.db = AnalyticsDBConnection()
        self.ops = AnalyticsDBOperations()
        self.consolidator = PlayerDataConsolidator()
        self.calculator = DerivedMetricsCalculator()
        
        self.pipeline_start_time = None
        self.pipeline_stats = {}
    
    def run_full_pipeline(self, target_gameweek: Optional[int] = None, force_refresh: bool = False) -> bool:
        """
        Run the complete analytics ETL pipeline
        
        Args:
            target_gameweek: Specific gameweek to process (None = current)
            force_refresh: Force refresh even if data already exists
            
        Returns:
            True if successful, False otherwise
        """
        self.pipeline_start_time = datetime.now()
        logger.info("🚀 Starting Analytics ETL Pipeline")
        
        try:
            # Step 1: Determine target gameweek
            gameweek = self._determine_target_gameweek(target_gameweek)
            if not gameweek:
                logger.error("Could not determine target gameweek")
                return False
            
            logger.info(f"📊 Processing gameweek: {gameweek}")
            
            # Step 2: Check if update needed
            if not force_refresh and not self._should_update_analytics(gameweek):
                logger.info(f"Analytics already up-to-date for gameweek {gameweek}")
                return True
            
            # Step 3: Validate raw data
            if not self._validate_raw_data(gameweek):
                logger.error("Raw data validation failed")
                return False
            
            # Step 4: Run ETL process
            success = self._execute_etl_process(gameweek)
            
            if success:
                # Step 5: Validate analytics data
                if self._validate_analytics_data(gameweek):
                    self._log_pipeline_success(gameweek)
                    return True
                else:
                    logger.error("Analytics data validation failed")
                    return False
            else:
                logger.error("ETL process failed")
                return False
                
        except Exception as e:
            logger.error(f"Pipeline failed with error: {e}")
            return False
        
        finally:
            self._log_pipeline_completion()
    
    def _determine_target_gameweek(self, target_gameweek: Optional[int]) -> Optional[int]:
        """Determine which gameweek to process"""
        if target_gameweek:
            logger.info(f"Using specified gameweek: {target_gameweek}")
            return target_gameweek
        
        # Get current gameweek from raw data
        current_gw = self.db.get_current_gameweek()
        if current_gw:
            logger.info(f"Auto-detected current gameweek: {current_gw}")
            return current_gw
        
        logger.error("Could not determine current gameweek")
        return None
    
    def _should_update_analytics(self, gameweek: int) -> bool:
        """Check if analytics update is needed"""
        analytics_gw = self.ops.get_current_analytics_gameweek()
        
        if analytics_gw is None:
            logger.info("No analytics data exists - update needed")
            return True
        
        if gameweek > analytics_gw:
            logger.info(f"Analytics behind (GW{analytics_gw} vs GW{gameweek}) - update needed")
            return True
        
        logger.info(f"Analytics current for GW{gameweek}")
        return False
    
    def _validate_raw_data(self, gameweek: int) -> bool:
        """Validate raw data exists and is complete"""
        try:
            with self.db.get_raw_connection() as conn:
                # Check player count
                player_count = conn.execute("""
                    SELECT COUNT(*) FROM player_standard 
                    WHERE current_through_gameweek = ?
                """, [gameweek]).fetchone()[0]
                
                if player_count < 300:  # Expect ~370 players
                    logger.error(f"Insufficient players in raw data: {player_count}")
                    return False
                
                # Check team count
                team_count = conn.execute("""
                    SELECT COUNT(DISTINCT Squad) FROM player_standard 
                    WHERE current_through_gameweek = ?
                """, [gameweek]).fetchone()[0]
                
                if team_count < 20:  # Premier League has 20 teams
                    logger.error(f"Insufficient teams in raw data: {team_count}")
                    return False
                
                logger.info(f"✅ Raw data validation passed: {player_count} players, {team_count} teams")
                return True
                
        except Exception as e:
            logger.error(f"Raw data validation error: {e}")
            return False
    
    def _execute_etl_process(self, gameweek: int) -> bool:
        """Execute the core ETL process"""
        logger.info("🔄 Starting ETL process...")
        
        try:
            with self.db.get_dual_connections() as (raw_conn, analytics_conn):
                
                # Step 1: Consolidate player data
                logger.info("📋 Consolidating player data...")
                consolidated_df = self.consolidator.consolidate_players(raw_conn, gameweek)
                
                if consolidated_df.empty:
                    logger.error("Player consolidation failed - no data")
                    return False
                
                summary = self.consolidator.get_consolidation_summary(consolidated_df)
                logger.info(f"✅ Consolidated {summary['total_players']} players with {summary['total_columns']} columns")
                self.pipeline_stats['consolidated_players'] = summary['total_players']
                
                # Step 2: Calculate team totals
                logger.info("🏟️ Calculating team totals...")
                team_totals = self.ops.get_team_totals(raw_conn, gameweek)
                
                if team_totals.empty:
                    logger.error("Team totals calculation failed")
                    return False
                
                logger.info(f"✅ Calculated totals for {len(team_totals)} teams")
                self.pipeline_stats['teams_processed'] = len(team_totals)
                
                # Step 3: Calculate derived metrics
                logger.info("🧮 Calculating derived metrics...")
                metrics_df = self.calculator.calculate_all_metrics(consolidated_df, team_totals)
                
                if metrics_df.empty:
                    logger.error("Derived metrics calculation failed")
                    return False
                
                metrics_summary = self.calculator.get_metrics_summary()
                logger.info(f"✅ Calculated {metrics_summary['total_metrics_calculated']} derived metrics")
                self.pipeline_stats['derived_metrics'] = metrics_summary['total_metrics_calculated']
                
                # Step 4: Handle SCD Type 2 updates
                logger.info("🕐 Processing SCD Type 2 updates...")
                
                # Mark existing records as historical
                if not self.ops.mark_records_as_historical(gameweek):
                    logger.error("Failed to mark records as historical")
                    return False
                
                # Prepare data for analytics insertion
                analytics_data = self._prepare_analytics_data(metrics_df, gameweek)
                
                # Step 5: Insert into analytics database
                logger.info("💾 Inserting data into analytics database...")
                if not self._insert_analytics_data(analytics_conn, analytics_data, gameweek):
                    logger.error("Failed to insert analytics data")
                    return False
                
                logger.info("✅ ETL process completed successfully")
                return True
                
        except Exception as e:
            logger.error(f"ETL process error: {e}")
            return False
    
    def _prepare_analytics_data(self, metrics_df: pd.DataFrame, gameweek: int) -> pd.DataFrame:
        """Prepare data for analytics database insertion"""
        analytics_df = metrics_df.copy()
        
        # Add required analytics columns
        current_date = datetime.now().date()
        analytics_df['season'] = "2024-25"  # TODO: Make dynamic
        analytics_df['gameweek'] = gameweek
        analytics_df['valid_from'] = current_date
        analytics_df['valid_to'] = None
        analytics_df['is_current'] = True
        
        # Generate player_key (auto-increment will be handled by database)
        # For now, we'll let the database handle this
        analytics_df = analytics_df.drop(columns=['player_key'], errors='ignore')
        
        # Clean column names and ensure they match analytics schema
        analytics_df = self._map_to_analytics_schema(analytics_df)
        
        return analytics_df
    
    def _map_to_analytics_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Map consolidated data to analytics schema column names"""
        # Create mapping from consolidated columns to analytics schema
        column_mapping = {
            'player_name': 'player_name',
            'squad': 'squad', 
            'Born': 'born_year',
            'Nation': 'nation',
            'position': 'position',
            'Age': 'age',
            # Add more mappings as needed
        }
        
        # Rename columns that have direct mappings
        df = df.rename(columns=column_mapping)
        
        # CRITICAL: Create required keys with null safety
        df['player_id'] = df['player_name'] + '_' + df['born_year'].fillna(0).astype(str)  # Business key
        
        # Generate player_key for analytics (unique for each player-squad-gameweek combo)
        import hashlib
        def generate_player_key(row):
            key_string = f"{row['player_id']}_{row['squad']}_{row['gameweek']}"
            return int(hashlib.md5(key_string.encode()).hexdigest()[:8], 16)
        
        df['player_key'] = df.apply(generate_player_key, axis=1)
        
        # Ensure all required columns exist (fill with defaults if missing)
        required_columns = [
            'player_key', 'player_id', 'player_name', 'squad', 'position', 'nation', 'age',
            'season', 'gameweek', 'valid_from', 'valid_to', 'is_current'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                if col in ['valid_to']:
                    df[col] = None
                elif col in ['is_current']:
                    df[col] = True
                else:
                    df[col] = ''
        
        return df
    
    def _insert_analytics_data(self, analytics_conn, analytics_data: pd.DataFrame, gameweek: int) -> bool:
        """Insert data into analytics database"""
        try:
            # Use DuckDB's efficient bulk insert with pandas DataFrame
            analytics_conn.execute("BEGIN TRANSACTION")
            
            # Register the DataFrame with DuckDB so we can query it
            analytics_conn.register('temp_analytics_data', analytics_data)
            
            # Get the columns that exist in analytics_players table
            table_info = analytics_conn.execute("PRAGMA table_info(analytics_players)").fetchall()
            table_columns = [col[1] for col in table_info]  # Keep all columns including player_key
            
            # Only select columns that exist in both DataFrame and table
            available_columns = [col for col in table_columns if col in analytics_data.columns]
            
            if not available_columns:
                raise Exception("No matching columns found between data and analytics table")
            
            columns_str = ', '.join(available_columns)
            
            # Bulk insert using DuckDB's efficient method
            insert_query = f"""
            INSERT INTO analytics_players ({columns_str})
            SELECT {columns_str} 
            FROM temp_analytics_data
            """
            
            analytics_conn.execute(insert_query)
            analytics_conn.execute("COMMIT")
            
            # Unregister the temp table
            analytics_conn.unregister('temp_analytics_data')
            
            logger.info(f"✅ Successfully bulk inserted {len(analytics_data)} records into analytics_players")
            self.pipeline_stats['records_inserted'] = len(analytics_data)
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert analytics data: {e}")
            analytics_conn.execute("ROLLBACK")
            return False
    
    def _validate_analytics_data(self, gameweek: int) -> bool:
        """Validate the analytics data after ETL"""
        try:
            is_valid, issues = self.ops.validate_analytics_data_quality(gameweek)
            
            if is_valid:
                logger.info("✅ Analytics data validation passed")
                return True
            else:
                logger.error(f"Analytics data validation failed: {issues}")
                return False
                
        except Exception as e:
            logger.error(f"Analytics validation error: {e}")
            return False
    
    def _log_pipeline_success(self, gameweek: int):
        """Log successful pipeline completion"""
        elapsed_time = datetime.now() - self.pipeline_start_time
        
        logger.info("🎉 ANALYTICS ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"📊 Gameweek: {gameweek}")
        logger.info(f"⏱️ Duration: {elapsed_time.total_seconds():.1f} seconds")
        logger.info(f"👥 Players processed: {self.pipeline_stats.get('consolidated_players', 'N/A')}")
        logger.info(f"🏟️ Teams processed: {self.pipeline_stats.get('teams_processed', 'N/A')}")
        logger.info(f"🧮 Metrics calculated: {self.pipeline_stats.get('derived_metrics', 'N/A')}")
        logger.info(f"💾 Records inserted: {self.pipeline_stats.get('records_inserted', 'N/A')}")
    
    def _log_pipeline_completion(self):
        """Log pipeline completion (success or failure)"""
        if self.pipeline_start_time:
            elapsed_time = datetime.now() - self.pipeline_start_time
            logger.info(f"Pipeline completed in {elapsed_time.total_seconds():.1f} seconds")

def main():
    """Main entry point for running the analytics pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Analytics ETL Pipeline')
    parser.add_argument('--gameweek', type=int, help='Specific gameweek to process')
    parser.add_argument('--force', action='store_true', help='Force refresh even if data exists')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create and run pipeline
    pipeline = AnalyticsETLPipeline()
    success = pipeline.run_full_pipeline(
        target_gameweek=args.gameweek,
        force_refresh=args.force
    )
    
    if success:
        print("✅ Analytics ETL Pipeline completed successfully")
        sys.exit(0)
    else:
        print("❌ Analytics ETL Pipeline failed")
        sys.exit(1)

if __name__ == "__main__":
    main()