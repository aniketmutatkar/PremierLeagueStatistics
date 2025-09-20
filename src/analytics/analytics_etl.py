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

from src.database.analytics_db import AnalyticsDBConnection, AnalyticsDBOperations
from src.analytics.data_consolidation import DataConsolidator
from src.analytics.scd_processor import SCDType2Processor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AnalyticsETL:
    """Production ETL pipeline for analytics layer"""
    
    def __init__(self):
        self.db = AnalyticsDBConnection()
        self.ops = AnalyticsDBOperations()
        self.consolidator = DataConsolidator()
        
        self.pipeline_start_time = None
        self.pipeline_stats = {}
    
    def run_full_pipeline(self, target_gameweek: Optional[int] = None, force_refresh: bool = False) -> bool:
        """
        Run the complete analytics ETL pipeline
        
        Args:
            target_gameweek: Specific gameweek to process (None = current)
            force_refresh: Force refresh even if data already exists
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.pipeline_start_time = datetime.now()
        logger.info("🚀 Starting Analytics ETL Pipeline")
        
        try:
            with self.db.get_dual_connections() as (raw_conn, analytics_conn):
                
                # Step 1: Determine target gameweek
                if target_gameweek is None:
                    target_gameweek = self._get_current_gameweek(raw_conn)
                    if target_gameweek is None:
                        logger.error("Could not determine target gameweek")
                        return False
                
                logger.info(f"🎯 Processing gameweek {target_gameweek}")
                
                # Step 2: Check if refresh is needed
                if not force_refresh and self._is_data_current(analytics_conn, target_gameweek):
                    logger.info("✅ Data is already current, skipping ETL")
                    return True
                
                # Step 3: Consolidate player data
                logger.info("🔄 Consolidating player data...")
                outfield_df, goalkeepers_df = self.consolidator.consolidate_players(raw_conn, target_gameweek)

                logger.info("🔄 Consolidating squad data...")
                squad_df = self.consolidator.consolidate_squads(raw_conn, target_gameweek)

                logger.info("🔄 Consolidating opponent data...")
                opponent_df = self.consolidator.consolidate_opponents(raw_conn, target_gameweek)
                
                if outfield_df.empty and goalkeepers_df.empty:
                    logger.error("No player data consolidated")
                    return False
                
                # Log consolidation results
                summary = self.consolidator.get_consolidation_summary(
                    outfield=outfield_df, 
                    goalkeepers=goalkeepers_df,
                    squads=squad_df,
                    opponents=opponent_df
                )
                logger.info(f"✅ Consolidated {summary.get('total_entities', 0)} total entities")
                logger.info(f"   - Outfield: {summary.get('outfield_count', 0)} players")
                logger.info(f"   - Goalkeepers: {summary.get('goalkeepers_count', 0)} players") 
                logger.info(f"   - Squads: {summary.get('squads_count', 0)} squads")
                logger.info(f"   - Opponents: {summary.get('opponents_count', 0)} opponents")

                # Validate consolidation
                validation = self.consolidator.validate_consolidation(
                    outfield=outfield_df,
                    goalkeepers=goalkeepers_df, 
                    squads=squad_df,
                    opponents=opponent_df
                )
                if not validation['success']:
                    logger.error(f"Consolidation validation failed: {validation['errors']}")
                    return False
                
                # Step 4: Handle SCD Type 2 updates
                logger.info("🕐 Processing SCD Type 2 updates...")
                
                scd_processor = SCDType2Processor(analytics_conn)
                # Process both tables with clean, consistent SCD logic
                if not scd_processor.process_all_updates(outfield_df, goalkeepers_df, target_gameweek, squad_df, opponent_df):
                    logger.error("SCD Type 2 processing failed")
                    return False
                
                logger.info(f"✅ SCD Type 2 processing completed for {len(outfield_df)} outfield + {len(goalkeepers_df)} goalkeepers")
                
                # Step 5: Final validation
                if not self._validate_analytics_data(analytics_conn, target_gameweek):
                    logger.error("Analytics data validation failed")
                    return False
                
                # Update pipeline stats
                elapsed_time = (datetime.now() - self.pipeline_start_time).total_seconds()
                self.pipeline_stats = {
                    'gameweek': target_gameweek,
                    'outfield_players': len(outfield_df),
                    'goalkeepers': len(goalkeepers_df),
                    'squads': len(squad_df),
                    'opponents': len(opponent_df),
                    'total_players': len(outfield_df) + len(goalkeepers_df),
                    'total_entities': len(outfield_df) + len(goalkeepers_df) + len(squad_df) + len(opponent_df),
                    'elapsed_time_seconds': elapsed_time,
                    'success': True
                }
                
                logger.info(f"🎉 ETL Pipeline completed successfully in {elapsed_time:.1f}s")
                return True
                
        except Exception as e:
            logger.error(f"ETL process error: {e}")
            self.pipeline_stats['success'] = False
            return False
    
    def _get_current_gameweek(self, raw_conn) -> Optional[int]:
        """Get current gameweek from raw database"""
        try:
            result = raw_conn.execute("""
                SELECT MAX(current_through_gameweek) as current_gw 
                FROM raw_fixtures
            """).fetchone()
            
            if result and result[0]:
                return result[0]
            
            logger.warning("Could not determine current gameweek from fixtures")
            return None
            
        except Exception as e:
            logger.error(f"Error getting current gameweek: {e}")
            return None
    
    def _is_data_current(self, analytics_conn, gameweek: int) -> bool:
        """Check if analytics data is already current for this gameweek"""
        try:
            # Check if we have current data for this gameweek
            result = analytics_conn.execute("""
                SELECT COUNT(*) FROM analytics_players 
                WHERE gameweek = ? AND is_current = true
            """, [gameweek]).fetchone()
            
            if result and result[0] > 0:
                logger.info(f"Found {result[0]} current records for gameweek {gameweek}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking data currency: {e}")
            return False
    
    def _validate_analytics_data(self, analytics_conn, gameweek: int) -> bool:
        """Validate inserted analytics data for all entity types"""
        try:
            # Check outfield players
            outfield_count = analytics_conn.execute("""
                SELECT COUNT(*) FROM analytics_players 
                WHERE gameweek = ? AND is_current = true
            """, [gameweek]).fetchone()[0]
            
            # Check goalkeepers
            goalkeeper_count = analytics_conn.execute("""
                SELECT COUNT(*) FROM analytics_keepers 
                WHERE gameweek = ? AND is_current = true
            """, [gameweek]).fetchone()[0]
            
            # Check squads
            squad_count = analytics_conn.execute("""
                SELECT COUNT(*) FROM analytics_squads 
                WHERE gameweek = ? AND is_current = true
            """, [gameweek]).fetchone()[0]
            
            # Check opponents
            opponent_count = analytics_conn.execute("""
                SELECT COUNT(*) FROM analytics_opponents 
                WHERE gameweek = ? AND is_current = true
            """, [gameweek]).fetchone()[0]
            
            logger.info(f"Validation: {outfield_count} outfield, {goalkeeper_count} goalkeepers, {squad_count} squads, {opponent_count} opponents")
            
            # Basic validation - should have some players
            if outfield_count == 0:
                logger.error("No outfield players found after insertion")
                return False
            
            # Check expected squad count (should be around 20 teams)
            if squad_count < 15 or squad_count > 25:
                logger.warning(f"Unexpected squad count: {squad_count} (expected 15-25)")
            
            # Check expected opponent count (should match squad count)
            if opponent_count != squad_count:
                logger.warning(f"Opponent count ({opponent_count}) doesn't match squad count ({squad_count})")
            
            # Check for realistic data (players should have some touches, etc.)
            realistic_data = analytics_conn.execute("""
                SELECT COUNT(*) FROM analytics_players 
                WHERE gameweek = ? AND is_current = true AND touches > 0
            """, [gameweek]).fetchone()[0]
            
            if realistic_data == 0:
                logger.error("No players have touches > 0 - data consolidation may have failed")
                return False
            
            logger.info(f"✅ Validation passed: {realistic_data}/{outfield_count} outfield players have touches > 0")
            return True
            
        except Exception as e:
            logger.error(f"Error validating analytics data: {e}")
            return False

    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get pipeline execution statistics"""
        return self.pipeline_stats.copy()
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status for all entity types"""
        try:
            with self.db.get_analytics_connection() as conn:
                # Get latest gameweek from players table
                latest_gw = conn.execute("""
                    SELECT MAX(gameweek) FROM analytics_players WHERE is_current = true
                """).fetchone()[0]
                
                if latest_gw is None:
                    return {'status': 'empty', 'message': 'No analytics data found'}
                
                # Get record counts for all entity types
                outfield_count = conn.execute("""
                    SELECT COUNT(*) FROM analytics_players 
                    WHERE gameweek = ? AND is_current = true
                """, [latest_gw]).fetchone()[0]
                
                goalkeeper_count = conn.execute("""
                    SELECT COUNT(*) FROM analytics_keepers 
                    WHERE gameweek = ? AND is_current = true
                """, [latest_gw]).fetchone()[0]
                
                # Check if squad/opponent tables exist and have data
                try:
                    squad_count = conn.execute("""
                        SELECT COUNT(*) FROM analytics_squads 
                        WHERE gameweek = ? AND is_current = true
                    """, [latest_gw]).fetchone()[0]
                except:
                    squad_count = 0
                
                try:
                    opponent_count = conn.execute("""
                        SELECT COUNT(*) FROM analytics_opponents 
                        WHERE gameweek = ? AND is_current = true
                    """, [latest_gw]).fetchone()[0]
                except:
                    opponent_count = 0
                
                return {
                    'status': 'ready',
                    'latest_gameweek': latest_gw,
                    'outfield_players': outfield_count,
                    'goalkeepers': goalkeeper_count,
                    'squads': squad_count,
                    'opponents': opponent_count,
                    'total_players': outfield_count + goalkeeper_count,
                    'total_entities': outfield_count + goalkeeper_count + squad_count + opponent_count
                }
                
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

# CLI interface for running the pipeline
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Analytics ETL Pipeline')
    parser.add_argument('--gameweek', type=int, help='Specific gameweek to process')
    parser.add_argument('--force', action='store_true', help='Force refresh even if data exists')
    parser.add_argument('--status', action='store_true', help='Show pipeline status')
    
    args = parser.parse_args()
    
    etl = AnalyticsETL()
    
    if args.status:
        status = etl.get_pipeline_status()
        print(f"Pipeline Status: {status}")
    else:
        success = etl.run_full_pipeline(args.gameweek, args.force)
        stats = etl.get_pipeline_stats()
        print(f"Pipeline {'Succeeded' if success else 'Failed'}: {stats}")
        sys.exit(0 if success else 1)