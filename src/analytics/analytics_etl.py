#!/usr/bin/env python3
"""
Analytics ETL Pipeline - NEW FIXTURE-BASED IMPLEMENTATION
Phase 3: Team-specific gameweek assignment in analytics layer
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List
import sys
from pathlib import Path

# Add src to path if running as script
if __name__ == "__main__":
    sys.path.append(str(Path(__file__).parent.parent / 'src'))

from src.database.analytics_db import AnalyticsDBConnection, AnalyticsDBOperations
from src.analytics.data_consolidation import DataConsolidator
from src.analytics.scd_processor import SCDType2Processor
from .fixtures import FixturesProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AnalyticsETL:
    """NEW: Production ETL pipeline with team-specific gameweek assignment"""
    
    def __init__(self):
        self.db = AnalyticsDBConnection()
        self.ops = AnalyticsDBOperations()
        self.consolidator = DataConsolidator()
        
        self.pipeline_start_time = None
        self.pipeline_stats = {}
    
    def run_full_pipeline(self, force_refresh: bool = False) -> bool:
        """
        NEW: Run analytics ETL with fixture-based gameweek assignment
        
        Args:
            force_refresh: Force refresh even if data already exists
            
        REMOVED: target_gameweek parameter - calculated from fixtures
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.pipeline_start_time = datetime.now()
        logger.info("🚀 Starting Analytics ETL Pipeline (NEW Fixture-Based Mode)")
        
        try:
            with self.db.get_dual_connections() as (raw_conn, analytics_conn):
                
                # NEW Step 1: Get team-specific gameweeks from fixtures
                logger.info("📊 Step 1: Calculating team-specific gameweeks from fixtures...")
                team_gameweeks = self._get_team_gameweeks_from_fixtures(raw_conn)
                
                if not team_gameweeks:
                    logger.error("Could not determine team gameweeks from fixtures")
                    return False
                
                min_gw = min(team_gameweeks.values())
                max_gw = max(team_gameweeks.values())
                teams_aligned = (min_gw == max_gw)
                
                logger.info(f"✅ Team gameweeks calculated:")
                logger.info(f"   Range: GW{min_gw} to GW{max_gw}")
                logger.info(f"   Teams aligned: {teams_aligned}")
                logger.info(f"   Total teams: {len(team_gameweeks)}")
                
                if not teams_aligned:
                    teams_behind = [t for t, gw in team_gameweeks.items() if gw < max_gw]
                    logger.info(f"   Teams behind: {len(teams_behind)} - {teams_behind}")
                
                # NEW Step 2: Check if refresh needed (team-by-team)
                if not force_refresh:
                    teams_needing_update = self._get_teams_needing_update(analytics_conn, team_gameweeks)

                    # NEW: Also check if fixtures need updating
                    fixtures_need_update = self._check_fixtures_need_update(analytics_conn, max_gw)

                    if not teams_needing_update and not fixtures_need_update:
                        logger.info("✅ All teams' data and fixtures are current, skipping ETL")
                        return True

                    if teams_needing_update:
                        logger.info(f"Teams needing update: {len(teams_needing_update)} - {teams_needing_update}")
                    if fixtures_need_update:
                        logger.info("Fixtures need update (incomplete fixtures in analytics)")
                
                # Step 3: Process fixtures (unchanged)
                logger.info("🏈 Step 3: Processing fixtures...")
                fixtures_processor = FixturesProcessor()
                fixtures_success = fixtures_processor.process_fixtures(raw_conn, analytics_conn, max_gw, force_refresh)
                if not fixtures_success:
                    logger.warning("⚠️ Fixtures processing failed, continuing with entity analytics")

                # Step 4: Consolidate data (NOTE: consolidator doesn't need gameweek parameter anymore)
                logger.info("🔄 Step 4: Consolidating all entity data...")
                
                outfield_df, goalkeepers_df = self.consolidator.consolidate_players(raw_conn)
                squad_df = self.consolidator.consolidate_squads(raw_conn)
                opponent_df = self.consolidator.consolidate_opponents(raw_conn)
                
                if outfield_df.empty and goalkeepers_df.empty:
                    logger.error("No player data consolidated")
                    return False
                
                # Log consolidation
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

                # Step 5: Assign team-specific gameweeks to each record
                logger.info("🎯 Step 5: Assigning team-specific gameweeks...")

                # Assign gameweeks based on team's completed fixtures
                outfield_df['gameweek'] = outfield_df['squad'].map(team_gameweeks)
                goalkeepers_df['gameweek'] = goalkeepers_df['squad'].map(team_gameweeks)

                if squad_df is not None and not squad_df.empty:
                    squad_df['gameweek'] = squad_df['squad_name'].map(team_gameweeks)

                if opponent_df is not None and not opponent_df.empty:
                    # FIX: Opponent team names have "vs " prefix, need to strip before mapping
                    opponent_df['squad_name_clean'] = opponent_df['squad_name'].str.replace('vs ', '', regex=False).str.strip()
                    opponent_df['gameweek'] = opponent_df['squad_name_clean'].map(team_gameweeks)
                    opponent_df = opponent_df.drop(columns=['squad_name_clean'])

                # Validate gameweek assignments
                unmapped_outfield = outfield_df[outfield_df['gameweek'].isna()]
                unmapped_goalkeepers = goalkeepers_df[goalkeepers_df['gameweek'].isna()]
                unmapped_squads = squad_df[squad_df['gameweek'].isna()] if squad_df is not None and not squad_df.empty else pd.DataFrame()
                unmapped_opponents = opponent_df[opponent_df['gameweek'].isna()] if opponent_df is not None and not opponent_df.empty else pd.DataFrame()

                if not unmapped_outfield.empty or not unmapped_goalkeepers.empty or not unmapped_squads.empty or not unmapped_opponents.empty:
                    logger.error("Found records with unmapped gameweeks:")
                    if not unmapped_outfield.empty:
                        logger.error(f"  Unmapped outfield players: {unmapped_outfield['squad'].unique()}")
                    if not unmapped_goalkeepers.empty:
                        logger.error(f"  Unmapped goalkeepers: {unmapped_goalkeepers['squad'].unique()}")
                    if not unmapped_squads.empty:
                        logger.error(f"  Unmapped squads: {unmapped_squads['squad_name'].unique()}")
                    if not unmapped_opponents.empty:
                        logger.error(f"  Unmapped opponent teams: {unmapped_opponents['squad_name'].unique()}")
                    return False

                logger.info(f"✅ Gameweeks assigned successfully:")
                logger.info(f"   Outfield: {len(outfield_df)} records")
                logger.info(f"   Goalkeepers: {len(goalkeepers_df)} records")
                logger.info(f"   Squads: {len(squad_df) if squad_df is not None else 0} records")
                logger.info(f"   Opponents: {len(opponent_df) if opponent_df is not None else 0} records")

                # Log gameweek distribution for players
                if not outfield_df.empty:
                    gw_dist = outfield_df.groupby('gameweek').size().to_dict()
                    logger.info(f"   Player distribution by gameweek:")
                    for gw, count in gw_dist.items():
                        logger.info(f"     GW{int(gw)}: {count} players")

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
                
                # NEW Step 6: Process SCD updates by gameweek groups
                logger.info("🕐 Step 6: Processing SCD Type 2 updates (by gameweek)...")
                
                scd_processor = SCDType2Processor(analytics_conn)
                
                # Get unique gameweeks present in the data
                all_gameweeks = set()
                if not outfield_df.empty:
                    all_gameweeks.update(outfield_df['gameweek'].unique())
                if not goalkeepers_df.empty:
                    all_gameweeks.update(goalkeepers_df['gameweek'].unique())
                if squad_df is not None and not squad_df.empty:
                    all_gameweeks.update(squad_df['gameweek'].unique())
                if opponent_df is not None and not opponent_df.empty:
                    all_gameweeks.update(opponent_df['gameweek'].unique())
                
                all_gameweeks = sorted([int(gw) for gw in all_gameweeks if pd.notna(gw)])
                
                logger.info(f"Processing {len(all_gameweeks)} gameweek(s): {all_gameweeks}")
                
                # Process each gameweek separately
                for gameweek in all_gameweeks:
                    logger.info(f"\n--- Processing Gameweek {gameweek} ---")
                    
                    # Filter data for this gameweek
                    gw_outfield = outfield_df[outfield_df['gameweek'] == gameweek].copy() if not outfield_df.empty else pd.DataFrame()
                    gw_goalkeepers = goalkeepers_df[goalkeepers_df['gameweek'] == gameweek].copy() if not goalkeepers_df.empty else pd.DataFrame()
                    gw_squads = squad_df[squad_df['gameweek'] == gameweek].copy() if squad_df is not None and not squad_df.empty else None
                    gw_opponents = opponent_df[opponent_df['gameweek'] == gameweek].copy() if opponent_df is not None and not opponent_df.empty else None
                    
                    logger.info(f"  Outfield: {len(gw_outfield)}, Keepers: {len(gw_goalkeepers)}, " +
                               f"Squads: {len(gw_squads) if gw_squads is not None else 0}, " +
                               f"Opponents: {len(gw_opponents) if gw_opponents is not None else 0}")
                    
                    # Process this gameweek's data
                    if not scd_processor.process_all_updates(gw_outfield, gw_goalkeepers, gameweek, gw_squads, gw_opponents):
                        logger.error(f"SCD Type 2 processing failed for gameweek {gameweek}")
                        return False
                    
                    logger.info(f"✅ Gameweek {gameweek} processed successfully")
                
                logger.info(f"\n✅ All SCD Type 2 processing completed")
                
                # Step 7: Final validation
                logger.info("🔍 Step 7: Validating analytics data...")
                if not self._validate_analytics_data(analytics_conn, all_gameweeks):
                    logger.error("Analytics data validation failed")
                    return False
                
                # Update pipeline stats
                elapsed_time = (datetime.now() - self.pipeline_start_time).total_seconds()
                self.pipeline_stats = {
                    'gameweek_range': f"{min_gw}-{max_gw}",
                    'teams_aligned': teams_aligned,
                    'outfield_players': len(outfield_df),
                    'goalkeepers': len(goalkeepers_df),
                    'squads': len(squad_df) if squad_df is not None else 0,
                    'opponents': len(opponent_df) if opponent_df is not None else 0,
                    'total_entities': len(outfield_df) + len(goalkeepers_df) + 
                                     (len(squad_df) if squad_df is not None else 0) + 
                                     (len(opponent_df) if opponent_df is not None else 0),
                    'elapsed_time_seconds': elapsed_time,
                    'success': True
                }
                
                logger.info(f"🎉 ETL Pipeline completed successfully in {elapsed_time:.1f}s")
                return True
                
        except Exception as e:
            logger.error(f"ETL process error: {e}")
            import traceback
            traceback.print_exc()
            self.pipeline_stats['success'] = False
            return False
    
    def _get_team_gameweeks_from_fixtures(self, raw_conn) -> Dict[str, int]:
        """
        NEW: Calculate team-specific gameweeks from fixtures table
        
        Returns:
            Dict mapping team_name -> latest_completed_gameweek
        """
        try:
            # Get all fixtures
            fixtures_df = raw_conn.execute("SELECT * FROM raw_fixtures").fetchdf()
            
            if fixtures_df.empty:
                logger.error("No fixtures found in raw database")
                return {}
            
            team_gameweeks = {}
            
            # Get all unique teams
            home_teams = set(fixtures_df['home_team'].dropna().unique())
            away_teams = set(fixtures_df['away_team'].dropna().unique())
            all_teams = home_teams | away_teams
            
            for team in all_teams:
                # Find completed fixtures for this team
                team_fixtures = fixtures_df[
                    ((fixtures_df['home_team'] == team) | (fixtures_df['away_team'] == team)) &
                    (fixtures_df['is_completed'] == True)
                ]
                
                if not team_fixtures.empty:
                    latest_gw = int(team_fixtures['gameweek'].max())
                    team_gameweeks[team] = latest_gw
                else:
                    team_gameweeks[team] = 0
            
            return team_gameweeks
            
        except Exception as e:
            logger.error(f"Error calculating team gameweeks: {e}")
            return {}
    
    def _get_teams_needing_update(self, analytics_conn, team_gameweeks: Dict[str, int]) -> List[str]:
        """
        NEW: Determine which teams need data updates
        
        Args:
            analytics_conn: Analytics database connection
            team_gameweeks: Current gameweeks for each team
            
        Returns:
            List of team names that need updates
        """
        teams_needing_update = []
        
        try:
            for team, current_gw in team_gameweeks.items():
                # Check what gameweek we have in analytics for this team
                result = analytics_conn.execute("""
                    SELECT MAX(gameweek) as analytics_gw
                    FROM analytics_players
                    WHERE squad = ? AND is_current = true
                """, [team]).fetchone()
                
                analytics_gw = result[0] if result and result[0] is not None else 0
                
                if current_gw > analytics_gw:
                    teams_needing_update.append(team)
            
            return teams_needing_update
            
        except Exception as e:
            logger.warning(f"Error checking teams needing update: {e}")
            # If we can't determine, assume all need updates
            return list(team_gameweeks.keys())

    def _check_fixtures_need_update(self, analytics_conn, current_gameweek: int) -> bool:
        """
        Check if fixtures table needs updating

        Returns:
            True if fixtures need update, False otherwise
        """
        try:
            # Check if analytics_fixtures table exists
            tables = analytics_conn.execute("SHOW TABLES").fetchall()
            table_names = [t[0] for t in tables]

            if 'analytics_fixtures' not in table_names:
                logger.info("analytics_fixtures table doesn't exist, will create")
                return True

            # Check if there are incomplete fixtures for current gameweek in analytics
            incomplete_count = analytics_conn.execute("""
                SELECT COUNT(*)
                FROM analytics_fixtures
                WHERE gameweek = ? AND is_completed = false
            """, [current_gameweek]).fetchone()[0]

            return incomplete_count > 0

        except Exception as e:
            logger.warning(f"Error checking fixtures update need: {e}")
            # On error, assume fixtures might need update
            return True

    def _validate_analytics_data(self, analytics_conn, gameweeks: List[int]) -> bool:
        """
        NEW: Validate analytics data for multiple gameweeks
        
        Args:
            analytics_conn: Analytics database connection
            gameweeks: List of gameweeks to validate
        """
        try:
            for gameweek in gameweeks:
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
                
                logger.info(f"GW{gameweek}: {outfield_count} outfield, {goalkeeper_count} keepers")
                
                if outfield_count == 0:
                    logger.error(f"No outfield players found for gameweek {gameweek}")
                    return False
            
            logger.info("✅ Analytics data validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Error validating analytics data: {e}")
            return False

    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get pipeline execution statistics"""
        return self.pipeline_stats.copy()
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status"""
        try:
            with self.db.get_analytics_connection() as conn:
                # Get latest gameweek from players table
                latest_gw = conn.execute("""
                    SELECT MAX(gameweek) FROM analytics_players WHERE is_current = true
                """).fetchone()[0]
                
                if latest_gw is None:
                    return {'status': 'empty', 'message': 'No analytics data found'}
                
                # Get record counts
                outfield_count = conn.execute("""
                    SELECT COUNT(*) FROM analytics_players 
                    WHERE gameweek = ? AND is_current = true
                """, [latest_gw]).fetchone()[0]
                
                goalkeeper_count = conn.execute("""
                    SELECT COUNT(*) FROM analytics_keepers 
                    WHERE gameweek = ? AND is_current = true
                """, [latest_gw]).fetchone()[0]
                
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
    
    parser = argparse.ArgumentParser(description='Run Analytics ETL Pipeline (NEW Fixture-Based)')
    parser.add_argument('--force', action='store_true', help='Force refresh even if data exists')
    parser.add_argument('--status', action='store_true', help='Show pipeline status')
    
    args = parser.parse_args()
    
    etl = AnalyticsETL()
    
    if args.status:
        status = etl.get_pipeline_status()
        print(f"Pipeline Status: {status}")
    else:
        success = etl.run_full_pipeline(args.force)
        stats = etl.get_pipeline_stats()
        print(f"Pipeline {'Succeeded' if success else 'Failed'}: {stats}")
        sys.exit(0 if success else 1)