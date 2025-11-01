#!/usr/bin/env python3
"""
Master Pipeline - PHASE 4: Team-by-Team Gameweek Comparison
Orchestrates complete data pipeline with intelligent team-specific run detection
"""
import sys
import subprocess
import argparse
import logging
import glob
from pathlib import Path
from datetime import datetime
from typing import Tuple, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class PipelineLogger:
    """Enhanced logging with file and console output"""
    def __init__(self, name: str):
        self.log_dir = Path("data/logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create timestamped log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"{name}_{timestamp}.log"
        
        # Setup logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Cleanup old logs (keep last 10)
        self.keep_logs = 10
        self._cleanup_old_logs()
    
    def _cleanup_old_logs(self):
        """Remove old log files, keeping only recent ones"""
        try:
            log_files = sorted(glob.glob(str(self.log_dir / "master_pipeline_*.log")))
            if len(log_files) > self.keep_logs:
                for old_log in log_files[:-self.keep_logs]:
                    try:
                        Path(old_log).unlink()
                    except Exception:
                        pass
        except Exception:
            pass

pipeline_logger = PipelineLogger("master_pipeline")
logger = pipeline_logger.logger

class MasterPipeline:
    """Orchestrates the complete data pipeline: Raw → Analytics"""
    
    def __init__(self):
        self.start_time = None
        self.pipeline_stats = {
            'raw_success': False,
            'analytics_success': False,
            'total_duration': None,
            'raw_duration': None,
            'analytics_duration': None,
            'raw_team_gameweeks': None,
            'analytics_team_gameweeks': None,
            'teams_updated': None,
            'records_processed': None
        }
    
    def run_complete_pipeline(self, force_raw: bool = False, force_analytics: bool = False) -> bool:
        """
        Run the complete data pipeline: Raw → Analytics
        
        Args:
            force_raw: Force raw pipeline even if data is current
            force_analytics: Force analytics pipeline even if data is current
            
        Returns:
            True if successful, False otherwise
        """
        self.start_time = datetime.now()
        
        logger.info("🚀 MASTER PIPELINE STARTED (TEAM-SPECIFIC MODE)")
        logger.info(f"Run started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 50)
        
        try:
            # Step 1: Intelligent decision making (team-by-team)
            logger.info("🔍 Checking pipeline requirements (team-by-team)...")
            should_run_raw, should_run_analytics = self._should_run_pipeline(force_raw, force_analytics)
            
            if not should_run_raw and not should_run_analytics:
                logger.info("✅ All teams up to date - no updates needed")
                return True
            
            # Step 2: Run Raw Pipeline (if needed)
            if should_run_raw:
                logger.info("\n📊 STEP 1: RAW DATA PIPELINE")
                logger.info("-" * 30)
                raw_success = self._run_raw_pipeline(force_raw)
                self.pipeline_stats['raw_success'] = raw_success
                
                if not raw_success:
                    logger.error("❌ Raw pipeline failed - stopping execution")
                    return False
            else:
                logger.info("⏭️  Skipping raw pipeline (data current)")
                self.pipeline_stats['raw_success'] = True
            
            # Step 3: Run Analytics Pipeline (if needed)
            if should_run_analytics:
                logger.info("\n🧮 STEP 2: ANALYTICS PIPELINE")
                logger.info("-" * 30)
                analytics_success = self._run_analytics_pipeline(force_analytics)
                self.pipeline_stats['analytics_success'] = analytics_success
                
                if not analytics_success:
                    logger.error("❌ Analytics pipeline failed")
                    return False
            else:
                logger.info("⏭️  Skipping analytics pipeline (data current)")
                self.pipeline_stats['analytics_success'] = True
            
            # Step 4: Final validation and reporting
            self._finalize_pipeline()
            return True
            
        except Exception as e:
            logger.error(f"💥 Master pipeline failed: {e}")
            return False
    
    def _should_run_pipeline(self, force_raw: bool, force_analytics: bool) -> Tuple[bool, bool]:
        """
        PHASE 4: Team-by-team intelligent decision on what pipelines to run
        Returns: (should_run_raw, should_run_analytics)
        """
        if force_raw or force_analytics:
            logger.info(f"Force flags: raw={force_raw}, analytics={force_analytics}")
            return force_raw, force_analytics

        try:
            # Get team-specific gameweeks from both databases
            raw_team_gws = self._get_raw_team_gameweeks()
            analytics_team_gws = self._get_analytics_team_gameweeks()

            self.pipeline_stats['raw_team_gameweeks'] = raw_team_gws
            self.pipeline_stats['analytics_team_gameweeks'] = analytics_team_gws

            if not raw_team_gws:
                logger.warning("No raw data found - will run full pipeline")
                return True, True

            # FIXED: Enhanced logging with detailed team gameweek ranges
            raw_min = min(raw_team_gws.values()) if raw_team_gws else 0
            raw_max = max(raw_team_gws.values()) if raw_team_gws else 0
            logger.info(f"Raw data: GW{raw_min}-{raw_max} across {len(raw_team_gws)} teams")

            # Show distribution
            from collections import Counter
            raw_gw_counts = Counter(raw_team_gws.values())
            logger.info(f"  Distribution: {dict(raw_gw_counts)}")

            if analytics_team_gws:
                analytics_min = min(analytics_team_gws.values())
                analytics_max = max(analytics_team_gws.values())
                logger.info(f"Analytics data: GW{analytics_min}-{analytics_max} across {len(analytics_team_gws)} teams")

                analytics_gw_counts = Counter(analytics_team_gws.values())
                logger.info(f"  Distribution: {dict(analytics_gw_counts)}")
            else:
                logger.info("Analytics database empty - will populate")

            # NEW: Check for incomplete fixtures in raw database
            incomplete_fixtures_info = self._check_incomplete_fixtures()
            if incomplete_fixtures_info['has_incomplete']:
                logger.info(f"Found {incomplete_fixtures_info['count']} incomplete fixture(s):")
                for fixture in incomplete_fixtures_info['fixtures'][:3]:  # Show first 3
                    logger.info(f"  • GW{fixture['gameweek']}: {fixture['home_team']} vs {fixture['away_team']}")
                if len(incomplete_fixtures_info['fixtures']) > 3:
                    logger.info(f"  ... and {len(incomplete_fixtures_info['fixtures']) - 3} more")

                # Check FBRef to see if any incomplete fixtures are now complete
                logger.info("Checking FBRef for updates to incomplete fixtures...")
                fbref_status = self._check_fbref_fixture_status()

                if fbref_status['has_new_completions']:
                    logger.info(f"✅ Found {fbref_status['new_completions_count']} newly completed fixture(s)")
                    logger.info("Running raw pipeline to update fixture data")
                    return True, True
                else:
                    logger.info("No incomplete fixtures have been completed yet")

            # Compare team-by-team to find which teams need updates
            teams_to_update = []
            for team, raw_gw in raw_team_gws.items():
                analytics_gw = analytics_team_gws.get(team, 0)
                if raw_gw > analytics_gw:
                    teams_to_update.append(f"{team} (GW{analytics_gw}→{raw_gw})")

            if teams_to_update:
                logger.info(f"Teams needing update ({len(teams_to_update)}):")
                for team_info in teams_to_update[:5]:  # Show first 5
                    logger.info(f"  • {team_info}")
                if len(teams_to_update) > 5:
                    logger.info(f"  ... and {len(teams_to_update) - 5} more")

                self.pipeline_stats['teams_updated'] = len(teams_to_update)
                return False, True  # Only analytics needed

            # FIXED: Check for stale data
            staleness = self._check_data_staleness()
            if staleness['is_stale']:
                logger.info(f"⚠️  Data is stale ({staleness['hours_old']} hours old)")
                logger.info(f"   Last updated: {staleness['last_updated']}")
                logger.info("Running raw pipeline to refresh stale data")
                return True, True

            # FIXED: Check if ANY team is behind FBRef max (not just if new GW exists)
            if self._should_check_fbref():
                logger.info("Checking FBRef for new gameweek...")
                fbref_max_gw = self._quick_gameweek_check()
                logger.info(f"FBRef max gameweek: {fbref_max_gw}")

                # FIXED: Check if ANY team is behind FBRef max
                teams_behind_fbref = []
                for team, team_gw in raw_team_gws.items():
                    if team_gw < fbref_max_gw:
                        teams_behind_fbref.append(f"{team} (GW{team_gw} < {fbref_max_gw})")

                if teams_behind_fbref:
                    logger.info(f"Found {len(teams_behind_fbref)} team(s) behind FBRef:")
                    for team_info in teams_behind_fbref[:5]:
                        logger.info(f"  • {team_info}")
                    if len(teams_behind_fbref) > 5:
                        logger.info(f"  ... and {len(teams_behind_fbref) - 5} more")
                    logger.info("Running raw pipeline to catch up")
                    return True, True
                else:
                    logger.info("All teams aligned with FBRef")
            else:
                logger.info("Skipping FBRef check (recently checked)")

            logger.info("All teams up to date")
            return False, False

        except Exception as e:
            logger.warning(f"Decision logic failed: {e}, defaulting to force run")
            return True, True
    
    def _get_raw_team_gameweeks(self) -> Dict[str, int]:
        """
        FIXED: Calculate team-specific gameweeks from ACTUAL SQUAD DATA

        This is the source of truth - it shows what data we've actually scraped,
        not just which fixtures are marked complete in the fixtures table.

        Returns: {'Man City': 8, 'Liverpool': 9, ...}
        """
        try:
            import duckdb

            conn = duckdb.connect('data/premierleague_raw.duckdb', read_only=True)

            # FIXED: Check squad_standard table for actual matches played
            # This is the REAL data we have, not just fixture metadata
            result = conn.execute("""
                SELECT Squad, "Playing Time MP" as matches_played
                FROM squad_standard
            """).fetchdf()

            conn.close()

            if result.empty:
                logger.warning("No squad data found in raw database")
                return {}

            # Convert to dict: {team: matches_played}
            team_gameweeks = dict(zip(result['Squad'], result['matches_played'].astype(int)))

            logger.debug(f"Raw team gameweeks from squad data: {team_gameweeks}")
            return team_gameweeks

        except Exception as e:
            logger.warning(f"Failed to get raw team gameweeks from squad data: {e}")
            # Fallback to fixture-based check if squad table doesn't exist
            return self._get_raw_team_gameweeks_from_fixtures()

    def _get_raw_team_gameweeks_from_fixtures(self) -> Dict[str, int]:
        """
        Fallback method: Get team gameweeks from fixtures table
        Only used if squad_standard table doesn't exist
        """
        try:
            import duckdb

            conn = duckdb.connect('data/premierleague_raw.duckdb', read_only=True)

            # Get all completed fixtures
            fixtures = conn.execute("""
                SELECT home_team, away_team, gameweek, is_completed
                FROM raw_fixtures
                WHERE is_completed = true
            """).fetchdf()

            conn.close()

            if fixtures.empty:
                return {}

            # Calculate max completed gameweek per team
            team_gameweeks = {}
            all_teams = set(fixtures['home_team'].unique()) | set(fixtures['away_team'].unique())

            for team in all_teams:
                team_fixtures = fixtures[
                    ((fixtures['home_team'] == team) | (fixtures['away_team'] == team))
                ]
                if not team_fixtures.empty:
                    team_gameweeks[team] = int(team_fixtures['gameweek'].max())

            return team_gameweeks

        except Exception as e:
            logger.warning(f"Failed to get raw team gameweeks from fixtures: {e}")
            return {}

    def _get_analytics_team_gameweeks(self) -> Dict[str, int]:
        """
        PHASE 4 NEW: Get team-specific gameweeks from analytics_players
        Returns: {'Man City': 6, 'Liverpool': 5, ...}
        """
        try:
            import duckdb
            
            # Check if analytics DB exists
            analytics_path = Path('data/premierleague_analytics.duckdb')
            if not analytics_path.exists():
                return {}
            
            conn = duckdb.connect(str(analytics_path), read_only=True)
            
            # Get max gameweek per team from current records
            result = conn.execute("""
                SELECT squad, MAX(gameweek) as max_gw
                FROM analytics_players
                WHERE is_current = true
                GROUP BY squad
            """).fetchdf()
            
            conn.close()
            
            if result.empty:
                return {}
            
            # Convert to dict
            return dict(zip(result['squad'], result['max_gw']))
            
        except Exception as e:
            logger.warning(f"Failed to get analytics team gameweeks: {e}")
            return {}
    
    def _should_check_fbref(self) -> bool:
        """Only check FBRef once per day to avoid unnecessary requests"""
        try:
            today = datetime.now().date()
            log_pattern = str(pipeline_logger.log_dir / f"master_pipeline_{today.strftime('%Y%m%d')}_*.log")
            today_logs = glob.glob(log_pattern)
            
            for log_file in today_logs:
                try:
                    with open(log_file, 'r') as f:
                        content = f.read()
                        if "Checking FBRef for new gameweek" in content:
                            return False
                except Exception:
                    continue
            
            return True
            
        except Exception:
            return True
    
    def _quick_gameweek_check(self) -> int:
        """
        PHASE 4 UPDATED: Quick check using new gameweek_status dict
        Returns max gameweek from FBRef
        """
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from src.scraping.fbref_scraper import FBRefScraper

            scraper = FBRefScraper()
            fixtures_url = scraper.sources_config['fixtures_sources']['current_season']['url']

            fixture_data = scraper.scrape_fixtures(fixtures_url)

            # PHASE 4: Use new gameweek_status dict structure
            return fixture_data['gameweek_status']['max_gameweek']

        except Exception as e:
            logger.warning(f"FBRef check failed: {e}")
            return 0

    def _check_data_staleness(self) -> Dict[str, Any]:
        """
        FIXED: Check if raw data is stale and needs refresh

        Returns:
            Dict with 'is_stale', 'last_updated', 'hours_old'
        """
        try:
            import duckdb
            from datetime import datetime, timedelta

            conn = duckdb.connect('data/premierleague_raw.duckdb', read_only=True)

            # Check last_updated from squad_standard
            result = conn.execute("""
                SELECT MAX(last_updated) as last_update
                FROM squad_standard
            """).fetchone()

            conn.close()

            if not result or not result[0]:
                return {'is_stale': True, 'last_updated': None, 'hours_old': 999}

            last_updated = result[0]
            if isinstance(last_updated, str):
                last_updated = datetime.strptime(last_updated, '%Y-%m-%d').date()

            now = datetime.now().date()
            age = (now - last_updated).days
            hours_old = age * 24

            # Consider stale if >24 hours old
            is_stale = hours_old > 24

            return {
                'is_stale': is_stale,
                'last_updated': last_updated,
                'hours_old': hours_old
            }

        except Exception as e:
            logger.warning(f"Staleness check failed: {e}")
            return {'is_stale': False, 'last_updated': None, 'hours_old': 0}

    def _normalize_fixture_id(self, fixture_id: str) -> str:
        """
        FIXED: Normalize fixture ID to handle format inconsistencies

        Handles:
        - GW9.0 → GW9 (remove decimal from gameweek)
        - Ensures consistent format for comparison

        Args:
            fixture_id: Original fixture ID (e.g., "2025-2026_GW9.0_Arsenal_vs_Chelsea")

        Returns:
            Normalized fixture ID (e.g., "2025-2026_GW9_Arsenal_vs_Chelsea")
        """
        import re
        # Replace GW{number}.0 with GW{number}
        normalized = re.sub(r'_GW(\d+)\.0_', r'_GW\1_', fixture_id)
        return normalized

    def _check_incomplete_fixtures(self) -> Dict[str, Any]:
        """
        Check for incomplete fixtures in raw database

        Returns:
            Dict with 'has_incomplete', 'count', and 'fixtures' list
        """
        try:
            import duckdb

            conn = duckdb.connect('data/premierleague_raw.duckdb', read_only=True)

            # Get all incomplete fixtures
            incomplete = conn.execute("""
                SELECT gameweek, home_team, away_team, match_date
                FROM raw_fixtures
                WHERE is_completed = false
                ORDER BY gameweek, match_date
            """).fetchall()

            conn.close()

            fixtures_list = []
            for row in incomplete:
                fixtures_list.append({
                    'gameweek': int(row[0]),
                    'home_team': row[1],
                    'away_team': row[2],
                    'match_date': row[3]
                })

            return {
                'has_incomplete': len(fixtures_list) > 0,
                'count': len(fixtures_list),
                'fixtures': fixtures_list
            }

        except Exception as e:
            logger.warning(f"Failed to check incomplete fixtures: {e}")
            return {'has_incomplete': False, 'count': 0, 'fixtures': []}

    def _check_fbref_fixture_status(self) -> Dict[str, Any]:
        """
        Check FBRef to see if any incomplete fixtures are now complete

        Returns:
            Dict with 'has_new_completions' and 'new_completions_count'
        """
        try:
            import duckdb
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from src.scraping.fbref_scraper import FBRefScraper

            # Get current incomplete fixtures from raw DB
            conn = duckdb.connect('data/premierleague_raw.duckdb', read_only=True)

            raw_incomplete = conn.execute("""
                SELECT fixture_id, gameweek, home_team, away_team
                FROM raw_fixtures
                WHERE is_completed = false
            """).fetchall()

            conn.close()

            if not raw_incomplete:
                return {'has_new_completions': False, 'new_completions_count': 0}

            # Get latest fixtures from FBRef
            scraper = FBRefScraper()
            fixtures_url = scraper.sources_config['fixtures_sources']['current_season']['url']
            fixture_data = scraper.scrape_fixtures(fixtures_url)

            if not fixture_data or 'fixtures' not in fixture_data:
                logger.warning("Could not fetch FBRef fixture data")
                return {'has_new_completions': False, 'new_completions_count': 0}

            # FIXED: Convert FBRef fixtures to dict with normalized IDs
            fbref_fixtures = fixture_data['fixtures']
            fbref_completed = {}

            for _, row in fbref_fixtures.iterrows():
                if row.get('is_completed', False):
                    fixture_id = row.get('fixture_id', '')
                    # Normalize FBRef fixture ID
                    normalized_id = self._normalize_fixture_id(fixture_id)
                    fbref_completed[normalized_id] = True

            # FIXED: Check with normalized fixture IDs
            new_completions = 0
            for raw_fixture in raw_incomplete:
                raw_fixture_id = raw_fixture[0]
                # Normalize DB fixture ID
                normalized_raw_id = self._normalize_fixture_id(raw_fixture_id)

                if normalized_raw_id in fbref_completed:
                    new_completions += 1
                    logger.info(f"  Newly completed: GW{raw_fixture[1]} {raw_fixture[2]} vs {raw_fixture[3]}")

            return {
                'has_new_completions': new_completions > 0,
                'new_completions_count': new_completions
            }

        except Exception as e:
            logger.warning(f"FBRef fixture status check failed: {e}")
            # On error, assume there might be updates and let raw pipeline handle it
            return {'has_new_completions': True, 'new_completions_count': 0}

    def _run_raw_pipeline(self, force: bool = False) -> bool:
        """Run the raw data pipeline with detailed tracking"""
        raw_start = datetime.now()
        
        try:
            cmd = [sys.executable, "pipelines/raw_pipeline.py"]
            if force:
                cmd.append("--force")
            
            logger.info(f"🔧 Executing raw pipeline...")
            if force:
                logger.info("  🔄 Force refresh enabled")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=1800
            )
            
            raw_duration = datetime.now() - raw_start
            self.pipeline_stats['raw_duration'] = raw_duration
            
            if result.returncode == 0:
                logger.info("✅ Raw pipeline completed successfully")
                logger.info(f"⏱️  Duration: {raw_duration.total_seconds():.1f} seconds")
                
                # Parse output for progress
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if "categories successful" in line or "tables populated" in line:
                        logger.info(f"📊 {line.strip()}")
                
                return True
            else:
                logger.error("❌ Raw pipeline failed")
                logger.error(f"Exit code: {result.returncode}")
                if result.stderr:
                    logger.error(f"Error: {result.stderr[:500]}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Raw pipeline timeout (30 minutes)")
            return False
        except Exception as e:
            logger.error(f"❌ Raw pipeline error: {e}")
            return False
    
    def _run_analytics_pipeline(self, force: bool = False) -> bool:
        """Run the analytics ETL pipeline with detailed tracking"""
        analytics_start = datetime.now()
        
        try:
            cmd = [sys.executable, "pipelines/analytics_pipeline.py"]
            if force:
                cmd.append("--force")
            
            logger.info(f"🔧 Executing analytics pipeline...")
            if force:
                logger.info("  🔄 Force refresh enabled")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=1800
            )
            
            analytics_duration = datetime.now() - analytics_start
            self.pipeline_stats['analytics_duration'] = analytics_duration
            
            if result.returncode == 0:
                logger.info("✅ Analytics pipeline completed successfully")
                logger.info(f"⏱️  Duration: {analytics_duration.total_seconds():.1f} seconds")
                
                # Parse output for progress
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if "records" in line.lower() or "processed" in line.lower():
                        logger.info(f"📊 {line.strip()}")
                
                return True
            else:
                logger.error("❌ Analytics pipeline failed")
                logger.error(f"Exit code: {result.returncode}")
                if result.stderr:
                    logger.error(f"Error: {result.stderr[:500]}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Analytics pipeline timeout (30 minutes)")
            return False
        except Exception as e:
            logger.error(f"❌ Analytics pipeline error: {e}")
            return False
    
    def _finalize_pipeline(self):
        """Final validation and reporting with clean summary"""
        total_duration = datetime.now() - self.start_time
        self.pipeline_stats['total_duration'] = total_duration
        
        logger.info("\n🎉 MASTER PIPELINE COMPLETED")
        logger.info("=" * 50)
        logger.info(f"⏱️  Total time: {total_duration.total_seconds():.1f}s")
        
        # Component breakdown
        if self.pipeline_stats['raw_duration']:
            raw_time = self.pipeline_stats['raw_duration'].total_seconds()
            logger.info(f"📊 Raw: {raw_time:.1f}s")
        
        if self.pipeline_stats['analytics_duration']:
            analytics_time = self.pipeline_stats['analytics_duration'].total_seconds()
            logger.info(f"🧮 Analytics: {analytics_time:.1f}s")
        
        # Team update info
        if self.pipeline_stats['teams_updated']:
            logger.info(f"👥 Teams updated: {self.pipeline_stats['teams_updated']}")
        
        # Quick validation
        logger.info("🔍 Running final validation...")
        try:
            success = self._quick_validation()
            if success:
                logger.info("✅ System validation passed")
            else:
                logger.warning("⚠️  Validation warnings detected")
        except Exception as e:
            logger.warning(f"⚠️  Validation check failed: {e}")
        
        logger.info(f"📁 Log saved: {pipeline_logger.log_file.name}")
    
    def _quick_validation(self) -> bool:
        """Run quick validation checks with detailed reporting"""
        try:
            import duckdb
            
            # Check analytics database
            analytics_path = Path('data/premierleague_analytics.duckdb')
            if not analytics_path.exists():
                logger.warning("  Analytics database does not exist")
                return False
            
            conn = duckdb.connect(str(analytics_path), read_only=True)
            
            # Check player count
            player_count = conn.execute("""
                SELECT COUNT(*) FROM analytics_players WHERE is_current = true
            """).fetchone()[0]
            
            # Check gameweek range
            gw_stats = conn.execute("""
                SELECT MIN(gameweek) as min_gw, MAX(gameweek) as max_gw
                FROM analytics_players WHERE is_current = true
            """).fetchone()
            
            conn.close()
            
            if player_count < 300:
                logger.warning(f"  Low player count: {player_count}")
                return False
            
            logger.info(f"  📊 GW{gw_stats[0]}-{gw_stats[1]} with {player_count:,} players")
            return True
            
        except Exception as e:
            logger.error(f"  Validation error: {e}")
            return False

def check_system_status():
    """Check overall system status"""
    print("🔍 SYSTEM STATUS CHECK (TEAM-SPECIFIC MODE)")
    print("=" * 50)
    
    try:
        # Check raw pipeline status
        print("\n📊 Raw Pipeline Status:")
        raw_result = subprocess.run(
            [sys.executable, "pipelines/raw_pipeline.py", "--status"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if raw_result.returncode == 0:
            print("✅ Raw pipeline status OK")
        else:
            print("❌ Raw pipeline status issues")
        
        # Check analytics pipeline status  
        print("\n🧮 Analytics Pipeline Status:")
        analytics_result = subprocess.run(
            [sys.executable, "pipelines/analytics_pipeline.py", "--status"],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if analytics_result.returncode == 0:
            print("✅ Analytics pipeline status OK")
        else:
            print("❌ Analytics pipeline status issues")
        
        return raw_result.returncode == 0 and analytics_result.returncode == 0
        
    except Exception as e:
        print(f"❌ Status check failed: {e}")
        return False

def main():
    """Main entry point for master pipeline"""
    parser = argparse.ArgumentParser(description='Master Data Pipeline (Team-Specific Mode)')
    parser.add_argument('--force-raw', action='store_true', help='Force raw pipeline refresh')
    parser.add_argument('--force-analytics', action='store_true', help='Force analytics pipeline refresh')
    parser.add_argument('--force-all', action='store_true', help='Force both pipelines')
    parser.add_argument('--status', action='store_true', help='Check system status only')
    parser.add_argument('--dry-run', action='store_true', help='Show what would run without executing')
    
    args = parser.parse_args()
    
    # Ensure log directory exists
    Path("data/logs").mkdir(parents=True, exist_ok=True)
    
    try:
        if args.status:
            success = check_system_status()
        elif args.dry_run:
            print("🔍 DRY RUN - Would execute:")
            print("1. Raw pipeline (scrape latest FBRef data)")
            print("2. Analytics pipeline (team-specific SCD Type 2 update)")
            print("3. Validation checks")
            success = True
        else:
            force_raw = args.force_all or args.force_raw
            force_analytics = args.force_all or args.force_analytics
            
            pipeline = MasterPipeline()
            success = pipeline.run_complete_pipeline(force_raw, force_analytics)
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()