#!/usr/bin/env python3
"""
Master Pipeline - Orchestrates Complete Data Pipeline
Runs Raw → Analytics pipeline with intelligent dependency management
"""

import sys
import subprocess
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
import time
import glob

class PipelineLogger:
    """Enhanced logging for pipeline operations with rotation and granular tracking"""
    
    def __init__(self, pipeline_name: str, log_dir: str = "data/logs", keep_logs: int = 5):
        import logging
        
        self.pipeline_name = pipeline_name
        self.log_dir = Path(log_dir)
        self.keep_logs = keep_logs
        
        # Ensure log directory exists
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create timestamped log file
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.log_file = self.log_dir / f"{pipeline_name}_{timestamp}.log"
        
        # Setup logging
        self._setup_logging()
        
        # Cleanup old logs
        self._cleanup_old_logs()
    
    def _setup_logging(self):
        """Configure logging with both file and console output"""
        import logging
        
        # Clear any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        # Create formatter (cleaner format)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # File handler
        file_handler = logging.FileHandler(self.log_file, mode='w')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        
        # Console handler (less verbose)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        
        # Configure root logger
        logging.root.setLevel(logging.INFO)
        logging.root.addHandler(file_handler)
        logging.root.addHandler(console_handler)
        
        # Get logger for this pipeline
        self.logger = logging.getLogger(self.pipeline_name)
    
    def _cleanup_old_logs(self):
        """Keep only the most recent N log files"""
        pattern = str(self.log_dir / f"{self.pipeline_name}_*.log")
        log_files = glob.glob(pattern)
        
        if len(log_files) > self.keep_logs:
            # Sort by modification time (newest first)
            log_files.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)
            
            # Remove old logs
            for old_log in log_files[self.keep_logs:]:
                try:
                    Path(old_log).unlink()
                except Exception:
                    pass  # Ignore cleanup errors

# Initialize enhanced logging
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
            'raw_gameweek': None,
            'analytics_gameweek': None,
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
        
        logger.info("🚀 MASTER PIPELINE STARTED")
        logger.info(f"Run started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 50)
        
        try:
            # Step 1: Intelligent decision making
            logger.info("🔍 Checking pipeline requirements...")
            should_run_raw, should_run_analytics = self._should_run_pipeline(force_raw, force_analytics)
            
            if not should_run_raw and not should_run_analytics:
                logger.info("✅ All data current - no updates needed")
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
        Intelligent decision on what pipelines to run
        Returns: (should_run_raw, should_run_analytics)
        """
        if force_raw or force_analytics:
            logger.info(f"Force flags: raw={force_raw}, analytics={force_analytics}")
            return force_raw, force_analytics
        
        try:
            # Get current local state (fast)
            raw_gw = self._get_raw_gameweek()
            analytics_gw = self._get_analytics_gameweek()
            
            logger.info(f"Current state: Raw=GW{raw_gw}, Analytics=GW{analytics_gw}")
            
            # Case 1: Analytics behind raw - run analytics only
            if analytics_gw < raw_gw:
                logger.info("Analytics behind raw - will update analytics")
                return False, True
            
            # Case 2: Check if new gameweek available on FBRef
            if self._should_check_fbref():
                logger.info("Checking FBRef for new gameweek...")
                fbref_gw = self._quick_gameweek_check()
                logger.info(f"FBRef gameweek: {fbref_gw}")
                
                if raw_gw < fbref_gw:
                    logger.info(f"New gameweek available: GW{fbref_gw}")
                    return True, True
                else:
                    logger.info("No new gameweek found")
            else:
                logger.info("Skipping FBRef check (recently checked)")
            
            # Case 3: Everything current
            logger.info("All systems up to date")
            return False, False
            
        except Exception as e:
            logger.warning(f"Decision logic failed: {e}, defaulting to force run")
            return True, True
    
    def _should_check_fbref(self) -> bool:
        """Only check FBRef once per day to avoid unnecessary requests"""
        try:
            # Check if we've already checked FBRef today
            today = datetime.now().date()
            
            # Look for today's log files to see if we already checked
            log_pattern = str(pipeline_logger.log_dir / f"master_pipeline_{today.strftime('%Y%m%d')}_*.log")
            today_logs = glob.glob(log_pattern)
            
            # If we have logs from today, check if we already made FBRef request
            for log_file in today_logs:
                try:
                    with open(log_file, 'r') as f:
                        content = f.read()
                        if "Checking FBRef for new gameweek" in content:
                            logger.info("Already checked FBRef today - skipping")
                            return False
                except Exception:
                    continue
            
            # No FBRef check found today - ok to check
            return True
            
        except Exception:
            # If anything fails, default to checking (safe fallback)
            return True
    
    def _quick_gameweek_check(self) -> int:
        """Quick check of current gameweek from FBRef fixtures page"""
        try:
            # Add src to path for imports
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from src.scraping.fbref_scraper import FBRefScraper
            
            scraper = FBRefScraper()
            fixtures_url = scraper.sources_config['fixtures_sources']['current_season']['url']
            
            # Use existing fixtures scraping (lightweight)
            fixture_data = scraper.scrape_fixtures(fixtures_url)
            return fixture_data['current_gameweek']
            
        except Exception as e:
            logger.warning(f"FBRef check failed: {e}")
            return 0  # Safe fallback
    
    def _get_raw_gameweek(self) -> int:
        """Get current gameweek from raw database"""
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from src.database.analytics_db import AnalyticsDBConnection
            
            db = AnalyticsDBConnection()
            return db.get_current_gameweek() or 0
        except Exception:
            return 0
    
    def _get_analytics_gameweek(self) -> int:
        """Get current gameweek from analytics database"""
        try:
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from src.database.analytics_db import AnalyticsDBOperations
            
            ops = AnalyticsDBOperations()
            return ops.get_current_analytics_gameweek() or 0
        except Exception:
            return 0
    
    def _run_raw_pipeline(self, force: bool = False) -> bool:
        """Run the raw data pipeline with detailed tracking"""
        raw_start = datetime.now()
        
        try:
            # Build command
            cmd = [sys.executable, "pipelines/raw_pipeline.py"]
            if force:
                cmd.append("--force")
            
            logger.info(f"🔧 Executing raw pipeline...")
            if force:
                logger.info("  🔄 Force refresh enabled")
            
            # Run raw pipeline
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout
            )
            
            raw_duration = datetime.now() - raw_start
            self.pipeline_stats['raw_duration'] = raw_duration
            
            if result.returncode == 0:
                logger.info("✅ Raw pipeline completed successfully")
                logger.info(f"⏱️  Duration: {raw_duration.total_seconds():.1f} seconds")
                
                # Parse output for gameweek info and progress
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if "Current gameweek detected:" in line:
                        gameweek = line.split(":")[-1].strip()
                        self.pipeline_stats['raw_gameweek'] = gameweek
                        logger.info(f"📈 Gameweek: {gameweek}")
                    elif "categories successful" in line:
                        logger.info(f"📊 {line.strip()}")
                    elif "tables populated" in line:
                        logger.info(f"💾 {line.strip()}")
                
                return True
            else:
                logger.error("❌ Raw pipeline failed")
                logger.error(f"Exit code: {result.returncode}")
                if result.stderr:
                    logger.error(f"Error: {result.stderr[:200]}...")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Raw pipeline timed out (30 minutes)")
            return False
        except Exception as e:
            logger.error(f"❌ Raw pipeline error: {e}")
            return False
    
    def _run_analytics_pipeline(self, force: bool = False) -> bool:
        """Run the analytics pipeline with detailed tracking"""
        analytics_start = datetime.now()
        
        try:
            # Build command
            cmd = [sys.executable, "pipelines/analytics_pipeline.py"]
            if force:
                cmd.append("--force")
            
            logger.info(f"🔧 Executing analytics pipeline...")
            if force:
                logger.info("  🔄 Force refresh enabled")
            
            # Run analytics pipeline
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            
            analytics_duration = datetime.now() - analytics_start
            self.pipeline_stats['analytics_duration'] = analytics_duration
            
            if result.returncode == 0:
                logger.info("✅ Analytics pipeline completed successfully")
                logger.info(f"⏱️  Duration: {analytics_duration.total_seconds():.1f} seconds")
                
                # Parse output for processing details
                output_lines = result.stdout.split('\n')
                for line in output_lines:
                    if "Players processed:" in line:
                        logger.info(f"👥 {line.strip()}")
                    elif "Records inserted:" in line:
                        logger.info(f"💾 {line.strip()}")
                    elif "SCD Type 2" in line:
                        logger.info(f"🔄 {line.strip()}")
                
                return True
            else:
                logger.error("❌ Analytics pipeline failed")
                logger.error(f"Exit code: {result.returncode}")
                if result.stderr:
                    logger.error(f"Error: {result.stderr[:200]}...")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Analytics pipeline timed out (10 minutes)")
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
        
        # Current state
        if self.pipeline_stats['raw_gameweek']:
            logger.info(f"📈 Current gameweek: {self.pipeline_stats['raw_gameweek']}")
        
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
            sys.path.insert(0, str(Path(__file__).parent.parent))
            from src.database.analytics_db import AnalyticsDBConnection, AnalyticsDBOperations
            
            db = AnalyticsDBConnection()
            ops = AnalyticsDBOperations()
            
            # Check basic stats
            raw_gw = db.get_current_gameweek()
            analytics_gw = ops.get_current_analytics_gameweek()
            player_count = ops.get_analytics_player_count()
            
            # Validation checks
            if raw_gw is None or analytics_gw is None:
                logger.warning("  Could not determine gameweek status")
                return False
            
            if raw_gw != analytics_gw:
                logger.warning(f"  Gameweek mismatch: Raw={raw_gw}, Analytics={analytics_gw}")
                return False
            
            if player_count < 300:  # Sanity check
                logger.warning(f"  Low player count: {player_count}")
                return False
            
            logger.info(f"  📊 GW{analytics_gw} with {player_count:,} players")
            return True
            
        except Exception as e:
            logger.error(f"  Validation error: {e}")
            return False

def check_system_status():
    """Check overall system status"""
    print("🔍 SYSTEM STATUS CHECK")
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
            print(raw_result.stderr)
        
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
            print(analytics_result.stderr)
        
        return raw_result.returncode == 0 and analytics_result.returncode == 0
        
    except Exception as e:
        print(f"❌ Status check failed: {e}")
        return False

def main():
    """Main entry point for master pipeline"""
    parser = argparse.ArgumentParser(description='Master Data Pipeline')
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
            print("2. Analytics pipeline (SCD Type 2 update)")
            print("3. Validation and reporting")
            success = True
        else:
            # Handle force flags
            force_raw = args.force_raw or args.force_all
            force_analytics = args.force_analytics or args.force_all
            
            # Run master pipeline
            pipeline = MasterPipeline()
            success = pipeline.run_complete_pipeline(
                force_raw=force_raw,
                force_analytics=force_analytics
            )
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Master pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()