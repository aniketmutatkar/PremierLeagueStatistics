#!/usr/bin/env python3
"""
Master Pipeline - Orchestrates Complete Data Pipeline
Runs Raw → Analytics pipeline with intelligent dependency management
"""

import sys
import logging
import subprocess
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/master_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
        
        logger.info("🚀 STARTING MASTER PIPELINE")
        logger.info(f"📅 Run started: {self.start_time}")
        logger.info("=" * 70)
        
        try:
            # Step 1: Check if we need to run anything
            if not self._should_run_pipeline(force_raw, force_analytics):
                logger.info("✅ All data is current - no pipeline run needed")
                return True
            
            # Step 2: Run Raw Pipeline (if needed)
            raw_success = self._run_raw_pipeline(force_raw)
            self.pipeline_stats['raw_success'] = raw_success
            
            if not raw_success:
                logger.error("❌ Raw pipeline failed - stopping master pipeline")
                return False
            
            # Step 3: Run Analytics Pipeline (always after successful raw)
            analytics_success = self._run_analytics_pipeline(force_analytics)
            self.pipeline_stats['analytics_success'] = analytics_success
            
            if not analytics_success:
                logger.error("❌ Analytics pipeline failed")
                return False
            
            # Step 4: Final validation and reporting
            self._finalize_pipeline()
            return True
            
        except Exception as e:
            logger.error(f"💥 Master pipeline failed: {e}")
            logger.exception("Full exception details:")
            return False
    
    def _should_run_pipeline(self, force_raw: bool, force_analytics: bool) -> bool:
        """Determine if pipeline needs to run"""
        if force_raw or force_analytics:
            return True
        
        try:
            # Check if raw data is behind current gameweek
            # This would require checking FBRef for latest gameweek vs our raw data
            # For now, assume we should run if forced or on schedule
            
            logger.info("🔍 Checking if pipeline run is needed...")
            
            # Add your logic here to check:
            # 1. Is it a new gameweek?  
            # 2. Has raw data been updated today?
            # 3. Is analytics behind raw?
            
            # For now, return True to always run when called
            return True
            
        except Exception as e:
            logger.warning(f"Could not determine pipeline status: {e}")
            return True  # Default to running
    
    def _run_raw_pipeline(self, force: bool = False) -> bool:
        """Run the raw data pipeline"""
        logger.info("\n📊 STEP 1: RUNNING RAW DATA PIPELINE")
        logger.info("-" * 40)
        
        raw_start = datetime.now()
        
        try:
            # Build command
            cmd = [sys.executable, "pipelines/raw_pipeline.py"]
            if force:
                cmd.append("--force")
            
            logger.info(f"🔧 Running: {' '.join(cmd)}")
            
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
                logger.info(f"⏱️ Raw pipeline duration: {raw_duration.total_seconds():.1f} seconds")
                
                # Parse output for gameweek info
                if "Current gameweek detected:" in result.stdout:
                    gw_line = [line for line in result.stdout.split('\n') if "Current gameweek detected:" in line][0]
                    gameweek = gw_line.split(":")[-1].strip()
                    self.pipeline_stats['raw_gameweek'] = gameweek
                    logger.info(f"📈 Raw data updated to gameweek: {gameweek}")
                
                return True
            else:
                logger.error("❌ Raw pipeline failed")
                logger.error(f"Exit code: {result.returncode}")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Raw pipeline timed out after 30 minutes")
            return False
        except Exception as e:
            logger.error(f"❌ Raw pipeline error: {e}")
            return False
    
    def _run_analytics_pipeline(self, force: bool = False) -> bool:
        """Run the analytics pipeline"""
        logger.info("\n🧮 STEP 2: RUNNING ANALYTICS PIPELINE")
        logger.info("-" * 40)
        
        analytics_start = datetime.now()
        
        try:
            # Build command
            cmd = [sys.executable, "pipelines/analytics_pipeline.py"]
            if force:
                cmd.append("--force")
            
            logger.info(f"🔧 Running: {' '.join(cmd)}")
            
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
                logger.info(f"⏱️ Analytics pipeline duration: {analytics_duration.total_seconds():.1f} seconds")
                
                # Parse output for processing info
                if "Players processed:" in result.stdout:
                    stats_lines = [line for line in result.stdout.split('\n') if "processed:" in line]
                    for line in stats_lines:
                        logger.info(f"📊 {line.strip()}")
                
                return True
            else:
                logger.error("❌ Analytics pipeline failed")
                logger.error(f"Exit code: {result.returncode}")
                logger.error(f"STDOUT: {result.stdout}")
                logger.error(f"STDERR: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Analytics pipeline timed out after 10 minutes")
            return False
        except Exception as e:
            logger.error(f"❌ Analytics pipeline error: {e}")
            return False
    
    def _finalize_pipeline(self):
        """Final validation and reporting"""
        total_duration = datetime.now() - self.start_time
        self.pipeline_stats['total_duration'] = total_duration
        
        logger.info("\n🎉 MASTER PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f"⏱️ Total duration: {total_duration.total_seconds():.1f} seconds")
        
        # Report component durations
        if self.pipeline_stats['raw_duration']:
            logger.info(f"📊 Raw pipeline: {self.pipeline_stats['raw_duration'].total_seconds():.1f}s")
        if self.pipeline_stats['analytics_duration']:
            logger.info(f"🧮 Analytics pipeline: {self.pipeline_stats['analytics_duration'].total_seconds():.1f}s")
        
        # Report data status
        if self.pipeline_stats['raw_gameweek']:
            logger.info(f"📈 Current gameweek: {self.pipeline_stats['raw_gameweek']}")
        
        # Quick validation
        try:
            success = self._quick_validation()
            if success:
                logger.info("✅ Quick validation passed")
            else:
                logger.warning("⚠️ Quick validation failed")
        except Exception as e:
            logger.warning(f"⚠️ Could not run quick validation: {e}")
    
    def _quick_validation(self) -> bool:
        """Run quick validation checks"""
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
                return False
            
            if raw_gw != analytics_gw:
                logger.warning(f"Gameweek mismatch: Raw={raw_gw}, Analytics={analytics_gw}")
                return False
            
            if player_count < 300:  # Sanity check
                logger.warning(f"Low player count: {player_count}")
                return False
            
            logger.info(f"📊 Validation: GW{analytics_gw}, {player_count:,} players")
            return True
            
        except Exception as e:
            logger.error(f"Validation error: {e}")
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
        logger.exception("Full exception details:")
        sys.exit(1)

if __name__ == "__main__":
    main()