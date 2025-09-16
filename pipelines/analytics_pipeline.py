#!/usr/bin/env python3
"""
Production Analytics Pipeline
Production-ready ETL pipeline for analytics layer with SCD Type 2
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.etl.analytics_etl import AnalyticsETL

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/logs/analytics_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ProductionAnalyticsPipeline:
    """Production analytics pipeline with comprehensive monitoring"""
    
    def __init__(self):
        self.pipeline = AnalyticsETL()
        self.start_time = None
        self.success = False
        
    def run(self, target_gameweek: Optional[int] = None, force_refresh: bool = False) -> bool:
        """
        Run the production analytics pipeline
        
        Args:
            target_gameweek: Specific gameweek to process (None = detect automatically)
            force_refresh: Force refresh even if data is current
            
        Returns:
            True if successful, False otherwise
        """
        self.start_time = datetime.now()
        
        try:
            logger.info("🚀 STARTING PRODUCTION ANALYTICS PIPELINE")
            logger.info(f"📅 Run started: {self.start_time}")
            logger.info(f"🎯 Target gameweek: {target_gameweek or 'Auto-detect'}")
            logger.info(f"🔄 Force refresh: {force_refresh}")
            
            # Run the pipeline
            success = self.pipeline.run_full_pipeline(
                target_gameweek=target_gameweek,
                force_refresh=force_refresh
            )
            
            if success:
                self._log_success()
                self.success = True
                return True
            else:
                self._log_failure("Pipeline returned False")
                return False
                
        except Exception as e:
            self._log_failure(f"Pipeline failed with exception: {e}")
            logger.exception("Full exception details:")
            return False
    
    def _log_success(self):
        """Log successful pipeline completion"""
        duration = datetime.now() - self.start_time
        
        logger.info("🎉 ANALYTICS PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"⏱️ Duration: {duration.total_seconds():.1f} seconds")
        
        # Get pipeline stats
        stats = self.pipeline.pipeline_stats
        if stats:
            logger.info(f"📊 Pipeline Statistics:")
            logger.info(f"   Players processed: {stats.get('players_processed', 'N/A')}")
            logger.info(f"   Teams processed: {stats.get('teams_processed', 'N/A')}")
            logger.info(f"   Records inserted: {stats.get('records_inserted', 'N/A')}")
            logger.info(f"   Gameweek: {stats.get('gameweek', 'N/A')}")
            logger.info(f"   Metrics calculated: {stats.get('metrics_calculated', 'N/A')}")
    
    def _log_failure(self, error_msg: str):
        """Log pipeline failure"""
        duration = datetime.now() - self.start_time if self.start_time else None
        
        logger.error("❌ ANALYTICS PIPELINE FAILED")
        logger.error(f"💥 Error: {error_msg}")
        if duration:
            logger.error(f"⏱️ Failed after: {duration.total_seconds():.1f} seconds")
        
        # Log system state for debugging
        try:
            from src.database.analytics_db import AnalyticsDBConnection
            db = AnalyticsDBConnection()
            
            raw_gw = db.get_current_gameweek()
            analytics_gw = db.get_current_analytics_gameweek()
            
            logger.error(f"🔍 System State:")
            logger.error(f"   Raw gameweek: {raw_gw}")
            logger.error(f"   Analytics gameweek: {analytics_gw}")
            
        except Exception as debug_error:
            logger.error(f"❌ Could not retrieve system state: {debug_error}")

def check_analytics_status():
    """Check current analytics pipeline status"""
    print("🔍 ANALYTICS PIPELINE STATUS CHECK")
    print("=" * 50)
    
    try:
        from src.database.analytics_db import AnalyticsDBConnection, AnalyticsDBOperations
        
        db = AnalyticsDBConnection()
        ops = AnalyticsDBOperations()
        
        # Get current state
        raw_gw = db.get_current_gameweek()
        analytics_gw = ops.get_current_analytics_gameweek()
        player_count = ops.get_analytics_player_count()
        
        # Check if refresh needed
        refresh_needed = raw_gw > analytics_gw if raw_gw and analytics_gw else False
        
        print(f"📊 Current Status:")
        print(f"   Raw gameweek: {raw_gw}")
        print(f"   Analytics gameweek: {analytics_gw}")
        print(f"   Analytics player count: {player_count:,}")
        print(f"   Refresh needed: {'✅ Yes' if refresh_needed else '❌ No'}")
        
        # Show recent activity
        with db.get_analytics_connection() as conn:
            recent_records = conn.execute("""
                SELECT gameweek, is_current, COUNT(*) as records
                FROM analytics_players 
                GROUP BY gameweek, is_current 
                ORDER BY gameweek DESC, is_current DESC
                LIMIT 5
            """).fetchall()
            
            print(f"\n📈 Recent Records:")
            for gw, is_current, count in recent_records:
                status = "Current" if is_current else "Historical"
                print(f"   GW{gw}: {count:,} {status} records")
        
        return True
        
    except Exception as e:
        print(f"❌ Status check failed: {e}")
        return False

def validate_analytics_data():
    """Run data validation checks"""
    print("\n🧪 ANALYTICS DATA VALIDATION")
    print("=" * 50)
    
    try:
        from validate_analytics_system import AnalyticsValidator
        
        with AnalyticsValidator() as validator:
            # Run core validations
            scd_valid = validator.validate_scd_type_2()
            metrics_valid = validator.validate_derived_metrics()
            quality_valid = validator.validate_data_quality()
            
            if all([scd_valid, metrics_valid, quality_valid]):
                print("✅ All validation checks passed")
                return True
            else:
                print("❌ Some validation checks failed")
                return False
                
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

def main():
    """Main entry point for production analytics pipeline"""
    parser = argparse.ArgumentParser(description='Production Analytics Pipeline')
    parser.add_argument('--gameweek', type=int, help='Target gameweek to process')
    parser.add_argument('--force', action='store_true', help='Force refresh even if current')
    parser.add_argument('--status', action='store_true', help='Check pipeline status only')
    parser.add_argument('--validate', action='store_true', help='Run data validation only')
    parser.add_argument('--quiet', action='store_true', help='Suppress console output')
    
    args = parser.parse_args()
    
    # Configure logging based on quiet flag
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    # Ensure log directory exists
    Path("data/logs").mkdir(parents=True, exist_ok=True)
    
    try:
        if args.status:
            success = check_analytics_status()
        elif args.validate:
            success = validate_analytics_data()
        else:
            # Run the analytics pipeline
            pipeline = ProductionAnalyticsPipeline()
            success = pipeline.run(
                target_gameweek=args.gameweek,
                force_refresh=args.force
            )
            
            # Optional validation after successful run
            if success and not args.quiet:
                print("\n" + "="*50)
                validate_analytics_data()
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.exception("Full exception details:")
        sys.exit(1)

if __name__ == "__main__":
    main()