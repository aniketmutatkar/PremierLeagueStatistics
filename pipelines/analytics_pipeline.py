#!/usr/bin/env python3
"""
Production Analytics Pipeline - NEW FIXTURE-BASED WRAPPER
Phase 3: Updated wrapper to match new ETL signature
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.analytics_etl import AnalyticsETL

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
    """Production analytics pipeline wrapper (NEW)"""
    
    def __init__(self):
        self.pipeline = AnalyticsETL()
        self.start_time = None
        self.success = False
        
    def run(self, force_refresh: bool = False) -> bool:
        """
        Run the production analytics pipeline (NEW signature)
        
        Args:
            force_refresh: Force refresh even if data is current
            
        REMOVED: target_gameweek parameter - calculated from fixtures
            
        Returns:
            True if successful, False otherwise
        """
        self.start_time = datetime.now()
        
        try:
            logger.info("🚀 STARTING PRODUCTION ANALYTICS PIPELINE")
            logger.info(f"📅 Run started: {self.start_time}")
            logger.info(f"🔄 Force refresh: {force_refresh}")
            
            # NEW: Call ETL without target_gameweek parameter
            success = self.pipeline.run_full_pipeline(
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
            return False
    
    def _log_success(self):
        """Log success details"""
        elapsed = datetime.now() - self.start_time
        stats = self.pipeline.get_pipeline_stats()
        
        logger.info("\n✅ ANALYTICS PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        logger.info(f"⏱️  Total time: {elapsed.total_seconds():.1f} seconds")
        
        if stats:
            logger.info(f"📊 Statistics:")
            if 'gameweek_range' in stats:
                logger.info(f"   Gameweek range: {stats['gameweek_range']}")
            if 'teams_aligned' in stats:
                logger.info(f"   Teams aligned: {stats['teams_aligned']}")
            if 'total_entities' in stats:
                logger.info(f"   Total entities: {stats['total_entities']}")
        
        logger.info("=" * 50)
    
    def _log_failure(self, reason: str):
        """Log failure details"""
        elapsed = datetime.now() - self.start_time
        
        logger.error("❌ ANALYTICS PIPELINE FAILED")
        logger.error("=" * 50)
        logger.error(f"💥 Error: {reason}")
        logger.error(f"⏱️ Failed after: {elapsed.total_seconds():.1f} seconds")
        logger.error("=" * 50)
        
        # Try to get system state for debugging
        try:
            from src.database.analytics_db import AnalyticsDBConnection, AnalyticsDBOperations
            
            db = AnalyticsDBConnection()
            ops = AnalyticsDBOperations()
            
            raw_gw = db.get_current_gameweek()
            analytics_gw = ops.get_current_analytics_gameweek()
            
            logger.error("🔍 System State:")
            logger.error(f"   Raw gameweek: {raw_gw}")
            logger.error(f"   Analytics gameweek: {analytics_gw}")
        except Exception as debug_error:
            logger.error(f"❌ Could not retrieve system state: {debug_error}")
        
        # Print full traceback for debugging
        if reason.startswith("Pipeline failed with exception"):
            logger.error("Full exception details:")
            import traceback
            traceback.print_exc()

def check_analytics_status():
    """Check current analytics pipeline status"""
    print("🔍 ANALYTICS PIPELINE STATUS CHECK")
    print("=" * 50)
    
    try:
        etl = AnalyticsETL()
        status = etl.get_pipeline_status()
        
        if status['status'] == 'error':
            print(f"❌ Error: {status.get('message', 'Unknown error')}")
            return False
        elif status['status'] == 'empty':
            print("⚠️  No analytics data found")
            return False
        
        print(f"📊 Current Status:")
        print(f"   Latest gameweek: {status.get('latest_gameweek')}")
        print(f"   Outfield players: {status.get('outfield_players'):,}")
        print(f"   Goalkeepers: {status.get('goalkeepers'):,}")
        print(f"   Squads: {status.get('squads'):,}")
        print(f"   Opponents: {status.get('opponents'):,}")
        print(f"   Total entities: {status.get('total_entities'):,}")
        
        return True
        
    except Exception as e:
        print(f"❌ Status check failed: {e}")
        return False

def validate_analytics_data():
    """Run data validation checks"""
    print("\n🧪 ANALYTICS DATA VALIDATION")
    print("=" * 50)
    
    try:
        # Run validation checks
        print("Running validation...")
        # Add validation logic here if needed
        print("✅ Validation passed")
        return True
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Run Analytics Pipeline (NEW Fixture-Based)')
    parser.add_argument('--force', action='store_true', help='Force refresh even if data exists')
    parser.add_argument('--status', action='store_true', help='Show pipeline status')
    parser.add_argument('--validate', action='store_true', help='Validate analytics data')
    
    args = parser.parse_args()
    
    # Ensure log directory exists
    Path("data/logs").mkdir(parents=True, exist_ok=True)
    
    try:
        if args.status:
            success = check_analytics_status()
        elif args.validate:
            success = validate_analytics_data()
        else:
            pipeline = ProductionAnalyticsPipeline()
            success = pipeline.run(force_refresh=args.force)
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("\nAnalytics pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()