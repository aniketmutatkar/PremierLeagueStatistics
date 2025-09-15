#!/usr/bin/env python3
"""
Test ETL Components
Test player consolidation and derived metrics calculation
"""

import sys
from pathlib import Path
import pandas as pd

# Add src to path 
sys.path.append(str(Path(__file__).parent / 'src'))

from database.analytics_db import AnalyticsDBConnection, AnalyticsDBOperations
from analytics.etl.player_consolidation import PlayerDataConsolidator
from analytics.etl.derived_metrics import DerivedMetricsCalculator

def test_etl_components():
    """Test that our ETL components work with real data"""
    print("🧪 Testing ETL Components...")
    
    # Setup
    db = AnalyticsDBConnection()
    ops = AnalyticsDBOperations()
    consolidator = PlayerDataConsolidator()
    calculator = DerivedMetricsCalculator()
    
    current_gw = db.get_current_gameweek()
    print(f"📊 Testing with gameweek: {current_gw}")
    
    # Test 1: Player Data Consolidation
    print(f"\n1️⃣ Testing player data consolidation...")
    
    try:
        with db.get_dual_connections() as (raw_conn, analytics_conn):
            # Test consolidation
            consolidated_df = consolidator.consolidate_players(raw_conn, current_gw)
            
            if consolidated_df.empty:
                print("❌ Consolidation failed - no data returned")
                return False
            
            # Get consolidation summary
            summary = consolidator.get_consolidation_summary(consolidated_df)
            print(f"✅ Consolidation successful:")
            print(f"   Players: {summary['total_players']}")
            print(f"   Columns: {summary['total_columns']}")
            print(f"   Teams: {summary['teams']}")
            print(f"   Total goals: {summary['total_goals']}")
            print(f"   Missing values: {summary['missing_values']}")
            
            # Test 2: Team Totals Calculation
            print(f"\n2️⃣ Testing team totals calculation...")
            team_totals = ops.get_team_totals(raw_conn, current_gw)
            
            if team_totals.empty:
                print("❌ Team totals calculation failed")
                return False
            
            print(f"✅ Team totals calculated for {len(team_totals)} teams")
            print(f"   Sample team totals:")
            print(team_totals.head(3).to_string(index=False))
            
            # Test 3: Derived Metrics Calculation
            print(f"\n3️⃣ Testing derived metrics calculation...")
            
            try:
                metrics_df = calculator.calculate_all_metrics(consolidated_df, team_totals)
                
                if metrics_df.empty:
                    print("❌ Derived metrics calculation failed")
                    return False
                
                metrics_summary = calculator.get_metrics_summary()
                print(f"✅ Derived metrics calculated:")
                print(f"   Total metrics: {metrics_summary['total_metrics_calculated']}")
                print(f"   Metrics calculated: {metrics_summary['metrics_list']}")
                
                # Show sample of derived metrics
                derived_cols = [col for col in metrics_df.columns if col in metrics_summary['metrics_list']]
                if derived_cols:
                    print(f"\n📊 Sample derived metrics data:")
                    sample_cols = ['player_name', 'squad'] + derived_cols[:5]  # Show first 5 metrics
                    print(metrics_df[sample_cols].head(3).to_string(index=False))
                
            except Exception as e:
                print(f"❌ Derived metrics calculation error: {e}")
                return False
            
            # Test 4: Data Quality Check
            print(f"\n4️⃣ Testing data quality validation...")
            
            # Check for key columns
            required_cols = ['player_name', 'squad', 'Born', 'goals', 'assists', 'minutes_played']
            missing_cols = [col for col in required_cols if col not in metrics_df.columns]
            
            if missing_cols:
                print(f"⚠️  Missing required columns: {missing_cols}")
            else:
                print("✅ All required columns present")
            
            # Check data ranges
            if 'goals' in metrics_df.columns:
                total_goals = metrics_df['goals'].sum()
                max_goals = metrics_df['goals'].max()
                print(f"✅ Goals check: {total_goals} total, {max_goals} max individual")
            
            if 'minutes_played' in metrics_df.columns:
                avg_minutes = metrics_df['minutes_played'].mean()
                print(f"✅ Minutes check: {avg_minutes:.1f} average minutes")
            
    except Exception as e:
        print(f"❌ ETL test failed: {e}")
        return False
    
    print("\n🎉 ETL components test completed successfully!")
    print("📝 Ready to build the full ETL pipeline...")
    return True

if __name__ == "__main__":
    success = test_etl_components()
    sys.exit(0 if success else 1)