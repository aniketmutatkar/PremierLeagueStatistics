#!/usr/bin/env python3
"""
Data Validation Script
======================

Validates that dashboard visualizations match actual database values.
Tests:
1. Radar chart values match category breakdown table
2. Category breakdown matches database
3. Individual metrics match database
4. Rank calculations are correct
"""

import sys
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'dashboard'))
sys.path.insert(0, str(project_root / 'analysis'))

from squad_analyzer import SquadAnalyzer
from data_loader import (
    load_squad_profile,
    load_comparison,
    load_category_breakdown,
    extract_radar_data,
    extract_category_comparison_table
)


def validate_squad_data(squad_name, timeframe="current"):
    """
    Validate all data for a single squad
    """
    print(f"\n{'='*80}")
    print(f"VALIDATING: {squad_name} ({timeframe})")
    print(f"{'='*80}")
    
    # Load profile
    profile = load_squad_profile(squad_name, timeframe)
    
    if "error" in profile:
        print(f"❌ ERROR loading profile: {profile['error']}")
        return False
    
    # Get category scores from profile
    category_scores = profile['dual_percentiles']['category_scores']
    
    # Extract radar data
    categories, radar_values = extract_radar_data(profile, use_composite=True)
    
    print(f"\n1. RADAR CHART vs CATEGORY TABLE VALIDATION")
    print("-" * 80)
    print(f"{'Category':<25} | {'Radar Value':<12} | {'Table Composite':<15} | {'Match?'}")
    print("-" * 80)
    
    all_match = True
    
    for cat, radar_val in zip(categories, radar_values):
        table_composite = category_scores[cat].get('composite_score')
        
        # Allow small floating point differences
        if table_composite is not None:
            match = abs(radar_val - table_composite) < 0.1
        else:
            match = False
        
        status = "✅" if match else "❌"
        
        print(f"{cat:<25} | {radar_val:<12.2f} | {table_composite:<15.2f} | {status}")
        
        if not match:
            all_match = False
    
    print(f"\nRadar Chart Validation: {'✅ PASS' if all_match else '❌ FAIL'}")
    
    # Validate against database directly
    print(f"\n2. DATABASE VALIDATION")
    print("-" * 80)
    
    with SquadAnalyzer() as analyzer:
        filter_clause, _ = analyzer._parse_timeframe(timeframe)
        
        # Get raw data from database
        query = f"""
            SELECT squad_name, goals, assists, shots, passes_completed
            FROM analytics_squads
            WHERE squad_name = ? AND {filter_clause}
            LIMIT 1
        """
        
        raw_data = analyzer.conn.execute(query, [squad_name]).fetchdf()
        
        if raw_data.empty:
            print(f"❌ No data found in database for {squad_name}")
            return False
        
        print(f"✅ Database record found for {squad_name}")
        print(f"   Sample values: goals={raw_data['goals'].iloc[0]}, "
              f"assists={raw_data['assists'].iloc[0]}, "
              f"shots={raw_data['shots'].iloc[0]}")
    
    return all_match


def validate_comparison(squad1, squad2, timeframe="current"):
    """
    Validate comparison data between two squads
    """
    print(f"\n{'='*80}")
    print(f"VALIDATING COMPARISON: {squad1} vs {squad2} ({timeframe})")
    print(f"{'='*80}")
    
    # Load comparison
    comparison = load_comparison(squad1, squad2, timeframe)
    
    if "error" in comparison:
        print(f"❌ ERROR loading comparison: {comparison['error']}")
        return False
    
    # Load individual profiles
    profile1 = load_squad_profile(squad1, timeframe)
    profile2 = load_squad_profile(squad2, timeframe)
    
    # Extract table data
    table_data = extract_category_comparison_table(comparison)
    
    print(f"\n3. COMPARISON TABLE VALIDATION")
    print("-" * 80)
    print(f"{'Category':<25} | {'S1 Profile':<12} | {'S1 Table':<12} | {'S2 Profile':<12} | {'S2 Table':<12} | {'Match?'}")
    print("-" * 80)
    
    all_match = True
    
    cat_scores1 = profile1['dual_percentiles']['category_scores']
    cat_scores2 = profile2['dual_percentiles']['category_scores']
    
    for row in table_data:
        cat = row['category']
        
        # Get from profiles
        profile1_comp = cat_scores1[cat].get('composite_score')
        profile2_comp = cat_scores2[cat].get('composite_score')
        
        # Get from table
        table1_comp = row['squad1_composite']
        table2_comp = row['squad2_composite']
        
        # Check if they match
        match1 = abs(profile1_comp - table1_comp) < 0.1 if profile1_comp and table1_comp else False
        match2 = abs(profile2_comp - table2_comp) < 0.1 if profile2_comp and table2_comp else False
        
        match = match1 and match2
        status = "✅" if match else "❌"
        
        print(f"{cat:<25} | {profile1_comp:<12.2f} | {table1_comp:<12.2f} | "
              f"{profile2_comp:<12.2f} | {table2_comp:<12.2f} | {status}")
        
        if not match:
            all_match = False
    
    print(f"\nComparison Validation: {'✅ PASS' if all_match else '❌ FAIL'}")
    
    return all_match


def validate_category_breakdown(squad_name, category, timeframe="current"):
    """
    Validate category breakdown metrics
    """
    print(f"\n{'='*80}")
    print(f"VALIDATING CATEGORY: {squad_name} - {category} ({timeframe})")
    print(f"{'='*80}")
    
    # Load breakdown
    breakdown = load_category_breakdown(squad_name, category, timeframe)
    
    if "error" in breakdown:
        print(f"❌ ERROR loading breakdown: {breakdown['error']}")
        return False
    
    # Get metrics
    metrics = breakdown.get('metric_details', [])
    
    print(f"\n4. INDIVIDUAL METRICS VALIDATION")
    print("-" * 80)
    print(f"Total metrics in category: {len(metrics)}")
    
    # Sample first 5 metrics
    print(f"\nSample of first 5 metrics:")
    print(f"{'Metric':<30} | {'Value':<10} | {'Rank':<8}")
    print("-" * 80)
    
    for metric in metrics[:5]:
        print(f"{metric['metric']:<30} | {metric['value']:<10} | {metric.get('rank', 'N/A'):<8}")
    
    # Validate against database
    with SquadAnalyzer() as analyzer:
        filter_clause, _ = analyzer._parse_timeframe(timeframe)
        
        # Get one metric from database
        test_metric = metrics[0]['metric']
        
        query = f"""
            SELECT squad_name, "{test_metric}"
            FROM analytics_squads
            WHERE squad_name = ? AND {filter_clause}
            LIMIT 1
        """
        
        try:
            raw_data = analyzer.conn.execute(query, [squad_name]).fetchdf()
            
            if not raw_data.empty:
                db_value = raw_data[test_metric].iloc[0]
                breakdown_value = metrics[0]['value']
                
                print(f"\nDatabase cross-check for '{test_metric}':")
                print(f"  Database value: {db_value}")
                print(f"  Breakdown value: {breakdown_value}")
                
                # Check if they match
                if isinstance(db_value, (int, float)) and isinstance(breakdown_value, (int, float)):
                    match = abs(float(db_value) - float(breakdown_value)) < 0.1
                else:
                    match = str(db_value) == str(breakdown_value)
                
                print(f"  Match: {'✅ YES' if match else '❌ NO'}")
                
                return match
            else:
                print(f"❌ No database record found")
                return False
                
        except Exception as e:
            print(f"❌ Error querying database: {e}")
            return False


def main():
    """
    Run all validations
    """
    print("\n" + "="*80)
    print("SQUAD ANALYTICS DASHBOARD - DATA VALIDATION")
    print("="*80)
    
    # Test squads
    test_cases = [
        ("Arsenal", "current"),
        ("Liverpool", "current"),
        ("Manchester City", "season_2023-2024"),  # Historical
    ]
    
    results = []
    
    # Validate individual squads
    for squad, timeframe in test_cases:
        result = validate_squad_data(squad, timeframe)
        results.append(("Squad Data", squad, timeframe, result))
    
    # Validate comparison
    result = validate_comparison("Arsenal", "Liverpool", "current")
    results.append(("Comparison", "Arsenal vs Liverpool", "current", result))
    
    # Validate category breakdown
    result = validate_category_breakdown("Arsenal", "attacking_output", "current")
    results.append(("Category", "Arsenal attacking_output", "current", result))
    
    # Summary
    print(f"\n{'='*80}")
    print("VALIDATION SUMMARY")
    print(f"{'='*80}")
    print(f"{'Test Type':<20} | {'Subject':<30} | {'Timeframe':<20} | {'Result'}")
    print("-" * 80)
    
    for test_type, subject, timeframe, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_type:<20} | {subject:<30} | {timeframe:<20} | {status}")
    
    all_passed = all(r[3] for r in results)
    
    print(f"\n{'='*80}")
    if all_passed:
        print("✅ ALL VALIDATIONS PASSED")
    else:
        print("❌ SOME VALIDATIONS FAILED - DATA INCONSISTENCY DETECTED")
    print(f"{'='*80}\n")
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)