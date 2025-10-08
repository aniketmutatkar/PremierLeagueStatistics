#!/usr/bin/env python3
"""
Test Historical Data with Rank System
======================================
Verify ranks work correctly across different seasons
"""

import subprocess
import sys

def run_command(description, command):
    """Run a CLI command and display results"""
    print("\n" + "="*80)
    print(f"TEST: {description}")
    print("="*80)
    print(f"Command: {' '.join(command)}")
    print("-"*80)
    
    try:
        result = subprocess.run(command, capture_output=False, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"❌ Error running command: {e}")
        return False

def main():
    print("\n" + "="*80)
    print("TESTING HISTORICAL DATA WITH RANK SYSTEM")
    print("="*80)
    
    tests = [
        {
            "description": "2023-2024 Season - Man City Attacking",
            "command": ["python3", "analysis/squad_cli.py", "--category", "attacking_output", 
                       "--squad", "Manchester City", "--timeframe", "season_2023-2024"]
        },
        {
            "description": "2023-2024 Season - Arsenal Comprehensive (Title Race)",
            "command": ["python3", "analysis/squad_cli.py", "--comprehensive", "Arsenal", 
                       "--timeframe", "season_2023-2024"]
        },
        {
            "description": "2022-2023 Season - Leicester Defending (Pre-relegation)",
            "command": ["python3", "analysis/squad_cli.py", "--category", "defending", 
                       "--squad", "Leicester City", "--timeframe", "season_2022-2023"]
        },
        {
            "description": "2023-2024 League Table",
            "command": ["python3", "analysis/squad_cli.py", "--table", 
                       "--timeframe", "season_2023-2024"]
        },
        {
            "description": "2023-2024 Top Attacking Teams",
            "command": ["python3", "analysis/squad_cli.py", "--top-attacking", 
                       "--timeframe", "season_2023-2024"]
        },
        {
            "description": "2022-2023 Squad Comparison - Liverpool vs Man City",
            "command": ["python3", "analysis/squad_cli.py", "--compare", "Liverpool", 
                       "Manchester City", "--timeframe", "season_2022-2023"]
        }
    ]
    
    results = []
    for i, test in enumerate(tests, 1):
        print(f"\n\n{'#'*80}")
        print(f"Running Test {i}/{len(tests)}")
        print(f"{'#'*80}")
        
        success = run_command(test["description"], test["command"])
        results.append((test["description"], success))
        
        if i < len(tests):
            input("\nPress Enter to continue to next test...")
    
    # Summary
    print("\n\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    for desc, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{status} - {desc}")
    
    print("\n" + "="*80)
    print("THINGS TO VERIFY:")
    print("="*80)
    print("1. ✅ All individual metrics show 'Rank (Out of 20)' column")
    print("2. ✅ Ranks display as 'X/20' format (not percentages)")
    print("3. ✅ Historical seasons show different ranks than current")
    print("4. ✅ No Python errors or crashes")
    print("5. ✅ Category-level ranks still show correctly")
    print("6. ✅ League tables from past seasons work")
    print("="*80)

if __name__ == "__main__":
    main()