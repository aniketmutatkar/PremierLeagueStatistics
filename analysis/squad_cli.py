#!/usr/bin/env python3
"""
Squad Analytics CLI
===================

CLI interface for SquadAnalyzer with:
- Dual percentile display (overall + positional)
- League-wide insights
- Squad comparisons
- Historical analysis support
"""

import argparse
from pathlib import Path
import sys

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'analysis'))

from squad_analyzer import SquadAnalyzer


def print_comprehensive_analysis(squad_name: str, timeframe: str = "current"):
    """Print comprehensive analysis with composite scores and ranks"""
    with SquadAnalyzer() as analyzer:
        profile = analyzer.get_comprehensive_squad_profile(squad_name, timeframe)
        
        if "error" in profile:
            print(f"❌ {profile['error']}")
            return
        
        basic = profile['basic_info']
        dual_percentiles = profile['dual_percentiles']
        
        print(f"\n⚽ {basic['squad_name'].upper()} - COMPREHENSIVE ANALYSIS")
        print("=" * 80)
        print(f"Season: {basic['season']} | Gameweek: {basic['gameweek']}")
        print(f"Timeframe: {basic['timeframe_description']}")
        print(f"Matches: {basic['matches_played']} | Minutes: {basic['minutes_played']}")
        
        print(f"\n📊 CATEGORY SCORES:")
        print("-" * 80)
        print(f"{'Category':<20} | {'Composite':<9} | {'Rank':<8} | {'Level'}")
        print("-" * 80)
        
        # Sort by composite score
        categories_sorted = sorted(
            dual_percentiles['category_scores'].items(),
            key=lambda x: x[1]['composite_score'] if x[1]['composite_score'] is not None else 0,
            reverse=True
        )
        
        for category, data in categories_sorted:
            composite = data['composite_score']
            rank = data['rank']
            percentile = data['overall_score']
            
            # Determine level based on percentile
            if percentile is not None:
                if percentile >= 90:
                    level = "🔥 Elite"
                elif percentile >= 75:
                    level = "⭐ Very Good"
                elif percentile >= 60:
                    level = "✅ Above Avg"
                elif percentile >= 45:
                    level = "➖ Average"
                elif percentile >= 30:
                    level = "⚠️  Below Avg"
                else:
                    level = "❌ Poor"
            else:
                level = "❌ No Data"
            
            composite_str = f"{composite:.1f}" if composite is not None else "N/A"
            rank_str = f"{rank}/20" if rank is not None else "N/A"
            
            print(f"{category:<20} | {composite_str:<9} | {rank_str:<8} | {level}")
        
        # Print insights
        insights = profile['squad_insights']
        
        if insights['top_strengths']:
            print(f"\n💪 TOP STRENGTHS:")
            for strength in insights['top_strengths']:
                rank_val = strength.get('rank', 'N/A')
                score = strength.get('score', 0)
                print(f"  • {strength['category']}: Rank {rank_val}/20 (Score: {score:.1f})")
        
        if insights['main_weaknesses']:
            print(f"\n⚠️  MAIN WEAKNESSES:")
            for weakness in insights['main_weaknesses']:
                rank_val = weakness.get('rank', 'N/A')
                score = weakness.get('score', 0)
                print(f"  • {weakness['category']}: Rank {rank_val}/20 (Score: {score:.1f})")


def print_category_breakdown(squad_name: str, category: str, timeframe: str = "current"):
    """Print detailed category breakdown with composite scores and rankings"""
    with SquadAnalyzer() as analyzer:
        breakdown = analyzer.get_category_breakdown(squad_name, category, timeframe)
        
        if "error" in breakdown:
            print(f"❌ {breakdown['error']}")
            return
        
        print(f"\n🔍 {breakdown['squad_name'].upper()} - {breakdown['category'].upper()} BREAKDOWN")
        print("=" * 80)
        print(f"📝 Description: {breakdown['description']}")
        
        # Show composite score and rank
        composite = breakdown['composite_score']
        rank = breakdown['rank']
        gap = breakdown['gap_from_first']
        
        print(f"\n📊 PERFORMANCE SUMMARY:")
        print(f"  Composite Score: {composite:.1f}/100")
        print(f"  Overall Rank: {rank}/20")
        
        if gap is not None and gap < 0:
            print(f"  Gap from #1: {gap:.1f} points behind")
        elif gap == 0:
            print(f"  Gap from #1: — (You are #1!)")
        
        # Show top 5 squads for context
        print(f"\n🏆 TOP 5 SQUADS IN {breakdown['category'].upper()}:")
        print("-" * 60)
        print(f"{'Rank':<5} | {'Squad':<20} | {'Score':<8} | {'Gap'}")
        print("-" * 60)
        
        for _, row in breakdown['top_5_squads'].iterrows():
            rank_num = int(row['rank'])
            squad = row['squad_name']
            score = row['composite_score']
            gap_val = row['gap_from_first']
            
            # Add emoji for top 3
            if rank_num == 1:
                emoji = "🥇"
            elif rank_num == 2:
                emoji = "🥈"
            elif rank_num == 3:
                emoji = "🥉"
            else:
                emoji = "  "
            
            # Highlight current squad
            if squad == breakdown['squad_name']:
                squad = f"→ {squad}"
            
            gap_str = f"{gap_val:+.1f}" if gap_val != 0 else "—"
            
            print(f"{emoji} {rank_num:<3} | {squad:<20} | {score:<8.1f} | {gap_str}")
        
        # Show ALL individual metrics
        print(f"\n📈 ALL INDIVIDUAL METRICS ({len(breakdown['metric_details'])} total):")
        print("-" * 80)
        print(f"{'Rank':<4} | {'Metric':<35} | {'Rank (Out of 20)':<16} | {'Level':<12} | {'Value'}")
        print("-" * 80)
        
        for i, metric in enumerate(breakdown['metric_details'], 1):  # ALL METRICS
            pct = metric['percentile']
            interpretation = metric['interpretation']
            value = metric['value']
            
            # Choose emoji based on interpretation
            if interpretation == "Elite":
                emoji = "🔥"
            elif interpretation == "Very Good":
                emoji = "⭐"
            elif interpretation == "Above Average":
                emoji = "✅"
            elif interpretation == "Average":
                emoji = "➖"
            elif interpretation == "Below Average":
                emoji = "⚠️"
            else:
                emoji = "❌"
            
            rank = metric.get('rank')
            total = metric.get('total_squads', 20)
            rank_str = f"{rank}/{total}" if rank is not None else "N/A"
            
            # Format value
            if isinstance(value, float):
                value_str = f"{value:.1f}"
            elif value is None:
                value_str = "N/A"
            else:
                value_str = str(value)
            
            print(f"{i:<4} | {metric['metric']:<35} | {rank_str:<16} | {emoji} {interpretation:<10} | {value_str}")
        
        print(f"\n💡 TIP: The composite score ({composite:.1f}/100) is calculated by normalizing")
        print(f"   and averaging all {len(breakdown['metric_details'])} metrics in this category.")


def print_squad_comparison(squad1: str, squad2: str, timeframe: str = "current"):
    """Print comprehensive comparison between two squads"""
    with SquadAnalyzer() as analyzer:
        comparison = analyzer.compare_squads(squad1, squad2, timeframe)
        
        if "error" in comparison:
            print(f"❌ {comparison['error']}")
            return
        
        print(f"\n⚔️  {squad1.upper()} vs {squad2.upper()}")
        print("=" * 70)
        print(f"Timeframe: {comparison['timeframe_info']}")
        
        print(f"\n📊 CATEGORY-BY-CATEGORY COMPARISON:")
        print("-" * 70)
        
        for category, comp in comparison['category_comparison'].items():
            winner = comp['winner']
            score1 = comp[f'{squad1}_score']
            score2 = comp[f'{squad2}_score']
            diff = abs(comp['difference'])
            
            if winner == squad1:
                print(f"🥇 {category:<20} | {squad1} wins by {diff:.1f}% ({score1:.1f}% vs {score2:.1f}%)")
            elif winner == squad2:
                print(f"🥈 {category:<20} | {squad2} wins by {diff:.1f}% ({score2:.1f}% vs {score1:.1f}%)")
            else:
                print(f"🤝 {category:<20} | Tie ({score1:.1f}% vs {score2:.1f}%)")
        
        summary = comparison['summary']
        print(f"\n🏆 OVERALL COMPARISON:")
        print(f"Category Wins: {summary['category_wins']}")
        print(f"Overall Winner: {summary['overall_winner']}")


def print_league_table(timeframe: str = "current"):
    """Print league table standings"""
    with SquadAnalyzer() as analyzer:
        table = analyzer.calculate_league_table(timeframe)
        
        print(f"\n🏆 PREMIER LEAGUE TABLE")
        print("=" * 80)
        print(f"Timeframe: {timeframe}")
        print(f"\n{table[['position', 'squad_name', 'matches_played', 'wins', 'draws', 'losses', 'points', 'goal_difference', 'goals']].to_string(index=False)}")


def print_top_attacking(timeframe: str = "current"):
    """Print top attacking teams with composite scores"""
    with SquadAnalyzer() as analyzer:
        composite_results = analyzer.calculate_category_composite_scores('attacking_output', timeframe)
        
        print(f"\n🎯 TOP 10 ATTACKING TEAMS")
        print("=" * 60)
        print(f"{'Rank':<5} | {'Squad':<20} | {'Score':<8} | {'Gap'}")
        print("-" * 60)
        
        for _, row in composite_results.head(10).iterrows():
            rank = int(row['rank'])
            squad = row['squad_name']
            score = row['composite_score']
            gap = row['gap_from_first']
            
            if rank == 1:
                emoji = "🥇"
            elif rank == 2:
                emoji = "🥈"
            elif rank == 3:
                emoji = "🥉"
            else:
                emoji = "  "
            
            gap_str = f"{gap:+.1f}" if gap != 0 else "—"
            
            print(f"{emoji} {rank:<3} | {squad:<20} | {score:<8.1f} | {gap_str}")


def print_top_defending(timeframe: str = "current"):
    """Print top defending teams with composite scores"""
    with SquadAnalyzer() as analyzer:
        composite_results = analyzer.calculate_category_composite_scores('defending', timeframe)
        
        print(f"\n🛡️  TOP 10 DEFENDING TEAMS")
        print("=" * 60)
        print(f"{'Rank':<5} | {'Squad':<20} | {'Score':<8} | {'Gap'}")
        print("-" * 60)
        
        for _, row in composite_results.head(10).iterrows():
            rank = int(row['rank'])
            squad = row['squad_name']
            score = row['composite_score']
            gap = row['gap_from_first']
            
            if rank == 1:
                emoji = "🥇"
            elif rank == 2:
                emoji = "🥈"
            elif rank == 3:
                emoji = "🥉"
            else:
                emoji = "  "
            
            gap_str = f"{gap:+.1f}" if gap != 0 else "—"
            
            print(f"{emoji} {rank:<3} | {squad:<20} | {score:<8.1f} | {gap_str}")


def print_game_control(timeframe: str = "current"):
    """Print top game control teams (combined possession + passing + ball_progression)"""
    with SquadAnalyzer() as analyzer:
        # Get composite scores for each category
        possession = analyzer.calculate_category_composite_scores('possession', timeframe)
        passing = analyzer.calculate_category_composite_scores('passing', timeframe)
        progression = analyzer.calculate_category_composite_scores('ball_progression', timeframe)
        
        # Merge and calculate average
        control = possession[['squad_name', 'composite_score']].rename(columns={'composite_score': 'possession_score'})
        control = control.merge(
            passing[['squad_name', 'composite_score']].rename(columns={'composite_score': 'passing_score'}),
            on='squad_name'
        )
        control = control.merge(
            progression[['squad_name', 'composite_score']].rename(columns={'composite_score': 'progression_score'}),
            on='squad_name'
        )
        
        control['game_control_score'] = (control['possession_score'] + control['passing_score'] + control['progression_score']) / 3
        control = control.sort_values('game_control_score', ascending=False).reset_index(drop=True)
        control['rank'] = range(1, len(control) + 1)
        
        print(f"\n🎮 TOP 10 GAME CONTROL TEAMS")
        print("=" * 90)
        print(f"{'Rank':<5} | {'Squad':<20} | {'Control':<8} | {'Possession':<10} | {'Passing':<8} | {'Progression'}")
        print("-" * 90)
        
        for _, row in control.head(10).iterrows():
            rank = int(row['rank'])
            squad = row['squad_name']
            gc_score = row['game_control_score']
            poss = row['possession_score']
            pass_score = row['passing_score']
            prog = row['progression_score']
            
            if rank == 1:
                emoji = "🥇"
            elif rank == 2:
                emoji = "🥈"
            elif rank == 3:
                emoji = "🥉"
            else:
                emoji = "  "
            
            print(f"{emoji} {rank:<3} | {squad:<20} | {gc_score:<8.1f} | {poss:<10.1f} | {pass_score:<8.1f} | {prog:.1f}")


def print_xg_analysis(timeframe: str = "current"):
    """Print xG over/underperformers"""
    with SquadAnalyzer() as analyzer:
        analyzer.build_master_dataset(timeframe)
        performance = analyzer.analyze_performance_profiles()
        
        if performance['xg_over'] is not None:
            print(f"\n📈 TOP 5 xG OVERPERFORMERS (Clinical Finishing)")
            print("=" * 70)
            print(performance['xg_over'].to_string(index=False))
            
            print(f"\n📉 TOP 5 xG UNDERPERFORMERS (Wasteful Finishing)")
            print("=" * 70)
            print(performance['xg_under'].to_string(index=False))
        else:
            print("❌ No xG data available for this timeframe")


def print_volatility_analysis(timeframe: str = "current"):
    """Print volatility analysis"""
    with SquadAnalyzer() as analyzer:
        analyzer.build_master_dataset(timeframe)
        volatility = analyzer.analyze_volatility()
        
        print(f"\n🌊 VOLATILITY ANALYSIS (Performance vs Table Neighbors)")
        print("=" * 70)
        print("High volatility = Performance doesn't match table position")
        print(volatility['high_volatility'].to_string(index=False))


def print_top6_analysis(timeframe: str = "current"):
    """Print top 6 tactical comparison"""
    with SquadAnalyzer() as analyzer:
        analyzer.build_master_dataset(timeframe)
        top6 = analyzer.analyze_top6_tactical()
        
        if 'error' in top6:
            print(f"❌ {top6['error']}")
            return
        
        print(f"\n⭐ TOP 6 TACTICAL COMPARISON")
        print("=" * 70)
        print("\n📊 CATEGORY SCORES (Overall Percentile):")
        print(top6['comparison'].to_string(index=False))
        
        print(f"\n🎯 TACTICAL STYLES:")
        print(top6['tactical_styles'].to_string(index=False))


def print_positional_context(timeframe: str = "current"):
    """Print positional context analysis"""
    with SquadAnalyzer() as analyzer:
        analyzer.build_master_dataset(timeframe)
        positional = analyzer.analyze_positional_context()
        
        print(f"\n📊 POSITIONAL CONTEXT (±3 Table Positions)")
        print("=" * 70)
        
        print(f"\n⬆️  TEAMS PUNCHING ABOVE THEIR WEIGHT:")
        print(positional['overperformers'].to_string(index=False))
        
        print(f"\n⬇️  TEAMS UNDERPERFORMING RELATIVE TO TABLE POSITION:")
        print(positional['underperformers'].to_string(index=False))


def interactive_menu(timeframe: str = "current"):
    """Interactive menu for squad exploration"""
    with SquadAnalyzer() as analyzer:
        categories = list(analyzer.stat_categories.keys())
        
        while True:
            print(f"\n🔍 SQUAD ANALYTICS INTERACTIVE MENU")
            print("=" * 50)
            print(f"Timeframe: {timeframe}")
            print("\n📋 LEAGUE OVERVIEW:")
            print("  1. League Table")
            print("  2. Top Attacking Teams")
            print("  3. Top Defending Teams")
            print("  4. Game Control Rankings")
            print("  5. xG Analysis")
            
            print("\n⚽ SQUAD ANALYSIS:")
            print("  6. Squad Profile")
            print("  7. Category Breakdown")
            print("  8. Squad Comparison")
            
            print("\n📊 ADVANCED INSIGHTS:")
            print("  9. Top 6 Tactical Comparison")
            print(" 10. Volatility Analysis")
            print(" 11. Positional Context")
            
            print("\n 12. Exit")
            
            try:
                choice = input("\nSelect option (1-12): ").strip()
                
                if choice == "1":
                    print_league_table(timeframe)
                
                elif choice == "2":
                    print_top_attacking(timeframe)
                
                elif choice == "3":
                    print_top_defending(timeframe)
                
                elif choice == "4":
                    print_game_control(timeframe)
                
                elif choice == "5":
                    print_xg_analysis(timeframe)
                
                elif choice == "6":
                    squad_name = input("Enter squad name: ").strip()
                    print_comprehensive_analysis(squad_name, timeframe)
                
                elif choice == "7":
                    squad_name = input("Enter squad name: ").strip()
                    print("\nAvailable categories:")
                    for i, cat in enumerate(categories, 1):
                        print(f"  {i}. {cat}")
                    cat_choice = input("Select category number: ").strip()
                    try:
                        cat_idx = int(cat_choice) - 1
                        if 0 <= cat_idx < len(categories):
                            print_category_breakdown(squad_name, categories[cat_idx], timeframe)
                        else:
                            print("Invalid category number")
                    except ValueError:
                        print("Please enter a valid number")
                
                elif choice == "8":
                    squad1 = input("Enter first squad name: ").strip()
                    squad2 = input("Enter second squad name: ").strip()
                    print_squad_comparison(squad1, squad2, timeframe)
                
                elif choice == "9":
                    print_top6_analysis(timeframe)
                
                elif choice == "10":
                    print_volatility_analysis(timeframe)
                
                elif choice == "11":
                    print_positional_context(timeframe)
                
                elif choice == "12":
                    print("Goodbye!")
                    break
                
                else:
                    print("Invalid choice. Try again.")
                    
            except ValueError:
                print("Please enter a valid number.")
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break


def list_available_squads(timeframe: str = "current"):
    """List available squads"""
    with SquadAnalyzer() as analyzer:
        filter_clause, timeframe_desc = analyzer._parse_timeframe(timeframe)
        
        query = f"""
            SELECT DISTINCT squad_name, matches_played, wins, draws, losses, points
            FROM analytics_squads
            WHERE {filter_clause}
            ORDER BY squad_name
        """
        
        squads = analyzer.conn.execute(query).fetchdf()
        
        print(f"\n📋 AVAILABLE SQUADS ({timeframe_desc})")
        print("=" * 60)
        
        for i, (_, squad) in enumerate(squads.iterrows(), 1):
            print(f"{i:2d}. {squad['squad_name']} - {squad['matches_played']} matches, {squad['points']} points")


def main():
    """Squad Analytics CLI"""
    parser = argparse.ArgumentParser(
        description='Premier League Squad Analytics with Dual Percentiles',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Comprehensive squad analysis
  python3 squad_cli.py --comprehensive "Arsenal" --timeframe current
  
  # Category breakdown
  python3 squad_cli.py --category "attacking_output" --squad "Arsenal"
  
  # Squad comparison
  python3 squad_cli.py --compare "Arsenal" "Liverpool"
  
  # League table
  python3 squad_cli.py --table
  
  # Top attacking teams
  python3 squad_cli.py --top-attacking
  
  # Interactive menu
  python3 squad_cli.py --interactive
  
  # Historical analysis
  python3 squad_cli.py --comprehensive "Arsenal" --timeframe season_2023-2024
        """
    )
    
    # Action arguments
    parser.add_argument('--comprehensive', help='Full comprehensive squad analysis')
    parser.add_argument('--category', help='Specific category breakdown')
    parser.add_argument('--squad', help='Squad name (used with --category)')
    parser.add_argument('--compare', nargs=2, metavar=('SQUAD1', 'SQUAD2'), 
                       help='Compare two squads')
    parser.add_argument('--table', action='store_true', help='Show league table')
    parser.add_argument('--top-attacking', action='store_true', help='Top attacking teams')
    parser.add_argument('--top-defending', action='store_true', help='Top defending teams')
    parser.add_argument('--game-control', action='store_true', help='Top game control teams')
    parser.add_argument('--xg-analysis', action='store_true', help='xG over/underperformers')
    parser.add_argument('--volatility', action='store_true', help='Volatility analysis')
    parser.add_argument('--top6', action='store_true', help='Top 6 tactical comparison')
    parser.add_argument('--positional-context', action='store_true', help='Positional context analysis')
    parser.add_argument('--interactive', action='store_true', help='Interactive menu')
    parser.add_argument('--list', action='store_true', help='List available squads')
    
    # Option arguments
    parser.add_argument('--timeframe', default='current', 
                       help='Analysis timeframe (current, season_YYYY-YYYY, last_N_seasons)')
    
    args = parser.parse_args()
    
    try:
        if args.comprehensive:
            print_comprehensive_analysis(args.comprehensive, args.timeframe)
        
        elif args.category and args.squad:
            print_category_breakdown(args.squad, args.category, args.timeframe)
        
        elif args.category and not args.squad:
            print("❌ --category requires --squad argument")
        
        elif args.compare:
            print_squad_comparison(args.compare[0], args.compare[1], args.timeframe)
        
        elif args.table:
            print_league_table(args.timeframe)
        
        elif args.top_attacking:
            print_top_attacking(args.timeframe)
        
        elif args.top_defending:
            print_top_defending(args.timeframe)
        
        elif args.game_control:
            print_game_control(args.timeframe)
        
        elif args.xg_analysis:
            print_xg_analysis(args.timeframe)
        
        elif args.volatility:
            print_volatility_analysis(args.timeframe)
        
        elif args.top6:
            print_top6_analysis(args.timeframe)
        
        elif args.positional_context:
            print_positional_context(args.timeframe)
        
        elif args.interactive:
            interactive_menu(args.timeframe)
        
        elif args.list:
            list_available_squads(args.timeframe)
        
        else:
            parser.print_help()
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        print(f"💡 Try: python3 squad_cli.py --list to see available squads")


if __name__ == "__main__":
    main()

"""
TEST COMMANDS - Copy/paste these to test the CLI
================================================

# 1. COMPREHENSIVE SQUAD PROFILE
python3 analysis/squad_cli.py --comprehensive "Arsenal"
python3 analysis/squad_cli.py --comprehensive "Manchester Utd"
python3 analysis/squad_cli.py --comprehensive "Liverpool"

# 2. CATEGORY BREAKDOWNS (see detailed metrics)
python3 analysis/squad_cli.py --category "attacking_output" --squad "Arsenal"
python3 analysis/squad_cli.py --category "defending" --squad "Liverpool"
python3 analysis/squad_cli.py --category "passing" --squad "Manchester City"
python3 analysis/squad_cli.py --category "ball_progression" --squad "Arsenal"

# 3. SQUAD COMPARISONS
python3 analysis/squad_cli.py --compare "Arsenal" "Liverpool"
python3 analysis/squad_cli.py --compare "Manchester City" "Chelsea"
python3 analysis/squad_cli.py --compare "Arsenal" "Manchester Utd"

# 4. LEAGUE TABLE & RANKINGS
python3 analysis/squad_cli.py --table
python3 analysis/squad_cli.py --top-attacking
python3 analysis/squad_cli.py --top-defending
python3 analysis/squad_cli.py --game-control
python3 analysis/squad_cli.py --xg-analysis

# 5. ADVANCED INSIGHTS
python3 analysis/squad_cli.py --volatility
python3 analysis/squad_cli.py --top6
python3 analysis/squad_cli.py --positional-context

# 6. INTERACTIVE MODE
python3 analysis/squad_cli.py --interactive

# 7. UTILITIES
python3 analysis/squad_cli.py --list
python3 analysis/squad_cli.py --help

# 8. HISTORICAL ANALYSIS (with --timeframe)
python3 analysis/squad_cli.py --comprehensive "Arsenal" --timeframe season_2023-2024
python3 analysis/squad_cli.py --table --timeframe season_2022-2023
python3 analysis/squad_cli.py --compare "Arsenal" "Liverpool" --timeframe season_2023-2024
python3 analysis/squad_cli.py --top-attacking --timeframe season_2022-2023

# 9. INTERESTING SPECIFIC TESTS
# Why is Man United so wasteful?
python3 analysis/squad_cli.py --category "attacking_output" --squad "Manchester Utd"

# Why is Liverpool's defending weak?
python3 analysis/squad_cli.py --category "defending" --squad "Liverpool"

# Why is Arsenal's ball progression so good?
python3 analysis/squad_cli.py --category "ball_progression" --squad "Arsenal"

# Are Bournemouth really that good? (High volatility)
python3 analysis/squad_cli.py --comprehensive "Bournemouth"
python3 analysis/squad_cli.py --compare "Bournemouth" "Arsenal"

# Is Tottenham's xG overperformance sustainable?
python3 analysis/squad_cli.py --category "attacking_output" --squad "Tottenham"
"""