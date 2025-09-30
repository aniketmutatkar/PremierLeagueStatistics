#!/usr/bin/env python3
"""
Enhanced CLI with Dual Percentiles Support
==========================================

CLI interface for the enhanced PlayerAnalyzer with:
- Dual percentile display (overall + position-specific)
- Clear timeframe indication
- Detailed comparison breakdowns
- Position-aware analysis
"""

import argparse
from pathlib import Path
import sys

# Add project paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'analysis'))

from player_analyzer import PlayerAnalyzer


def print_comprehensive_analysis(player_name: str, timeframe: str = "current"):
    """Print comprehensive analysis with dual percentiles"""
    with PlayerAnalyzer() as analyzer:
        profile = analyzer.get_comprehensive_player_profile(player_name, timeframe)
        
        if "error" in profile:
            print(f"❌ {profile['error']}")
            return
        
        basic = profile['basic_info']
        dual_percentiles = profile['dual_percentiles']
        
        print(f"\n👤 {basic['player_name'].upper()} - COMPREHENSIVE ANALYSIS")
        print("=" * 70)
        print(f"Position: {basic['position']} | Squad: {basic['squad']}")
        print(f"Timeframe: {basic['timeframe_description']}")
        print(f"Minutes: {basic['minutes_played']} | Matches: {basic['matches_played']}")
        
        print(f"\n📊 DUAL PERCENTILE CATEGORY SCORES:")
        print("-" * 70)
        print(f"{'Category':<20} | {'Overall':<8} | {'Position':<8} | {'Level'}")
        print("-" * 70)
        
        # Sort by position score (more relevant), fallback to overall
        categories_sorted = sorted(
            dual_percentiles['category_scores'].items(),
            key=lambda x: x[1]['position_score'] if x[1]['position_score'] is not None else x[1]['overall_score'] if x[1]['overall_score'] is not None else 0,
            reverse=True
        )
        
        for category, data in categories_sorted:
            overall_score = data['overall_score']
            position_score = data['position_score']
            metrics_count = data['metrics_analyzed']
            
            # Use position score for level assessment if available
            primary_score = position_score if position_score is not None else overall_score
            
            if primary_score is not None:
                if primary_score >= 80:
                    level = "🔥 Elite"
                elif primary_score >= 70:
                    level = "⭐ Strong"
                elif primary_score >= 50:
                    level = "✅ Average"
                elif primary_score >= 30:
                    level = "⚠️ Weak"
                else:
                    level = "❌ Poor"
            else:
                level = "❓ No Data"
            
            overall_str = f"{overall_score:.1f}%" if overall_score is not None else "N/A"
            position_str = f"{position_score:.1f}%" if position_score is not None else "N/A"
            
            print(f"{category:<20} | {overall_str:<8} | {position_str:<8} | {level} ({metrics_count})")
        
        # Show insights
        insights = profile['player_insights']
        print(f"\n🎯 KEY INSIGHTS:")
        
        versatility = insights['versatility_assessment']
        print(f"  • Versatility: {versatility['assessment']} (Score: {versatility.get('versatility_score', 'N/A')})")
        
        if insights['top_strengths']:
            strengths = [f"{s['category']} ({s['score']:.1f}%)" for s in insights['top_strengths'][:3]]
            print(f"  • Top Strengths: {', '.join(strengths)}")
        
        if insights['main_weaknesses']:
            weaknesses = [f"{w['category']} ({w['score']:.1f}%)" for w in insights['main_weaknesses'][:3]]
            print(f"  • Main Weaknesses: {', '.join(weaknesses)}")
        
        if insights['hidden_talents']:
            talents = [t['category'] for t in insights['hidden_talents'][:2]]
            print(f"  • Hidden Talents: {', '.join(talents)}")


def print_category_breakdown(player_name: str, category: str, timeframe: str = "current"):
    """Print detailed category breakdown with dual percentiles"""
    with PlayerAnalyzer() as analyzer:
        breakdown = analyzer.get_category_breakdown(player_name, category, timeframe)
        
        if "error" in breakdown:
            print(f"❌ {breakdown['error']}")
            return
        
        player_info = breakdown['player_info']
        timeframe_info = breakdown['timeframe_info']
        
        print(f"\n🔍 {player_info['name'].upper()} - {breakdown['category'].upper()} BREAKDOWN")
        print("=" * 80)
        print(f"📊 Category Scores: Overall {breakdown['overall_category_score']}% | Position {breakdown['position_category_score']}%")
        print(f"📝 Description: {breakdown['category_description']}")
        print(f"⏰ Timeframe: {timeframe_info['description']}")
        print(f"🎯 Position Context: {player_info['primary_position']} among {player_info['comparison_group']}")
        
        print(f"\n📈 INDIVIDUAL METRICS ({len(breakdown['metric_details'])} total):")
        print("-" * 80)
        print(f"{'Rank':<4} | {'Metric':<25} | {'Overall':<7} | {'Position':<7} | {'Level':<12} | {'Value'}")
        print("-" * 80)
        
        for i, metric in enumerate(breakdown['metric_details'], 1):
            overall_pct = metric['overall_percentile']
            position_pct = metric['position_percentile']
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
            
            overall_str = f"{overall_pct:.1f}%" if overall_pct is not None else "N/A"
            position_str = f"{position_pct:.1f}%" if position_pct is not None else "N/A"
            
            print(f"{i:<4} | {metric['metric']:<25} | {overall_str:<7} | {position_str:<7} | {emoji} {interpretation:<10} | {value}")


def print_position_analysis(player_name: str, timeframe: str = "current"):
    """Print position-specific analysis"""
    with PlayerAnalyzer() as analyzer:
        position_analysis = analyzer.get_position_analysis(player_name, timeframe)
        
        if "error" in position_analysis:
            print(f"❌ {position_analysis['error']}")
            return
        
        player_info = position_analysis['position_info']
        expectations = position_analysis['position_expectations']
        
        print(f"\n🎯 {player_info['name'].upper()} - POSITION ANALYSIS")
        print("=" * 60)
        print(f"Position: {player_info['position']} (Primary: {player_info['primary_position']})")
        print(f"Comparison Group: {', '.join(player_info['comparison_group'])}")
        
        for priority_level in ['primary', 'secondary', 'tertiary']:
            if priority_level in expectations:
                expectation_data = expectations[priority_level]
                
                print(f"\n📊 {priority_level.upper()} EXPECTATIONS:")
                print(f"Categories: {', '.join(expectation_data['categories'])}")
                
                if 'overall_assessment' in expectation_data:
                    assessment = expectation_data['overall_assessment']
                    print(f"Average Position Percentile: {assessment['average_position_percentile']}%")
                    print(f"Assessment: {assessment['assessment']}")
                
                # Show individual category performance
                for category, performance in expectation_data['category_performance'].items():
                    pos_pct = performance['position_percentile']
                    expectation_met = performance['expectation_met']
                    print(f"  • {category}: {pos_pct:.1f}% ({expectation_met} expectations)")


def print_detailed_comparison(player1: str, player2: str, timeframe: str = "current"):
    """Print comprehensive comparison with detailed breakdowns"""
    with PlayerAnalyzer() as analyzer:
        comparison = analyzer.compare_players_comprehensive(player1, player2, timeframe)
        
        if "error" in comparison:
            print(f"❌ {comparison['error']}")
            return
        
        print(f"\n⚔️ {player1.upper()} vs {player2.upper()}")
        print("=" * 70)
        print(f"Timeframe: {comparison['timeframe_info']}")
        
        print(f"\n📊 CATEGORY-BY-CATEGORY COMPARISON:")
        print("-" * 50)
        
        for category, comp in comparison['category_comparison'].items():
            winner = comp['winner']
            score1 = comp[f'{player1}_score']
            score2 = comp[f'{player2}_score']
            diff = abs(comp['difference'])
            
            if winner == player1:
                print(f"🥇 {category:<20} | {player1} wins by {diff:.1f}% ({score1:.1f}% vs {score2:.1f}%)")
            elif winner == player2:
                print(f"🥈 {category:<20} | {player2} wins by {diff:.1f}% ({score2:.1f}% vs {score1:.1f}%)")
            else:
                print(f"🤝 {category:<20} | Tie ({score1:.1f}% vs {score2:.1f}%)")
        
        summary = comparison['summary']
        print(f"\n🏆 OVERALL COMPARISON:")
        print(f"Category Wins: {summary['category_wins']}")
        print(f"Overall Winner: {summary['overall_winner']}")


def interactive_category_explorer(player_name: str, timeframe: str = "current"):
    """Interactive exploration with enhanced features"""
    with PlayerAnalyzer() as analyzer:
        categories = list(analyzer.stat_categories.keys())
        
        print(f"\n🔍 INTERACTIVE EXPLORER for {player_name} ({timeframe})")
        print("=" * 50)
        
        for i, category in enumerate(categories, 1):
            print(f"{i}. {category}")
        
        print(f"{len(categories) + 1}. Comprehensive analysis")
        print(f"{len(categories) + 2}. Position analysis")
        print(f"{len(categories) + 3}. Exit")
        
        while True:
            try:
                choice = input(f"\nSelect option (1-{len(categories) + 3}): ").strip()
                
                if not choice or choice == str(len(categories) + 3):
                    print("Goodbye!")
                    break
                
                choice_num = int(choice)
                
                if choice_num == len(categories) + 1:
                    print_comprehensive_analysis(player_name, timeframe)
                elif choice_num == len(categories) + 2:
                    print_position_analysis(player_name, timeframe)
                elif 1 <= choice_num <= len(categories):
                    selected_category = categories[choice_num - 1]
                    print_category_breakdown(player_name, selected_category, timeframe)
                else:
                    print("Invalid choice. Try again.")
                    
            except ValueError:
                print("Please enter a valid number.")
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break


def list_available_players(timeframe: str = "current", limit: int = 20):
    """List available players with clear timeframe"""
    with PlayerAnalyzer() as analyzer:
        filter_clause, timeframe_desc = analyzer._parse_timeframe(timeframe)
        
        query = f"""
            SELECT DISTINCT player_name, squad, position, minutes_played
            FROM analytics_players
            WHERE {filter_clause}
            ORDER BY minutes_played DESC
            LIMIT {limit}
        """
        
        players = analyzer.conn.execute(query).fetchdf()
        
        print(f"\n📋 TOP {limit} PLAYERS ({timeframe_desc})")
        print("=" * 60)
        
        for i, (_, player) in enumerate(players.iterrows(), 1):
            print(f"{i:2d}. {player['player_name']} ({player['squad']}) - {player['position']} - {player['minutes_played']}min")


def main():
    """Enhanced CLI with dual percentiles and position awareness"""
    parser = argparse.ArgumentParser(
        description='Enhanced Premier League Player Analytics with Dual Percentiles',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Comprehensive analysis with dual percentiles
  python3 cli.py --comprehensive "Mohamed Salah" --timeframe current
  
  # Category breakdown with position context
  python3 cli.py --category "attacking_output" --player "Mohamed Salah"
  
  # Position-specific analysis
  python3 cli.py --position-analysis "Mohamed Salah"
  
  # Detailed comparison
  python3 cli.py --compare "Mohamed Salah" "Erling Haaland"
  
  # Interactive exploration
  python3 cli.py --interactive "Mohamed Salah"
  
  # Career analysis
  python3 cli.py --comprehensive "James Milner" --timeframe career
        """
    )
    
    # Action arguments
    parser.add_argument('--comprehensive', help='Full comprehensive analysis with dual percentiles')
    parser.add_argument('--category', help='Specific category breakdown')
    parser.add_argument('--player', help='Player name (used with --category)')
    parser.add_argument('--position-analysis', help='Position-specific analysis for player')
    parser.add_argument('--interactive', help='Interactive category explorer')
    parser.add_argument('--compare', nargs=2, metavar=('PLAYER1', 'PLAYER2'), 
                       help='Detailed comparison between two players')
    parser.add_argument('--list', action='store_true', help='List available players')
    parser.add_argument('--debug', help='Debug player data issues')
    
    # Option arguments
    parser.add_argument('--timeframe', default='current', 
                       help='Analysis timeframe (current, career, season_YYYY-YYYY, last_N_seasons)')
    parser.add_argument('--limit', type=int, default=20, 
                       help='Limit for player list')
    
    args = parser.parse_args()
    
    try:
        if args.comprehensive:
            print_comprehensive_analysis(args.comprehensive, args.timeframe)
        
        elif args.category and args.player:
            print_category_breakdown(args.player, args.category, args.timeframe)
        
        elif args.category and not args.player:
            print("❌ --category requires --player argument")
        
        elif args.position_analysis:
            print_position_analysis(args.position_analysis, args.timeframe)
        
        elif args.interactive:
            interactive_category_explorer(args.interactive, args.timeframe)
        
        elif args.compare:
            print_detailed_comparison(args.compare[0], args.compare[1], args.timeframe)
        
        elif args.list:
            list_available_players(args.timeframe, args.limit)
        
        elif args.debug:
            # Simple debug function
            with PlayerAnalyzer() as analyzer:
                basic_info = analyzer.get_player_basic_info(args.debug, args.timeframe)
                print(f"Debug info for {args.debug}:")
                print(basic_info)
        
        else:
            parser.print_help()
    
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"💡 Try: python3 cli.py --list to see available players")


if __name__ == "__main__":
    main()