#!/usr/bin/env python3
"""
Enhanced PlayerAnalyzer with Dual Percentiles
==============================================

Comprehensive player analysis with:
- Dual percentile system (overall + position-specific)
- Cleaned metric categories (removed misleading metrics)
- Position-aware analysis exposed
- Clear timeframe display
- All 154 columns utilized properly
"""

import sys
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Tuple

# Add project paths
project_root = Path(__file__).parent.parent
src_path = project_root / 'src'
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


class PlayerAnalyzer:
    """Enhanced player analysis with dual percentile system and position awareness"""
    
    def __init__(self, db_path: str = "data/premierleague_analytics.duckdb", min_minutes: int = 270):
        self.db_path = db_path
        self.min_minutes = min_minutes
        self.conn = None
        
        # Cleaned stat categories - removed misleading role-specific metrics
        self.stat_categories = {
            'attacking_output': {
                'metrics': [
                    'goals', 'assists', 'expected_goals', 'expected_assisted_goals', 
                    'shots', 'shots_on_target', 'goals_minus_expected', 
                    'non_penalty_goals', 'goals_per_shot', 'goals_per_shot_on_target'
                ],
                'description': 'Direct goal involvement - scoring and assisting'
            },
            
            'creativity': {
                'metrics': [
                    'key_passes', 'shot_creating_actions', 'goal_creating_actions',
                    'expected_assists', 'assists_minus_expected', 'crosses', 
                    'through_balls', 'passes_final_third', 'passes_penalty_area',
                    'crosses_penalty_area', 'sca_pass_live', 'gca_pass_live'
                ],
                'description': 'Chance creation and playmaking ability'
            },
            
            'passing': {
                'metrics': [
                    'passes_completed', 'passes_attempted', 'pass_completion_rate',
                    'progressive_passes', 'total_pass_distance', 'progressive_pass_distance',
                    'short_passes_completed', 'medium_passes_completed', 'long_passes_completed',
                    'short_pass_completion_rate', 'medium_pass_completion_rate', 'long_pass_completion_rate',
                    'live_ball_passes', 'switches'
                ],
                'description': 'Ball distribution and passing accuracy'
            },
            
            'ball_progression': {
                'metrics': [
                    'progressive_carries', 'carries', 'carry_distance', 'progressive_carry_distance',
                    'carries_final_third', 'carries_penalty_area', 'take_ons_attempted',
                    'take_ons_successful', 'take_on_success_rate', 'sca_take_on', 'gca_take_on'
                ],
                'description': 'Moving ball forward through carrying and dribbling'
            },
            
            'defending': {
                'metrics': [
                    'tackles', 'tackles_won', 'interceptions', 'blocks', 'clearances',
                    'tackles_plus_interceptions', 'challenges_attempted', 'tackle_success_rate',
                    'tackles_def_third', 'tackles_mid_third', 'tackles_att_third',
                    'shots_blocked', 'passes_blocked'
                ],
                'description': 'Defensive actions and ball recovery'
            },
            
            'physical_duels': {
                'metrics': [
                    'aerial_duels_won', 'aerial_duels_lost', 'aerial_duel_success_rate',
                    'fouls_drawn', 'ball_recoveries', 'challenges_lost', 'challenge_tackles'
                ],
                'description': 'Physical contests and duels'
            },
            
            'ball_involvement': {
                'metrics': [
                    'touches', 'touches_def_third', 'touches_mid_third', 'touches_att_third',
                    'touches_att_penalty', 'touches_def_penalty', 'touches_live_ball',
                    'passes_received', 'progressive_passes_received_detail'
                ],
                'description': 'Overall ball involvement and positioning'
            },
            
            'discipline_reliability': {
                'metrics': [
                    'yellow_cards', 'red_cards', 'second_yellow_cards', 'fouls_committed',
                    'miscontrols', 'dispossessed', 'errors', 'offsides', 'take_ons_tackled_rate'
                ],
                'description': 'Discipline and ball security (lower is better for most)'
            }
        }
        
        # Position groupings for hybrid position handling
        self.position_groups = {
            'FW': ['FW', 'FW,MF', 'MF,FW', 'FW,DF', 'DF,FW'],
            'MF': ['MF', 'MF,FW', 'FW,MF', 'DF,MF', 'MF,DF'],
            'DF': ['DF', 'DF,MF', 'MF,DF', 'DF,FW']
        }
        
        # Position-specific priorities
        self.position_priorities = {
            'FW': {
                'primary': ['attacking_output', 'creativity'],
                'secondary': ['ball_progression', 'physical_duels'],
                'tertiary': ['passing', 'ball_involvement', 'defending']
            },
            'MF': {
                'primary': ['passing', 'creativity', 'ball_involvement'],
                'secondary': ['ball_progression', 'defending'],
                'tertiary': ['attacking_output', 'physical_duels']
            },
            'DF': {
                'primary': ['defending', 'passing'],
                'secondary': ['physical_duels', 'ball_involvement'],
                'tertiary': ['ball_progression', 'creativity', 'attacking_output']
            }
        }
    
    def connect(self):
        """Connect to analytics database"""
        if not Path(self.db_path).exists():
            raise FileNotFoundError(f"Analytics database not found at {self.db_path}")
        self.conn = duckdb.connect(self.db_path)
        
    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
    
    def _parse_timeframe(self, timeframe: str) -> str:
        """Parse timeframe into SQL filter with clear description"""
        if timeframe == "current":
            return "is_current = true", "Current Season (Latest Gameweek)"
        elif timeframe == "career":
            return f"minutes_played >= {self.min_minutes}", "Career (All Seasons Combined)"
        elif timeframe.startswith("season_"):
            season = timeframe.replace("season_", "")
            return f"season = '{season}' AND minutes_played >= {self.min_minutes}", f"Season {season}"
        elif timeframe.startswith("last_") and timeframe.endswith("_seasons"):
            n_seasons = int(timeframe.split("_")[1])
            recent_seasons = self.conn.execute("""
                SELECT DISTINCT season 
                FROM analytics_players 
                ORDER BY season DESC 
                LIMIT ?
            """, [n_seasons]).fetchall()
            seasons_list = "', '".join([s[0] for s in recent_seasons])
            return f"season IN ('{seasons_list}') AND minutes_played >= {self.min_minutes}", f"Last {n_seasons} Seasons"
        else:
            return "is_current = true", "Current Season (Default)"
    
    def _get_position_group(self, position: str) -> List[str]:
        """Get position group for comparison"""
        for group, positions in self.position_groups.items():
            if position in positions:
                return positions, group
        return [position], position.split(',')[0]
    
    def get_player_basic_info(self, player_name: str, timeframe: str = "current") -> Dict:
        """Get basic player information with timeframe context"""
        filter_clause, timeframe_desc = self._parse_timeframe(timeframe)
        
        query = f"""
            SELECT player_name, position, squad, season, minutes_played, matches_played
            FROM analytics_players
            WHERE player_name = ? AND {filter_clause}
            ORDER BY gameweek DESC
            LIMIT 1
        """
        
        result = self.conn.execute(query, [player_name]).fetchone()
        
        if not result:
            return {"error": f"No data found for {player_name} in timeframe: {timeframe_desc}"}
        
        return {
            'player_name': result[0],
            'position': result[1], 
            'squad': result[2],
            'season': result[3],
            'minutes_played': result[4],
            'matches_played': result[5],
            'timeframe': timeframe,
            'timeframe_description': timeframe_desc
        }
    
    def calculate_dual_percentiles(self, player_name: str, timeframe: str = "current") -> Dict:
        """Calculate both overall and position-specific percentiles"""
        filter_clause, timeframe_desc = self._parse_timeframe(timeframe)
        
        # Get player data
        player_data = self.conn.execute(f"""
            SELECT *
            FROM analytics_players
            WHERE player_name = ? AND {filter_clause}
        """, [player_name]).fetchdf()
        
        if player_data.empty:
            return {"error": f"No data found for {player_name}"}
        
        # Handle multiple records (career mode)
        if len(player_data) > 1:
            # Sum counting stats, average rate stats
            counting_stats = ['goals', 'assists', 'shots', 'passes_completed', 'tackles', 'touches']
            for stat in counting_stats:
                if stat in player_data.columns:
                    player_data.loc[0, stat] = player_data[stat].sum()
            
            rate_stats = ['pass_completion_rate', 'shot_accuracy', 'tackle_success_rate']
            for stat in rate_stats:
                if stat in player_data.columns:
                    player_data.loc[0, stat] = player_data[stat].mean()
            
            player_data = player_data.head(1)
        
        player_record = player_data.iloc[0]
        player_position = player_record['position']
        position_group_list, primary_position = self._get_position_group(player_position)
        
        # Get overall comparison data
        overall_comparison = self.conn.execute(f"""
            SELECT * FROM analytics_players WHERE {filter_clause}
        """).fetchdf()
        
        # Get position-specific comparison data
        position_filter = "', '".join(position_group_list)
        position_comparison = self.conn.execute(f"""
            SELECT * FROM analytics_players 
            WHERE {filter_clause} AND position IN ('{position_filter}')
        """).fetchdf()
        
        # Calculate category scores with dual percentiles
        category_scores = {}
        
        for category_name, category_info in self.stat_categories.items():
            metrics = category_info['metrics']
            metric_breakdown = {}
            overall_percentiles = []
            position_percentiles = []
            
            for metric in metrics:
                if metric in player_data.columns:
                    player_value = player_record[metric]
                    
                    # Overall percentile
                    if metric in overall_comparison.columns:
                        overall_values = overall_comparison[metric].dropna()
                        if len(overall_values) > 0 and pd.notna(player_value):
                            if category_name == 'discipline_reliability':
                                overall_pct = (overall_values > player_value).sum() / len(overall_values) * 100
                            else:
                                overall_pct = (overall_values < player_value).sum() / len(overall_values) * 100
                        else:
                            overall_pct = None
                    else:
                        overall_pct = None
                    
                    # Position-specific percentile
                    if metric in position_comparison.columns:
                        position_values = position_comparison[metric].dropna()
                        if len(position_values) > 0 and pd.notna(player_value):
                            if category_name == 'discipline_reliability':
                                position_pct = (position_values > player_value).sum() / len(position_values) * 100
                            else:
                                position_pct = (position_values < player_value).sum() / len(position_values) * 100
                        else:
                            position_pct = None
                    else:
                        position_pct = None
                    
                    if overall_pct is not None or position_pct is not None:
                        metric_breakdown[metric] = {
                            'value': float(player_value),
                            'overall_percentile': round(overall_pct, 1) if overall_pct is not None else None,
                            'position_percentile': round(position_pct, 1) if position_pct is not None else None,
                            'overall_sample_size': len(overall_values) if overall_pct is not None else 0,
                            'position_sample_size': len(position_values) if position_pct is not None else 0
                        }
                        
                        if overall_pct is not None:
                            overall_percentiles.append(overall_pct)
                        if position_pct is not None:
                            position_percentiles.append(position_pct)
            
            # Calculate category averages
            if overall_percentiles or position_percentiles:
                category_scores[category_name] = {
                    'overall_score': round(np.mean(overall_percentiles), 1) if overall_percentiles else None,
                    'position_score': round(np.mean(position_percentiles), 1) if position_percentiles else None,
                    'metric_breakdown': metric_breakdown,
                    'description': category_info['description'],
                    'metrics_analyzed': len(metric_breakdown),
                    'position_context': f"{primary_position} comparison group"
                }
        
        return {
            'player_info': {
                'name': player_name,
                'position': player_position,
                'primary_position': primary_position,
                'comparison_group': position_group_list
            },
            'category_scores': category_scores,
            'timeframe_info': {
                'timeframe': timeframe,
                'description': timeframe_desc
            }
        }
    
    def get_position_analysis(self, player_name: str, timeframe: str = "current") -> Dict:
        """Get position-specific analysis with expectations"""
        dual_percentiles = self.calculate_dual_percentiles(player_name, timeframe)
        if "error" in dual_percentiles:
            return dual_percentiles
        
        player_info = dual_percentiles['player_info']
        category_scores = dual_percentiles['category_scores']
        primary_position = player_info['primary_position']
        
        if primary_position not in self.position_priorities:
            return {"error": f"Position {primary_position} not in analysis framework"}
        
        priorities = self.position_priorities[primary_position]
        position_analysis = {
            'position_info': player_info,
            'position_expectations': {}
        }
        
        # Analyze by priority level
        for priority_level in ['primary', 'secondary', 'tertiary']:
            priority_categories = priorities[priority_level]
            expectation_analysis = {
                'categories': priority_categories,
                'category_performance': {},
                'overall_assessment': {}
            }
            
            position_scores = []
            for category in priority_categories:
                if category in category_scores:
                    pos_score = category_scores[category]['position_score']
                    overall_score = category_scores[category]['overall_score']
                    
                    if pos_score is not None:
                        position_scores.append(pos_score)
                        expectation_analysis['category_performance'][category] = {
                            'position_percentile': pos_score,
                            'overall_percentile': overall_score,
                            'expectation_met': self._assess_position_expectation(pos_score, priority_level)
                        }
            
            if position_scores:
                avg_position_score = np.mean(position_scores)
                expectation_analysis['overall_assessment'] = {
                    'average_position_percentile': round(avg_position_score, 1),
                    'priority_level': priority_level,
                    'assessment': self._assess_priority_performance(avg_position_score, priority_level)
                }
            
            position_analysis['position_expectations'][priority_level] = expectation_analysis
        
        return position_analysis
    
    def get_category_breakdown(self, player_name: str, category: str, timeframe: str = "current") -> Dict:
        """Get detailed breakdown with dual percentiles"""
        if category not in self.stat_categories:
            return {"error": f"Category '{category}' not found"}
        
        dual_percentiles = self.calculate_dual_percentiles(player_name, timeframe)
        if "error" in dual_percentiles or category not in dual_percentiles['category_scores']:
            return {"error": f"No data for {player_name} in category {category}"}
        
        category_data = dual_percentiles['category_scores'][category]
        player_info = dual_percentiles['player_info']
        timeframe_info = dual_percentiles['timeframe_info']
        
        # Sort metrics by position percentile (more relevant)
        metrics_sorted = sorted(
            category_data['metric_breakdown'].items(),
            key=lambda x: x[1]['position_percentile'] if x[1]['position_percentile'] is not None else x[1]['overall_percentile'] if x[1]['overall_percentile'] is not None else 0,
            reverse=True
        )
        
        breakdown = {
            'player_info': player_info,
            'timeframe_info': timeframe_info,
            'category': category,
            'category_description': category_data['description'],
            'overall_category_score': category_data['overall_score'],
            'position_category_score': category_data['position_score'],
            'metric_details': []
        }
        
        for metric_name, metric_data in metrics_sorted:
            overall_pct = metric_data['overall_percentile']
            position_pct = metric_data['position_percentile']
            value = metric_data['value']
            
            # Use position percentile for interpretation if available
            primary_pct = position_pct if position_pct is not None else overall_pct
            
            if primary_pct is not None:
                if primary_pct >= 90:
                    interpretation = "Elite"
                elif primary_pct >= 75:
                    interpretation = "Very Good"
                elif primary_pct >= 60:
                    interpretation = "Above Average"
                elif primary_pct >= 40:
                    interpretation = "Average"
                elif primary_pct >= 25:
                    interpretation = "Below Average"
                else:
                    interpretation = "Poor"
            else:
                interpretation = "No Data"
            
            breakdown['metric_details'].append({
                'metric': metric_name,
                'value': value,
                'overall_percentile': overall_pct,
                'position_percentile': position_pct,
                'primary_percentile': primary_pct,
                'interpretation': interpretation,
                'overall_sample_size': metric_data['overall_sample_size'],
                'position_sample_size': metric_data['position_sample_size']
            })
        
        return breakdown
    
    def get_comprehensive_player_profile(self, player_name: str, timeframe: str = "current") -> Dict:
        """Get complete comprehensive analysis with dual percentiles"""
        # Get basic info
        basic_info = self.get_player_basic_info(player_name, timeframe)
        if "error" in basic_info:
            return basic_info
        
        # Get dual percentiles
        dual_percentiles = self.calculate_dual_percentiles(player_name, timeframe)
        if "error" in dual_percentiles:
            return dual_percentiles
        
        # Get position analysis
        position_analysis = self.get_position_analysis(player_name, timeframe)
        
        # Generate insights
        strengths = self._identify_strengths(dual_percentiles['category_scores'], use_position=True)
        weaknesses = self._identify_weaknesses(dual_percentiles['category_scores'], use_position=True)
        hidden_talents = self._identify_hidden_talents(dual_percentiles['category_scores'], basic_info['position'])
        versatility = self._calculate_versatility_score(dual_percentiles['category_scores'])
        
        return {
            'basic_info': basic_info,
            'dual_percentiles': dual_percentiles,
            'position_analysis': position_analysis,
            'player_insights': {
                'top_strengths': strengths[:3],
                'main_weaknesses': weaknesses[:3],
                'hidden_talents': hidden_talents,
                'versatility_assessment': versatility
            }
        }
    
    def compare_players_comprehensive(self, player1: str, player2: str, timeframe: str = "current") -> Dict:
        """Enhanced comparison with detailed breakdowns"""
        profile1 = self.get_comprehensive_player_profile(player1, timeframe)
        profile2 = self.get_comprehensive_player_profile(player2, timeframe)
        
        if "error" in profile1:
            return {"error": f"Player 1 ({player1}): {profile1['error']}"}
        if "error" in profile2:
            return {"error": f"Player 2 ({player2}): {profile2['error']}"}
        
        # Extract category scores
        categories1 = profile1['dual_percentiles']['category_scores']
        categories2 = profile2['dual_percentiles']['category_scores']
        
        comparison = {
            'players': [player1, player2],
            'timeframe_info': profile1['basic_info']['timeframe_description'],
            'category_comparison': {},
            'detailed_comparison': {},
            'summary': {}
        }
        
        # Category-by-category comparison
        player1_wins = 0
        player2_wins = 0
        
        for category in self.stat_categories.keys():
            if category in categories1 and category in categories2:
                # Use position percentiles for comparison if available
                score1 = categories1[category]['position_score'] or categories1[category]['overall_score']
                score2 = categories2[category]['position_score'] or categories2[category]['overall_score']
                
                if score1 is not None and score2 is not None:
                    difference = score1 - score2
                    winner = player1 if score1 > score2 else player2 if score2 > score1 else 'tie'
                    
                    if winner == player1:
                        player1_wins += 1
                    elif winner == player2:
                        player2_wins += 1
                    
                    comparison['category_comparison'][category] = {
                        f'{player1}_score': score1,
                        f'{player2}_score': score2,
                        'difference': round(difference, 1),
                        'winner': winner,
                        'category_description': categories1[category]['description']
                    }
        
        comparison['summary'] = {
            'category_wins': {player1: player1_wins, player2: player2_wins},
            'overall_winner': player1 if player1_wins > player2_wins else player2 if player2_wins > player1_wins else 'balanced'
        }
        
        return comparison
    
    # Helper methods
    def _assess_position_expectation(self, score: float, priority_level: str) -> str:
        """Assess if score meets position expectations"""
        if priority_level == 'primary':
            return "exceeds" if score >= 75 else "meets" if score >= 50 else "below"
        elif priority_level == 'secondary':
            return "exceeds" if score >= 65 else "meets" if score >= 40 else "below"
        else:
            return "exceptional" if score >= 70 else "good" if score >= 50 else "standard"
    
    def _assess_priority_performance(self, avg_score: float, priority_level: str) -> str:
        """Overall assessment for priority level"""
        if priority_level == 'primary':
            if avg_score >= 70: return "Excellent - exceeds position requirements"
            elif avg_score >= 50: return "Good - meets position requirements"
            else: return "Concerning - below position expectations"
        elif priority_level == 'secondary':
            if avg_score >= 60: return "Strong supporting skills"
            elif avg_score >= 40: return "Adequate supporting skills"
            else: return "Weak supporting skills"
        else:
            if avg_score >= 65: return "Exceptional versatility"
            elif avg_score >= 45: return "Good versatility"
            else: return "Limited versatility"
    
    def _identify_strengths(self, category_scores: Dict, use_position: bool = True) -> List[Dict]:
        """Identify top performing categories"""
        strengths = []
        for category, data in category_scores.items():
            score = data['position_score'] if use_position and data['position_score'] is not None else data['overall_score']
            if score is not None and score >= 70:
                strengths.append({
                    'category': category,
                    'score': score,
                    'score_type': 'position' if use_position and data['position_score'] is not None else 'overall',
                    'description': data['description']
                })
        return sorted(strengths, key=lambda x: x['score'], reverse=True)
    
    def _identify_weaknesses(self, category_scores: Dict, use_position: bool = True) -> List[Dict]:
        """Identify lowest performing categories"""
        weaknesses = []
        for category, data in category_scores.items():
            score = data['position_score'] if use_position and data['position_score'] is not None else data['overall_score']
            if score is not None and score <= 30:
                weaknesses.append({
                    'category': category,
                    'score': score,
                    'score_type': 'position' if use_position and data['position_score'] is not None else 'overall',
                    'description': data['description']
                })
        return sorted(weaknesses, key=lambda x: x['score'])
    
    def _identify_hidden_talents(self, category_scores: Dict, position: str) -> List[Dict]:
        """Identify unexpected strengths for position"""
        primary_position = position.split(',')[0]
        if primary_position not in self.position_priorities:
            return []
        
        tertiary_categories = self.position_priorities[primary_position]['tertiary']
        hidden_talents = []
        
        for category in tertiary_categories:
            if category in category_scores:
                score = category_scores[category]['position_score'] or category_scores[category]['overall_score']
                if score is not None and score >= 70:
                    hidden_talents.append({
                        'category': category,
                        'score': score,
                        'description': category_scores[category]['description'],
                        'insight': f"Exceptional {category.replace('_', ' ')} for a {primary_position}"
                    })
        
        return sorted(hidden_talents, key=lambda x: x['score'], reverse=True)
    
    def _calculate_versatility_score(self, category_scores: Dict) -> Dict:
        """Calculate versatility using position-aware scoring"""
        position_scores = []
        overall_scores = []
        
        for data in category_scores.values():
            if data['position_score'] is not None:
                position_scores.append(data['position_score'])
            if data['overall_score'] is not None:
                overall_scores.append(data['overall_score'])
        
        # Use position scores if available, otherwise overall
        primary_scores = position_scores if position_scores else overall_scores
        
        if not primary_scores:
            return {"assessment": "No data available"}
        
        avg_score = np.mean(primary_scores)
        std_score = np.std(primary_scores)
        
        # Versatility = high average with low standard deviation
        versatility = avg_score - (std_score * 0.5)
        
        if versatility >= 70:
            assessment = "Highly versatile - strong across multiple areas"
        elif versatility >= 50:
            assessment = "Moderately versatile - balanced player"
        elif versatility >= 30:
            assessment = "Specialist - strong in specific areas"
        else:
            assessment = "Limited - needs development in multiple areas"
        
        return {
            "versatility_score": round(versatility, 1),
            "average_score": round(avg_score, 1),
            "consistency": round(100 - std_score, 1),
            "assessment": assessment,
            "score_type": "position-aware" if position_scores else "overall"
        }


# Test function
def test_enhanced_analyzer():
    """Test the enhanced analyzer with dual percentiles"""
    with PlayerAnalyzer() as analyzer:
        
        print("=== ENHANCED PLAYER ANALYZER WITH DUAL PERCENTILES ===")
        
        # Test comprehensive profile
        print("\n1. Comprehensive Profile with Position Awareness:")
        profile = analyzer.get_comprehensive_player_profile("Mohamed Salah", "current")
        
        if "error" not in profile:
            basic = profile['basic_info']
            print(f"Player: {basic['player_name']} ({basic['position']}) - {basic['timeframe_description']}")
            
            categories = profile['dual_percentiles']['category_scores']
            print(f"\nDual Percentile Category Scores:")
            for category, data in categories.items():
                overall = data['overall_score']
                position = data['position_score'] 
                print(f"  {category}: Overall {overall}% | Position {position}% ({data['metrics_analyzed']} metrics)")
        
        # Test detailed breakdown
        print("\n2. Detailed Category Breakdown:")
        breakdown = analyzer.get_category_breakdown("Mohamed Salah", "attacking_output", "current")
        
        if "error" not in breakdown:
            print(f"Category: {breakdown['category']} - Overall: {breakdown['overall_category_score']}% | Position: {breakdown['position_category_score']}%")
            print(f"Top 3 metrics:")
            for i, metric in enumerate(breakdown['metric_details'][:3], 1):
                
                overall_pct = metric['overall_percentile']
                position_pct = metric['position_percentile']
                print(f"  {i}. {metric['metric']}: Overall {overall_pct}% | Position {position_pct}% | Value: {metric['value']}")


if __name__ == "__main__":
    test_enhanced_analyzer()