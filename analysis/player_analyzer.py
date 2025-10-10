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
import unicodedata
from difflib import SequenceMatcher

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
        
        # Metric polarity classification - defines which metrics are inverted (lower = better)
        self.NEGATIVE_METRICS = {
            # Discipline
            'yellow_cards', 'red_cards', 'second_yellow_cards', 'fouls_committed',
            
            # Errors
            'errors', 'miscontrols', 'dispossessed', 'own_goals',
            
            # Lost duels/challenges
            'challenges_lost', 'take_ons_tackled', 'take_ons_tackled_rate',
            'aerial_duels_lost',
            
            # Offsides
            'offsides', 'offsides_pass_types',
            
            # Conceded
            'penalty_kicks_conceded',
            
            # Blocked (when YOUR passes are blocked)
            'blocked_passes',

            # Goalkeeper stats
            'goals_against', 'goals_against_per_90', 'shots_on_target_against',
            'penalty_goals_against', 'free_kick_goals_against',
            'corner_kick_goals_against', 'crosses_faced', 'losses'
        }

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

        self.goalkeeper_categories = {
            'shot_stopping': {
                'metrics': [
                    'saves', 'save_percentage', 'shots_on_target_against',
                    'post_shot_expected_goals', 'post_shot_xg_per_shot',
                    'post_shot_xg_performance', 'post_shot_xg_performance_per_90',
                    'goals_against', 'goals_against_per_90'
                ],
                'description': 'Shot stopping ability and saves'
            },
            
            'distribution': {
                'metrics': [
                    'goalkeeper_long_passes_completed', 'goalkeeper_long_passes_attempted',
                    'goalkeeper_long_pass_accuracy', 'goalkeeper_pass_attempts',
                    'launch_percentage', 'average_pass_length',
                    'goal_kicks_attempted', 'goal_kick_launch_percentage',
                    'goal_kick_average_length'
                ],
                'description': 'Passing and distribution from the back'
            },
            
            'sweeping': {
                'metrics': [
                    'defensive_actions_outside_penalty_area',
                    'defensive_actions_outside_penalty_area_per_90',
                    'average_distance_defensive_actions'
                ],
                'description': 'Sweeper keeper actions outside the penalty area'
            },
            
            'penalty_saving': {
                'metrics': [
                    'penalty_kicks_saved', 'penalty_save_percentage',
                    'penalty_kicks_against', 'penalty_kicks_attempted_against',
                    'penalty_goals_against'
                ],
                'description': 'Penalty kick saving performance'
            },
            
            'cross_claiming': {
                'metrics': [
                    'crosses_stopped', 'cross_stop_percentage', 'crosses_faced'
                ],
                'description': 'Ability to claim crosses and deal with aerial balls'
            },
            
            'clean_sheets': {
                'metrics': [
                    'clean_sheets', 'clean_sheet_percentage', 'wins', 'draws', 'losses'
                ],
                'description': 'Clean sheets and match results'
            },
            
            'goals_prevented': {
                'metrics': [
                    'post_shot_xg_performance', 'post_shot_xg_performance_per_90',
                    'goals_against', 'post_shot_expected_goals'
                ],
                'description': 'Goals prevented above expectation (PSxG - GA)'
            },
            
            'ball_playing': {
                'metrics': [
                    'throws', 'goalkeeper_pass_attempts', 'passes_completed',
                    'pass_completion_rate', 'progressive_passes'
                ],
                'description': 'Ball playing and progressive passing from keeper'
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
        
        # Try analytics_players first
        query = f"""
            SELECT player_name, position, squad, season, minutes_played, matches_played
            FROM analytics_players
            WHERE player_name = ? AND {filter_clause}
            ORDER BY gameweek DESC
            LIMIT 1
        """
        
        result = self.conn.execute(query, [player_name]).fetchone()
        
        # If not found, try analytics_keepers
        if not result:
            query = f"""
                SELECT player_name, position, squad, season, minutes_played, matches_played
                FROM analytics_keepers
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
        """
        Calculate both overall and position-specific percentiles.
        Keeps goalkeepers and outfield players completely separate.
        """
        filter_clause, timeframe_desc = self._parse_timeframe(timeframe)
        
        # Step 1: Determine if this player is a goalkeeper
        player_in_keepers = self.conn.execute(f"""
            SELECT COUNT(*) FROM analytics_keepers 
            WHERE player_name = ? AND {filter_clause}
        """, [player_name]).fetchone()[0]

        is_goalkeeper = player_in_keepers > 0
        
        # Step 2: Get player data from correct table
        if is_goalkeeper:
            player_table = "analytics_keepers"
            categories_to_use = self.goalkeeper_categories
        else:
            player_table = "analytics_players"
            categories_to_use = self.stat_categories
        
        player_data = self.conn.execute(f"""
            SELECT * FROM {player_table}
            WHERE player_name = ? AND {filter_clause}
        """, [player_name]).fetchdf()

        if player_data.empty:
            return {"error": f"No data found for {player_name}"}
        
        # Apply per-90 normalization
        player_data = self._normalize_to_per_90(player_data)
        
        # Handle multiple records (career mode)
        if len(player_data) > 1:
            player_data = player_data.head(1)
        
        player_record = player_data.iloc[0]
        player_position = player_record['position']
        position_group_list, primary_position = self._get_position_group(player_position)
        
        # Step 3: Get comparison data - SEPARATE for GK vs Outfield
        if is_goalkeeper:
            # For goalkeepers: compare only to other goalkeepers
            overall_comparison = self.conn.execute(f"""
                SELECT * FROM analytics_keepers WHERE {filter_clause}
            """).fetchdf()
            
            position_comparison = overall_comparison.copy()  # Same thing for GKs
            
        else:
            # For outfield players: compare only to other outfield players
            overall_comparison = self.conn.execute(f"""
                SELECT * FROM analytics_players WHERE {filter_clause}
            """).fetchdf()
            
            # Position-specific comparison
            position_filter = "', '".join(position_group_list)
            position_comparison = self.conn.execute(f"""
                SELECT * FROM analytics_players 
                WHERE {filter_clause} AND position IN ('{position_filter}')
            """).fetchdf()
        
        # Normalize comparison data
        overall_comparison = self._normalize_to_per_90(overall_comparison)
        position_comparison = self._normalize_to_per_90(position_comparison)
        
        # Step 4: Calculate category scores using appropriate categories
        category_scores = {}
        
        for category_name, category_info in categories_to_use.items():
            metrics = category_info['metrics']
            metric_breakdown = {}
            overall_percentiles = []
            position_percentiles = []
            
            for metric in metrics:
                # Get the metric value (handles per-90 conversion if needed)
                metric_to_use, player_value = self._get_metric_for_percentile(metric, player_data)
                
                if player_value is None or pd.isna(player_value):
                    continue
                
                # Calculate overall percentile
                overall_pct = None
                if metric_to_use in overall_comparison.columns:
                    overall_values = overall_comparison[metric_to_use].dropna()
                    if len(overall_values) > 0:
                        overall_pct = self._calculate_percentile_with_polarity(
                            metric, player_value, overall_values
                        )
                
                # Calculate position percentile
                position_pct = None
                if metric_to_use in position_comparison.columns:
                    position_values = position_comparison[metric_to_use].dropna()
                    if len(position_values) > 0:
                        position_pct = self._calculate_percentile_with_polarity(
                            metric, player_value, position_values
                        )
                
                if overall_pct is not None or position_pct is not None:
                    metric_breakdown[metric] = {
                        'value': float(player_value),
                        'metric_used': metric_to_use,
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
                'comparison_group': position_group_list,
                'is_goalkeeper': is_goalkeeper
            },
            'category_scores': category_scores,
            'timeframe_info': {
                'timeframe': timeframe,
                'description': timeframe_desc
            }
        }

    def _calculate_percentile_with_polarity(self, metric: str, player_value: float, 
                                        comparison_values: pd.Series) -> Optional[float]:
        """
        Calculate percentile with proper polarity handling.
        
        Args:
            metric: Name of the metric
            player_value: Player's value for this metric
            comparison_values: Series of comparison values (already filtered/dropna)
            
        Returns:
            Percentile (0-100) or None if cannot calculate
        """
        if len(comparison_values) == 0 or pd.isna(player_value):
            return None
        
        # Negative metrics: lower is better, so invert the comparison
        if metric in self.NEGATIVE_METRICS:
            # Player with FEWER errors/cards/etc should have HIGHER percentile
            percentile = (comparison_values > player_value).sum() / len(comparison_values) * 100
        else:
            # Positive metrics: higher is better (standard calculation)
            percentile = (comparison_values < player_value).sum() / len(comparison_values) * 100
        
        return percentile

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
        """Enhanced comparison with detailed breakdowns - blocks GK vs outfield"""
        profile1 = self.get_comprehensive_player_profile(player1, timeframe)
        profile2 = self.get_comprehensive_player_profile(player2, timeframe)
        
        if "error" in profile1:
            return {"error": f"Player 1 ({player1}): {profile1['error']}"}
        if "error" in profile2:
            return {"error": f"Player 2 ({player2}): {profile2['error']}"}
        
        # Check if cross-position comparison (GK vs outfield)
        player1_is_gk = profile1['dual_percentiles']['player_info'].get('is_goalkeeper', False)
        player2_is_gk = profile2['dual_percentiles']['player_info'].get('is_goalkeeper', False)
        
        if player1_is_gk != player2_is_gk:
            return {
                "error": "Cannot compare goalkeepers to outfield players. Please select two goalkeepers or two outfield players."
            }
        
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
        
        # Determine which categories to use
        if player1_is_gk:
            categories_to_compare = self.goalkeeper_categories.keys()
        else:
            categories_to_compare = self.stat_categories.keys()
        
        # Category-by-category comparison using OVERALL percentiles
        player1_wins = 0
        player2_wins = 0
        
        for category in categories_to_compare:
            if category in categories1 and category in categories2:
                # Use OVERALL percentiles for objective comparison
                score1 = categories1[category]['overall_score']
                if score1 is None:
                    score1 = categories1[category]['position_score']
                
                score2 = categories2[category]['overall_score']
                if score2 is None:
                    score2 = categories2[category]['position_score']
                
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

    # ============================================================================
    # DETAILED COMPARISON METHODS
    # ============================================================================

    def compare_players_detailed(self, player1: str, player2: str, timeframe: str = "current") -> Dict:
        """
        Detailed player comparison with metric-by-metric breakdown
        
        Add this method to your PlayerAnalyzer class
        """
        # Get comprehensive profiles
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
            'category_summary': {},
            'detailed_breakdown': {},
            'summary': {
                'category_wins': {player1: 0, player2: 0},
                'metric_wins': {player1: 0, player2: 0}
            }
        }
        
        # Category-by-category with metric details
        for category in self.stat_categories.keys():
            if category in categories1 and category in categories2:
                cat_data1 = categories1[category]
                cat_data2 = categories2[category]
                
                # Category-level comparison
                score1 = cat_data1['position_score'] or cat_data1['overall_score']
                score2 = cat_data2['position_score'] or cat_data2['overall_score']
                
                if score1 is not None and score2 is not None:
                    cat_winner = player1 if score1 > score2 else player2 if score2 > score1 else 'tie'
                    if cat_winner == player1:
                        comparison['summary']['category_wins'][player1] += 1
                    elif cat_winner == player2:
                        comparison['summary']['category_wins'][player2] += 1
                    
                    comparison['category_summary'][category] = {
                        f'{player1}_score': score1,
                        f'{player2}_score': score2,
                        'difference': round(score1 - score2, 1),
                        'winner': cat_winner
                    }
                    
                    # Metric-level breakdown within this category
                    metric_breakdown = {}
                    metrics1 = cat_data1['metric_breakdown']
                    metrics2 = cat_data2['metric_breakdown']
                    
                    # Get common metrics
                    common_metrics = set(metrics1.keys()) & set(metrics2.keys())
                    
                    for metric in common_metrics:
                        m1_data = metrics1[metric]
                        m2_data = metrics2[metric]
                        
                        # Use position percentile if available
                        pct1 = m1_data['position_percentile'] if m1_data['position_percentile'] is not None else m1_data['overall_percentile']
                        pct2 = m2_data['position_percentile'] if m2_data['position_percentile'] is not None else m2_data['overall_percentile']
                        
                        if pct1 is not None and pct2 is not None:
                            metric_winner = player1 if pct1 > pct2 else player2 if pct2 > pct1 else 'tie'
                            if metric_winner == player1:
                                comparison['summary']['metric_wins'][player1] += 1
                            elif metric_winner == player2:
                                comparison['summary']['metric_wins'][player2] += 1
                            
                            metric_breakdown[metric] = {
                                f'{player1}_percentile': pct1,
                                f'{player2}_percentile': pct2,
                                f'{player1}_value': m1_data['value'],
                                f'{player2}_value': m2_data['value'],
                                'percentile_difference': round(pct1 - pct2, 1),
                                'winner': metric_winner
                            }
                    
                    comparison['detailed_breakdown'][category] = metric_breakdown
        
        # Overall winner determination
        cat_diff = comparison['summary']['category_wins'][player1] - comparison['summary']['category_wins'][player2]
        if cat_diff > 0:
            comparison['summary']['overall_winner'] = player1
        elif cat_diff < 0:
            comparison['summary']['overall_winner'] = player2
        else:
            # Tie on categories, use metric wins
            metric_diff = comparison['summary']['metric_wins'][player1] - comparison['summary']['metric_wins'][player2]
            comparison['summary']['overall_winner'] = player1 if metric_diff > 0 else player2 if metric_diff < 0 else 'balanced'
        
        return comparison


    # ============================================================================
    # SIMILAR PLAYER FINDER METHODS
    # ============================================================================

    def find_similar_players(self, player_name: str, timeframe: str = "current", 
                        top_n: int = 5, same_position_only: bool = True) -> Dict:
        """
        Find statistically similar players with improved discrimination.
        Uses weighted Euclidean distance with variance-based weights.
        """
        # Get target player's profile
        target_profile = self.calculate_dual_percentiles(player_name, timeframe)
        if "error" in target_profile:
            return target_profile
        
        target_categories = target_profile['category_scores']
        target_position = target_profile['player_info']['primary_position']
        
        # Create vector of category scores (using position scores)
        target_vector = []
        category_names = []
        for category, data in target_categories.items():
            score = data['position_score'] if data['position_score'] is not None else data['overall_score']
            if score is not None:
                target_vector.append(score)
                category_names.append(category)
        
        if not target_vector:
            return {"error": "No valid category scores for target player"}
        
        target_vector = np.array(target_vector)
        
        # Get all players for comparison
        filter_clause, timeframe_desc = self._parse_timeframe(timeframe)
        
        if same_position_only:
            position_group, _ = self._get_position_group(target_position)
            position_filter = "', '".join(position_group)
            
            # Check if target is a goalkeeper
            if 'GK' in target_position:
                player_query = f"""
                    SELECT DISTINCT player_name, position
                    FROM analytics_keepers
                    WHERE {filter_clause}
                """
            else:
                player_query = f"""
                    SELECT DISTINCT player_name, position
                    FROM analytics_players
                    WHERE {filter_clause} AND position IN ('{position_filter}')
                """
        else:
            # For cross-position search, union both tables
            player_query = f"""
                SELECT DISTINCT player_name, position FROM analytics_players WHERE {filter_clause}
                UNION ALL
                SELECT DISTINCT player_name, position FROM analytics_keepers WHERE {filter_clause}
            """
        
        potential_players = self.conn.execute(player_query).fetchdf()
        
        # Calculate variance across all players for weighting
        all_vectors = []
        for _, row in potential_players.iterrows():
            candidate_profile = self.calculate_dual_percentiles(row['player_name'], timeframe)
            if "error" not in candidate_profile:
                candidate_vector = []
                for category in category_names:
                    if category in candidate_profile['category_scores']:
                        data = candidate_profile['category_scores'][category]
                        score = data['position_score'] if data['position_score'] is not None else data['overall_score']
                        candidate_vector.append(score if score is not None else 0)
                    else:
                        candidate_vector.append(0)
                all_vectors.append(candidate_vector)
        
        # Calculate variance for each category (higher variance = more discriminating)
        if len(all_vectors) > 1:
            all_vectors_array = np.array(all_vectors)
            category_variances = np.var(all_vectors_array, axis=0)
            # Normalize variances to create weights (higher variance = higher weight)
            weights = category_variances / np.sum(category_variances)
        else:
            weights = np.ones(len(category_names)) / len(category_names)
        
        # Calculate weighted similarity for each player
        similarities = []
        
        for _, row in potential_players.iterrows():
            candidate_name = row['player_name']
            
            # Skip target player
            if candidate_name == player_name:
                continue
            
            # Get candidate profile
            candidate_profile = self.calculate_dual_percentiles(candidate_name, timeframe)
            if "error" in candidate_profile:
                continue
            
            candidate_categories = candidate_profile['category_scores']
            
            # Create vector
            candidate_vector = []
            for category in category_names:
                if category in candidate_categories:
                    data = candidate_categories[category]
                    score = data['position_score'] if data['position_score'] is not None else data['overall_score']
                    candidate_vector.append(score if score is not None else 0)
                else:
                    candidate_vector.append(0)
            
            candidate_vector = np.array(candidate_vector)
            
            # Calculate WEIGHTED Euclidean distance
            if len(candidate_vector) == len(target_vector):
                # Weighted distance
                weighted_diff = np.sqrt(weights) * (target_vector - candidate_vector)
                distance = np.linalg.norm(weighted_diff)
                
                # Convert to similarity score (0-100)
                # More aggressive scaling to spread out the scores
                max_possible_distance = np.sqrt(np.sum(weights * 100**2))  # Max if all categories differ by 100
                similarity_score = 100 * (1 - (distance / max_possible_distance))
                
                similarities.append({
                    'player_name': candidate_name,
                    'position': row['position'],
                    'similarity_score': round(max(0, similarity_score), 1),
                    'distance': round(distance, 2)
                })
        
        # Sort by similarity
        similarities.sort(key=lambda x: x['similarity_score'], reverse=True)
        
        return {
            'target_player': {
                'name': player_name,
                'position': target_position,
                'category_profile': {cat: target_categories[cat]['position_score'] or target_categories[cat]['overall_score'] 
                                for cat in category_names}
            },
            'timeframe_info': {
                'timeframe': timeframe,
                'description': timeframe_desc
            },
            'similar_players': similarities[:top_n],
            'comparison_method': 'weighted-position-aware' if same_position_only else 'weighted-all-players',
            'categories_compared': category_names
        }

    def _normalize_name(self, name: str) -> str:
        """
        Normalize name for fuzzy matching
        - Remove accents
        - Convert to lowercase
        - Remove extra whitespace
        """
        # Remove accents
        normalized = unicodedata.normalize('NFD', name)
        without_accents = ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')
        
        # Lowercase and strip
        cleaned = without_accents.lower().strip()
        
        # Normalize whitespace
        cleaned = ' '.join(cleaned.split())
        
        return cleaned


    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity score between two strings (0-100)"""
        return SequenceMatcher(None, str1, str2).ratio() * 100


    def find_player_by_fuzzy_name(self, player_name: str, timeframe: str = "current", 
                                threshold: float = 70.0, max_results: int = 5) -> Dict:
        """
        Find player using fuzzy name matching
        
        Args:
            player_name: Name to search for (can have typos/accents)
            timeframe: Which timeframe to search in
            threshold: Minimum similarity score (0-100) to consider a match
            max_results: Maximum number of matches to return
            
        Returns:
            Dict with exact matches, fuzzy matches, or error
        """
        filter_clause, timeframe_desc = self._parse_timeframe(timeframe)
        
        # Normalize input name
        normalized_input = self._normalize_name(player_name)
        
        # Get all distinct player names from database
        query = f"""
            SELECT DISTINCT player_name
            FROM analytics_players
            WHERE {filter_clause}
        """
        
        all_players = self.conn.execute(query).fetchdf()
        
        if all_players.empty:
            return {"error": "No players found in database"}
        
        # Try exact match first (case-insensitive, accent-insensitive)
        exact_matches = []
        fuzzy_matches = []
        
        for _, row in all_players.iterrows():
            actual_name = row['player_name']
            normalized_actual = self._normalize_name(actual_name)
            
            # Check for exact normalized match
            if normalized_input == normalized_actual:
                exact_matches.append(actual_name)
                continue
            
            # Calculate similarity
            similarity = self._calculate_similarity(normalized_input, normalized_actual)
            
            if similarity >= threshold:
                fuzzy_matches.append({
                    'name': actual_name,
                    'similarity': round(similarity, 1)
                })
        
        # Sort fuzzy matches by similarity
        fuzzy_matches.sort(key=lambda x: x['similarity'], reverse=True)
        
        # Limit results
        fuzzy_matches = fuzzy_matches[:max_results]
        
        return {
            'search_term': player_name,
            'normalized_search': normalized_input,
            'exact_matches': exact_matches,
            'fuzzy_matches': fuzzy_matches,
            'threshold': threshold
        }


    def get_player_with_fuzzy_match(self, player_name: str, timeframe: str = "current", 
                                    auto_select: bool = False) -> Tuple[Optional[str], Dict]:
        """
        Get player name with automatic fuzzy matching
        
        Args:
            player_name: Name to search for
            timeframe: Which timeframe to search
            auto_select: If True and only one match found, return it automatically
            
        Returns:
            (matched_name, match_info) tuple
        """
        match_result = self.find_player_by_fuzzy_name(player_name, timeframe)
        
        if "error" in match_result:
            return None, match_result
        
        # Check for exact match
        if match_result['exact_matches']:
            return match_result['exact_matches'][0], {
                'match_type': 'exact',
                'original': player_name,
                'matched': match_result['exact_matches'][0]
            }
        
        # Check fuzzy matches
        fuzzy = match_result['fuzzy_matches']
        
        if not fuzzy:
            return None, {
                'error': f"No matches found for '{player_name}'",
                'suggestion': "Try checking spelling or use --list to see available players"
            }
        
        # Auto-select if only one match and flag is set
        if len(fuzzy) == 1 and auto_select:
            return fuzzy[0]['name'], {
                'match_type': 'fuzzy_auto',
                'original': player_name,
                'matched': fuzzy[0]['name'],
                'similarity': fuzzy[0]['similarity']
            }
        
        # Multiple matches or no auto-select
        return None, {
            'match_type': 'fuzzy_multiple',
            'original': player_name,
            'candidates': fuzzy
        }
    
    def _normalize_to_per_90(self, player_data: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize counting stats to per-90 where not already provided.
        
        For metrics that are totals (goals, assists, touches, etc.), calculate per_90
        if a per_90 version doesn't already exist in the data.
        
        Args:
            player_data: DataFrame with player stats including minutes_played
            
        Returns:
            DataFrame with per_90 normalized metrics added
        """
        df = player_data.copy()
        
        # Calculate 90s played
        if 'minutes_played' in df.columns:
            df['games_90s'] = df['minutes_played'] / 90
        else:
            return df  # Can't normalize without minutes
        
        # List of counting stats that should be normalized
        # These are TOTALS that need per_90 calculation
        counting_stats_to_normalize = [
            # Attacking
            'goals', 'assists', 'shots', 'shots_on_target', 'non_penalty_goals',
            
            # Creativity
            'key_passes', 'shot_creating_actions', 'goal_creating_actions',
            'crosses', 'through_balls', 'passes_final_third', 'passes_penalty_area',
            'crosses_penalty_area', 'sca_pass_live', 'gca_pass_live',
            
            # Passing
            'passes_completed', 'passes_attempted', 'progressive_passes',
            'short_passes_completed', 'medium_passes_completed', 'long_passes_completed',
            'live_ball_passes', 'switches',
            
            # Ball Progression
            'progressive_carries', 'carries', 'carries_final_third', 'carries_penalty_area',
            'take_ons_attempted', 'take_ons_successful', 'sca_take_on', 'gca_take_on',
            
            # Defending
            'tackles', 'tackles_won', 'interceptions', 'blocks', 'clearances',
            'tackles_def_third', 'tackles_mid_third', 'tackles_att_third',
            'shots_blocked', 'passes_blocked', 'tackles_plus_interceptions',
            
            # Physical
            'aerial_duels_won', 'aerial_duels_lost', 'fouls_drawn', 'ball_recoveries',
            'challenges_lost', 'challenge_tackles', 'challenges_attempted',
            
            # Ball Involvement
            'touches', 'touches_def_third', 'touches_mid_third', 'touches_att_third',
            'touches_att_penalty', 'touches_def_penalty', 'touches_live_ball',
            'passes_received', 'progressive_passes_received_detail',
            
            # Discipline
            'yellow_cards', 'red_cards', 'second_yellow_cards', 'fouls_committed',
            'miscontrols', 'dispossessed', 'errors', 'offsides', 'take_ons_tackled'
        ]
        
        # For each counting stat, create per_90 version if it doesn't exist
        for stat in counting_stats_to_normalize:
            per_90_name = f"{stat}_per_90"
            
            # Only calculate if:
            # 1. The raw stat exists
            # 2. The per_90 version doesn't already exist
            # 3. games_90s > 0
            if stat in df.columns and per_90_name not in df.columns:
                # Calculate per_90 for all rows where games_90s > 0
                mask = df['games_90s'] > 0
                df.loc[mask, per_90_name] = (df.loc[mask, stat] / df.loc[mask, 'games_90s']).round(2)
        
        return df


    def _get_metric_for_percentile(self, metric: str, player_data: pd.DataFrame) -> tuple:
        """
        Get the correct metric to use for percentile calculation.
        Prioritizes per_90 versions for counting stats.
        
        Args:
            metric: Original metric name
            player_data: DataFrame with player stats
            
        Returns:
            tuple: (metric_to_use, value)
        """
        # Check if a per_90 version exists
        per_90_metric = f"{metric}_per_90"
        
        # Rates/percentages should NOT be normalized (already per-possession or per-attempt)
        rate_metrics = [
            'pass_completion_rate', 'shot_accuracy', 'tackle_success_rate',
            'take_on_success_rate', 'aerial_duel_success_rate', 'short_pass_completion_rate',
            'medium_pass_completion_rate', 'long_pass_completion_rate',
            'goals_per_shot', 'goals_per_shot_on_target', 'save_percentage',
            'clean_sheet_percentage', 'take_ons_tackled_rate'
        ]
        
        # Distance metrics should NOT be normalized (context matters)
        distance_metrics = [
            'total_pass_distance', 'progressive_pass_distance',
            'carry_distance', 'progressive_carry_distance',
            'average_shot_distance'
        ]
        
        # If it's a rate or distance, use the original metric
        if metric in rate_metrics or metric in distance_metrics:
            if metric in player_data.columns:
                return metric, player_data[metric].iloc[0] if len(player_data) > 0 else None
            else:
                return metric, None
        
        # For counting stats, prefer per_90 version
        if per_90_metric in player_data.columns:
            return per_90_metric, player_data[per_90_metric].iloc[0] if len(player_data) > 0 else None
        elif metric in player_data.columns:
            return metric, player_data[metric].iloc[0] if len(player_data) > 0 else None
        else:
            return metric, None
        
    def is_goalkeeper(self, position: str) -> bool:
        """Check if position is goalkeeper"""
        return 'GK' in position.upper()

    def get_categories_for_position(self, position: str) -> dict:
        """
        Get appropriate stat categories based on position
        
        Args:
            position: Player position string
            
        Returns:
            dict: Either goalkeeper_categories or stat_categories (outfield)
        """
        if self.is_goalkeeper(position):
            return self.goalkeeper_categories
        else:
            return self.stat_categories


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