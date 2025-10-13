#!/usr/bin/env python3
"""
SquadAnalyzer - Comprehensive Team Performance Analysis
========================================================

Single unified class for:
- Individual squad analysis (profiles, percentiles, comparisons)
- League-wide insights (rankings, tactical styles, volatility)
- Multi-dimensional analysis (opponent-adjusted, connected insights)
"""

import sys
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

# Add project paths
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


class SquadAnalyzer:
    """Unified squad analysis with individual and league-wide methods"""
    
    def __init__(self, db_path: str = "data/premierleague_analytics.duckdb", min_matches: int = 3):
        self.db_path = db_path
        self.min_matches = min_matches
        self.conn = None
        
        # Cache for league-wide analysis
        self.master_dataset = None
        self.league_table = None
        
        # Squad stat categories (9 categories)
        self.stat_categories = {
            'attacking_output': {
                'metrics': [
                    'goals', 'assists', 'expected_goals', 'expected_assisted_goals',
                    'shots', 'shots_on_target', 'goals_minus_expected',
                    'non_penalty_goals', 'goals_per_shot', 'goals_per_shot_on_target',
                    'goals_per_90', 'assists_per_90', 'goals_plus_assists_per_90',
                    'non_penalty_goals_per_90', 'goals_plus_assists_minus_pks_per_90',
                    'expected_goals_per_90', 'expected_assisted_goals_per_90',
                    'xg_plus_xag_per_90', 'non_penalty_xg_per_90', 'non_penalty_xg_plus_xag_per_90',
                    'goals_plus_assists', 'non_penalty_expected_goals', 'non_penalty_xg_plus_xag',
                    'shot_accuracy', 'shots_per_90', 'shots_on_target_per_90',
                    'average_shot_distance', 'free_kick_shots', 'non_penalty_goals_minus_expected',
                    'penalty_kicks_made', 'penalty_kicks_attempted', 'penalty_kicks_won',
                    'offsides'
                ],
                'description': 'Goals, shots, finishing efficiency, and direct attacking threat'
            },
            
            'creativity': {
                'metrics': [
                    'key_passes', 'shot_creating_actions', 'goal_creating_actions',
                    'shot_creating_actions_per_90', 'goal_creating_actions_per_90',
                    'sca_pass_live', 'sca_pass_dead', 'sca_take_on', 'sca_shot', 'sca_defense',
                    'gca_pass_live', 'gca_pass_dead', 'gca_take_on', 'gca_shot',
                    'passes_final_third', 'passes_penalty_area', 'crosses_penalty_area',
                    'through_balls', 'switches', 'crosses',
                    'corner_kicks', 'inswinging_corners', 'outswinging_corners', 'straight_corners',
                    'assists_passing'
                ],
                'description': 'Chance creation, playmaking, and setting up goals'
            },
            
            'passing': {
                'metrics': [
                    'passes_completed', 'passes_attempted', 'pass_completion_rate',
                    'total_pass_distance', 'progressive_pass_distance', 'progressive_passes',
                    'short_passes_completed', 'short_passes_attempted', 'short_pass_completion_rate',
                    'medium_passes_completed', 'medium_passes_attempted', 'medium_pass_completion_rate',
                    'long_passes_completed', 'long_passes_attempted', 'long_pass_completion_rate',
                    'live_ball_passes', 'dead_ball_passes', 'free_kick_passes',
                    'throw_ins', 'completed_passes_types', 'blocked_passes',
                    'goalkeeper_long_passes_completed', 'goalkeeper_long_passes_attempted',
                    'goalkeeper_long_pass_accuracy', 'goalkeeper_pass_attempts',
                    'average_pass_length', 'throws', 'offsides_pass_types'
                ],
                'description': 'Pass completion, accuracy, and distribution quality'
            },
            
            'ball_progression': {
                'metrics': [
                    'progressive_carries', 'carries', 'carry_distance', 'progressive_carry_distance',
                    'carries_final_third', 'carries_penalty_area',
                    'take_ons_attempted', 'take_ons_successful', 'take_on_success_rate',
                    'take_ons_tackled', 'take_ons_tackled_rate',
                    'miscontrols', 'dispossessed', 'passes_received', 'progressive_passes_received_detail',
                    'sca_fouled', 'gca_fouled', 'gca_defense'
                ],
                'description': 'Moving the ball forward through carries and dribbles'
            },
            
            'defending': {
                'metrics': [
                    'tackles', 'tackles_won', 'tackles_def_third', 'tackles_mid_third', 'tackles_att_third',
                    'challenge_tackles', 'tackle_success_rate', 'tackles_plus_interceptions',
                    'challenges_attempted', 'challenges_lost',
                    'blocks', 'shots_blocked', 'passes_blocked', 'interceptions', 'clearances',
                    'goals_against', 'goals_against_per_90', 'shots_on_target_against',
                    'saves', 'save_percentage', 'clean_sheets', 'clean_sheet_percentage',
                    'post_shot_expected_goals', 'post_shot_xg_per_shot',
                    'post_shot_xg_performance', 'post_shot_xg_performance_per_90',
                    'errors', 'own_goals', 'own_goals_for',
                    'penalty_kicks_conceded', 'penalty_kicks_attempted_against', 'penalty_kicks_against',
                    'penalty_kicks_saved', 'penalty_kicks_missed_by_opponent',
                    'penalty_save_percentage', 'penalty_goals_against',
                    'free_kick_goals_against', 'corner_kick_goals_against'
                ],
                'description': 'Defensive actions, ball recovery, and preventing opposition'
            },
            
            'physical_duels': {
                'metrics': [
                    'aerial_duels_won', 'aerial_duels_lost', 'aerial_duel_success_rate',
                    'fouls_committed', 'fouls_drawn', 'ball_recoveries',
                    'crosses_faced', 'crosses_stopped', 'cross_stop_percentage',
                    'defensive_actions_outside_penalty_area', 'defensive_actions_outside_penalty_area_per_90',
                    'average_distance_defensive_actions',
                    'yellow_cards', 'red_cards', 'second_yellow_cards'
                ],
                'description': 'Physical contests, aerial duels, fouls, and discipline'
            },
            
            'possession': {
                'metrics': [
                    'touches', 'touches_def_penalty', 'touches_def_third',
                    'touches_mid_third', 'touches_att_third', 'touches_att_penalty', 'touches_live_ball',
                    'launch_percentage', 'goal_kicks_attempted', 'goal_kick_launch_percentage',
                    'goal_kick_average_length', 'crosses_misc'
                ],
                'description': 'Ball control, touches, and possession quality'
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
    
    def _parse_timeframe(self, timeframe: str) -> Tuple[str, str]:
        """Parse timeframe into SQL filter with description"""
        if timeframe == "current":
            return "is_current = true", "Current Season (Latest Gameweek)"
        elif timeframe == "career":
            return f"matches_played >= {self.min_matches}", "All Seasons Combined"
        elif timeframe.startswith("season_"):
            season = timeframe.replace("season_", "")
            return f"season = '{season}' AND matches_played >= {self.min_matches}", f"Season {season}"
        elif timeframe.startswith("last_") and timeframe.endswith("_seasons"):
            n_seasons = int(timeframe.split("_")[1])
            recent_seasons = self.conn.execute("""
                SELECT DISTINCT season 
                FROM analytics_squads 
                ORDER BY season DESC 
                LIMIT ?
            """, [n_seasons]).fetchall()
            seasons_list = "', '".join([s[0] for s in recent_seasons])
            return f"season IN ('{seasons_list}') AND matches_played >= {self.min_matches}", f"Last {n_seasons} Seasons"
        else:
            return "is_current = true", "Current Season (Default)"
    
    # Metric polarity classification - defines which metrics are inverted (lower = better)
    NEGATIVE_METRICS = {
        # Discipline
        'yellow_cards',
        'red_cards',
        'second_yellow_cards',
        'fouls_committed',
        
        # Errors & Loss of Possession
        'errors',
        'miscontrols',
        'dispossessed',
        'own_goals',
        
        # Lost Duels/Challenges
        'challenges_lost',
        'aerial_duels_lost',
        'take_ons_tackled',
        'take_ons_tackled_rate',
        
        # Offsides
        'offsides',
        'offsides_pass_types',
        
        # Blocked (YOUR actions being blocked)
        'blocked_passes',
        'passes_blocked',
        
        # Defensive Concessions (goals/shots against you)
        'goals_against',
        'goals_against_per_90',
        'shots_on_target_against',
        'penalty_kicks_conceded',
        'penalty_kicks_attempted_against',
        'penalty_kicks_against',
        'penalty_goals_against',
        'free_kick_goals_against',
        'corner_kick_goals_against',
        
        # Team Performance
        'losses',
        
        # Shot Quality (closer is better, so higher average distance is worse)
        'average_shot_distance',
    }
    
    def _is_higher_better(self, metric: str) -> bool:
        """Determine if higher values are better for a metric"""
        return metric not in self.NEGATIVE_METRICS
    
    def _normalize_metric(self, values: pd.Series, is_higher_better: bool) -> pd.Series:
        """
        Normalize metric values to 0-100 scale using min-max normalization.
        
        Args:
            values: Series of metric values
            is_higher_better: Whether higher values are better for this metric
        
        Returns:
            Normalized series (0-100 scale)
        """
        min_val = values.min()
        max_val = values.max()
        
        # Avoid division by zero
        if max_val == min_val:
            return pd.Series([50.0] * len(values), index=values.index)
        
        if is_higher_better:
            # Higher is better: scale so max=100, min=0
            normalized = ((values - min_val) / (max_val - min_val)) * 100
        else:
            # Lower is better: invert so min=100, max=0
            normalized = ((max_val - values) / (max_val - min_val)) * 100
        
        return normalized


    def calculate_category_composite_scores(self, category: str, timeframe: str = "current") -> pd.DataFrame:
        """
        Calculate composite scores for a category by normalizing and averaging all metrics.
        
        Returns DataFrame with columns: squad_name, composite_score, rank, percentile, gap_from_first
        """
        if category not in self.stat_categories:
            return pd.DataFrame()
        
        filter_clause, _ = self._parse_timeframe(timeframe)
        
        # Get all squads data
        all_squads = self.conn.execute(f"""
            SELECT * FROM analytics_squads WHERE {filter_clause}
        """).fetchdf()
        
        if all_squads.empty:
            return pd.DataFrame()
        
        # Get metrics for this category
        metrics = self.stat_categories[category]['metrics']
        
        # Normalize each metric and collect
        normalized_scores = []
        
        for metric in metrics:
            if metric in all_squads.columns:
                values = all_squads[metric].dropna()
                
                if len(values) > 0:
                    is_higher_better = self._is_higher_better(metric)
                    
                    # Create full series with NaN for missing values
                    full_values = all_squads[metric]
                    normalized = self._normalize_metric(full_values.fillna(full_values.median()), is_higher_better)
                    normalized_scores.append(normalized)
        
        # Calculate composite score (average of all normalized metrics)
        if normalized_scores:
            composite_df = pd.DataFrame(normalized_scores).T
            composite_score = composite_df.mean(axis=1)
        else:
            composite_score = pd.Series([50.0] * len(all_squads))
        
        # Build results dataframe
        results = pd.DataFrame({
            'squad_name': all_squads['squad_name'],
            'composite_score': composite_score
        })
        
        # Sort by composite score and assign ranks
        results = results.sort_values('composite_score', ascending=False).reset_index(drop=True)
        results['rank'] = range(1, len(results) + 1)
        
        # Calculate percentile from rank
        total_teams = len(results)
        results['percentile'] = ((total_teams - results['rank']) / total_teams * 100).round(1)
        
        # Calculate gap from first place
        first_place_score = results.iloc[0]['composite_score']
        results['gap_from_first'] = results['composite_score'] - first_place_score
        
        return results

    # ========================================================================
    # INDIVIDUAL SQUAD METHODS
    # ========================================================================
    
    def get_squad_basic_info(self, squad_name: str, timeframe: str = "current") -> Dict:
        """Get basic squad information with timeframe context"""
        filter_clause, timeframe_desc = self._parse_timeframe(timeframe)
        
        query = f"""
            SELECT squad_name, season, matches_played, minutes_played, gameweek
            FROM analytics_squads
            WHERE squad_name = ? AND {filter_clause}
            ORDER BY gameweek DESC
            LIMIT 1
        """
        
        squad_data = self.conn.execute(query, [squad_name]).fetchdf()
        
        if squad_data.empty:
            return {"error": f"No data found for {squad_name} in timeframe '{timeframe}'"}
        
        squad_record = squad_data.iloc[0]
        
        return {
            'squad_name': squad_record['squad_name'],
            'season': squad_record['season'],
            'matches_played': int(squad_record['matches_played']),
            'minutes_played': int(squad_record['minutes_played']),
            'gameweek': int(squad_record['gameweek']),
            'timeframe_description': timeframe_desc
        }
    
    def calculate_dual_percentiles(self, squad_name: str, timeframe: str = "current") -> Dict:
        """
        Calculate dual percentiles using composite scores:
        - Overall: vs all Premier League squads  
        - Positional: vs squads within ±3 table positions
        """
        filter_clause, timeframe_desc = self._parse_timeframe(timeframe)
        
        # Get squad data
        squad_query = f"""
            SELECT * FROM analytics_squads
            WHERE squad_name = ? AND {filter_clause}
            ORDER BY gameweek DESC
            LIMIT 1
        """
        
        squad_data = self.conn.execute(squad_query, [squad_name]).fetchdf()
        
        if squad_data.empty:
            return {"error": f"No data found for {squad_name}"}
        
        squad_record = squad_data.iloc[0]
        
        # Calculate league table to get positions
        league_table = self.calculate_league_table(timeframe)
        
        # Find this squad's table position
        squad_position_data = league_table[league_table['squad_name'] == squad_name]
        
        if squad_position_data.empty:
            squad_position = None
        else:
            squad_position = squad_position_data['position'].iloc[0]
        
        # Calculate category scores
        category_scores = {}
        
        for category_name, category_info in self.stat_categories.items():
            # Get composite scores for this category
            composite_results = self.calculate_category_composite_scores(category_name, timeframe)
            
            if composite_results.empty:
                category_scores[category_name] = {
                    'overall_score': None,
                    'positional_score': None,
                    'composite_score': None,
                    'rank': None,
                    'gap_from_first': None,
                    'metrics_analyzed': 0,
                    'description': category_info['description'],
                    'metric_breakdown': {}
                }
                continue
            
            # Get this squad's composite data
            squad_composite = composite_results[composite_results['squad_name'] == squad_name]
            
            if squad_composite.empty:
                overall_score = None
                composite_score = None
                rank = None
                gap = None
            else:
                overall_score = squad_composite.iloc[0]['percentile']
                composite_score = squad_composite.iloc[0]['composite_score']
                rank = squad_composite.iloc[0]['rank']
                gap = squad_composite.iloc[0]['gap_from_first']
            
            # Calculate positional percentile (vs ±3 table neighbors)
            if squad_position is not None:
                position_range = 3
                neighbor_squads = league_table[
                    (league_table['position'] >= squad_position - position_range) &
                    (league_table['position'] <= squad_position + position_range) &
                    (league_table['squad_name'] != squad_name)
                ]['squad_name'].tolist()
                
                # Filter composite results to neighbors
                neighbor_composites = composite_results[composite_results['squad_name'].isin(neighbor_squads)]
                
                if not neighbor_composites.empty and composite_score is not None:
                    # Calculate percentile among neighbors
                    better_neighbors = (neighbor_composites['composite_score'] < composite_score).sum()
                    total_neighbors = len(neighbor_composites)
                    positional_score = (better_neighbors / total_neighbors * 100) if total_neighbors > 0 else None
                else:
                    positional_score = None
            else:
                positional_score = None
            
            # Get individual metric breakdowns (for drill-down)
            metric_breakdown = {}
            metrics = category_info['metrics']
            
            # Get all squads for percentile calculation
            all_squads = self.conn.execute(f"""
                SELECT * FROM analytics_squads WHERE {filter_clause}
            """).fetchdf()
            
            for metric in metrics:
                if metric in squad_data.columns:
                    squad_value = squad_record[metric]
                    
                    if metric in all_squads.columns:
                        overall_values = all_squads[metric].dropna()
                        if len(overall_values) > 0 and pd.notna(squad_value):
                            if self._is_higher_better(metric):
                                overall_pct = (overall_values < squad_value).sum() / len(overall_values) * 100
                                # Rank: count squads with BETTER (higher) values + 1
                                metric_rank = (overall_values > squad_value).sum() + 1
                            else:
                                overall_pct = (overall_values > squad_value).sum() / len(overall_values) * 100
                                # Rank: count squads with BETTER (lower) values + 1
                                metric_rank = (overall_values < squad_value).sum() + 1
                            total_squads = len(overall_values)
                        else:
                            overall_pct = None
                            metric_rank = None
                            total_squads = None
                    else:
                        overall_pct = None
                        metric_rank = None
                        total_squads = None
                    
                    metric_breakdown[metric] = {
                        'value': squad_value,
                        'percentile': overall_pct,
                        'rank': metric_rank,  # NEW
                        'total_squads': total_squads,  # NEW
                        'normalized': None
                    }
            
            category_scores[category_name] = {
                'overall_score': round(overall_score, 1) if overall_score is not None else None,
                'positional_score': round(positional_score, 1) if positional_score is not None else None,
                'composite_score': round(composite_score, 1) if composite_score is not None else None,
                'rank': int(rank) if rank is not None else None,
                'gap_from_first': round(gap, 1) if gap is not None else None,
                'metrics_analyzed': len(metrics),
                'description': category_info['description'],
                'metric_breakdown': metric_breakdown
            }
        
        return {
            'squad_name': squad_name,
            'timeframe': timeframe_desc,
            'category_scores': category_scores,
            'overall_sample_size': len(all_squads),
            'positional_sample_size': len(neighbor_squads) if squad_position else 0
        }

    def get_category_breakdown(self, squad_name: str, category: str, timeframe: str = "current") -> Dict:
        """Get detailed breakdown of a specific category with composite scores"""
        if category not in self.stat_categories:
            return {"error": f"Category '{category}' not found"}
        
        dual_percentiles = self.calculate_dual_percentiles(squad_name, timeframe)
        
        if "error" in dual_percentiles:
            return dual_percentiles
        
        category_data = dual_percentiles['category_scores'][category]
        
        # Get top 5 squads for comparison
        composite_results = self.calculate_category_composite_scores(category, timeframe)
        top_5 = composite_results.head(5)[['rank', 'squad_name', 'composite_score', 'gap_from_first']]
        
        # Sort individual metrics by percentile
        metrics_sorted = []
        for metric_name, metric_data in category_data['metric_breakdown'].items():
            pct = metric_data['percentile']
            if pct is not None:
                metrics_sorted.append((metric_name, metric_data))
        
        metrics_sorted.sort(key=lambda x: x[1]['percentile'], reverse=True)
        
        breakdown = {
            'squad_name': squad_name,
            'category': category,
            'description': category_data['description'],
            'overall_category_score': category_data['overall_score'],
            'positional_category_score': category_data['positional_score'],
            'composite_score': category_data['composite_score'],
            'rank': category_data['rank'],
            'gap_from_first': category_data['gap_from_first'],
            'top_5_squads': top_5,
            'metric_details': []
        }
        
        for metric_name, metric_data in metrics_sorted:
            pct = metric_data['percentile']
            value = metric_data['value']
            
            if pct is not None:
                if pct >= 90:
                    interpretation = "Elite"
                elif pct >= 75:
                    interpretation = "Very Good"
                elif pct >= 60:
                    interpretation = "Above Average"
                elif pct >= 40:
                    interpretation = "Average"
                elif pct >= 25:
                    interpretation = "Below Average"
                else:
                    interpretation = "Poor"
            else:
                interpretation = "No Data"
            
            breakdown['metric_details'].append({
                'metric': metric_name,
                'value': value,
                'percentile': pct,
                'rank': metric_data.get('rank'),  # NEW
                'total_squads': metric_data.get('total_squads'),  # NEW
                'interpretation': interpretation
            })
        
        return breakdown

    def get_comprehensive_squad_profile(self, squad_name: str, timeframe: str = "current") -> Dict:
        """Get complete analysis for a single squad"""
        basic_info = self.get_squad_basic_info(squad_name, timeframe)
        if "error" in basic_info:
            return basic_info
        
        dual_percentiles = self.calculate_dual_percentiles(squad_name, timeframe)
        if "error" in dual_percentiles:
            return dual_percentiles
        
        strengths = self._identify_strengths(dual_percentiles['category_scores'])
        weaknesses = self._identify_weaknesses(dual_percentiles['category_scores'])
        
        return {
            'basic_info': basic_info,
            'dual_percentiles': dual_percentiles,
            'squad_insights': {
                'top_strengths': strengths[:3],
                'main_weaknesses': weaknesses[:3]
            }
        }
    
    def _identify_strengths(self, category_scores: Dict) -> List[Dict]:
        """Identify squad strengths"""
        strengths = []
        
        for category, data in category_scores.items():
            score = data.get('composite_score')
            rank = data.get('rank')
            
            if score is not None and score >= 60:
                strengths.append({
                    'category': category,
                    'score': score,
                    'rank': rank,
                    'description': data['description']
                })
        
        return sorted(strengths, key=lambda x: x['score'], reverse=True)

    def _identify_weaknesses(self, category_scores: Dict) -> List[Dict]:
        """Identify squad weaknesses"""
        weaknesses = []
        
        for category, data in category_scores.items():
            score = data.get('composite_score')
            rank = data.get('rank')
            
            if score is not None and score <= 40:
                weaknesses.append({
                    'category': category,
                    'score': score,
                    'rank': rank,
                    'description': data['description']
                })
        
        return sorted(weaknesses, key=lambda x: x['score'])

    def compare_squads(self, squad1: str, squad2: str, timeframe: str = "current") -> Dict:
        """Compare two squads head-to-head"""
        profile1 = self.get_comprehensive_squad_profile(squad1, timeframe)
        profile2 = self.get_comprehensive_squad_profile(squad2, timeframe)
        
        if "error" in profile1:
            return {"error": f"Squad 1 ({squad1}): {profile1['error']}"}
        if "error" in profile2:
            return {"error": f"Squad 2 ({squad2}): {profile2['error']}"}
        
        categories1 = profile1['dual_percentiles']['category_scores']
        categories2 = profile2['dual_percentiles']['category_scores']
        
        comparison = {
            'squads': [squad1, squad2],
            'timeframe_info': profile1['basic_info']['timeframe_description'],
            'category_comparison': {},
            'summary': {}
        }
        
        squad1_wins = 0
        squad2_wins = 0
        
        for category in self.stat_categories.keys():
            if category in categories1 and category in categories2:
                score1 = categories1[category]['positional_score'] or categories1[category]['overall_score']
                score2 = categories2[category]['positional_score'] or categories2[category]['overall_score']
                
                if score1 is not None and score2 is not None:
                    difference = score1 - score2
                    
                    if abs(difference) < 5:
                        winner = "tie"
                    elif difference > 0:
                        winner = squad1
                        squad1_wins += 1
                    else:
                        winner = squad2
                        squad2_wins += 1
                    
                    comparison['category_comparison'][category] = {
                        f'{squad1}_score': score1,
                        f'{squad2}_score': score2,
                        'difference': round(difference, 1),
                        'winner': winner,
                        'description': categories1[category]['description']
                    }
        
        if squad1_wins > squad2_wins:
            overall_winner = squad1
        elif squad2_wins > squad1_wins:
            overall_winner = squad2
        else:
            overall_winner = "tie"
        
        comparison['summary'] = {
            'category_wins': {squad1: squad1_wins, squad2: squad2_wins},
            'overall_winner': overall_winner
        }
        
        return comparison
    
    # ========================================================================
    # LEAGUE-WIDE ANALYSIS METHODS
    # ========================================================================
    
    def calculate_league_table(self, timeframe: str = "current") -> pd.DataFrame:
        """Calculate league table standings"""
        filter_clause, _ = self._parse_timeframe(timeframe)
        
        query = f"""
            SELECT 
                squad_name,
                wins,
                draws,
                losses,
                goals,
                goals_against,
                matches_played
            FROM analytics_squads
            WHERE {filter_clause}
        """
        
        standings = self.conn.execute(query).fetchdf()
        
        standings['points'] = (standings['wins'] * 3) + (standings['draws'] * 1)
        standings['goal_difference'] = standings['goals'] - standings['goals_against']
        
        standings = standings.sort_values(
            by=['points', 'goal_difference', 'goals'],
            ascending=[False, False, False]
        ).reset_index(drop=True)
        
        standings['position'] = range(1, len(standings) + 1)
        
        return standings
    
    def build_master_dataset(self, timeframe: str = "current") -> pd.DataFrame:
        """Build comprehensive dataset with all squads"""
        self.league_table = self.calculate_league_table(timeframe)
        
        master_data = []
        
        for _, row in self.league_table.iterrows():
            squad_name = row['squad_name']
            position = row['position']
            
            profile = self.get_comprehensive_squad_profile(squad_name, timeframe)
            
            if "error" not in profile:
                category_scores = profile['dual_percentiles']['category_scores']
                
                record = {
                    'squad_name': squad_name,
                    'position': position,
                    'points': row['points'],
                    'goal_difference': row['goal_difference'],
                    'wins': row['wins'],
                    'draws': row['draws'],
                    'losses': row['losses'],
                    'goals': row['goals'],
                    'goals_against': row['goals_against']
                }
                
                for category, data in category_scores.items():
                    record[f'{category}_overall'] = data['overall_score']
                    record[f'{category}_positional'] = data['positional_score']
                    record[f'{category}_metrics_count'] = data['metrics_analyzed']
                
                master_data.append(record)
        
        self.master_dataset = pd.DataFrame(master_data)
        return self.master_dataset
    
    def analyze_performance_profiles(self) -> Dict:
        """Analyze core performance profiles across the league"""
        if self.master_dataset is None:
            raise ValueError("Must call build_master_dataset() first")
        
        df = self.master_dataset
        
        # Best attacking teams
        attacking_rank = df.nlargest(10, 'attacking_output_overall')[['position', 'squad_name', 'attacking_output_overall', 'goals']]
        
        # Best defending teams
        defending_rank = df.nlargest(10, 'defending_overall')[['position', 'squad_name', 'defending_overall', 'goals_against']]
        
        # Game control index
        df['game_control_index'] = (
            df['possession_overall'] + 
            df['passing_overall'] + 
            df['ball_progression_overall']
        ) / 3
        
        control_rank = df.nlargest(10, 'game_control_index')[['position', 'squad_name', 'game_control_index', 'possession_overall', 'passing_overall', 'ball_progression_overall']]
        
        # xG analysis
        xg_analysis = []
        for _, row in df.iterrows():
            squad_name = row['squad_name']
            
            xg_data = self.conn.execute(f"""
                SELECT goals, expected_goals, goals_minus_expected
                FROM analytics_squads
                WHERE squad_name = ? AND is_current = true
            """, [squad_name]).fetchone()
            
            if xg_data and xg_data[1] is not None:
                xg_analysis.append({
                    'squad_name': squad_name,
                    'position': row['position'],
                    'goals': xg_data[0],
                    'expected_goals': round(xg_data[1], 1),
                    'goals_minus_xg': round(xg_data[2], 1)
                })
        
        xg_df = pd.DataFrame(xg_analysis)
        
        overperformers = xg_df.nlargest(5, 'goals_minus_xg')[['position', 'squad_name', 'goals', 'expected_goals', 'goals_minus_xg']] if not xg_df.empty else None
        underperformers = xg_df.nsmallest(5, 'goals_minus_xg')[['position', 'squad_name', 'goals', 'expected_goals', 'goals_minus_xg']] if not xg_df.empty else None
        
        return {
            'attacking': attacking_rank,
            'defending': defending_rank,
            'control': control_rank,
            'xg_over': overperformers,
            'xg_under': underperformers
        }
    
    def analyze_top6_tactical(self, top6_squads: List[str] = None) -> Dict:
        """Tactical comparison of top 6 teams"""
        if self.master_dataset is None:
            raise ValueError("Must call build_master_dataset() first")
        
        if top6_squads is None:
            top6_squads = ['Arsenal', 'Liverpool', 'Manchester City', 'Chelsea', 'Manchester Utd', 'Tottenham']
        
        df = self.master_dataset
        top6_data = df[df['squad_name'].isin(top6_squads)].copy()
        
        if top6_data.empty:
            return {"error": "No top 6 teams found in dataset"}
        
        categories = ['attacking_output', 'creativity', 'passing', 'ball_progression', 
                     'defending', 'physical_duels', 'possession']
        
        comparison = top6_data[['squad_name'] + [f'{cat}_overall' for cat in categories]].copy()
        comparison.columns = ['Team'] + categories
        
        # Tactical style identification
        styles = []
        for _, row in top6_data.iterrows():
            squad = row['squad_name']
            
            possession_style = (row['passing_overall'] + row['possession_overall']) / 2
            counter_style = row['ball_progression_overall'] - (row['possession_overall'] * 0.5)
            defensive_style = row['defending_overall'] - (row['attacking_output_overall'] * 0.3)
            
            style_scores = {
                'Possession-based': possession_style,
                'Counter-attacking': counter_style,
                'Defensive': defensive_style
            }
            
            primary_style = max(style_scores, key=style_scores.get)
            
            cat_scores = [(cat, row[f'{cat}_overall']) for cat in categories]
            cat_scores.sort(key=lambda x: x[1], reverse=True)
            
            styles.append({
                'squad': squad,
                'primary_style': primary_style,
                'top_strength': cat_scores[0][0],
                'top_strength_score': cat_scores[0][1],
                'weakness': cat_scores[-1][0],
                'weakness_score': cat_scores[-1][1]
            })
        
        return {
            'top6_data': top6_data,
            'comparison': comparison,
            'tactical_styles': pd.DataFrame(styles)
        }
    
    def analyze_positional_context(self, position_range: int = 3) -> Dict:
        """Analyze teams in context of ±N table positions"""
        if self.master_dataset is None:
            raise ValueError("Must call build_master_dataset() first")
        
        df = self.master_dataset
        
        insights = []
        categories = ['attacking_output', 'defending', 'possession', 'passing']
        
        for _, row in df.iterrows():
            squad = row['squad_name']
            pos = row['position']
            
            neighbors = df[
                (df['position'] >= pos - position_range) & 
                (df['position'] <= pos + position_range) & 
                (df['squad_name'] != squad)
            ]
            
            if len(neighbors) > 0:
                comparison = {
                    'squad': squad,
                    'position': pos,
                    'neighbors_count': len(neighbors)
                }
                
                for cat in categories:
                    squad_score = row[f'{cat}_overall']
                    neighbor_avg = neighbors[f'{cat}_overall'].mean()
                    diff = squad_score - neighbor_avg
                    
                    comparison[f'{cat}_diff'] = diff
                
                insights.append(comparison)
        
        insights_df = pd.DataFrame(insights)
        insights_df['avg_diff'] = insights_df[[f'{cat}_diff' for cat in categories]].mean(axis=1)
        
        overperformers = insights_df.nlargest(5, 'avg_diff')[['position', 'squad', 'avg_diff', 'attacking_output_diff', 'defending_diff']]
        underperformers = insights_df.nsmallest(5, 'avg_diff')[['position', 'squad', 'avg_diff', 'attacking_output_diff', 'defending_diff']]
        
        return {
            'insights': insights_df,
            'overperformers': overperformers,
            'underperformers': underperformers
        }
    
    def analyze_volatility(self) -> Dict:
        """Analyze dual percentile gaps (volatility)"""
        if self.master_dataset is None:
            raise ValueError("Must call build_master_dataset() first")
        
        df = self.master_dataset
        categories = ['attacking_output', 'creativity', 'passing', 'defending']
        
        volatility_data = []
        
        for _, row in df.iterrows():
            squad = row['squad_name']
            pos = row['position']
            
            vol_record = {
                'squad': squad,
                'position': pos
            }
            
            volatilities = []
            for cat in categories:
                overall = row[f'{cat}_overall']
                positional = row[f'{cat}_positional']
                
                if overall is not None and positional is not None:
                    vol = overall - positional
                    vol_record[f'{cat}_volatility'] = vol
                    volatilities.append(abs(vol))
            
            vol_record['avg_volatility'] = np.mean(volatilities) if volatilities else 0
            volatility_data.append(vol_record)
        
        vol_df = pd.DataFrame(volatility_data)
        
        high_vol = vol_df.nlargest(5, 'avg_volatility')[['position', 'squad', 'avg_volatility', 'attacking_output_volatility', 'defending_volatility']]
        
        return {
            'volatility': vol_df,
            'high_volatility': high_vol
        }
    
    def analyze_opponent_adjusted_metrics(self) -> Dict:
        """Analyze performance adjusted for opponent quality"""
        if self.master_dataset is None:
            raise ValueError("Must call build_master_dataset() first")
        
        opponent_insights = []
        
        for _, row in self.master_dataset.iterrows():
            squad_name = row['squad_name']
            
            squad_attacking = self.conn.execute(f"""
                SELECT goals, expected_goals, shots
                FROM analytics_squads
                WHERE squad_name = ? AND is_current = true
            """, [squad_name]).fetchone()
            
            opponent_attacking = self.conn.execute(f"""
                SELECT goals, expected_goals, shots
                FROM analytics_opponents
                WHERE squad_name = ? AND is_current = true
            """, [f'vs {squad_name}']).fetchone()
            
            if squad_attacking and opponent_attacking:
                insight = {
                    'squad': squad_name,
                    'position': row['position'],
                    'squad_goals': squad_attacking[0],
                    'squad_xg': round(squad_attacking[1], 1) if squad_attacking[1] else None,
                    'opponent_goals_against': opponent_attacking[0],
                    'opponent_xg_against': round(opponent_attacking[1], 1) if opponent_attacking[1] else None,
                    'defensive_impact': squad_attacking[0] - opponent_attacking[0] if opponent_attacking[0] else None
                }
                
                opponent_insights.append(insight)
        
        opp_df = pd.DataFrame(opponent_insights)
        
        return {'opponent_data': opp_df}
    
    def analyze_connected_insights(self) -> Dict:
        """Multi-dimensional connected insights"""
        if self.master_dataset is None:
            raise ValueError("Must call build_master_dataset() first")
        
        df = self.master_dataset
        
        # 1. High Control + Low Position
        df['control_vs_position'] = df['game_control_index'] - (100 - (df['position'] * 5))
        unlucky = df.nlargest(5, 'control_vs_position')[['position', 'squad_name', 'game_control_index', 'points']]
        
        # 2. Balanced vs Specialists
        category_cols = [f'{cat}_overall' for cat in ['attacking_output', 'defending', 'passing', 'ball_progression']]
        df['category_std'] = df[category_cols].std(axis=1)
        df['category_mean'] = df[category_cols].mean(axis=1)
        
        balanced = df.nsmallest(5, 'category_std')[['position', 'squad_name', 'category_mean', 'category_std']]
        specialists = df.nlargest(5, 'category_std')[['position', 'squad_name', 'category_mean', 'category_std']]
        
        # 3. Position vs Quality Gap
        df['quality_score'] = df[category_cols].mean(axis=1)
        df['position_quality_gap'] = df['quality_score'] - (100 - (df['position'] * 5))
        
        overachieving = df.nlargest(5, 'position_quality_gap')[['position', 'squad_name', 'quality_score', 'points']]
        underachieving = df.nsmallest(5, 'position_quality_gap')[['position', 'squad_name', 'quality_score', 'points']]
        
        return {
            'high_control_low_position': unlucky,
            'balanced_teams': balanced,
            'specialist_teams': specialists,
            'overachieving': overachieving,
            'underachieving': underachieving
        }
    
    def generate_comprehensive_report(self, timeframe: str = "current") -> Dict:
        """Generate comprehensive league-wide insights report"""
        print("="*80)
        print("COMPREHENSIVE SQUAD INSIGHTS ANALYSIS")
        print("="*80)
        print(f"Timeframe: {timeframe}\n")
        
        # Phase 1: Build foundation
        print("Phase 1: Building master dataset...")
        self.build_master_dataset(timeframe)
        
        print("\n" + "="*80)
        print("LEAGUE TABLE")
        print("="*80)
        print(self.league_table[['position', 'squad_name', 'matches_played', 'wins', 'draws', 'losses', 'points', 'goal_difference']].head(10).to_string(index=False))
        
        # Phase 2: Performance profiles
        print("\n" + "="*80)
        print("PHASE 2: CORE PERFORMANCE PROFILES")
        print("="*80)
        
        performance = self.analyze_performance_profiles()
        
        print("\n🎯 TOP 10 ATTACKING TEAMS:")
        print(performance['attacking'].to_string(index=False))
        
        print("\n🛡️  TOP 10 DEFENDING TEAMS:")
        print(performance['defending'].to_string(index=False))
        
        print("\n🎮 TOP 10 GAME CONTROL TEAMS:")
        print(performance['control'].to_string(index=False))
        
        if performance['xg_over'] is not None:
            print("\n📈 TOP 5 xG OVERPERFORMERS (Clinical Finishing):")
            print(performance['xg_over'].to_string(index=False))
            
            print("\n📉 TOP 5 xG UNDERPERFORMERS (Wasteful Finishing):")
            print(performance['xg_under'].to_string(index=False))
        
        # Phase 3: Top 6 tactical
        print("\n" + "="*80)
        print("PHASE 3: TOP 6 TACTICAL COMPARISON")
        print("="*80)
        
        top6 = self.analyze_top6_tactical()
        
        if 'error' not in top6:
            print("\n📊 CATEGORY SCORES (Overall Percentile):")
            print(top6['comparison'].to_string(index=False))
            
            print("\n🎯 TACTICAL STYLES:")
            print(top6['tactical_styles'].to_string(index=False))
        
        # Phase 4: Positional context
        print("\n" + "="*80)
        print("PHASE 4: TABLE POSITION CONTEXT (±3 Teams)")
        print("="*80)
        
        positional = self.analyze_positional_context()
        
        print("\n⬆️  TEAMS PUNCHING ABOVE THEIR WEIGHT:")
        print(positional['overperformers'].to_string(index=False))
        
        print("\n⬇️  TEAMS UNDERPERFORMING RELATIVE TO TABLE POSITION:")
        print(positional['underperformers'].to_string(index=False))
        
        # Phase 5: Volatility
        print("\n" + "="*80)
        print("PHASE 5: VOLATILITY ANALYSIS")
        print("="*80)
        
        volatility = self.analyze_volatility()
        
        print("\n🌊 HIGHEST VOLATILITY TEAMS:")
        print(volatility['high_volatility'].to_string(index=False))
        print("\nInterpretation: High volatility = Performance doesn't match table neighbors")
        
        # Phase 6: Opponent-adjusted
        print("\n" + "="*80)
        print("PHASE 6: OPPONENT-ADJUSTED METRICS")
        print("="*80)
        
        opponent = self.analyze_opponent_adjusted_metrics()
        
        print("\n🎯 SQUAD vs OPPONENT PERFORMANCE (Top 10):")
        print(opponent['opponent_data'][['position', 'squad', 'squad_goals', 'squad_xg', 'opponent_goals_against', 'opponent_xg_against']].head(10).to_string(index=False))
        
        # Phase 7: Connected insights
        print("\n" + "="*80)
        print("PHASE 7: CONNECTED MULTI-DIMENSIONAL INSIGHTS")
        print("="*80)
        
        connected = self.analyze_connected_insights()
        
        print("\n🎮 HIGH GAME CONTROL + LOW TABLE POSITION (Potentially Unlucky):")
        print(connected['high_control_low_position'].to_string(index=False))
        
        print("\n⚖️  BALANCED TEAMS (Strong across all categories):")
        print(connected['balanced_teams'].to_string(index=False))
        
        print("\n🎯 SPECIALIST TEAMS (Strong in specific areas):")
        print(connected['specialist_teams'].to_string(index=False))
        
        print("\n📊 OVERACHIEVING (High table position, lower quality metrics):")
        print(connected['overachieving'].to_string(index=False))
        
        print("\n📊 UNDERACHIEVING (Low table position, higher quality metrics):")
        print(connected['underachieving'].to_string(index=False))
        
        print("\n" + "="*80)
        print("✅ COMPREHENSIVE ANALYSIS COMPLETE")
        print("="*80)
        
        return {
            'master_dataset': self.master_dataset,
            'league_table': self.league_table,
            'performance': performance,
            'top6': top6,
            'positional': positional,
            'volatility': volatility,
            'opponent': opponent,
            'connected': connected
        }


# Test function
def test_unified_analyzer():
    """Test unified SquadAnalyzer with both individual and league-wide methods"""
    with SquadAnalyzer() as analyzer:
        
        print("="*80)
        print("UNIFIED SQUAD ANALYZER TEST")
        print("="*80)
        
        # Test individual methods
        print("\n--- INDIVIDUAL SQUAD METHODS ---")
        
        print("\n1. Basic Info:")
        basic = analyzer.get_squad_basic_info("Arsenal", "current")
        if "error" not in basic:
            print(f"Squad: {basic['squad_name']}, GW: {basic['gameweek']}, Matches: {basic['matches_played']}")
        
        print("\n2. Category Breakdown:")
        breakdown = analyzer.get_category_breakdown("Arsenal", "attacking_output", "current")
        if "error" not in breakdown:
            print(f"Category: {breakdown['category']}, Score: {breakdown['overall_category_score']}%")
            print(f"Top metric: {breakdown['metric_details'][0]['metric']} ({breakdown['metric_details'][0]['interpretation']})")
        
        print("\n3. Squad Comparison:")
        comparison = analyzer.compare_squads("Arsenal", "Liverpool", "current")
        if "error" not in comparison:
            print(f"Winner: {comparison['summary']['overall_winner']}")
            print(f"Category wins: {comparison['summary']['category_wins']}")
        
        # Test league-wide methods
        print("\n\n--- LEAGUE-WIDE ANALYSIS METHODS ---")
        
        print("\n4. League Table Calculation:")
        table = analyzer.calculate_league_table("current")
        print(f"Top 5:\n{table[['position', 'squad_name', 'points', 'goal_difference']].head(5).to_string(index=False)}")
        
        print("\n5. Building Master Dataset:")
        master = analyzer.build_master_dataset("current")
        print(f"Dataset size: {len(master)} squads with {len(master.columns)} columns")
        
        print("\n✅ All methods working!")


def main():
    """Run comprehensive insights analysis"""
    with SquadAnalyzer() as analyzer:
        results = analyzer.generate_comprehensive_report("current")
    
    return results


if __name__ == "__main__":
    # Uncomment to test
    # test_unified_analyzer()
    
    # Run full analysis
    results = main()