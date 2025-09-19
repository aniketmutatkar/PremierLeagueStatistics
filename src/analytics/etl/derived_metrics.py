"""
Derived Metrics Calculator - CORRECTED to use actual analytics_players column names
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DerivedMetricsCalculator:
    """Calculates derived metrics using actual analytics_players column names"""
    
    def __init__(self):
        self.metrics_calculated = []
    
    def calculate_all_metrics(self, player_df: pd.DataFrame, team_totals: pd.DataFrame) -> pd.DataFrame:
        """Calculate all 15 derived metrics using correct analytics column names"""
        logger.info("Starting derived metrics calculation with correct analytics column names")
        
        if player_df.empty:
            logger.error("No player data provided for metrics calculation")
            return player_df
        
        df = player_df.copy()
        
        # Merge team totals for context metrics (if provided)
        if not team_totals.empty:
            df = df.merge(team_totals, left_on='squad', right_on='team_name', how='left')
        
        # Calculate each category of metrics
        df = self._calculate_attacking_metrics(df)
        df = self._calculate_possession_metrics(df)
        df = self._calculate_defensive_metrics(df)
        df = self._calculate_context_metrics(df)
        df = self._calculate_form_metrics(df)
        
        logger.info(f"Derived metrics calculation complete: {len(self.metrics_calculated)} metrics added")
        return df
    
    def _calculate_attacking_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate attacking derived metrics using actual analytics column names"""
        try:
            # 1. Goals vs Expected - CORRECTED
            if 'goals' in df.columns and 'expected_goals' in df.columns:
                df['goals_vs_expected'] = df['goals'] - df['expected_goals']
                self.metrics_calculated.append('goals_vs_expected')
            
            # 2. Non-penalty Goals vs Expected - CORRECTED
            if 'non_penalty_goals' in df.columns and 'non_penalty_expected_goals' in df.columns:
                df['npgoals_vs_expected'] = df['non_penalty_goals'] - df['non_penalty_expected_goals']
                self.metrics_calculated.append('npgoals_vs_expected')
            
            # 3. Key Pass Conversion - CORRECTED (key_passes exists in analytics)
            if 'key_passes' in df.columns and 'assists' in df.columns:
                df['key_pass_conversion'] = np.where(
                    df['key_passes'] > 0,
                    df['assists'] / df['key_passes'],
                    0
                )
                self.metrics_calculated.append('key_pass_conversion')
            
            # 4. Expected Goal Involvement per 90 - CORRECTED
            if all(col in df.columns for col in ['non_penalty_expected_goals', 'expected_assisted_goals', 'minutes_90s']):
                df['expected_goal_involvement_per_90'] = np.where(
                    df['minutes_90s'] > 0,
                    (df['non_penalty_expected_goals'] + df['expected_assisted_goals']) / df['minutes_90s'],
                    0
                )
                self.metrics_calculated.append('expected_goal_involvement_per_90')
            
            logger.debug("Attacking metrics calculated")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating attacking metrics: {e}")
            return df
    
    def _calculate_possession_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate possession-based derived metrics using actual analytics column names"""
        try:
            # 5. Progressive Actions per 90 - CORRECTED
            if all(col in df.columns for col in ['progressive_carries', 'progressive_passes', 'minutes_90s']):
                df['progressive_actions_per_90'] = np.where(
                    df['minutes_90s'] > 0,
                    (df['progressive_carries'] + df['progressive_passes']) / df['minutes_90s'],
                    0
                )
                self.metrics_calculated.append('progressive_actions_per_90')
            
            # 6. Possession Efficiency - CORRECTED (touches exists in analytics)
            if 'touches' in df.columns and all(col in df.columns for col in ['progressive_carries', 'progressive_passes']):
                df['possession_efficiency'] = np.where(
                    df['touches'] > 0,
                    (df['progressive_carries'] + df['progressive_passes']) / df['touches'],
                    0
                )
                self.metrics_calculated.append('possession_efficiency')
            
            # 7. Final Third Involvement - CORRECTED (touches_att_third exists in analytics)
            if 'touches_att_third' in df.columns and 'touches' in df.columns:
                df['final_third_involvement'] = np.where(
                    df['touches'] > 0,
                    df['touches_att_third'] / df['touches'],
                    0
                )
                self.metrics_calculated.append('final_third_involvement')
            
            logger.debug("Possession metrics calculated")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating possession metrics: {e}")
            return df
    
    def _calculate_defensive_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate defensive derived metrics using actual analytics column names"""
        try:
            # 8. Defensive Actions per 90 - CORRECTED (defense columns exist in analytics)
            if all(col in df.columns for col in ['tackles', 'interceptions', 'blocks', 'minutes_90s']):
                df['defensive_actions_per_90'] = np.where(
                    df['minutes_90s'] > 0,
                    (df['tackles'] + df['interceptions'] + df['blocks']) / df['minutes_90s'],
                    0
                )
                self.metrics_calculated.append('defensive_actions_per_90')
            
            # 9. Aerial Duel Success Rate - CORRECTED (aerial columns exist in analytics)
            if 'aerial_duels_won' in df.columns and 'aerial_duels_lost' in df.columns:
                total_aerials = df['aerial_duels_won'] + df['aerial_duels_lost']
                df['aerial_duel_success_rate'] = np.where(
                    total_aerials > 0,
                    df['aerial_duels_won'] / total_aerials,
                    0
                )
                self.metrics_calculated.append('aerial_duel_success_rate')
            
            logger.debug("Defensive metrics calculated")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating defensive metrics: {e}")
            return df
    
    def _calculate_context_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate context-based derived metrics (team share metrics)"""
        try:
            # 10. Goal Share of Team - CORRECTED
            if 'goals' in df.columns and 'team_total_goals' in df.columns:
                df['goal_share_of_team'] = np.where(
                    df['team_total_goals'] > 0,
                    df['goals'] / df['team_total_goals'],
                    0
                )
                self.metrics_calculated.append('goal_share_of_team')
            
            # 11. Assist Share of Team - CORRECTED
            if 'assists' in df.columns and 'team_total_assists' in df.columns:
                df['assist_share_of_team'] = np.where(
                    df['team_total_assists'] > 0,
                    df['assists'] / df['team_total_assists'],
                    0
                )
                self.metrics_calculated.append('assist_share_of_team')
            
            # 12. Minutes Percentage of Team - CORRECTED
            if 'minutes_played' in df.columns and 'team_total_minutes' in df.columns:
                df['minutes_percentage_of_team'] = np.where(
                    df['team_total_minutes'] > 0,
                    df['minutes_played'] / df['team_total_minutes'],
                    0
                )
                self.metrics_calculated.append('minutes_percentage_of_team')
            
            logger.debug("Context metrics calculated")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating context metrics: {e}")
            return df
    
    def _calculate_form_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate form-based derived metrics - CORRECTED FORMULA"""
        try:
            # 13. Goals Last 5 GW (simplified for current gameweek)
            if 'goals' in df.columns:
                df['goals_last_5gw'] = df['goals']  # Will be proper rolling average later
                self.metrics_calculated.append('goals_last_5gw')
            
            # 14. Assists Last 5 GW (simplified for current gameweek)
            if 'assists' in df.columns:
                df['assists_last_5gw'] = df['assists']  # Will be proper rolling average later
                self.metrics_calculated.append('assists_last_5gw')
            
            # 15. Form Score - CORRECTED CALCULATION
            if all(col in df.columns for col in ['goals', 'assists', 'minutes_played']):
                # Form score: weighted performance per 90 minutes (FIXED FORMULA)
                df['form_score'] = np.where(
                    df['minutes_played'] > 0,
                    ((df['goals'] * 3 + df['assists'] * 2) * 90) / df['minutes_played'],
                    0
                )
                self.metrics_calculated.append('form_score')
            
            logger.debug("Form metrics calculated (corrected)")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating form metrics: {e}")
            return df
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of calculated metrics"""
        return {
            "total_metrics_calculated": len(self.metrics_calculated),
            "metrics_list": self.metrics_calculated,
            "categories": {
                "attacking": [m for m in self.metrics_calculated if any(term in m for term in ['goal', 'expected', 'conversion'])],
                "possession": [m for m in self.metrics_calculated if any(term in m for term in ['progressive', 'possession', 'third'])],
                "defensive": [m for m in self.metrics_calculated if any(term in m for term in ['defensive', 'aerial'])],
                "context": [m for m in self.metrics_calculated if 'share' in m or 'percentage' in m],
                "form": [m for m in self.metrics_calculated if any(term in m for term in ['last', 'form'])]
            }
        }

    def calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Legacy method name for backwards compatibility"""
        # This method exists for compatibility with existing code
        dummy_team_totals = pd.DataFrame()
        return self.calculate_all_metrics(df, dummy_team_totals)