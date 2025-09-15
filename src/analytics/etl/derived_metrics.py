"""
Derived Metrics Calculator - Calculates 15 derived metrics for analytics layer
"""
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DerivedMetricsCalculator:
    """Calculates derived metrics from consolidated player data"""
    
    def __init__(self):
        self.metrics_calculated = []
    
    def calculate_all_metrics(self, player_df: pd.DataFrame, team_totals: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate all 15 derived metrics
        
        Args:
            player_df: Consolidated player data
            team_totals: Team totals for context metrics
            
        Returns:
            DataFrame with derived metrics added
        """
        logger.info("Starting derived metrics calculation")
        
        if player_df.empty:
            logger.error("No player data provided for metrics calculation")
            return player_df
        
        df = player_df.copy()
        
        # Merge team totals for context metrics
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
        """Calculate attacking derived metrics"""
        try:
            # 1. Goals vs Expected
            if 'goals' in df.columns and 'expected_goals' in df.columns:
                df['goals_vs_expected'] = df['goals'] - df['expected_goals']
                self.metrics_calculated.append('goals_vs_expected')
            
            # 2. Non-penalty Goals vs Expected
            if 'non_penalty_goals' in df.columns and 'non_penalty_expected_goals' in df.columns:
                df['npgoals_vs_expected'] = df['non_penalty_goals'] - df['non_penalty_expected_goals']
                self.metrics_calculated.append('npgoals_vs_expected')
            
            # 3. Key Pass Conversion (need to find key passes from passing table)
            key_pass_col = self._find_column_containing(df, ['KP', 'key_pass', 'Key Pass'])
            if key_pass_col and 'assists' in df.columns:
                df['key_pass_conversion'] = np.where(
                    df[key_pass_col] > 0,
                    df['assists'] / df[key_pass_col],
                    0
                )
                self.metrics_calculated.append('key_pass_conversion')
            
            # 4. Expected Goal Involvement per 90
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
        """Calculate possession-based derived metrics"""
        try:
            # 5. Progressive Actions per 90
            if all(col in df.columns for col in ['progressive_carries', 'progressive_passes', 'minutes_90s']):
                df['progressive_actions_per_90'] = np.where(
                    df['minutes_90s'] > 0,
                    (df['progressive_carries'] + df['progressive_passes']) / df['minutes_90s'],
                    0
                )
                self.metrics_calculated.append('progressive_actions_per_90')
            
            # 6. Possession Efficiency (need total touches)
            touches_col = self._find_column_containing(df, ['Touch', 'touches', 'Att'])
            if touches_col and 'progressive_carries' in df.columns and 'progressive_passes' in df.columns:
                df['possession_efficiency'] = np.where(
                    df[touches_col] > 0,
                    (df['progressive_carries'] + df['progressive_passes']) / df[touches_col],
                    0
                )
                self.metrics_calculated.append('possession_efficiency')
            
            # 7. Final Third Involvement (need attacking third touches)
            att_3rd_col = self._find_column_containing(df, ['Att 3rd', 'attacking_third', 'Att3rd'])
            if att_3rd_col and touches_col:
                df['final_third_involvement'] = np.where(
                    df[touches_col] > 0,
                    df[att_3rd_col] / df[touches_col],
                    0
                )
                self.metrics_calculated.append('final_third_involvement')
            
            logger.debug("Possession metrics calculated")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating possession metrics: {e}")
            return df
    
    def _calculate_defensive_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate defensive derived metrics"""
        try:
            # 8. Defensive Actions per 90
            tackles_col = self._find_column_containing(df, ['Tkl', 'tackles', 'Tackles'])
            int_col = self._find_column_containing(df, ['Int', 'interceptions'])
            blocks_col = self._find_column_containing(df, ['Blocks', 'blocks'])
            
            if tackles_col and int_col and blocks_col and 'minutes_90s' in df.columns:
                df['defensive_actions_per_90'] = np.where(
                    df['minutes_90s'] > 0,
                    (df[tackles_col] + df[int_col] + df[blocks_col]) / df['minutes_90s'],
                    0
                )
                self.metrics_calculated.append('defensive_actions_per_90')
            
            # 9. Aerial Duel Success Rate
            aerial_won_col = self._find_column_containing(df, ['Won', 'aerial_won', 'AerWon'])
            aerial_lost_col = self._find_column_containing(df, ['Lost', 'aerial_lost', 'AerLost'])
            
            if aerial_won_col and aerial_lost_col:
                total_aerials = df[aerial_won_col] + df[aerial_lost_col]
                df['aerial_duel_success_rate'] = np.where(
                    total_aerials > 0,
                    df[aerial_won_col] / total_aerials,
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
            # 10. Goal Share of Team
            if 'goals' in df.columns and 'team_total_goals' in df.columns:
                df['goal_share_of_team'] = np.where(
                    df['team_total_goals'] > 0,
                    df['goals'] / df['team_total_goals'],
                    0
                )
                self.metrics_calculated.append('goal_share_of_team')
            
            # 11. Assist Share of Team
            if 'assists' in df.columns and 'team_total_assists' in df.columns:
                df['assist_share_of_team'] = np.where(
                    df['team_total_assists'] > 0,
                    df['assists'] / df['team_total_assists'],
                    0
                )
                self.metrics_calculated.append('assist_share_of_team')
            
            # 12. Minutes Percentage of Team
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
        """Calculate form-based derived metrics (simplified for single gameweek)"""
        try:
            # 13-15. Form metrics (simplified for now - need historical data for proper calculation)
            # For now, use current gameweek performance as proxy
            
            # 13. Goals Last 5 GW (current GW only for now)
            if 'goals' in df.columns:
                df['goals_last_5gw'] = df['goals']  # Will be proper rolling average later
                self.metrics_calculated.append('goals_last_5gw')
            
            # 14. Assists Last 5 GW (current GW only for now)
            if 'assists' in df.columns:
                df['assists_last_5gw'] = df['assists']  # Will be proper rolling average later
                self.metrics_calculated.append('assists_last_5gw')
            
            # 15. Form Score (weighted performance indicator)
            if all(col in df.columns for col in ['goals', 'assists', 'minutes_90s']):
                df['form_score'] = np.where(
                    df['minutes_90s'] > 0,
                    (df['goals'] * 3 + df['assists'] * 2) / df['minutes_90s'],  # Weighted per 90
                    0
                )
                self.metrics_calculated.append('form_score')
            
            logger.debug("Form metrics calculated (simplified)")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating form metrics: {e}")
            return df
    
    def _find_column_containing(self, df: pd.DataFrame, search_terms: list) -> str:
        """Find column that contains any of the search terms (case insensitive)"""
        for term in search_terms:
            for col in df.columns:
                if term.lower() in col.lower():
                    return col
        return None
    
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