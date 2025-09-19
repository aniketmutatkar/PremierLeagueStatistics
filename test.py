#!/usr/bin/env python3
"""
Debug why derived metrics have wrong values
"""
import duckdb

def debug_metrics():
    """Debug why the derived metrics calculations are wrong"""
    
    print("🔍 DEBUGGING DERIVED METRICS VALUES")
    print("=" * 50)
    
    with duckdb.connect("data/premierleague_analytics.duckdb") as conn:
        
        # Check specific players with high form scores
        print("📊 CHECKING HIGH FORM SCORE PLAYERS:")
        debug_data = conn.execute("""
            SELECT player_name, goals, assists, minutes_played, minutes_90s,
                   touches, progressive_carries, progressive_passes,
                   form_score, possession_efficiency
            FROM analytics_players 
            WHERE is_current = true AND form_score > 10
            ORDER BY form_score DESC 
            LIMIT 5
        """).fetchall()
        
        for row in debug_data:
            name, goals, assists, min_played, min_90s, touches, prog_c, prog_p, form, poss_eff = row
            print(f"\n{name}:")
            print(f"   Goals: {goals}, Assists: {assists}")
            print(f"   Minutes: {min_played}, Minutes/90: {min_90s}")
            print(f"   Touches: {touches}")
            print(f"   Progressive: {prog_c} carries + {prog_p} passes")
            print(f"   Form Score: {form:.3f}")
            print(f"   Possession Efficiency: {poss_eff:.3f}")
            
            # Manual calculation check
            if min_played and min_played > 0:
                manual_form = ((goals * 3 + assists * 2) * 90) / min_played
                print(f"   Manual Form Calc: ((({goals}*3) + ({assists}*2)) * 90) / {min_played} = {manual_form:.3f}")
            
            if touches and touches > 0:
                manual_poss_eff = (prog_c + prog_p) / touches
                print(f"   Manual Poss Eff: ({prog_c} + {prog_p}) / {touches} = {manual_poss_eff:.3f}")
        
        # Check for players with 0 touches
        print(f"\n🔍 CHECKING TOUCHES VALUES:")
        touches_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_players,
                COUNT(CASE WHEN touches = 0 THEN 1 END) as zero_touches,
                COUNT(CASE WHEN touches IS NULL THEN 1 END) as null_touches,
                MIN(touches) as min_touches,
                MAX(touches) as max_touches,
                AVG(touches) as avg_touches
            FROM analytics_players WHERE is_current = true
        """).fetchone()
        
        total, zero, null_val, min_val, max_val, avg = touches_stats
        print(f"   Total players: {total}")
        print(f"   Zero touches: {zero}")
        print(f"   NULL touches: {null_val}")
        print(f"   Min touches: {min_val}")
        print(f"   Max touches: {max_val}")
        print(f"   Avg touches: {avg:.1f}")
        
        # Check how many metrics actually got calculated
        print(f"\n📈 CHECKING WHICH DERIVED METRICS EXIST:")
        derived_metrics = [
            'goals_vs_expected', 'npgoals_vs_expected', 'key_pass_conversion',
            'expected_goal_involvement_per_90', 'progressive_actions_per_90',
            'possession_efficiency', 'final_third_involvement', 'defensive_actions_per_90',
            'aerial_duel_success_rate', 'goals_last_5gw', 'assists_last_5gw', 
            'form_score', 'goal_share_of_team', 'assist_share_of_team', 'minutes_percentage_of_team'
        ]
        
        for metric in derived_metrics:
            non_zero = conn.execute(f"""
                SELECT COUNT(*) FROM analytics_players 
                WHERE is_current = true AND {metric} != 0
            """).fetchone()[0]
            
            null_count = conn.execute(f"""
                SELECT COUNT(*) FROM analytics_players 
                WHERE is_current = true AND {metric} IS NULL
            """).fetchone()[0]
            
            print(f"   {metric}: {non_zero} non-zero, {null_count} NULL")

if __name__ == "__main__":
    debug_metrics()