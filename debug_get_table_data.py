# Save as debug_get_table_data.py
import sys
sys.path.append('src')
from database.analytics_db import AnalyticsDBConnection
from analytics.etl.player_consolidation import PlayerDataConsolidator
import pandas as pd

db = AnalyticsDBConnection()
consolidator = PlayerDataConsolidator()

with db.get_raw_connection() as conn:
    print("=== DEBUGGING _get_table_data() ===")
    
    # Test the method on one problematic table
    table_name = "player_shooting"
    gameweek = 4
    
    print(f"Testing {table_name} for gameweek {gameweek}")
    
    # Step 1: Check table info
    columns_info = pd.read_sql(f"PRAGMA table_info({table_name})", conn)
    columns = columns_info['name'].tolist()
    print(f"1. Columns available: {len(columns)}")
    print(f"   Has Player: {'Player' in columns}")
    print(f"   Has Born: {'Born' in columns}")
    print(f"   Has Squad: {'Squad' in columns}")
    
    # Step 2: Check the SQL query
    query = f"SELECT * FROM {table_name} WHERE current_through_gameweek = ?"
    df = pd.read_sql(query, conn, params=[gameweek])
    print(f"2. SQL query returned: {len(df)} records")
    
    if len(df) > 0:
        print(f"   Sample row columns: {list(df.columns)}")
        print(f"   Sample Player values: {df['Player'].head(3).tolist()}")
        print(f"   Sample Born values: {df['Born'].head(3).tolist()}")
        print(f"   Sample Squad values: {df['Squad'].head(3).tolist()}")
        
        # Step 3: Try key creation manually
        try:
            df['player_key'] = df['Player'] + '_' + df['Born'].astype(str)
            print(f"3. player_key creation: SUCCESS")
            print(f"   Sample player_keys: {df['player_key'].head(3).tolist()}")
        except Exception as e:
            print(f"3. player_key creation: FAILED - {e}")
        
        try:
            df['dedup_key'] = df['Player'] + '_' + df['Born'].astype(str) + '_' + df['Squad']
            print(f"4. dedup_key creation: SUCCESS")
        except Exception as e:
            print(f"4. dedup_key creation: FAILED - {e}")
        
        # Step 4: Check deduplication
        initial_count = len(df)
        df_dedup = df.drop_duplicates(subset=['dedup_key'], keep='first')
        final_count = len(df_dedup)
        print(f"5. Deduplication: {initial_count} -> {final_count}")
        
        # Step 5: Check final DataFrame
        print(f"6. Final DataFrame:")
        print(f"   Columns: {list(df_dedup.columns)}")
        print(f"   Has player_key: {'player_key' in df_dedup.columns}")
        
    else:
        print("   DataFrame is empty after SQL query")
    
    # Step 6: Test the actual method
    print(f"\n7. Testing actual _get_table_data() method:")
    result_df = consolidator._get_table_data(conn, table_name, gameweek)
    print(f"   Method returned: {len(result_df)} records")
    print(f"   Columns: {list(result_df.columns) if not result_df.empty else 'Empty DataFrame'}")
    print(f"   Has player_key: {'player_key' in result_df.columns if not result_df.empty else 'N/A'}")