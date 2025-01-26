import pandas as pd
from database import createConnection
from datetime import datetime

conn = createConnection('data/rawdata.db')

# Get current date
current_date = datetime.now().strftime("%Y-%m-%d")

# Open file for writing
with open("data/HaalandStats.txt", "w") as f:
    # Write the date at the beginning of the file
    f.write(f"Date: {current_date}\n\n")

    base_query = '''
    SELECT
        pos.Player,
        pos.Pos,
        pos.Squad,
        pos.[90s],
        {column} AS 'Metric'
    FROM player_possession pos
    JOIN player_passing pas ON pas.Player = pos.Player
    JOIN player_standard std ON std.Player = pos.Player
    WHERE pos.[90s] > 12 and (pos.Pos like '%FW%' or pos.Pos like '%MF%')
    '''

    order_columns = [
        'pos.[Touches Touches] / pos.[90s]',
        'pos.[Touches Att 3rd] / pos.[90s]',
        'pos.[Carries Carries] / pos.[90s]',
        'pos.[Carries 1/3] / pos.[90s]',
        'pas.[Total Cmp] / pas.[90s]',
        'std.[Progression PrgC] / pos.[90s]',
        'pos.[Take-Ons Succ] / pos.[90s]'
    ]

    column_names = [
        'Touches per 90',
        'Touches Att 3rd per 90',
        'Carries per 90',
        'Carries 1/3 per 90',
        'Passes Cmp per 90',
        'Progressive Carries per 90',
        'Succ Take-Ons per 90'
    ]

    for column, name in zip(order_columns, column_names):
        query = base_query.format(column=column) + f" ORDER BY Metric ASC"
        df = pd.read_sql_query(query, conn)

        # Round all float columns to 2 decimal places
        df = df.round(2)
        
        total_count = len(df)
        
        haaland_position = df.index[df['Player'] == 'Erling Haaland'].tolist()
        
        if haaland_position:
            haaland_rank = haaland_position[0] + 1
            if haaland_rank > 10:
                df_display = pd.concat([df.head(10), df.iloc[haaland_position[0]:haaland_position[0]+1]])
            else:
                df_display = df.head(10)
        else:
            haaland_rank = "Not found"
            df_display = df.head(10)

        haaland_rank = total_count + 1 - haaland_rank if isinstance(haaland_rank, int) else "Not found"
        
        # Write to file instead of printing
        f.write(f"\nTop 10 (and Haaland) by {name}\n")
        f.write(f"Erling Haaland's rank: {haaland_rank} out of {total_count}\n")
        f.write(df_display.to_string())
        f.write("\n\n" + "="*80 + "\n")

conn.close()

print("Results have been written to HaalandStats.txt")
