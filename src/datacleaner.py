import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def cleanData(rawdata):
    logging.info("Starting data cleaning process.")
    current_date = datetime.now().strftime('%Y-%m-%d')  # Format date as string
    
    if len(rawdata) < 3:
        raise ValueError("Expected at least three DataFrames")

    for i, df in enumerate(rawdata):
        logging.debug(f"Cleaning DataFrame {i+1}/{len(rawdata)}.")
        rawdata[i].columns = [' '.join(col).strip() for col in rawdata[i].columns]
        rawdata[i] = rawdata[i].reset_index(drop=True)
        
        new_columns = []
        for cols in rawdata[i].columns:
            if 'level_0' in cols:
                new_col = cols.split()[-1]  # takes the last name
            else:
                new_col = cols
            new_columns.append(new_col)
        
        rawdata[i].columns = new_columns
        rawdata[i] = rawdata[i].fillna(0)

        # Add a column with the current date
        rawdata[i]['Current Date'] = current_date

    # Rename DataFrames
    SquadStats = rawdata[0]
    OpponentStats = rawdata[1]
    PlayerStats = rawdata[2]

    # Format Columns in Player Standard Stats DataFrame
    if 'Age' in PlayerStats.columns:
        PlayerStats['Age'] = PlayerStats['Age'].str[:2]
    if 'Nation' in PlayerStats.columns:
        PlayerStats['Nation'] = PlayerStats['Nation'].str.split(' ').str.get(1)
    if 'Rk' in PlayerStats.columns:
        PlayerStats = PlayerStats.drop(columns=['Rk'], errors='ignore')
    if 'Matches' in PlayerStats.columns:
        PlayerStats = PlayerStats.drop(columns=['Matches'], errors='ignore')

    # Drop all the rows that have NaN in the row
    initial_player_count = len(PlayerStats)
    PlayerStats.dropna(inplace=True)
    final_player_count = len(PlayerStats)
    
    logging.info(f"Dropped {initial_player_count - final_player_count} rows with NaN values from PlayerStats.")

    # Convert all the Data types of the numeric columns from object to numeric
    for col in SquadStats.columns[1:-1]:
        SquadStats[col] = pd.to_numeric(SquadStats[col], errors='coerce')
    for col in OpponentStats.columns[1:-1]:
        OpponentStats[col] = pd.to_numeric(OpponentStats[col], errors='coerce')
    for col in PlayerStats.columns[4:-1]:
        PlayerStats[col] = pd.to_numeric(PlayerStats[col], errors='coerce')

    logging.info("Data cleaning process completed successfully.")
    
    return [SquadStats, OpponentStats, PlayerStats]