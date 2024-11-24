import pandas as pd

def cleanData(rawdata):
    for i, df in enumerate(rawdata):
        rawdata[i].columns = [' '.join(col).strip() for col in rawdata[i].columns]
        rawdata[i] = rawdata[i].reset_index(drop=True)
        new_columns = []
        for cols in rawdata[i].columns:
            if 'level_0' in cols:
                new_col = cols.split()[-1] # takes the last name
            else:
                new_col = cols
            new_columns.append(new_col)
        rawdata[i].columns = new_columns
        rawdata[i] = rawdata[i].fillna(0)

    # Rename DataFrames
    SquadStats = rawdata[0]
    OpponentStats = rawdata[1]
    PlayerStats = rawdata[2]

    # Format Columns in Player Standard Stats DataFrame
    PlayerStats['Age'] = PlayerStats['Age'].str[:2]
    PlayerStats['Nation'] = PlayerStats['Nation'].str.split(' ').str.get(1)
    PlayerStats = PlayerStats.drop(columns=['Rk', 'Matches'])

    # Drop all the rows that have NaN in the row
    PlayerStats.dropna(inplace=True)

    # Convert all the Data types of the numeric columns from object to numeric
    for col in SquadStats.columns[1:]:
        SquadStats[col] = pd.to_numeric(SquadStats[col], errors='coerce')
    for col in OpponentStats.columns[1:]:
        OpponentStats[col] = pd.to_numeric(OpponentStats[col], errors='coerce')
    for col in PlayerStats.columns[4:]:
        PlayerStats[col] = pd.to_numeric(PlayerStats[col], errors='coerce')

    return [SquadStats, OpponentStats, PlayerStats]

# if __name__ == '__main__':
