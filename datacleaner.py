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
    SquadStandardStats = rawdata[0]
    OpponentStandardStats = rawdata[1]
    PlayerStandardStats = rawdata[2]

    # Format Columns in Player Standard Stats DataFrame
    PlayerStandardStats['Age'] = PlayerStandardStats['Age'].str[:2]
    PlayerStandardStats['Nation'] = PlayerStandardStats['Nation'].str.split(' ').str.get(1)
    PlayerStandardStats = PlayerStandardStats.drop(columns=['Rk', 'Matches'])

    # Drop all the rows that have NaN in the row
    PlayerStandardStats.dropna(inplace=True)

    # Convert all the Data types of the numeric columns from object to numeric
    for col in SquadStandardStats.columns[1:]:
        SquadStandardStats[col] = pd.to_numeric(SquadStandardStats[col], errors='coerce')
    for col in OpponentStandardStats.columns[1:]:
        OpponentStandardStats[col] = pd.to_numeric(OpponentStandardStats[col], errors='coerce')
    for col in PlayerStandardStats.columns[4:]:
        PlayerStandardStats[col] = pd.to_numeric(PlayerStandardStats[col], errors='coerce')


if __name__ == '__main__':
