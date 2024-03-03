#%%

import pandas as pd
from datetime import datetime
import os
from tqdm import tqdm

def read_data(url):
    try:
        df = pd.read_csv(url)
        return df
    except Exception as e:
        print(f"Error reading URL {url}: {e}")
        return None

def concatenate_dataframes(dfs):
    return pd.concat(dfs, ignore_index=True)

def rename_columns(df):
    df = df[["Div", "Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR", "B365H", "B365D", "B365A", "B365>2.5", "B365<2.5"]]
    df.columns = ["League", "Date", "Home", "Away", "Goals_H", "Goals_A", "Result", "Odd_H", "Odd_D", "Odd_A", "Odd_Over25", "Odd_Under25"]
    return df

def map_league_names(df):
    league_mapping = {
        "E0": "England 1",
        "E1": "England 2",
        "SP1": "Spain 1",
        "SP2": "Spain 2",
        "D1": "Germany 1",
        "D2": "Germany 2",
        "I1": "Italy 1",
        "I2": "Italy 2",
        "F1": "France 1",
        "F2": "France 2",
        'N1': "Netherlands 1", 
        'B1': "Belgium 1",
        "P1" : "Portugal 1"
    }
    df["League"] = df["League"].replace(league_mapping)
    return df

def process_date_column(df):
    df["Date"] = pd.to_datetime(df["Date"], format='%d/%m/%Y')
    df["Date"] = df["Date"].dt.date
    df = df.sort_values(by="Date")
    return df

def reset_and_reindex(df):
    df = df.reset_index(drop=True)
    df.index += 1
    return df

def save_to_parquet(df, filename):
    if not os.path.exists("data"):
        os.makedirs("data")
    
    filepath = os.path.join("data", filename)
    df.to_parquet(filepath, index=False, engine='pyarrow')
    print(f"DataFrame saved to {filepath} in Parquet format.")

def main():
    seasons = ["1920", "2021", "2122", "2223", "2324"]
    leagues = ["E0", "E1", "SP1", "SP2", "D1", "D2", "I1", "I2", "F1", "F2", 'N1', 'B1', "P1"]
    dfs = []

    for season in tqdm(seasons, desc="Processing"):
        for league in leagues:
            url = f"https://www.football-data.co.uk/mmz4281/{season}/{league}.csv"
            df = read_data(url)
            if df is not None:
                dfs.append(df)

    df = concatenate_dataframes(dfs)
    df = rename_columns(df)
    df = map_league_names(df)
    df = process_date_column(df)
    df = reset_and_reindex(df)

    return df

if __name__ == "__main__":
    final_df = main()
    print(final_df)

final_df = main()
current_date = datetime.today().strftime('%Y-%m-%d')
filename_with_date = f"football_data_{current_date}.parquet"
save_to_parquet(final_df, filename_with_date)
