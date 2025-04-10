"""
This script merges multiple CSV files containing open interest data for a given spot.
It reads all CSV files matching the pattern `strike_oi_oi_{spot}*.csv` in the input directory,
concatenates them into a single DataFrame, sorts the DataFrame by date, removes duplicates,
and saves the merged DataFrame to a new CSV file in the output directory.
"""

import pandas as pd
import glob
import os
import datetime
import click
from pathlib import Path
import sys

sys.path.append((Path(__file__).resolve().parent.parent / 'dsp_scripts').as_posix())
from dsp_config import DATA_DIR

INPUT_DIR = DATA_DIR / 'dsp_input'
OUTPUT_DIR = DATA_DIR / 'oi_output'

def merge(spot: str):
    fs = glob.glob(f"{INPUT_DIR}/strike_oi_oi_{spot}*.csv")
    dfs = [pd.read_csv(f) for f in fs]
    df = pd.concat(dfs, ignore_index=True)
    df['dt'] = pd.to_datetime(df['dt'])
    df = df.sort_values(by=['dt'])
    df = df.drop_duplicates(subset=['dt'], keep='first')
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(f"{OUTPUT_DIR}/oi_{spot}.csv", index=False)
    print(f"merged {len(fs)} files into {OUTPUT_DIR}/strike_oi_oi_{spot}.csv")

merge('510500')
