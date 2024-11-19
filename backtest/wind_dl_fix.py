import tqdm
import pandas as pd
import glob

def fix_cifs():
    cifs = glob.glob('./db/ci_*.csv')
    for cif in cifs:
        df = pd.read_csv(cif)
        df['contractunit'] = df['contractunit'].astype(int)
        df.to_csv(cif, index=False)

def fix_mdfs():
    mdfs = glob.glob('./db/md_*.csv')
    for mdf in tqdm.tqdm(mdfs):
        df = pd.read_csv(mdf)
        df['volume'] = df['volume'].astype(int)
        df['openinterest'] = df['openinterest'].astype(int)
        df.to_csv(mdf, index=False)

if __name__ == '__main__':
    fix_mdfs()
