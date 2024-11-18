import glob
import pandas as pd
import tqdm

def convert_mdt_df(df: pd.DataFrame):
    df = df.rename(columns={
        'time': 'dt',
        'name': 'code',
        'ask1': 'ask_price',
        'asize1': 'ask_size',
        'bid1': 'bid_price',
        'bsize1': 'bid_size',
        'ask2': 'ask_2_price',
        'asize2': 'ask_2_size',
        'bid2': 'bid_2_price',
        'bsize2': 'bid_2_size',
        'last': 'last_price',
        'position': 'open_interest',
    })
    df['code'] = df['code'].apply(lambda x:
            x[:-3] if (x.endswith('.SH') or x.endswith('.SZ')) else x)
    df['tick_num'] = -1
    df['ask_size'] = df['ask_size'].astype('Int64')
    df['ask_2_size'] = df['ask_2_size'].astype('Int64')
    df['bid_size'] = df['bid_size'].astype('Int64')
    df['bid_2_size'] = df['bid_2_size'].astype('Int64')
    df['open_interest'] = df['open_interest'].astype('Int64')
    df['volume'] = df['volume'].astype('Int64')
    df = df[['dt', 'code',
            'ask_price', 'ask_size',
            'bid_price', 'bid_size',
            'ask_2_price', 'ask_2_size',
            'bid_2_price', 'bid_2_size',
            'last_price', 'open_interest', 'volume', 'tick_num'
    ]]
    return df

def convert_mdt(fname):
    df = pd.read_csv(f'./wind/{fname}')
    df = convert_mdt_df(df)
    df.to_csv(f'../db/{fname}', index=False)

def batch_convert():
    fs = glob.glob('./wind/mdt_*.csv')
    for fpath in tqdm.tqdm(fs):
        fname = fpath.split('\\')[-1]
        convert_mdt(fname)

if __name__ == '__main__':
    batch_convert()

    
