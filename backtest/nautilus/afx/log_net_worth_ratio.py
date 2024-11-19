# after process for buy_net_worth.csv

import click
import pandas as pd

def process(prefix: str):
    input = f"output/{prefix}_net_worth.csv"
    output = input[:input.rfind('.')]+'_fixed.csv'

    df = pd.read_csv(input)
    df.columns = ['dt', 'net']
    df['gain'] = (df['net'] - 1_000_000) / 10000
    df['dt'] = pd.to_datetime(df['dt'])
    df = df.set_index('dt')
    df = df.resample('1d').first().ffill()
    df.to_csv(output)

@click.command()
@click.option('-p', '--prefix', type=str, help="prefix")
def click_main(prefix: str):
    process(prefix)
    
def main():
    click_main()

if __name__ == '__main__':
    main()