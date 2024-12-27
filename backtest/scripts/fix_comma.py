import pandas as pd
import click

def fix_comma(file_path: str):
    df = pd.read_csv(file_path, thousands=',')
    df.to_csv(file_path, index=False)

@click.command()
@click.option('-f', '--file_path', type=str, help='file path')
def click_main(file_path: str):
    fix_comma(file_path)

if __name__ == '__main__':
    click_main()
