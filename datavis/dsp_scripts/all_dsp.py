"""
Dsp Pack
"""

import click
import s0_md_query as s0
import s1_dsp as s1
import s2_dsp_intersect as s2
import s3_plot_dsp_surf as s3
import s4_plot_dsp_inter as s4

def all_dsp(spot: str, suffix: str, refresh: bool, year: int, month: int):
    if refresh:
        s0.auto_dl(spot, year=year, month=month, md_date=suffix)
    s1.main(spot=spot, suffix=suffix)
    s2.intersect_merge_files(spot, suffix)
    s3.main(spot, suffix)
    s4.main(spot, suffix)

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-d', '--date', type=str, help="format is %Y%m%d")
@click.option('-r', '--refresh', is_flag=True, default=False, help="Download new data from database.")
@click.option('-y', '--year', type=int)
@click.option('-m', '--month', type=int)
def click_main(spot: str, date: str, refresh: bool, year: int, month: int):
    all_dsp(spot, date, refresh, year, month)

if __name__ == '__main__':
    click_main()
