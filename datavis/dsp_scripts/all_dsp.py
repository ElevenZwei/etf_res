"""
Dsp Pack
"""

import click
import s0_md_query as s0
import s1_dsp as s1
import s2_dsp_intersect as s2
import s3_plot_dsp_surf as s3
import s4_plot_dsp_inter as s4

def all_dsp(spot: str, suffix: str, refresh: bool):
    if refresh:
        s0.auto_dl(spot, md_date=suffix)
    s1.main(spot=spot, suffix=suffix)
    s2.intersect_merge_files(spot, suffix)
    s3.main(spot, suffix)
    s4.main(spot, suffix)

@click.command()
@click.option('-s', '--spot', type=str, help="spot code: 159915 510050")
@click.option('-d', '--suffix', type=str, help="csv file name suffix.")
@click.option('-r', '--refresh', is_flag=True, default=False, help="Download new data from database.")
def click_main(spot: str, suffix: str, refresh: bool):
    all_dsp(spot, suffix, refresh)

if __name__ == '__main__':
    click_main()
