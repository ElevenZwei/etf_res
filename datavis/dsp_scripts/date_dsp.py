"""
Dsp Pack
"""

import calendar
from datetime import date, datetime
import click
import s0_md_query as s0
import s1_dsp as s1
import s2_dsp_intersect as s2
import s3_plot_dsp_surf as s3
import s4_plot_dsp_inter as s4

from dsp_config import gen_suffix

def fourth_wednesday(year: int, month: int) -> date:
    month_calendar = calendar.monthcalendar(year, month)
    wednesdays = [week[calendar.WEDNESDAY] for week in month_calendar if week[calendar.WEDNESDAY] != 0]
    if len(wednesdays) >= 4:
        return date(year, month, wednesdays[3])
    return None

def default_suffix(date: str, year: int = None, month: int = None):
    # default expiry date is the fourth wednesday of the month.
    if year is None or month is None:
        year = int(date[:4])
        month = int(date[4:6])
    exp = fourth_wednesday(year, month)
    if exp < datetime.strptime(date, '%Y%m%d').date():
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        exp = fourth_wednesday(year, month)
    suffix = gen_suffix(exp.strftime('%Y%m%d'), date)
    return suffix

def download_data(spot: str, date: str, year: int, month: int):
    return s0.auto_dl(spot, year=year, month=month, md_date=date)

def calc_data(spot: str, suffix: str, wide: bool):
    s1.main(spot=spot, suffix=suffix, wide=wide)
    s2.intersect_merge_files(spot, suffix=suffix, wide=wide)

def plot_data(spot: str, suffix: str, show: bool, save: bool, wide: bool):
    s3.main(spot, suffix=suffix, show=show, save=save, wide=wide)
    s4.main(spot, suffix=suffix, show=show, save=save, wide=wide)

def date_dsp(spot: str, date: str,
             refresh: bool, plot: bool,
             year: int, month: int,
             show: bool, save: bool, wide: bool):
    suffix = default_suffix(date, year, month)
    if not plot:
        if refresh:
            suffix = s0.auto_dl(spot, year=year, month=month, md_date=date)
        calc_data(spot, suffix, wide=wide)
    plot_data(spot, suffix, show=show, save=save, wide=wide)

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-d', '--date', type=str, help="format is %Y%m%d")
@click.option('-r', '--refresh', is_flag=True, default=False, help="Download new data from database.")
@click.option('-p', '--plot', is_flag=True, default=False, help="Plot only, use existing data.")
@click.option('-y', '--year', type=int)
@click.option('-m', '--month', type=int)
@click.option('--show', type=bool, default=True, help="show plot.")
@click.option('--save', type=bool, default=True, help="save to html.")
@click.option('--wide', type=bool, default=False, help="use wide plot.")
def click_main(spot: str, date: str,
        refresh: bool, plot: bool,
        year: int, month: int,
        show: bool, save: bool, wide: bool):
    date_dsp(spot, date,
             refresh=refresh, plot=plot,
             year=year, month=month,
             show=show, save=save, wide=wide)

if __name__ == '__main__':
    click_main()
