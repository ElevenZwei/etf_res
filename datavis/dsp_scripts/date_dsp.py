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
import s5_oi as s5
import s6_plot_oi_surf as s6
import s7_oi_stats as s7
import s8_plot_oi_stats as s8
import s9_trade_signal as s9

from dsp_config import gen_suffix, gen_wide_suffix

def fourth_wednesday(year: int, month: int) -> date:
    month_calendar = calendar.monthcalendar(year, month)
    wednesdays = [week[calendar.WEDNESDAY] for week in month_calendar if week[calendar.WEDNESDAY] != 0]
    if len(wednesdays) >= 4:
        return date(year, month, wednesdays[3])
    return None

def default_suffix(bg_str: str, ed_str: str, year: int = None, month: int = None):
    # default expiry date is the fourth wednesday of the month.
    if year is None or month is None:
        year = int(ed_str[:4])
        month = int(ed_str[4:6])
    exp = fourth_wednesday(year, month)
    if exp < datetime.strptime(ed_str, '%Y%m%d').date():
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        exp = fourth_wednesday(year, month)
    date_suffix = bg_str if bg_str == ed_str else f'{bg_str}_{ed_str}'
    suffix = gen_suffix(exp.strftime('%Y%m%d'), date_suffix)
    return suffix

def calc_data(spot: str, suffix: str, wide: bool):
    s1.calc_dsp_surface(spot=spot, suffix=suffix, wide=wide)
    s5.calc_intersect(spot, suffix, wide=wide)
    s5.calc_surface(spot, suffix)
    s7.calc_stats_csv(spot, suffix + '_s5', wide=wide)

    # old method
    # s1.calc_dsp_intersects(spot=spot, suffix=suffix, wide=wide)
    # s2.intersect_merge_files(spot, suffix=suffix, wide=wide)

def plot_data(spot: str, suffix: str, show: bool, save: bool, wide: bool, plot_num: int):
    if plot_num <= 0:
        s3.main(spot, suffix=suffix, show=show, save=save, wide=wide)
        s6.main(spot, suffix=suffix, show=show, save=save)
        s8.main(spot, suffix=suffix + '_s5', show=show, save=save, wide=wide)
    elif plot_num == 1:
        s3.main(spot, suffix=suffix, show=show, save=save, wide=wide)
    elif plot_num == 2:
        s6.main(spot, suffix=suffix, show=show, save=save)
    elif plot_num == 3:
        s8.main(spot, suffix=suffix + '_s5', show=show, save=save, wide=wide)

    # old method
    # s4.main(spot, suffix=suffix + '_s5', show=show, save=save, wide=wide)
    # s4.main(spot, suffix=suffix, show=show, save=save, wide=wide)

def calc_signal(spot: str, suffix: str, wide: bool):
    df = s9.calc_signal_csv(spot, suffix + '_s5', wide=wide)
    print(s9.filter_signal_nonzero(df))

def download_data(spot: str, bg_str: str, ed_str: str, year: int, month: int, minute_bar: bool):
    return s0.auto_dl(spot, year=year, month=month, bg_str=bg_str, ed_str=ed_str, minute_bar=minute_bar)

def date_dsp(spot: str,
             bg_str: str, ed_str: str,
             refresh: bool, plot: int, signal: bool,
             year: int, month: int, minute_bar: bool,
             show: bool, save: bool, wide: bool):
    suffix = default_suffix(bg_str=bg_str, ed_str=ed_str, year=year, month=month)
    if not signal:
        if plot == 0:
            if refresh:
                suffix = download_data(spot, year=year, month=month,
                        bg_str=bg_str, ed_str=ed_str, minute_bar=minute_bar)
            calc_data(spot, suffix, wide=wide)
        plot_data(spot, suffix, show=show, save=save, wide=wide, plot_num=plot)
    calc_signal(spot, suffix, wide=wide)

@click.command()
@click.option('-s', '--spot', type=str, required=True, help="spot code: 159915 510050")
@click.option('-d', '--date', type=str, help="format is %Y%m%d")
@click.option('-e', '--end-date', type=str, help="format is %Y%m%d")
@click.option('-r', '--refresh', is_flag=True, default=False, help="Download new data from database.")
@click.option('-p', '--plot', is_flag=False, default=0, flag_value=-1, help="Plot only, use existing data.")
@click.option('-g', '--signal', is_flag=True, default=False, help="Generate signal only, use existing data.")
@click.option('-y', '--year', type=int)
@click.option('-m', '--month', type=int)
@click.option('--bar', '--minute-bar', is_flag=True, help="download from minute bar table.")
@click.option('--show', type=bool, default=True, help="show plot.")
@click.option('--save', type=bool, default=True, help="save to html.")
@click.option('--wide', type=bool, default=False, help="use wide plot.")
def click_main(spot: str, date: str, end_date: str,
        refresh: bool, plot: int, signal: bool,
        year: int, month: int, bar: bool,
        show: bool, save: bool, wide: bool):
    if end_date is None:
        end_date = date
    date_dsp(spot, bg_str=date, ed_str=end_date,
             refresh=refresh, plot=plot, signal=signal,
             year=year, month=month, minute_bar=bar,
             show=show, save=save, wide=wide)

if __name__ == '__main__':
    click_main()
