"""
这个在服务器上面定期运行然后把交易点位储存到数据库里面。
"""

import click
import datetime
from typing import Optional

import s0_md_query as s0
import s5_oi as s5
import s7_oi_stats as s7
import s9_trade_signal as s9
from dsp_config import DATA_DIR, PG_DB_CONF, gen_suffix

FOCUS_ST = ['ts1', 'totp2', 'toss3', 'tosr2', 'sigma1']
FOCUS_SPOT_ST = {
    '159915': [
        'totp2', 'toss3', 'sigma1',
    ],
    '510500': [
        'totp2', 'tosr2', 'sigma1',
    ],
}

def func(spot: str, dt: str, year: int, month: int):
    wide = False
    print(f'calc {spot} {dt}')
    suffix = s0.auto_dl(spot, year=year, month=month, bg_str=dt, ed_str=dt)
    s5.calc_intersect(spot, suffix, wide=wide)
    sufs5 = suffix + '_s5'
    s7.calc_stats_csv(spot, sufs5, wide=wide, show_pos=False)
    sig_df = s9.calc_signal_csv(spot, sufs5, wide=wide)
    
    sig_cols = [x for x in sig_df.columns if x.endswith('_signal')]
    sig_cols_set = {x.replace('_signal', '') for x in sig_cols}
    focus_cols = []
    for prefix in FOCUS_SPOT_ST.get(spot, FOCUS_ST):
        if prefix in sig_cols_set:
            focus_cols.append(f'{prefix}_signal')

    focus_sig = sig_df.loc[(sig_df[focus_cols] != 0).any(axis=1)]
    focus_sig = focus_sig[['dt', 'spot_price', *focus_cols]]
    focus_sig = focus_sig.rename(columns={
        'dt': 'dt',
        'spot_price': 'spot',
        **{col: col.replace('_signal', '') for col in focus_cols}
    })
    focus_cols = [col.replace('_signal', '') for col in focus_cols]
    # print(f'{spot} {dt} signal:')
    # print(focus_sig)
    # calculate sum for each signal
    focus_pos = focus_sig[focus_cols].sum()
    # print(f'{spot} {dt} pos:')
    # print(focus_pos)
    return focus_sig, focus_pos

def main(date: Optional[str] = None):
    if date is None:
        dt = datetime.datetime.now().date()
        dt_str = dt.strftime('%Y%m%d')
    else:
        dt_str = date
    spot_list = ['159915', '510500']
    res = [func(spot, dt_str, None, None) for spot in spot_list]
    transpose_res = list(zip(*res))
    for spot, focus_sig in zip(spot_list, transpose_res[0]):
        focus_sig['code'] = spot
        print(f'{spot} {dt_str} signal:')
        print(focus_sig)
    for spot, focus_pos in zip(spot_list, transpose_res[1]):
        focus_pos['code'] = spot
        print(f'{spot} {dt_str} pos:')
        print(focus_pos)

@click.command()
@click.option('-d', '--date', type=str, default=None)
def click_main(date: Optional[str]):
    main(date=date)

if __name__ == '__main__':
    click_main()
