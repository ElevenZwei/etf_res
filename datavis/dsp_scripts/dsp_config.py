from dataclasses import dataclass, field
from pathlib import Path
import os
import pandas as pd

def get_file_dir():
    fpath = Path(__file__).resolve()
    return fpath.parent

DATA_DIR = get_file_dir() / '..' / 'data'

os.makedirs(f"{DATA_DIR}/dsp_input", exist_ok=True)
os.makedirs(f"{DATA_DIR}/dsp_conv", exist_ok=True)
os.makedirs(f"{DATA_DIR}/dsp_plot", exist_ok=True)
os.makedirs(f"{DATA_DIR}/html_oi", exist_ok=True)
os.makedirs(f"{DATA_DIR}/png_oi", exist_ok=True)
os.makedirs(f"{DATA_DIR}/tmp", exist_ok=True)

def gen_suffix(expiry_date: str, md_date: str):
    return f'exp{expiry_date}_date{md_date}'

def gen_wide_suffix(wide: bool):
    return '_wide' if wide else ''

def plot_dt_str(df: pd.DataFrame, col: str = 'dt'):
    df[col] = pd.to_datetime(df[col]).apply(lambda x: x.strftime("%m-%d %H:%M:%S"))
    return df

@dataclass(frozen=True)
class SpotConfig:
    oi_ts_gaussian_sigmas: list[float] = field(default_factory=list)
    oi_strike_gaussian_sigmas: list[float] = field(default_factory=list)
    oi_strike_gaussian_sigmas_wide: list[float] = field(default_factory=list)
    oi_plot_intersect_zoom: int = 500

    def get_strike_sigmas(self, wide: bool):
        if wide and len(self.oi_strike_gaussian_sigmas_wide) > 0:
            return self.oi_strike_gaussian_sigmas_wide
        else:
            return self.oi_strike_gaussian_sigmas

SPOT_DEFAULT_TS_SIGMAS = [120, 300, 600, 1200]
SPOT_CONFIGS = {
    '159915': SpotConfig(
        oi_ts_gaussian_sigmas=SPOT_DEFAULT_TS_SIGMAS,
        # oi_strike_gaussian_sigmas=[0.05, 0.075, 0.1, 0.15, 0.2],
        oi_strike_gaussian_sigmas=[0.1, 0.15, 0.2, 0.25, 0.3],
        oi_strike_gaussian_sigmas_wide=[0.2, 0.25, 0.3, 0.35, 0.4],
        # oi_strike_gaussian_sigmas_wide=[0.3, 0.4, 0.5, 0.6, 0.8],
        oi_plot_intersect_zoom=1000,
    ),
    '510050': SpotConfig(
        oi_ts_gaussian_sigmas=SPOT_DEFAULT_TS_SIGMAS,
        oi_strike_gaussian_sigmas=[0.075, 0.1, 0.125, 0.15, 0.2],
        oi_plot_intersect_zoom=5000,
    ),
    '510300': SpotConfig(
        oi_ts_gaussian_sigmas=SPOT_DEFAULT_TS_SIGMAS,
        oi_strike_gaussian_sigmas=[0.2, 0.3, 0.4, 0.5],
        oi_plot_intersect_zoom=1000,
    ),
    '510500': SpotConfig(
        oi_ts_gaussian_sigmas=SPOT_DEFAULT_TS_SIGMAS,
        oi_strike_gaussian_sigmas=[0.3, 0.4, 0.5, 0.6],
        oi_plot_intersect_zoom=3000,
    ),
    '588000': SpotConfig(
        oi_ts_gaussian_sigmas=SPOT_DEFAULT_TS_SIGMAS,
        oi_strike_gaussian_sigmas=[0.025, 0.05, 0.075, 0.1, 0.15],
        oi_plot_intersect_zoom=1000,
    ),
    'default': SpotConfig(
        oi_ts_gaussian_sigmas=SPOT_DEFAULT_TS_SIGMAS,
        oi_strike_gaussian_sigmas=[0.3, 0.4, 0.5, 0.6, 0.8],
        oi_plot_intersect_zoom=500,
    ),
}

def get_spot_config(spot: str) -> SpotConfig:
    if spot in SPOT_CONFIGS:
        return SPOT_CONFIGS[spot]
    return SPOT_CONFIGS['default']
