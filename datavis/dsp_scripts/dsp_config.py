from dataclasses import dataclass, field
from pathlib import Path
import os

def get_file_dir():
    fpath = Path(__file__).resolve()
    return fpath.parent

DATA_DIR = get_file_dir() / '..' / 'data'

os.makedirs(f"{DATA_DIR}/dsp_input", exist_ok=True)
os.makedirs(f"{DATA_DIR}/dsp_conv", exist_ok=True)
os.makedirs(f"{DATA_DIR}/dsp_plot", exist_ok=True)
os.makedirs(f"{DATA_DIR}/tmp", exist_ok=True)

@dataclass(frozen=True)
class SpotConfig:
    oi_ts_gaussian_sigmas: list[float] = field(default_factory=list)
    oi_strike_gaussian_sigmas: list[float] = field(default_factory=list)
    oi_plot_intersect_zoom: int = 500

SPOT_DEFAULT_TS_SIGMAS = [120, 300, 600, 1200]
SPOT_CONFIGS = {
    '159915': SpotConfig(
        oi_ts_gaussian_sigmas=SPOT_DEFAULT_TS_SIGMAS,
        # oi_strike_gaussian_sigmas=[0.05, 0.075, 0.1, 0.15, 0.2],
        oi_strike_gaussian_sigmas=[0.1, 0.15, 0.2, 0.25, 0.3],
        # oi_strike_gaussian_sigmas=[0.3, 0.4, 0.5, 0.6, 0.8],
        oi_plot_intersect_zoom=1000,
    ),
    '510050': SpotConfig(
        oi_ts_gaussian_sigmas=SPOT_DEFAULT_TS_SIGMAS,
        oi_strike_gaussian_sigmas=[0.075, 0.1, 0.125, 0.15, 0.2],
        oi_plot_intersect_zoom=5000,
    ),
    '510500': SpotConfig(
        oi_ts_gaussian_sigmas=SPOT_DEFAULT_TS_SIGMAS,
        oi_strike_gaussian_sigmas=[0.4, 0.5, 0.6, 0.8],
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
