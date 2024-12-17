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

from dataclasses import dataclass

@dataclass
class SpotConfig:
    oi_strike_gaussian_deltas = []
    oi_plot_intersect_zoom = 500

SPOT_CONFIGS = {
    '159915': SpotConfig(
        oi_strike_gaussian_deltas=[0.3, 0.4, 0.5, 0.6, 0.8],
        oi_plot_intersect_zoom=500,
    ),
    '510050': SpotConfig(
        oi_strike_gaussian_deltas=[0.2, 0.3, 0.4, 0.5],
        oi_plot_intersect_zoom=1000,
    ),
    '510500': SpotConfig(
        oi_strike_gaussian_deltas=[0.5, 0.6, 0.8, 1],
        oi_plot_intersect_zoom=3000,
    ),
    'default': SpotConfig(
        oi_strike_gaussian_deltas=[0.3, 0.4, 0.5, 0.6, 0.8],
        oi_plot_intersect_zoom=500,
    ),
}

def get_spot_config(spot: str) -> SpotConfig:
    if spot in SPOT_CONFIGS:
        return SPOT_CONFIGS[spot]
    return SPOT_CONFIGS['default']
