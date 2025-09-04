from pathlib import Path
import os

def get_file_dir():
    fpath = Path(__file__).resolve()
    return fpath.parent

DATA_DIR = get_file_dir() / 'data'

os.makedirs(DATA_DIR / 'input', exist_ok=True)
os.makedirs(DATA_DIR / 'output', exist_ok=True)
os.makedirs(DATA_DIR / 'tmp', exist_ok=True)
