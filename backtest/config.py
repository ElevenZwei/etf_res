from pathlib import Path

def get_file_dir():
    fpath = Path(__file__).resolve()
    return fpath.parent

DATA_DIR = get_file_dir() / 'data'
