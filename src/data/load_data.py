import numpy as np
import pandas as pd
from pathlib import Path



def read_file(filepath):
    """
    File loader util
    """
    if isinstance(filepath, str):
        filepath = Path(filepath)

    if filepath.suffix == '.csv':
        df = pd.read_csv(filepath, header=0)
    elif filepath.suffix == '.parquet':
        df = pd.read_parquet(filepath)
    else:
        raise NotImplementedError('Your extension is not implemented yet. Prefer parquet or csv.')

    return df
