import gspread
import numpy as np
import pandas as pd
from src.config import DATASETS

def load_data_from_drive(filename):
    """ Download data from Google Spreadsheet and load it into a DataFrame object"""
    gc = gspread.oauth()
    worksheet = gc.open(filename).sheet1
    # get_all_values gives a list of rows.
    rows = worksheet.get_all_values()
    df = pd.DataFrame(np.array(rows)[1:], columns=np.array(rows[0]))
    return df

def read_file(filepath):
    """
    File loader util
    """
    if self.filename.endswith('.csv'):
        df = pd.read_csv(join(stg.DATA_DIR, self.filename))
    elif self.filename.endswith('.parquet'):
        df = pd.read_parquet(join(stg.DATA_DIR, self.filename))
    else:
        raise NotImplementedError('Your extension is not implemented yet. Prefer parquet or csv.')

    return df


if __name__ == "__main__":

    #Replace with your Google Spreadsheet file name
    filename = DATASETS['email_db']
    df = load_data_from_drive(filename)
    print(df.head())