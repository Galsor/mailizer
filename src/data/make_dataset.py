# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

from src.data.data_prep_pipeline import PrepareEmailDB
from src.data.load_data import read_file
from src.config import DATASETS, MELUSINE_COLS, RAW_DATA_DIR, PROCESSED_DATA_DIR, DATASET_CONFIG_PATH


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
def main(input_filepath,):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('Prepare data set from raw data')
    
    input_filepath = Path(input_filepath)
    file_stem = input_filepath.stem
    config_filepath = (DATASET_CONFIG_PATH / (file_stem+".yml"))
    output_filename = f"clean_{input_filepath.name}"

    if config_filepath.is_file():
        config = DATASETS[file_stem]
        
        df = read_file(input_filepath)
        renamed_df = rename_columns(df, config)
        clean_df = PrepareEmailDB.fit_transform(renamed_df)
        
        clean_df.to_csv(PROCESSED_DATA_DIR / output_filename)
        return clean_df
    else:
        logger.error(f"No config file found for {file_stem}. Rename your file or add a dataset configuration in references/datasets.")


def rename_columns(df, config):
    new_cols_names = {c: MELUSINE_COLS[i] for i, c in enumerate(config["melusine_columns"])}
    return df.rename(new_cols_names, axis=1)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
