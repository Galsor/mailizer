# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

from src.data.data_prep_pipeline import PrepareEmailDB
from src.data.load_data import read_csv
from src.config import DATASETS, MELUSINE_COLS, DATA_DIR


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
@click.argument('dataset_yml_config', type=click.Path(exists=True))
def main(input_filepath, dataset_yml_config):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('Prepare data set from raw data')
    
    config = DATASETS[Path(dataset_yml_config).stem]
    df = read_file(input_filepath)
    renamed_df = rename_columns(df, config)
    clean_df = PrepareEmailDB.fit_transform(renamed_df)
    new_filename = f"clean_{input_filepath.name}"
    clean_df.to_csv(DATA_DIR / "processed" / new_filename)
    return clean_df


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
