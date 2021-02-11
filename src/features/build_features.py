import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

from src.config import PROCESSED_DATA_DIR, PIPELINE_STEPS, FILE_PREFIX
from src.features.preprocessing_pipeline import PreprocessingPipeline
from src.features.metadata_pipeline import MetadataPipeline
from src.models.keywords import keywords_generator
from src.data.load_data import read_file


@click.command()
@click.argument('input_filepath', type=click.Path(exists=True))
def main(input_filepath):
    
    logger = logging.getLogger(__name__)
    logger.info('Generate features from cleaned data')

    input_filepath = Path(input_filepath)
    output_filename = FILE_PREFIX[Path(__file__).name] + input_filepath.name
    output_filepath = PROCESSED_DATA_DIR / output_filename
    
    if input_filepath.name.startswith(FILE_PREFIX[PIPELINE_STEPS[0]]):
        
        df = read_file(input_filepath)

        # Apply various data augmentations and a tokenizer
        df = PreprocessingPipeline.fit_transform(df)

        # Apply MetaData processing pipeline to DataFrame
        df_meta = MetadataPipeline.fit_transform(df)
        
        # Keywords extraction
        df = keywords_generator.fit_transform(df)
        
        df.to_csv(output_filepath)
        return df
    else:
        logger.error()




if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()