from pathlib import Path
import yaml
import logging

# Project Paths
PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
DATASET_CONFIG_PATH = PROJECT_DIR / "references" / "datasets"
GOOGLE_CREDENTIALS_PATH = "/home/antiez/.config/gspread/credentials.json"


PIPELINE_STEPS = ["make_dataset.py", "build_features.py"]
#Data file prefix following each DAG
FILE_PREFIX = { 
                PIPELINE_STEPS[0]: "clean_", 
                PIPELINE_STEPS[1]: "featurized_",
                }


#Register your dataset in mailizer/references
def read_datasets_config():
    """ Read datasets configurations files in references/datasets and upload their information"""
    logger = logging.getLogger(__name__)
    datasets_config = {}
    paths = DATASET_CONFIG_PATH.glob('**/*')
    files = [x for x in paths if x.is_file()]
    for fp in files:
        with open(fp, 'r') as stream:
            try:
                datasets_config[fp.stem]=(yaml.safe_load(stream))
            except yaml.YAMLError as exc:
                logger.error(exc)
    return datasets_config

DATASETS = read_datasets_config()

# Melusine Config
MELUSINE_COLS = ["body", "header", "date", "from", "to"]

