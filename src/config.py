from pathlib import Path
import yaml
import logging

# Project Paths


PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_DIR / "data"
DATASET_CONFIG_PATH = PROJECT_DIR / "references" / "datasets"
GOOGLE_CREDENTIALS_PATH = "/home/antiez/.config/gspread/credentials.json"


#Register your dataset in mailizer/references
def read_datasets_config():
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

DATASETS = read_datasets_config()

# Melusine Config
MELUSINE_COLS = ["body", "header", "date", "from", "to"]

