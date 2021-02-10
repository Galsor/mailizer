from bs4 import BeautifulSoup as soup
import re
from melusine.utils.transformer_scheduler import TransformerScheduler
from src.config import MELUSINE_COLS


def clean_html_body(raw):
    """ Remove HTML tags and special caracteres from email bodies.
    This method can be used in Melusine pipelines"""
    body = raw[MELUSINE_COLS[0]]
    return soup(body, features="html.parser").text

def parse_from(row):
    """
    Preprocessing step used to split Gmail 'from' data into user name and email
    This function can be used in Melusine pipelines
    """
    from_col = row[MELUSINE_COLS[3]]
    from_col = from_col.replace(r'\[.*?$', '')    # Clean suffix
    email = re.findall(r'[A-Za-z0-9-\._]+[@][\w-]+[.]\w+', from_col)[0]
    name = from_col.split(email)[0]
    if "groups.io" in name:
        name = name.split("via groups.io")[0]
    elif '<' in name:
        name = name[:-2]
    return name, email


PrepareEmailDB = TransformerScheduler(
    functions_scheduler=[
        (parse_from, None ,["name", "from"]),
        (clean_html_body, None , ["body"]),
        (lambda x:"hec-entrepreneurs@groups.io", None, ["to"]),
    ]
)

