from sklearn.pipeline import Pipeline
from melusine.prepare_email.metadata_engineering import MetaDate, MetaExtension, Dummifier

# Pipeline to extract dummified metadata
MetadataPipeline = Pipeline([
    ('MetaExtension', MetaExtension()),
    ('MetaDate', MetaDate()),
    ('Dummifier', Dummifier())
])