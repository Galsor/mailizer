
# Prepare email
from melusine.utils.transformer_scheduler import TransformerScheduler
from melusine.prepare_email.manage_transfer_reply import \
    check_mail_begin_by_transfer, update_info_for_transfer_mail, add_boolean_answer, add_boolean_transfer
from melusine.prepare_email.build_historic import build_historic
from melusine.prepare_email.mail_segmenting import structure_email
from melusine.prepare_email.body_header_extraction import extract_last_body, extract_header
from melusine.prepare_email.cleaning import clean_body, clean_header

# Scikit-Learn API
from sklearn.pipeline import Pipeline

# NLP tools
from melusine.nlp_tools.phraser import Phraser
from melusine.nlp_tools.phraser import phraser_on_body
from melusine.nlp_tools.phraser import phraser_on_header
from melusine.nlp_tools.tokenizer import Tokenizer



# Transformer object to manage transfers and replies
ManageTransferReply = TransformerScheduler(
    functions_scheduler=[
        (check_mail_begin_by_transfer, None, ['is_begin_by_transfer']),
        (update_info_for_transfer_mail, None, None),
        (add_boolean_answer, None, ['is_answer']),
        (add_boolean_transfer, None, ['is_transfer'])
    ]
)

# Transformer object to segment the different messages in the email, parse their metadata and
# tag the different part of the messages
Segmenting = TransformerScheduler(
    functions_scheduler=[
        (build_historic, None, ['structured_historic']),
        (structure_email, None, ['structured_body'])
    ]
)

# Transformer object to extract the body of the last message of the email and clean it as
# well as the header
LastBodyHeaderCleaning = TransformerScheduler(
    functions_scheduler=[
        (extract_last_body, None, ['last_body']),
        (clean_body, None, ['clean_body']),
        (clean_header, None, ['clean_header'])
    ]
)

# Transformer object to apply the phraser on the texts
phraser = Phraser()
PhraserTransformer = TransformerScheduler(
    functions_scheduler=[
        (phraser_on_body, (phraser,), ['clean_body']),
        (phraser_on_header, (phraser,), ['clean_header'])
    ]
)

# Tokenizer object
tokenizer = Tokenizer(input_column="clean_body")

# Full preprocessing pipeline
PreprocessingPipeline = Pipeline([
    ('PrepareEmailDB', PrepareEmailDB),
    ('ManageTransferReply', ManageTransferReply),
    ('Segmenting', Segmenting),
    ('LastBodyHeaderCleaning', LastBodyHeaderCleaning),
    ('PhraserTransformer', PhraserTransformer),
    ('tokenizer', tokenizer)
])

def rename_columns()

def run_preprocessing(df):
    """Apply preprocessing pipeline to DataFrame"""
    df = PreprocessingPipeline.fit_transform(df)
    df.to_csv(DATA_PATH/'processed'/"preprocessed_data.csv")
    return df


# Apply MetaData processing pipeline to DataFrame
df_meta = MetadataPipeline.fit_transform(df_emails)

# Keywords extraction
df_emails = keywords_generator.fit_transform(df_emails)

# Train an embedding with the 'clean_body' data
pretrained_embedding.train(df_emails)

# Create a 'clean_text' column from the 'clean_header' and 'clean_body' columns
df_emails['clean_text'] = df_emails['clean_header']+'. '+df_emails['clean_body']

# Create a training set DataFrame with MetaData + the 'clean_text' columns
X = pd.concat([df_emails['clean_text'],df_meta],axis=1)