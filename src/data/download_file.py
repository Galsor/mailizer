import io
import pandas as pd
import pickle
import logging
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient.http import MediaIoBaseDownload
from src.config import GOOGLE_CREDENTIALS_PATH, EMAIL_DB, DATA_DIR


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def download_google_spreadsheet(file_id):
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    **Note**: The maximum export size from spreadsheet is 10MB. 
    """
    logger = logging.getLogger(__name__)
    logger.info('Downloading data file from Google Drive...')
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                GOOGLE_CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    drive_service = build('drive', 'v3', credentials=creds)
    
    request = drive_service.files().export(fileId=file_id, mimeType="text/csv").execute()
    if request:
        filepath = DATA_DIR / "raw/raw_data.csv"
        with open(filepath, wb) as f:
            f.write(data)            

        logger.info(f'Raw data downloaded at {filepath}')
    else :
        logger.error('Requested data could not be founded in Google Drive')



if __name__ == '__main__':
    download_google_spreadsheet(EMAIL_DB["id"])