from googleapiclient.http import MediaFileUpload
from Google import Create_Service


def update_file():
    CLIENT_SECRET_FILE = 'client_secret.json'
    API_NAME = 'drive'
    API_VERSION = 'v3'
    SCOPES = ['https://www.googleapis.com/auth/drive']

    service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

    #Upload a file
    file_id = '1eLJxTjDVw6AXdK21YgOMyirNAo53R99N'

    media_content = MediaFileUpload('csv_data/live.csv', mimetype='text/csv')

    service.files().update(
        fileId=file_id,
        media_body=media_content
    ).execute()

