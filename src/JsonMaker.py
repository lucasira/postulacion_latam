from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import zipfile
import os

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

class JsonMaker:
    def __init__(self):
        self.jsonfile = None

    def downloadfile(self):
        try:
            # Load credentials from the service account file
            creds = service_account.Credentials.from_service_account_file(
                'brave-dimension-439021-g0-4d414cb48451.json', scopes=SCOPES)

            service = build('drive', 'v3', credentials=creds)

            # Replace with the actual ID of your zip file in Google Drive
            file_id = '1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis'
            
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                print(f"Download {int(status.progress() * 100)}%.")

            fh.seek(0)
            
            # Write the content to a local zip file
            with open('downloaded_file.zip', 'wb') as f:
                f.write(fh.getvalue())

            print("Zip file downloaded successfully.")

            # Extract the contents of the zip file
            with zipfile.ZipFile('downloaded_file.zip', 'r') as zip_ref:
                zip_ref.extractall('extracted_files')

            print("Zip file contents extracted successfully.")

            # Optionally, list the extracted files
            print("Extracted files:")
            for filename in os.listdir('extracted_files'):
                print(filename)
                file = filename

            # Accessing the JSON file directly if the name is known
            json_filename = file  # Replace with actual JSON file name if known
            with open(f'extracted_files/{json_filename}', 'r') as json_file:
                self.jsonfile = json_file.read()
                print("JSON content:", self.jsonfile[:100])  # Print first 100 characters
        except Exception as e:
            print(f"An error occurred: {e}")

    def GetFile(self):
        # Load credentials from the service account file
        creds = service_account.Credentials.from_service_account_file(
            'brave-dimension-439021-g0-4d414cb48451.json', scopes=SCOPES)

        service = build('drive', 'v3', credentials=creds)

        # Replace with the actual ID of your zip file in Google Drive
        file_id = '1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis'
        
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%.")

        fh.seek(0)
        
        # Write the content to a local zip file
        with open('downloaded_file.zip', 'wb') as f:
            f.write(fh.getvalue())

        print("Zip file downloaded successfully.")

        # Extract the contents of the zip file
        with zipfile.ZipFile('downloaded_file.zip', 'r') as zip_ref:
            zip_ref.extractall('extracted_files')

        print("Zip file contents extracted successfully.")

        # Optionally, you can list the extracted files
        print("Extracted files:")
        for filename in os.listdir('extracted_files'):
            print(filename)

        # If you know the exact name of the JSON file inside the zip, you can access it directly
        # For example, if the JSON file is named 'data.json':
        with open(f'extracted_files/{filename}', 'r') as json_file:
            self.jsonfile = json_file.read()
            print("JSON content:", self.jsonfile[:100])  # Print first 100 characters

if __name__ == '__main__':
    jsonMaker = JsonMaker()
    jsonMaker.downloadfile()

