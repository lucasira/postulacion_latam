from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import zipfile
import os

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

class JsonMaker:
    def __init__(self, creds):
        self.creds = creds
        self.jsonfile = None

    def downloadfile(self, file_id, zip_filename='downloaded_file.zip', extract_filename=None):
        try:
            # Check if the zip file already exists
            if os.path.exists(zip_filename):
                print(f"Zip file '{zip_filename}' already exists. Skipping download.")
            else:
                # Load credentials from the service account file
                creds = service_account.Credentials.from_service_account_file(self.creds, scopes=SCOPES)

                service = build('drive', 'v3', credentials=creds)

                request = service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                    print(f"Download {int(status.progress() * 100)}%.")

                fh.seek(0)
                
                # Write the content to the specified local zip file
                with open(zip_filename, 'wb') as f:
                    f.write(fh.getvalue())

                print(f"Zip file '{zip_filename}' downloaded successfully.")

            # Check if the file exists before attempting to unzip
            if not os.path.exists(zip_filename):
                print(f"Error: The file '{zip_filename}' does not exist. Cannot proceed with extraction.")
                return

            # Create an extraction directory based on the zip filename
            extraction_dir = os.path.splitext(zip_filename)[0]
            
            # Check if the extraction directory already exists
            if os.path.exists(extraction_dir):
                print(f"Extraction directory '{extraction_dir}' already exists. Skipping extraction.")
            else:
                # Extract the contents of the zip file
                with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                    if extract_filename:
                        # Extract only the specified file
                        try:
                            zip_ref.extract(extract_filename, extraction_dir)
                            print(f"File '{extract_filename}' extracted successfully to '{extraction_dir}'.")
                        except KeyError:
                            print(f"Error: File '{extract_filename}' not found in the zip archive.")
                            return
                    else:
                        # Extract all contents
                        zip_ref.extractall(extraction_dir)
                        print(f"All zip file contents extracted successfully to '{extraction_dir}'.")

            # List the extracted files
            print("Extracted files:")
            extracted_files = os.listdir(extraction_dir)
            for file in extracted_files:
                print(file)

            # Accessing the JSON file
            if extract_filename:
                json_filename = os.path.join(extraction_dir, extract_filename)
            else:
                # If no specific file was extracted, use the first file in the directory
                json_filename = os.path.join(extraction_dir, extracted_files[0])

            if os.path.exists(json_filename):
                with open(json_filename, 'r') as json_file:
                    self.jsonfile = json_file.read()
                    print("JSON content:", self.jsonfile[:100])  # Print first 100 characters
            else:
                print(f"Error: The JSON file '{json_filename}' does not exist.")

        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == '__main__':
    jsonMaker = JsonMaker('brave-dimension-439021-g0-4d414cb48451.json')
    jsonMaker.downloadfile('1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis', 'my_custom_filename.zip', 'specific_file.json')