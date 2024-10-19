from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import zipfile
import os
import shutil
from config import FILE_ID, CREDS, ZIP_FILENAME, JSON_FILENAME

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

class JsonMaker:
    def __init__(self):
        self.creds = CREDS
        self.file_id = FILE_ID
        self.zip_filename = ZIP_FILENAME
        self.json_filename = JSON_FILENAME
        self.jsonfile = None
        self.service = None
        self.process_file()


    def authenticate(self):
        creds = service_account.Credentials.from_service_account_file(self.creds, scopes=SCOPES)
        self.service = build('drive', 'v3', credentials=creds)

    def download_file(self):
        if not self.service:
            self.authenticate()

        request = self.service.files().get_media(fileId=self.file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Descarga {int(status.progress() * 100)}%.")

        fh.seek(0)
        
        with open(self.zip_filename, 'wb') as f:
            f.write(fh.getvalue())

        print(f"Archivo zip '{self.zip_filename}' descargado exitosamente.")

    def extract_and_rename(self):
        temp_dir = 'temp_extraction'
        os.makedirs(temp_dir, exist_ok=True)
        
        with zipfile.ZipFile(self.zip_filename, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        
        extracted_files = os.listdir(temp_dir)
        if not extracted_files:
            print("Error: No se encontraron archivos en el archivo zip.")
            return False
        
        # Asume que el primer archivo es el que queremos
        source_file = os.path.join(temp_dir, extracted_files[0])
        
        # Renombrar el archivo
        shutil.move(source_file, self.json_filename)
        
        # Limpia temp_dir
        shutil.rmtree(temp_dir)
        
        print(f"Archivo extraído y renombrado a '{self.json_filename}'.")
        return True


    def process_file(self):
        try:
            if not os.path.exists(self.json_filename):
                if not os.path.exists(self.zip_filename):
                    print(f"Archivo zip '{self.zip_filename}' no encontrado. Descargando...")
                    self.download_file()
                
                print(f"Extrayendo y renombrando archivo de '{self.zip_filename}'...")
                if not self.extract_and_rename():
                    return
            else:
                print(f"El archivo '{self.json_filename}' ya existe. No es necesario descargar o extraer.")


        except Exception as e:
            print(f"Ocurrió un error: {e}")

if __name__ == '__main__':
    jsonMaker = JsonMaker()
    # jsonMaker.process_file(FILE_ID, ZIP_FILENAME, JSON_FILENAME)