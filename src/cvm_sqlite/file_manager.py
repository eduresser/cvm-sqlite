import os
import tempfile
import requests
import shutil
import zipfile
from typing import List, Optional
from urllib.parse import urlparse

class FileManager:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()

    def download_file(self, url: str) -> Optional[str]:
        file_name = os.path.basename(urlparse(url).path)
        file_path = os.path.join(self.temp_dir, file_name)
        response = requests.get(url)
        if response.status_code == 200:
            with open(file_path, 'wb') as file:
                file.write(response.content)
            return file_path
        return None

    def unzip_file(self, zip_file_path: str) -> List[str]:
        extraction_path = os.path.dirname(zip_file_path)
        extracted_files = []
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                for file in zip_ref.namelist():
                    zip_ref.extract(file, extraction_path)
                    extracted_files.append(os.path.join(extraction_path, file))
            return extracted_files
        except Exception as e:
            print(f"Error unzipping file: {str(e)}")
            return []

    def delete_file(self, file_path: str) -> None:
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Error deleting file: {str(e)}")

    def cleanup(self, remove_temp_dir: bool = False) -> None:
        for root, dirs, files in os.walk(self.temp_dir, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
        
        if remove_temp_dir:
            shutil.rmtree(self.temp_dir)