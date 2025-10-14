import boto3
from datetime import datetime
import os
from create_config import read_config

class FileUpload:
    def __init__(self, bucket_name):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
    
    def upload_parquet(self, file_path):

        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return

        #Extract filename and extension
        base_name = os.path.basename(file_path)
        name, ext = os.path.splitext(base_name)

        # Add timestamp for S3 version
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_with_timestamp = f"{name}_{timestamp}{ext}"

        # S3 folder and key
        folder_name = "Steven"
        s3_key = f"{folder_name}/{file_with_timestamp}"

        try:
            # Upload file to S3
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            print(f"Uploaded successfully to: s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Upload failed: {e}")


if __name__ == "__main__":
    config_data = read_config()
    BUCKET_NAME = config_data['Aws']['bucket_name']
    FILE_PATH = r'C:\Users\ZML-WIN-StevenD-01\Desktop\Project1_1\employee_data.parquet'
    
    uploader = FileUpload(BUCKET_NAME)
    uploader.upload_parquet(FILE_PATH)
