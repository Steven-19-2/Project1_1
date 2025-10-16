import configparser
from rds_task import RDSImporter  # RDS logic
from s3_task import main as s3_main  # use s3_task for data generation & upload
from upload_file import main as s3_upload_main, FileUpload
# ------------------ Abstract Base ------------------
from abc import ABC, abstractmethod

class DataImporter(ABC):
    @abstractmethod
    def import_data(self, *args, **kwargs):
        pass

# ------------------ Factory ------------------
class ImporterFactory:
    @staticmethod
    def get_importer(import_type, config):
        if import_type == "S3":
            # For S3, we just call the s3_task main function
            return S3Wrapper(config)
        elif import_type == "RDS":
            return RDSImporter(config)
        else:
            raise ValueError("Invalid import_type. Use 'S3' or 'RDS'.")

# ------------------ S3 Wrapper ------------------
class S3Wrapper(DataImporter):
    """
    Wrapper around s3_task.main() to comply with DataImporter interface.
    """
    def __init__(self, config):
        self.config = config
        self.bucket_name = config['bucket_name']

    def import_data(self, file_path=None):
        # If file_path is provided, you can modify s3_task to accept it
        print("Generating and uploading data to S3 via s3_task.py ...")
        parquet_file= s3_main()  # currently your s3_task generates 1M rows and outputs CSV & Parquet
        print(f"Uploading generated Parquet to S3: {parquet_file}")
        uploader = FileUpload(self.bucket_name)
        uploader.upload_parquet(parquet_file)

# ------------------ Main ------------------
def main():
    config = configparser.ConfigParser()
    config.read("config.ini")

    import_type = input("Enter import type (S3/RDS): ").strip()

    if import_type not in ["S3", "RDS"]:
        print("Invalid import type. Choose S3 or RDS.")
        return

    conf_section = config[import_type]
    importer = ImporterFactory.get_importer(import_type, conf_section)
    importer.import_data()

    if import_type == "RDS":
        # optional: view all records
        employees = importer.handler.get_all()
        print(f"Total records in RDS DB: {len(employees)}")


if __name__ == "__main__":
    main()
