from abc import ABC, abstractmethod
import boto3
#base class
class DataImporter(ABC):
    @abstractmethod
    def import_data(self, *args, **kwargs):
        pass

class S3Importer(DataImporter):

    def __init__(self, bucket_name):
        self.s3 = boto3.client("s3")
        self.bucket_name = bucket_name

    def import_data(self, *args, **kwargs):
        return super().import_data(*args, **kwargs)

