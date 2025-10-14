import boto3
from abc import ABC, abstractmethod
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import date
import configparser
from faker import Faker
import random

Base = declarative_base()

class Employee(Base):
    __tablename__ = "emp_Steven"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))
    salary = Column(Float)
    salary_date = Column(Date)

class DataImporter(ABC):
    @abstractmethod
    def import_data(self, *args, **kwargs):
        pass



class RDSTableHandler:
    def __init__(self, config):
        self.database_url = (
            f"{config['dialect']}+{config['driver']}://"
            f"{config['username']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
        )
        print(f"Connecting to DB: {self.database_url}")
        self.engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)  # create table if not exists

    def is_table_empty(self):
        session = self.Session()
        count = session.query(Employee).count()
        session.close()
        return count == 0

    def insert_sample_records(self, n=10000):
        session = self.Session()
        fake = Faker()
        employees = [
            Employee(name=fake.name(), salary=round(random.uniform(30_000, 150_000), 2), salary_date=fake.date_between(start_date='-1y', end_date='today'))
            for i in range(n)
        ]
        session.bulk_save_objects(employees)
        session.commit()
        session.close()

    def get_all(self):
        session = self.Session()
        employees = session.query(Employee).all()
        session.close()
        return employees


# ------------------ RDS Importer ------------------
class RDSImporter(DataImporter):
    def __init__(self, db_config):
        self.handler = RDSTableHandler(db_config)

    def import_data(self):
        self.handler.create_table_if_not_exists()
        if self.handler.is_table_empty():
            print("Inserting sample records into RDS...")
            self.handler.insert_sample_records(10000)
            print("Inserted 10,000 sample records successfully.")
        else:
            print("Table already has data. Skipping insertion.")


# ------------------ S3 Importer ------------------
class S3Importer(DataImporter):
    def __init__(self, bucket_name, aws_access_key_id, aws_secret_access_key, region):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )
        self.bucket_name = bucket_name

    def import_data(self, file_path, s3_key=None):
        s3_key = s3_key or file_path.split("/")[-1]
        self.s3.upload_file(file_path, self.bucket_name, s3_key)
        print(f"File '{file_path}' uploaded to S3 bucket '{self.bucket_name}' as '{s3_key}'")


# ------------------ Factory ------------------
class ImporterFactory:
    @staticmethod
    def get_importer(import_type, config):
        if import_type == "S3":
            return S3Importer(
                config["bucket_name"],
                config["aws_access_key_id"],
                config["aws_secret_access_key"],
                config["region"]
            )
        elif import_type == "RDS":
            return RDSImporter(config)
        else:
            raise ValueError("Invalid import_type. Use 'S3' or 'RDS'.")


# ------------------ Main ------------------
def main():
    config = configparser.ConfigParser()
    config.read("config.ini")

    import_type = input("Enter import type (S3/RDS): ").strip()

    if import_type == "S3":
        s3_config = config["S3"]
        importer = ImporterFactory.get_importer("S3", s3_config)
        file_path = input("Enter local file path to upload: ").strip()
        importer.import_data(file_path)

    elif import_type == "RDS":
        rds_config = config["RDS"]
        importer = ImporterFactory.get_importer("RDS", rds_config)
        importer.import_data()

        # optional: view all records
        handler = importer.handler
        employees = handler.get_all()
        print(f"Total records in DB: {len(employees)}")

    else:
        print("Invalid import type. Choose S3 or RDS.")


if __name__ == "__main__":
    main()
