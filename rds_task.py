from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import date
from models import Base, Employee
from faker import Faker
import random
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import sessionmaker, declarative_base
import configparser
Base = declarative_base()
class DataImporter(ABC):
    @abstractmethod
    def import_data(self, *args, **kwargs):
        pass


class RDSImporter(DataImporter):
    def __init__(self, db_config):
        self.handler = RDSTableHandler(db_config)

    def import_data(self):
        self.handler.create_table_if_not_exists()
        if self.handler.is_table_empty():
            self.handler.insert_sample_records(10000)
            print("Inserted 10,000 sample records")
        else:
            print("Table already has data. Skipping insertion.")


class RDSTableHandler:
    def __init__(self, config):
        self.database_url = (
            f"{config['dialect']}+{config['driver']}://"
            f"{config['username']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
        )
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
        fake=Faker()
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
    

def main():
    config = configparser.ConfigParser()
    config.read("config.ini")

    import_type = input("Enter import type (S3/RDS): ").strip()

    if import_type == "S3":
        s3_config = config["S3"]
        importer = "S3"
        file_path = input("Enter local file path to upload: ").strip()
        importer.import_data(file_path)



        # optional: view all records
        handler = importer.handler
        employees = handler.get_all()
        print(f"Total records in DB: {len(employees)}")

    else:
        print("Invalid import type. Choose S3 or RDS.")


if __name__ == "__main__":
    main()
