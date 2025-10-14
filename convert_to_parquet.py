import pandas as pd
from dataclasses import dataclass
   
@dataclass
class CovertToParquet:
    def convert(self):
        df = pd.read_csv('employees1_2025-10-14_19-01-20.csv')
        df.to_parquet('employee_data1_1.parquet', index=False)
        print("CSV file has been converted to Parquet format and saved as 'employee_data1.parquet'")


if __name__ == "__main__":
    converter = CovertToParquet()
    converter.convert()

