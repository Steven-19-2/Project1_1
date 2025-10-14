from faker import Faker
import random
from dataclasses import dataclass
from datetime import date, datetime
import csv
from multiprocessing import Pool, cpu_count
from typing import List
import logging

logging.basicConfig(filename='app.log', level=logging.INFO, filemode='a')

logger = logging.getLogger("GenerateData")
@dataclass
class Employee:
    empid: int
    name: str
    salary: float
    salary_date: date

class GenerateData :   
    def __init__(self, total_records: int = 100000):
        #self.employees = []
        self.fake = Faker()
        self.total_records = total_records


    #generating for diff chuncks
    def _generate_chunk(self, start_end):
        start, end = start_end
        fake = Faker()
        employees = []
        for i in range(start, end):
            empid = i + 1
            name = fake.name()
            salary = round(random.uniform(30_000, 150_000), 2)
            salary_date = fake.date_between(start_date='-1y', end_date='today')
            employees.append(Employee(empid, name, salary, salary_date))
        return employees

    def generate_employee_data(self) -> List[Employee] :
        num_cores = cpu_count()
        logger.info("count %d" ,num_cores)
        chunk_size = self.total_records // num_cores

        ranges = [(i * chunk_size, (i + 1) * chunk_size if i < num_cores - 1 else self.total_records)
                  for i in range(num_cores)]

        with Pool(processes=num_cores) as pool:  #create a multiprocessing.Pool object with num_cores worker processes.
            results = pool.map(self._generate_chunk, ranges) #apply funct to each tuple in ranges, runs parallely distributing work across difeerent workers
            #resuls is a list of list 
        # flatten list of lists
        all_employees = [emp for chunk in results for emp in chunk]
        print(f"Finished generating {len(all_employees):,} employees.")
        return all_employees

    
    def save_data_to_file(self, filename:str, employees: List[Employee]):
        with open(filename, mode="w", newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["empid", "name", "salary", "salary_date"])
            for emp in employees:
                writer.writerow([emp.empid, emp.name, emp.salary, emp.salary_date])
        print("File saved")
    
if __name__ == "__main__":
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    generator = GenerateData(total_records=100)
    employees = generator.generate_employee_data()
    # for emp in generator.employees:
    #     print(emp)
    filename = f"employees1_{timestamp}.csv"
    generator.save_data_to_file(filename, employees)
    
            
            