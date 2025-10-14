from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Employee(Base):
    __tablename__ = "emp_Steven"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))
    salary = Column(Float)
    salary_date = Column(Date)
