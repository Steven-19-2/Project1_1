from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from rds_task import RDSTableHandler
import configparser
from datetime import date
from sqlalchemy import select
from sqlalchemy.orm import Session
from models import Employee

# ------------------ Load RDS config ------------------
config = configparser.ConfigParser()
config.read("config.ini")
rds_config = config["RDS"]

# Initialize table handler
handler = RDSTableHandler(rds_config)

# ------------------ FastAPI app ------------------
app = FastAPI(title="RDS Employee CRUD API")

# ------------------ Pydantic models ------------------
class EmployeeCreate(BaseModel):
    name: str
    salary: float
    salary_date: date

class EmployeeUpdate(BaseModel):
    name: str | None = None
    salary: float | None = None
    salary_date: date | None = None

class EmployeeOut(BaseModel):
    id: int
    name: str
    salary: float
    salary_date: date

    class Config:
        orm_mode = True

# ------------------ CRUD Endpoints ------------------

@app.get("/items", response_model=List[EmployeeOut])
def get_all_items(limit=10, offset=0):
   
    session: Session = handler.Session()
    stmt = select(Employee).limit(limit)
    employees = session.execute(stmt).scalars().all()
    return employees


@app.get("/items/{emp_id}", response_model=EmployeeOut)
def get_item(emp_id: int):
    session = handler.Session()
    emp = session.query(handler.Employee).get(emp_id)
    session.close()
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    return emp

@app.post("/items", response_model=EmployeeOut)
def create_item(emp_data: EmployeeCreate):
    session = handler.Session()
    new_emp = handler.Employee(
        name=emp_data.name,
        salary=emp_data.salary,
        salary_date=emp_data.salary_date
    )
    session.add(new_emp)
    session.commit()
    session.refresh(new_emp)
    session.close()
    return new_emp

@app.put("/items/{emp_id}", response_model=EmployeeOut)
def update_item(emp_id: int, emp_data: EmployeeUpdate):
    session = handler.Session()
    emp = session.query(handler.Employee).get(emp_id)
    if not emp:
        session.close()
        raise HTTPException(status_code=404, detail="Employee not found")
    
    if emp_data.name is not None:
        emp.name = emp_data.name
    if emp_data.salary is not None:
        emp.salary = emp_data.salary
    if emp_data.salary_date is not None:
        emp.salary_date = emp_data.salary_date

    session.commit()
    session.refresh(emp)
    session.close()
    return emp

@app.delete("/items/{emp_id}")
def delete_item(emp_id: int):
    session = handler.Session()
    emp = session.query(handler.Employee).get(emp_id)
    if not emp:
        session.close()
        raise HTTPException(status_code=404, detail="Employee not found")
    
    session.delete(emp)
    session.commit()
    session.close()
    return {"detail": f"Employee {emp_id} deleted successfully"}
