from fastapi import FastAPI
import sqlite3
from faker import Faker
from pydantic import BaseModel
from fuzzywuzzy import process
import os

app = FastAPI()

# Create a Faker generator
fake = Faker()

# Define the number of rows
num_rows = 5

class BodyRequest(BaseModel):
    tablename: str
    columns: list
    count: int


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
