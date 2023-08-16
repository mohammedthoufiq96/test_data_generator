import re
import sqlite3
from fastapi import FastAPI, Depends, HTTPException, status
import csv
from faker import Faker
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from fuzzywuzzy import process
import os
from fastapi.responses import FileResponse


app = FastAPI()

# Create a Faker generator
fake = Faker("en_GB")

# Define the number of rows
num_rows = 5

# Define the column headers

tablename=""
class BodyRequest(BaseModel):
    tablename:str
    columns: list
    count:int
# Generate and write data to CSV
@app.post("/generatedata/",response_class=HTMLResponse)
async def read_item(body: BodyRequest):
    # headers = ['Name', 'Email', 'Phone', 'Address']
    print("api_in")
    headers_input = body.columns
    count=body.count
    tablename=body.tablename
    tablename=check_word_count(tablename)
    generated_csv_content = [",".join(headers_input)]
    with open(tablename+'.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(headers_input)
        
        for _ in range(count):
            generated_data=[]
            for head in headers_input:
                if(head=="mobilenumber" or head.__contains__("mobile") or head.__contains__("mob")):
                    head="phonenumber"
                closest_match, score = process.extractOne(head, dir(fake))
                print(closest_match)
                print(type(closest_match))
                if hasattr(fake, closest_match):
                    faker_function = getattr(fake, closest_match)
                    if head=="phonenumber":
                        num=generate_custom_phone_number()
                        print(num)
                        generated_data.append(num)
                    else:
                        # generated_data.append(f"'{faker_function()}'")
                        generated_data.append(faker_function())
                # print(generated_data)
                else:
                    generated_data.append("")

            # name = fake.first_name
            # email = fake.email()
            # phone = fake.phone_number()
            # address = fake.address()
            
            # generated_csv_content.append(",".join(generated_data))
            csvwriter.writerow(generated_data)
        # csv_content = "\n".join(generated_csv_content)
        # print(csv_content)
        row = generated_data
        # csvwriter.writerow(row)
        script_path = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_path, tablename+'.csv')
        # link = f'<a href="{file_path}">Click here to access the file</a>'

        return file_path
    


    


@app.post("/generatedatatest/")
async def read_item(body: BodyRequest):
    headers_input = body.columns
    count = body.count
    tablename = body.tablename

    # Connect to the SQLite database
    db_path = os.path.join(os.path.dirname(__file__), "data.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table
    create_table_query = f"CREATE TABLE IF NOT EXISTS {tablename} ({', '.join(headers_input)})"
    cursor.execute(create_table_query)
    conn.commit()

    # Insert data into table
    for _ in range(count):
        generated_data = []
        for head in headers_input:
            if head == "mobilenumber":
                head = "phonenumber"
            closest_match, score = process.extractOne(head, dir(fake))
            if hasattr(fake, closest_match):
                faker_function = getattr(fake, closest_match)
                generated_data.append(f"'{faker_function()}'")
            else:
                generated_data.append("''")

        insert_query = f"INSERT INTO {tablename} VALUES ({', '.join(generated_data)})"
        cursor.execute(insert_query)
        conn.commit()

    conn.close()

    return {"message": "Data inserted into SQLite database"}



def generate_custom_phone_number():
    uk_phone_number = fake.phone_number()
    return uk_phone_number
    # phone_number = fake.numerify(text='9##-###-####')
    # return phone_number 
    # formatted_phone_number = re.sub(r'(\d{1})(\d{2})(\d{3})(\d{3})', r'9\1\2-\3-\4', phone_number)
    # return formatted_phone_number


@app.get("/download_file")
async def download_file(filepath:str):
    print("table_name:"+filepath)
    file_path = filepath
    print(file_path)
    csv_file_path_url = file_path.replace("\\", "/")
    base_url = "https://test-data-generator-u9tl.onrender.com"
    # csv_url = f"{base_url}/download/{csv_file_path_url}"
    print(os.path.basename(csv_file_path_url))
    return FileResponse(file_path, headers={"Content-Disposition": f"attachment; filename={os.path.basename(csv_file_path_url)}"})

    # return {"csv_url": csv_url}
    # return FileResponse(file_path, headers={"Content-Disposition": "attachment; filename=newtesttable.csv"})


@app.get("/get-csv-link", response_class=HTMLResponse)
async def get_csv_link():
    # Path to the CSV file
    csv_file_path = "/opt/render/project/src/newtesttable.csv"

    # Generate the download link
    download_link = f'<a href="/download-csv">Download CSV File</a>'

    return f"<html><body>{download_link}</body></html>"

def check_word_count(input_string):
    if " " in input_string:
        one_string = input_string.replace(" ", "")
        return one_string
    else:
        return input_string

