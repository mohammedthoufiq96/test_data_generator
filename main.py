import re
from fastapi import FastAPI, Depends, HTTPException, status
import csv
from faker import Faker
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from fuzzywuzzy import process
import os
from fastapi.responses import FileResponse
import mysql.connector
from mysql.connector import Error
import csv


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
    filetype:str
    textformat:str
    # columnswithdatabase:list
# Generate and write data to CSV
@app.post("/generatedata/",response_class=HTMLResponse)
async def read_item(body: BodyRequest):
    # headers = ['Name', 'Email', 'Phone', 'Address']
    print("api_in")
    headers_input = body.columns
    count=body.count
    tablename=body.tablename
    tablename=check_word_count(tablename)
    textformat=body.textformat
    
    generated_csv_content = [",".join(headers_input)]
    # with open(tablename+'.csv', 'w', newline='') as csvfile:
    #     csvwriter = csv.writer(csvfile)
    #     csvwriter.writerow(headers_input)

    filename = tablename + '.'+body.filetype.lower() # Change the extension based on your desired file format
    data = headers_input  # Data you want to write
    print(data)
    with open(filename, 'w', newline='') as file:
        if filename.endswith('.csv'):
            import csv
            csvwriter = csv.writer(file)
            csvwriter.writerow(data)
        elif filename.endswith('.txt'):
            import csv
            if(textformat.lower().__contains__("csv")):
                 csv_writer = csv.writer(file)
                 csv_writer.writerow(data)
            elif textformat.lower().__contains__("tsv"):
                file.write('\t'.join(map(str, data)) + '\n')

        # For plain text files, you can simply write the data directly
            # file.write('\t'.join(data))  # Example: Tab-separated values
        elif filename.endswith('.json'):
            import json
            # json_data = [{data: row[i] for i in range(len(data))}
            # for row in data[1:]
            # ]
            # json_string = json.dumps(json_data, indent=2)
            # file.write(json_string)

            json.dump(data, file)
    # Add more conditions for other file formats as needed
        else:
            raise ValueError(f"Unsupported file format: {filename}")

        
        for _ in range(count):
            generated_data=[]
            for head in headers_input:
                if(head=="mobilenumber" or head.__contains__("mobile") or head.__contains__("mob")):
                    head="phonenumber"
                closest_match, score = process.extractOne(head, dir(fake))
                # print(closest_match)
                # print(type(closest_match))
                if hasattr(fake, closest_match):
                    faker_function = getattr(fake, closest_match)
                    if head=="phonenumber":
                        num=generate_custom_phone_number()
                        # print(num)
                        generated_data.append(num)
                    else:
                        # generated_data.append(f"'{faker_function()}'")
                        generated_data.append(faker_function())
                # print(generated_data)
                else:
                    generated_data.append("")
            # print(generated_data)
            # finaldata=headers_input.append(generated_data)
            # print(finaldata)
            final_data=[]
            final_data.extend(generated_data)
            if filename.endswith('.csv'):
                csvwriter.writerow(generated_data)
            elif filename.endswith('.txt'):
                print("generting data")
                print("textformat:"+textformat)
                if(textformat.lower().__contains__("csv")):
                #  csv_writer = csv.writer(file)
                 csv_writer.writerow(generated_data)
                elif textformat=="tsv":
                # tsv_writer = csv.writer(file, delimiter='\t')
                # tsv_writer.writerows(generated_data)  
                    print("generting data")
                    print(generated_data)
                    file.write('\t'.join(map(str, generated_data)) + '\n')
                # file.write('\t'.join(generated_data))
            elif filename.endswith('.json'):
                  json.dump(generated_data, file)
                #   data.append()
            
        print(final_data)
        row = generated_data
        # print(row)
        script_path = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_path, filename)

        return file_path
    


    


# @app.post("/generatedatatest/")
# async def read_item(body: BodyRequest):
#     headers_input = body.columns
#     count = body.count
#     tablename = body.tablename

#     # Connect to the SQLite database
#     db_path = os.path.join(os.path.dirname(__file__), "data.db")
#     conn = sqlite3.connect(db_path)
#     cursor = conn.cursor()

#     # Create table
#     create_table_query = f"CREATE TABLE IF NOT EXISTS {tablename} ({', '.join(headers_input)})"
#     cursor.execute(create_table_query)
#     conn.commit()

#     # Insert data into table
#     for _ in range(count):
#         generated_data = []
#         for head in headers_input:
#             if head == "mobilenumber":
#                 head = "phonenumber"
#             closest_match, score = process.extractOne(head, dir(fake))
#             if hasattr(fake, closest_match):
#                 faker_function = getattr(fake, closest_match)
#                 generated_data.append(f"'{faker_function()}'")
#             else:
#                 generated_data.append("''")

#         insert_query = f"INSERT INTO {tablename} VALUES ({', '.join(generated_data)})"
#         cursor.execute(insert_query)
#         conn.commit()

#     conn.close()

    # return {"message": "Data inserted into SQLite database"}



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
    


@app.get("/extractdata/")
async def extract_data(bot_input:str):
    document_links={
        "Test Team Onboarding Checklist from Azure can be found":"https://tinyurl.com/testteamchecklist",
        "The Test Strategy document for LDS from Azure can be found":"https://tinyurl.com/teststrategydocLDS",
        "The Master Test plan document for LDS from Azure can be found":"https://tinyurl.com/masterplanLDS",
        "The test automation best practice document for LDS from Azure can be found ":"https://tinyurl.com/testauLDS",
        "Master Test plan doc for LDS link from SharePoint is ":"https://tinyurl.com/MastertestplanLDS",
        "Test Strategy document for LDS link from SharePoint is":"https://tinyurl.com/teststrategyLDS",
        "Test automation best practice document for LDS link from SharePoint is":"https://tinyurl.com/testautoLDS",
        "Test team onboarding checklist document link from SharePoint is" :"https://tinyurl.com/testteamonboarding",
        "This is the SharePoint link for all documents ":"https://tinyurl.com/alldocssharepoint"
    }
    matching_links = []

    for key in document_links:
        if bot_input.lower() in key.lower():
            matching_links.append(document_links[key])
    
    if matching_links:
        return matching_links
    else:
        return "No matching document links found."
    

# @app.get("/testdata/")
# async def startup():
#     if conn.is_closed():
#         conn.connect()

class BodyRequest(BaseModel):
    columns: list
    columnswithdatabase:list
    count: int
    tablename: str

@app.post("/generatedatatest/")
async def read_item(body: BodyRequest):
    headers_input = body.columns
    column_name=body.columnswithdatabase
    count = body.count
    tablename = body.tablename
    tablename=check_word_count(tablename)
    connection=None
    # Connect to MySQL database
    try:
        connection = mysql.connector.connect(
            host="ec2-3-6-90-112.ap-south-1.compute.amazonaws.com",
            user="jagan",
            password="Jagan@1997",
            database="test_data_generation"
        )
        print(connection)
        if connection.is_connected():
            cursor = connection.cursor()
            create_table_query = f"CREATE TABLE IF NOT EXISTS {tablename} ({', '.join(column_name)})"
            print(create_table_query)
            cursor.execute(create_table_query)
            connection.commit()
            print("cursor")
        else:
            print("not connected")

    #     print("create table")
    #     create_table_query = """
    # CREATE TABLE IF NOT EXISTS employees (
    #     id INT AUTO_INCREMENT PRIMARY KEY,
    #     first_name VARCHAR(255),
    #     last_name VARCHAR(255),
    #     email VARCHAR(255)
    # )
    # """
        print("creating table")
        cursor.execute(create_table_query)
        connection.commit()
        print("table created Successfully")
            # Insert data into table
        if connection.is_connected():
            cursor = connection.cursor()
            for _ in range(count):
                generated_data = []
                for head in headers_input:
                    if(head=="mobilenumber" or head.__contains__("mobile") or head.__contains__("mob")):
                        head = "phonenumber"
                    closest_match, score = process.extractOne(head, dir(fake))
                    if hasattr(fake, closest_match):
                        faker_function = getattr(fake, closest_match)
                        generated_data.append(f"'{faker_function()}'")
                    else:
                        generated_data.append("''")
                print(generated_data)

                insert_query = f"INSERT INTO {tablename} VALUES ({', '.join(generated_data)})"
                cursor.execute(insert_query)
                print("inserted")
                connection.commit()

            cursor.close()

    except Error as e:
        print("Error:", e)

    finally:
        if connection.is_connected():
            connection.close()

    return {"message": "Data inserted into MySQL database"}