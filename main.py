import datetime
import random
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
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import os
from testdata import *


app = FastAPI()

# Create a Faker generator
fake = Faker("en_GB")


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
    datawithtype= headers_input  
    # column_definitions=data
    # print(column_definitions.__len__)
    # Data you want to write
    print(datawithtype)
    i=1
    data=[]
    for column_def in datawithtype:
        # print(column_def)
        if(column_def.lower().__contains__("varchar") or column_def.lower().__contains__("int") or column_def.lower().__contains__("date")):

    # Split each column definition by space to separate the data type and size
            # print("columndef:"+column_def)
            parts = column_def.split(" ")

            # print(parts)
    
    # Take only the first part (the column name) and append it to the new list
            column_name = parts[0]
            # print("datatype:"+parts[1])
            datatype=parts[1]
            data.append(column_name)
            max_length = None
            if datatype.lower().__contains__("varchar"):
                    if column_def.lower().__contains__("("):
                        max_length = int(parts[1].split("(")[1].rstrip(")"))
                        # print("stringlength")
                        # print(max_length)
                        minlength=max_length-1
                        maxcount = (10 **max_length)-1
                        mincount=10 **minlength
                    else:
                        maxcount=100
                        mincount=1

            elif datatype.lower().__contains__("int"):
                    if column_def.lower().__contains__("("):
                        max_length = int(parts[1].split("(")[1].rstrip(")"))
                        # print("intlength")
                        # print(max_length)
                        minlength=max_length-1
                        maxcount = (10 **max_length)-1
                        mincount=10 **minlength
                    else:
                        maxcount=100
                        mincount=1
        else:
            column_name=datawithtype
        i=i+1
    # print(data)
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

            # json.dump(data, file)
    # Add more conditions for other file formats as needed
        else:
            raise ValueError(f"Unsupported file format: {filename}")

        
        for _ in range(count):
            generated_data=[]
            for head in headers_input:
                # print("--------------"+head)
                if(head.lower()=="mobilenumber" or head.lower().__contains__("mobile") or head.lower().__contains__("mob") or head.lower().__contains__("phone")):
                    head="phonenumber"
                elif(head.lower()=="name" or head.lower().__contains__("name")):
                    head="name"
                elif(head.lower().__contains__("payment_id") or head.lower().__contains__("paymentid") or head.lower().__contains__("int")):
                    head="uuid4"
                    if(head.lower().__contains__("int")):
                        head="random"
                elif(head.lower()=="dob" or head.lower().__contains__("birth") or head.lower().__contains__("dob")):
                    head="date of birth"
                elif(head.lower().__contains__('age') or head.lower().__contains__('number') or head.lower().__contains__('no') or head.lower().__contains__('num') or head.lower().__contains__("id")):
                    head = "randomage"
                    
                elif(head.lower().__contains__('payment_mode')):
                    head="credit_card_provider"
                elif(head.lower().__contains__('status')):
                    head="status"
                elif(head.lower().__contains__("currentdate") or head.lower().__contains__("currenttime")):
                    head="currentdate"
                elif(head.lower().__contains__('joining') or head.lower().__contains__('date')):
                    if(head.lower().__contains__("time")):
                        head="datetime"
                    else:
                        head="joining"
               
                
                closest_match, score = process.extractOne(head, dir(fake))
                # print(closest_match)
                # print(type(closest_match))
                if hasattr(fake, closest_match):
                    faker_function = getattr(fake, closest_match)
                    if head=="phonenumber":
                        num=generate_custom_phone_number()
                        # print(num)
                        generated_data.append(num)
                    elif(head=="random"):
                            age = random.randint(mincount, maxcount)
                            generated_data.append(age)
                    elif(head=="randomage"):
                            age = random.randint(18, 99)
                            generated_data.append(age)
                    elif(head=="credit_card_provider"):
                        payment_mode = fake.credit_card_provider()
                        generated_data.append(payment_mode)
                    elif(head=="status"):
                        status = random.randint(1,6)
                        generated_data.append(status)
                    elif(head=="joining"):
                        join = fake.date()
                        generated_data.append(join)
                    elif(head=="currentdate"):
                        from datetime import date 
                        today =  datetime.datetime.now()
                        formatted_datetime = today.strftime('%Y-%m-%d %H:%M:%S')
                        generated_data.append(formatted_datetime)
                    elif(head=="datetime"):
                        join = fake.date()
                        hour = random.randint(0, 23)
                        minute = random.randint(0, 59)
                        second = random.randint(0, 59)

                        fake_time = join+" "+f"{hour:02d}:{minute:02d}:{second:02d}"
                        generated_data.append(fake_time)

                        
                    else:
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
                # print("generting data")
                # print("textformat:"+textformat)
                if(textformat.lower().__contains__("csv")):
                #  csv_writer = csv.writer(file)
                 csv_writer.writerow(generated_data)
                elif textformat=="tsv":
                # tsv_writer = csv.writer(file, delimiter='\t')
                # tsv_writer.writerows(generated_data)  
                    # print("generting data")
                    # print(generated_data)
                    file.write('\t'.join(map(str, generated_data)) + '\n')
                # file.write('\t'.join(generated_data))
            elif filename.endswith('.json'):
                  result_dict = {key: value for key, value in zip(data, generated_data)}

                  json.dump(result_dict, file)
                #   data.append()
            
        # print(final_data)
        row = generated_data
        # print(row)
        script_path = os.path.dirname(os.path.abspath(__file__))
        
        # print(script_path)
        file_path = os.path.join(script_path, filename)

        return file_path
    


def generate_custom_phone_number():
    uk_phone_number = fake.phone_number()
    return uk_phone_number
    # phone_number = fake.numerify(text='9##-###-####')
    # return phone_number 
    # formatted_phone_number = re.sub(r'(\d{1})(\d{2})(\d{3})(\d{3})', r'9\1\2-\3-\4', phone_number)
    # return formatted_phone_number
USERNAME = ''
PASSWORD = ''

user_tokens = {
    "testuser": "12345"
}

# Model for API token
class APIToken(BaseModel):
    api_token: str

# Create a function to check if the API token is valid
def verify_api_token(api_token: str = Depends(lambda token: "12345")):
    if api_token not in user_tokens.values():
        raise HTTPException(status_code=401, detail="Invalid API token")
    return api_token

@app.get("/download_file")
async def download_file(filepath:str):
    # print("table_name:"+filepath)
    file_path = filepath
    # print(file_path)
    csv_file_path_url = file_path.replace("\\", "/")
    base_url = "https://test-data-generator-u9tl.onrender.com"
    # csv_url = f"{base_url}/download?{csv_file_path_url}"
    # print(os.path.basename(csv_file_path_url))
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

# @app.post("/generatedatatest/")
# async def read_item(body: BodyRequest):
#     headers_input = body.columns
#     column_name=body.columnswithdatabase
#     count = body.count
#     tablename = body.tablename
#     tablename=check_word_count(tablename)
#     connection=None
#     # Connect to MySQL database
#     try:
#         connection = mysql.connector.connect(
#             host="ec2-3-6-90-112.ap-south-1.compute.amazonaws.com",
#             user="jagan",
#             password="Jagan@1997",
#             database="test_data_generation"
#         )
#         print(connection)
#         if connection.is_connected():
#             cursor = connection.cursor()
#             try:
#                 create_table_query = f"CREATE TABLE IF NOT EXISTS {tablename} ({', '.join(column_name)})"
#                 print(create_table_query)
#                 cursor.execute(create_table_query)
#                 connection.commit()
#             except:
#                 return {"message": "Table name already exists"}
#         else:
#             print("not connected")

#     #     print("create table")
#     #     create_table_query = """
#     # CREATE TABLE IF NOT EXISTS employees (
#     #     id INT AUTO_INCREMENT PRIMARY KEY,
#     #     first_name VARCHAR(255),
#     #     last_name VARCHAR(255),
#     #     email VARCHAR(255)
#     # )
#     # """
#         print("creating table")
#         cursor.execute(create_table_query)
#         connection.commit()
#         print("table created Successfully")
#             # Insert data into table
#         if connection.is_connected():
#             cursor = connection.cursor()
#             for _ in range(count):
#                 generated_data = []
#                 column_names = []
#                 for head in headers_input:
#                     parts = head.split()
#                     print(parts)
#                     head = parts[0]
#                     column_names.append(head)
#                     datatype=parts[1]
#                     print("-----------"+datatype+"-----------------------")
#                     if datatype.lower()=="date" :
#                         head="date"
#                     elif(datatype.lower()=="datetime"):
#                         head="datetime"
#                     elif(datatype.lower()=="int"):
#                         print("int")
#                     else:
#                         max_length = int(parts[1].split("(")[1].rstrip(")"))
#                     if(head.lower()=="mobilenumber" or head.lower().__contains__("mobile") or head.lower().__contains__("mob") or  head.lower().__contains__("phone")):
#                         if(datatype.lower().__contains__("int")):
#                             head="mobile"
#                         else:
#                             head="phonenumber"

#                     # elif((head.lower()=="name" or head.lower().__contains__("name")) and ("first" not in head.lower() or "last" not in head.lower())):
#                     #     head="name"
#                     #     # print("name")
#                     elif(head.lower()=="payment_id" or head.lower()=="paymentid" or head.lower().__contains__("id")):
#                         head="uuid4"
                        
#                     elif(head.lower()=="dob" or head.lower().__contains__("birth") or head.lower().__contains__("dob")):
#                         head="date of birth"
#                     elif(head.lower().__contains__('age') or head.lower().__contains__('s_no') or head.lower().__contains__('no') or head.lower().__contains__('number')):
#                         head = "random"
#                     elif(head.lower().__contains__('payment_mode')):
#                         head="credit_card_provider"
#                     elif(head.lower().__contains__('status')):
#                         head="status"
#                     elif(head.lower().__contains__('joining')):
#                         head="joining"
                    
#                     closest_match, score = process.extractOne(head, dir(fake))
#                     if hasattr(fake, closest_match):
#                         faker_function = getattr(fake, closest_match)
#                         if(head=="uuid4"):
#                             generated_value=fake.random_int(min=1, max=999)
#                         elif(head=="random"):
#                             generated_value = random.randint(18, 80)
#                         elif(head=="status"):
#                             generated_value = random.randint(1,6)
#                         elif(head=="date"):
#                             print('date')
#                             generated_value=fake.date()
#                             print('date:'+generated_value)
#                         elif(head=="datetime"):
#                             generated_value=fake.date_time()
#                         elif(head=="mobile"):
#                             # generated_value=fake.random_int(min=13, max=14)
#                             generated_value=""
#                             for _ in range(max_length):  # Generate 9 more random digits
#                                  digit = random.randint(0, 9)
#                                  generated_value += str(digit)
#                                  if(len(generated_value)==max_length):
#                                      break
#                         else:
#                             generated_value=faker_function()
#                         # print("generated_value= "+generated_value)
#                         print(datatype)
#                         if(datatype.lower().__contains__("varchar")):
#                             generated_value=str(generated_value)
#                             if max_length is not None and len(generated_value) > max_length:
#                                 generated_value = generated_value[:max_length-1]
#                             # print("aftergeneratedvalue= "+generated_value)
#                                 generated_data.append(f"'{generated_value}'")
#                             else:
#                                 generated_data.append(f"'{generated_value}'")
#                         else:
#                             generated_data.append(f"'{generated_value}'")
                            
                        
                        
                        
            
#                     # record[field_name] = generated_value
#                     else:
#                         generated_data.append("''")
#                 print(generated_data)
#                 columns_string = ', '.join(column_names)
#                 insert_query = f"INSERT INTO {tablename} ({columns_string}) VALUES ({', '.join(generated_data)})"
#                 print(insert_query)
#                 cursor.execute(insert_query)
#                 print("inserted")
#                 connection.commit()

#             cursor.close()

#     except Error as e:
#         print("Error:", e)

#     finally:
#         if connection.is_connected():
#             connection.close()

#     return {"message": "Data inserted into MySQL database"}


@app.get("/tablecheck")
async def tablecheck(tablenames:str):
    try:
        connection = mysql.connector.connect(
            host="ec2-3-6-90-112.ap-south-1.compute.amazonaws.com",
            user="jagan",
            password="Jagan@1997",
            database="test_data_generation"
        )
        print(connection)
        tablename = tablenames
        cursor = connection.cursor()

# Use a parameterized query to avoid SQL injection
        c_table_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'test_data_generation' AND table_name = %s);"

# Execute the query with the tablename as a parameter
        cursor.execute(c_table_query, (tablename,))

# Fetch the result
        result = cursor.fetchone()

# Check if the table exists
        table_exists = bool(result[0])

# Print the result
        print(f"Table '{tablename}' exists: {table_exists}")

# Close the cursor and connection
        cursor.close()
        connection.close()
            # except:
                
            #     return {"message": "Table name already exists"}
      
    except:
        return "Problem in connection Please try agin later"
    

    return table_exists


@app.post("/generatedatatest/")
async def read_item(body: BodyRequest):
    connection = create_connection()
    if not connection:
        return {"message": "Unable to connect to the database"}

    try:
        tablename = body.tablename
        tablename = check_word_count(tablename)  # You can add your logic for tablename here
        responsemessage=create_table(connection, tablename, body.columns)
        responsemessage=insert_data(connection, tablename, body.columns, body.count)
        return responsemessage
    finally:
        if connection.is_connected():
            connection.close()