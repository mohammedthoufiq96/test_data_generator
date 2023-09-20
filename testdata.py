import datetime
from fastapi import FastAPI
import mysql.connector
from mysql.connector import Error
from faker import Faker
import random
from fuzzywuzzy import process
from pydantic import BaseModel

app = FastAPI()
fake = Faker()

class BodyRequest(BaseModel):
    columns: list
    columnswithdatabase: list
    count: int
    tablename: str

def create_connection():
    try:
        connection = mysql.connector.connect(
            host="ec2-3-6-90-112.ap-south-1.compute.amazonaws.com",
            user="jagan",
            password="Jagan@1997",
            database="test_data_generation"
        )
        return connection
    except Error as e:
        # print("Error:", e)
        return None

def create_table(connection, tablename, column_names):
    try:
        cursor = connection.cursor()
        create_table_query = f"CREATE TABLE IF NOT EXISTS test_data_generation.{tablename} ({', '.join(column_names)})"
        # print(create_table_query)
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        return "create table"
    except Error as e:
        # print("Error creating table:", e)
        return "Error creating table"
        
        # return Error creating table

def insert_data(connection, tablename, headers_input, count):
    try:
        cursor = connection.cursor()
        column_names=[]
        column_headers=[]
        querydata=''
        headwithtime=False
       
        # print("yres")
        # print(headers_input)
        for input_string in headers_input:
    # Split the input string by comma
            input_string.strip()
            column_definitions = input_string.split(',')
            column_headers=column_definitions
    
    # Extract column names from each definition and add them to the list
            for definition in column_definitions:
                column_name = definition.split()[0].strip()
                column_names.append(column_name)
                
                
        # print(column_headers)
        # print(column_names)
        for _ in range(count):
           
            generated_data = []
            # column_names = []
            # print("test")
            # print(column_headers)
            # print(len(column_headers))
            # components = [component.replace("'", "").replace('"', '') for component in headers_input]

            # print(components)
            i=0
            # headers_input = headers_input.split(',')
            for head in headers_input:
                head=head.strip()
                # print("--------------"+head)
                
                if  head.__contains__("varchar") or head.__contains__("int"):
                    # print("---------------in-----------------------")
                    parts = head.split(" ")
                # print(parts)
                    head_part = parts[0].strip()
                    datatype = parts[1].strip()
                    max_length = None
                    if datatype.lower().__contains__("varchar"):
                        max_length = int(parts[1].split("(")[1].rstrip(")"))
                        # print("stringlength")
                        # print(max_length)
                        minlength=max_length-1
                        maxcount = (10 **max_length)-1
                        mincount=10 **minlength
                    elif datatype.lower().__contains__("int"):
                        if head.lower().__contains__("("):
                            max_length = int(parts[1].split("(")[1].rstrip(")"))
                            if max_length>9:
                                max_length=8
                        # print("intlength")
                        # print(max_length)
                            minlength=max_length-1
                            maxcount = (10 **max_length)-1
                            mincount=10 **minlength
                        else:
                            maxcount=100
                            mincount=1
                else:
                    max_length=100
                    minlength=max_length-1
                    maxcount = (10 **max_length)-1
                    mincount=10 **minlength
                    datatype='date'
                

                # print(datatype)
                # print(head)
                # Handle data generation
                if head.lower().__contains__("mobilenumber") or head.lower().__contains__("mobile") or  head.lower().__contains__("phone") or head.lower().__contains__("mob") or head.lower().__contains__("ph") :
                    if datatype.lower().__contains__("int"):
                        head= "mobile"
                    else:
                        head = "phonenumber"
                elif head.lower().__contains__("payment_id") or head.lower().__contains__("paymentid") or head.lower().__contains__("id") or head.lower().__contains__("total") or head.lower().__contains__("int"):
                     if head.lower().__contains__("int"):
                        head = "random"
                     else:
                        head = "uuid4"
                   
                elif "dob" in head.lower() or "birth" in head.lower() or "dob" in head.lower() or head.lower().__contains__('date') and "current" not in head.lower():
                    if head.lower().__contains__('time'):
                        headwithtime=True
                    head = "date of birth"
                elif(head.lower().__contains__("currentdate") or head.lower().__contains__("currenttime") or head.lower().__contains__("createddate")):
                    head="currentdate"
                elif "age" in head.lower() or "s_no" in head.lower() or "no" in head.lower() or "number" in head.lower():
                    if head.lower().__contains__("int"):
                        head = "random"
                    elif head.lower().__contains__("age"):
                        head="age"
                    else:
                        head = "uuid4"
                elif "payment_mode" in head.lower():
                    head = "credit_card_provider"
                elif "status" in head.lower():
                    head = "status"
                elif "joining" in head.lower():
                    if "time" in head.lower():
                        head = "date of birth"
                        headwithtime=True
                    else:
                        head="date of birth"
                elif(head.lower().__contains__("float") or head.lower().__contains__('double') or head.lower().__contains__('decimal')):
                    head = "random_decimal"
                elif(head.lower().__contains__("gender")):
                    head = "gender"
                
                # print(head)
                closest_match, _ = process.extractOne(head, dir(fake))
                faker_function = getattr(fake, closest_match)
                generated_value = None

                if head == "uuid4":
                   import secrets
                   generated_value=secrets.token_hex(max_length-1)
                elif head == "random":
                    generated_value = random.randint(mincount, maxcount)
                elif head == "status":
                    generated_value = random.randint(1, 6)
                elif head == "date":
                    generated_value = fake.date()
                elif head == "datetime":
                    join = fake.date()
                    hour = random.randint(0, 23)
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    generated_value = join+" "+f"{hour:02d}:{minute:02d}:{second:02d}"
                    # print(generated_value)
                elif head == "mobile":
                    generated_value = "".join([str(random.randint(0, 9)) for _ in range(max_length-3)])
                elif head=="currentdate":
                    from datetime import date

                    today =  datetime.datetime.now()
                    formatted_datetime = today.strftime('%Y-%m-%d %H:%M:%S')
                    generated_value=formatted_datetime

                elif head=="random_decimal":
                    generated_value=round(random.uniform(1,10), 2)
                elif head=="age":
                    generated_value = random.randint(1, 99)
                else:
                    generated_value = faker_function()
                
                if(headwithtime==True):
                    hour = random.randint(0, 23)
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    generated_value = generated_value+" "+f"{hour:02d}:{minute:02d}:{second:02d}"
                    headwithtime=False


                if datatype.lower().__contains__("varchar"):
                    generated_value = str(generated_value)
                    if max_length is not None and len(generated_value) > max_length:
                        generated_value = generated_value[:max_length - 1]
                    generated_data.append(f"'{generated_value}'")
                else:
                    generated_data.append(f"'{generated_value}'")
                # i=i+1
                # column_names.append(head)
            
            final_data=', '.join(generated_data)
            if querydata=="":
                querydata="("+final_data+")"
            else:
                querydata=querydata+",("+final_data+")"
        
          


            columns_string = ', '.join(column_names)
            # print(generated_data)
        insert_query = f"INSERT INTO {tablename} ({columns_string}) VALUES {querydata}"
        # print(insert_query)
        cursor.execute(insert_query)
        connection.commit()

        cursor.close()
        return "Data inserted Successfully"
    except Error as e:
        # print("Error inserting data:", e)
        return "Error inserting data"
       

# def newgeneratedata():


