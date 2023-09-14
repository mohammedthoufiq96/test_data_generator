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
       
        # print("yres")
        # print(headers_input)
        for input_string in headers_input:
    # Split the input string by comma
            column_definitions = input_string.split(',')
            column_headers=column_definitions
    
    # Extract column names from each definition and add them to the list
            for definition in column_definitions:
                column_name = definition.split()[0]
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
                # print("--------------"+head)
                
                if  head.__contains__("varchar") or head.__contains__("int"):
                    # print("---------------in-----------------------")
                    parts = head.split(" ")
                # print(parts)
                    head_part = parts[0]
                    datatype = parts[1]
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
                        # print("intlength")
                        # print(max_length)
                            minlength=max_length-1
                            maxcount = (10 **max_length)-1
                            mincount=10 **minlength
                        else:
                            maxcount=100
                            mincount=1
                

                # print(datatype)

                # Handle data generation
                if head.lower().__contains__("mobilenumber") or head.lower().__contains__("mobile") or  head.lower().__contains__("phone") :
                    if datatype.lower().__contains__("int"):
                        head= "mobile"
                    else:
                        head = "phonenumber"
                elif head.lower().__contains__("payment_id") or head.lower().__contains__("paymentid") or head.lower().__contains__("id") or head.lower().__contains__("total") or head.lower().__contains__("int"):
                     if head.lower().__contains__("int"):
                        head = "random"
                     else:
                        head = "uuid4"
                   
                elif "dob" in head.lower() or "birth" in head.lower() or "dob" in head.lower():
                    head = "date of birth"
                elif "age" in head.lower() or "s_no" in head.lower() or "no" in head.lower() or "number" in head.lower():
                    head = "random"
                elif "payment_mode" in head.lower():
                    head = "credit_card_provider"
                elif "status" in head.lower():
                    head = "status"
                elif "joining" in head.lower():
                    head = "joining"
                elif(head.lower().__contains__("currentdate") or head.lower().__contains__("currenttime") or head.lower().__contains__("createddate")):
                
                    head="currentdate"
                elif(head.lower().__contains__("float") or head.lower().__contains__('double') or head.lower().__contains__('decimal')):
                    head = "random_decimal"

                closest_match, _ = process.extractOne(head, dir(fake))
                faker_function = getattr(fake, closest_match)
                generated_value = None

                if head == "uuid4":
                    generated_value = fake.random_int(min=mincount, max=maxcount)
                elif head == "random":
                    generated_value = random.randint(mincount, maxcount)
                elif head == "status":
                    generated_value = random.randint(1, 6)
                elif head == "date":
                    generated_value = fake.date()
                elif head == "datetime":
                    generated_value = fake.date_time()
                elif head == "mobile":
                    generated_value = "".join([str(random.randint(0, 9)) for _ in range(max_length-3)])
                elif head=="currentdate":
                    from datetime import date

                    today =  datetime.datetime.now()
                    formatted_datetime = today.strftime('%Y-%m-%d %H:%M:%S')
                    generated_value=formatted_datetime
                elif head=="random_decimal":
                    generated_value=round(random.uniform(1,10), 2)
                else:
                    generated_value = faker_function()

                if datatype.lower().__contains__( "varchar"):
                    generated_value = str(generated_value)
                    if max_length is not None and len(generated_value) > max_length:
                        generated_value = generated_value[:max_length - 1]
                    generated_data.append(f"'{generated_value}'")
                else:
                    generated_data.append(f"'{generated_value}'")
                # i=i+1
                # column_names.append(head)

            columns_string = ', '.join(column_names)
            # print(generated_data)
            insert_query = f"INSERT INTO {tablename} ({columns_string}) VALUES ({', '.join(generated_data)})"
            print(insert_query)
            cursor.execute(insert_query)
            connection.commit()

        cursor.close()
        return "Data inserted Successfully"
    except Error as e:
        print("Error inserting data:", e)
        return "Error inserting data"
       


