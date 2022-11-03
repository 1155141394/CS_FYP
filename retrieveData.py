import re
import datetime
import psycopg2
import os
import time

# Connect to Postgresql database
conn = psycopg2.connect(database="example", user="postgres", password="1234", host="localhost", port="5432")

# Let user input command
command = input("Please enter your SQL language:")


cur = conn.cursor()
sql_select = command

cur.execute(sql_select)
conn.commit()
data = cur.fetchall()

if data:
    # Data in database
    print(data)
else:
    # Data is stored in S3. Retrieve data from S3
    print("Data is not in the TimescaleDB.\n Search for data in S3.")
    info = re.findall(r"select (.+?) from (.+?) where time = '(.+?)';",command,flags=re.IGNORECASE)
    table_name = info[0][1]
    time = info[0][2]
    time_date = datetime.datetime.strptime(time, "%Y-%m-%d")
    next_date = time_date + datetime.timedelta(days=1)
    next = next_date.strftime("%Y-%m-%d")
    file_name = table_name + "_" + time + "-" + next + ".csv"
    
    state = os.system("aws s3 cp s3://csfyp2023/%s/%s ../%s/tempt.csv"%(table_name,file_name,table_name))
        
    #   Copy the data into PostgresqlDB
    if state == 0:
        sql_copy = "COPY hardware_usage from '/var/lib/postgresql/%s/tempt.csv' DELIMITER ',' CSV HEADER;" % (table_name)
        cur.execute(sql_copy)
        conn.commit()
        sql_select = "select * from %s where time > '%s' and time < '%s';"%(table_name,time,next)
        cur.execute(sql_select)
        conn.commit()
        print(cur.fetchall())
    else:
        print("Data is not available.")


