import re
import psycopg2
import os
import time
import boto3
import time
from datetime import date, datetime, timedelta
import pandas as pd
s3 = boto3.client('s3')

def time_index(start_t, end_t):
    hours=[]
    if start_t == None:
        end_h = end_t.hour
        end_index = end_h//2 + 1
        for i in range(1,end_index+1):
                hours.append(i)
        return hours
    elif end_t == None:
        start_h = start_t.hour
        start_index = start_h//2 + 1
        for i in range(start_index,13):
            hours.append(i)
        return hours
    else:
        start_h = start_t.hour
        end_h = end_t.hour
        start_index = start_h//2 + 1
        end_index = end_h//2 + 1
        for i in range(start_index,end_index+1):
            hours.append(i)
        return hours

def data(expression, key):
    # print(expression)
    # print(key)
    resp = s3.select_object_content(
        Bucket='csfyp2023',
        Key=key,
        ExpressionType='SQL',
        Expression=expression,
        InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'},
        OutputSerialization = {'CSV': {}},
    )
    com_rec = ""
    for event in resp['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            com_rec = com_rec + records
            # print(records)
            # for line in (records.splitlines()):
                # print(line)
            #    data.append(line.split(","))

        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            print("Stats details bytesScanned: ")
            print(statsDetails['BytesScanned'])
            print("Stats details bytesProcessed: ")
            print(statsDetails['BytesProcessed'])
            print("Stats details bytesReturned: ")
            print(statsDetails['BytesReturned'])
    for line in (com_rec.splitlines()):
        #print(line)
        data.append(line.split(","))

def s3_select(table_name, beg_t, end_t):
    times = [] # record the date used to retrieve data
    retrieve_file = []
    # table_name = input("Please input the table you want to search:") # Get table name from user
    # beg_t = input("Please input the start time:") # Get the start time
    # end_t = input("Please input the end time:") # Get the end time

    # Change the string to datetime type
    beg_t = datetime.strptime(beg_t, '%Y-%m-%d %H:%M:%S')
    end_t = datetime.strptime(end_t, '%Y-%m-%d %H:%M:%S')


    # Determine if the time interval is bigger than one day
    if end_t.date() > beg_t.date():
        temp_t = beg_t + timedelta(days=1)
        times.append([beg_t, temp_t])
        while temp_t.date() < end_t.date():
            times.append([temp_t, temp_t+timedelta(days=1)])
            temp_t = temp_t+ timedelta(days=1)
        times.append([temp_t, end_t])

        for i in times:
            if i[0].strftime("%Y-%m-%d") == i[1].strftime("%Y-%m-%d"):
                file_name = table_name + r"/%s/" % (i[0].strftime("%Y-%m-%d"))
                indexes = time_index(None,i[1])
                for index in indexes:
                    retrieve_file.append(file_name+str(index)+'.csv')
                
            else:
                file_name = table_name + r"/%s/" % (i[0].strftime("%Y-%m-%d"))
                indexes = time_index(i[0],None)
                for index in indexes:
                    retrieve_file.append(file_name+str(index)+'.csv')


    elif end_t.date() == beg_t.date():
        file_name = table_name + r"/%s/" % (beg_t.strftime("%Y-%m-%d"))
        indexes = time_index(beg_t,end_t)
        for index in indexes:
            retrieve_file.append(file_name+str(index)+'.csv')

    # Get the start time
    pg_beg = time.time()
    data = []
    # loop to retrieve the data from s3
   
    if len(retrieve_file) == 1:
        basic_exp = "SELECT * FROM s3object s where s.\"time\" between " # Base expression
        expression = basic_exp + "'%s' and '%s';" % (beg_t, end_t)
        key = retrieve_file[0]
        data = data(expression, key)
        df = pd.DataFrame(data)
        tmp = 'tmp.csv'
        df.to_csv('/var/lib/postgresql/'+tmp, index=False, header=False)
        pg_end = time.time()
        retrieve_file = []
        retrieve_file.append(tmp)
        return retrieve_file
    else:
        after_expression = "SELECT * FROM s3object s where s.\"time\" > '%s';"%(beg_t)
        key = retrieve_file[0]
        data = data(after_expression, key)
        for 

        

# Connect to Postgresql database
conn = psycopg2.connect(database="example", user="postgres", password="1234", host="localhost", port="5432")

# Let user input command
table_name = input("Please enter your query table name:")
start_time = input("Your query start time:")
end_time = input("Your query end time:")

sql_select = "select * from %s where time > '%s' and time < '%s';"%(table_name, start_time, end_time)

cur = conn.cursor()

cur.execute(sql_select)
conn.commit()
data = cur.fetchall()

if data:
    # Data in database
    print(data)
else:
    # Data is stored in S3. Retrieve data from S3
    print("Data is not in the TimescaleDB.\nSearch for data in S3.")
    opt = input('Select your query method:')

    begin = time.time()
    s3 = s3_select(table_name, start_time, end_time)
    if len(s3) == 1:
        s3 = s3[0]
        sql_copy = "COPY hardware_usage from '/var/lib/postgresql/%s' DELIMITER ',' CSV HEADER;"%(s3)
        cur.execute(sql_copy)
        conn.commit()
        os.system("rm -rf ./%s"%(s3))
    else:
        s3 = s3_select(table_name, start_time, end_time)
        # Copy the s3 files into PostgresqlDB
        for s3_file in s3:
            state = os.system("aws s3 cp s3://csfyp2023/%s /var/lib/postgresql/%s/tempt.json"%(s3_file,table_name))
            if state != 0:
                print("There is no data in " + s3_file)
                continue
            else:
                sql_copy = "COPY hardware_usage from '/var/lib/postgresql/%s/tempt.csv' DELIMITER ',' CSV HEADER;" % (table_name)
                cur.execute(sql_copy)
                conn.commit()
    
    
    cur.execute(sql_select)
    conn.commit()
    print(cur.fetchall())

    finish = time.time()
    cost = finish - begin
    print("The query cost %f seconds"%(cost))

    # need to delay one day of end_date
    end_date = datetime.strptime(end_time[:10], '%Y-%m-%d')
    end_date += timedelta(days=1)
    end_time = end_date.strftime('%Y-%m-%d')
    # drop the data that was inserted
    # sql_drop = "SELECT drop_chunks('%s', older_than => DATE '%s', newer_than => DATE '%s');"%(table_name, end_time, start_time[:10])
    sql_drop = "delete from hardware_usage;"
    cur.execute(sql_drop)
    conn.commit()
    #print(cur.fetchall())
    




