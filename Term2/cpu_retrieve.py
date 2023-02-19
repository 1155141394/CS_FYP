import re
import psycopg2
import os
import time
import boto3
import time
from datetime import date, datetime, timedelta
import pandas as pd
from save_data_to_s3 import *

def s3_data(expression, key):
    s3 = boto3.client('s3')
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
    beg_t = datetime.datetime.strptime(beg_t, '%Y-%m-%d %H:%M:%S')
    end_t = datetime.datetime.strptime(end_t, '%Y-%m-%d %H:%M:%S')


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
        data = s3_data(expression, key)
        df = pd.DataFrame(data)
        df.to_csv('/var/lib/postgresql/tmp.csv', index=False, header=False)
    else:
        after_expression = "SELECT * FROM s3object s where s.\"time\" > '%s';"%(beg_t)
        key = retrieve_file[0]
        data = s3_data(after_expression, key)
        df = pd.DataFrame(data)
        df.to_csv('/var/lib/postgresql/tmp0.csv', index=False, header=False)
        for i in range(1,len(retrieve_file)-1):
            state = os.system("aws s3 cp s3://csfyp2023/%s /var/lib/postgresql/tmp%s.csv"%(retrieve_file[i],str(i)))
            if state != 0:
                print("There is no data in " + retrieve_file[i])

        before_expression = "SELECT * FROM s3object s where s.\"time\" < '%s';"%(end_t)
        key = retrieve_file[len(retrieve_file)-1]   
        data = s3_data(before_expression, key)
        df = pd.DataFrame(data)
        df.to_csv('/var/lib/postgresql/tmp%s.csv'%(len(retrieve_file)-1), index=False, header=False)
        
        # 输入待合并文件所在文件夹
        path = r'/var/lib/postgresql/'

        file_list = []
        for file in os.listdir(path):
            df = pd.read_csv(path + file)
            file_list.append(df)

        result = pd.concat(file_list)   # 合并文件
        result.to_csv(path + 'merge_res.csv', index=False, encoding='gbk')  # 保存合并后的文件

if __name__ == "__main__":
    s3_select('1', '2023-02-19 11:01:54', '2023-02-19 11:05:54')





