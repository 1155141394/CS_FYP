import re
import psycopg2
import os
import time
import boto3
import time
from datetime import date, datetime, timedelta
import pandas as pd
from tools import *


def find_rows(arr, index1, index2):
    rows = []
    for i, row in enumerate(arr):
        if index1 != -1 and index2 != -1:
            if row[index1] == 1 and row[index2] == 1:
                rows.append(i)
        elif index1 == -1 and index2 != -1:
            if row[index2] == 1:
                rows.append(i)
        elif index1 != -1 and index2 == -1:
            if row[index1] == 1:
                rows.append(i)
    return rows


def get_params_from_sql(sql_query):
    import re
    #用于提取表名的正则表达式
    table_regex = r'from\s+`?(\w+)`?'
    #用于提取其他参数的正则表达式
    params_regex = r'(select|from|where|order by|limit|group by)\s+`?(\w+)`?(.*?)(?=(select|from|where|order by|limit|group by|$))'

    result = {}

    # 提取表名
    table_name = re.search(table_regex, sql_query)
    if table_name:
        result['table_name'] = table_name.group(1)

    # 提取其他参数
    params = re.findall(params_regex, sql_query, re.IGNORECASE)
    for param in params:
        result[param[0]] = (param[1], param[2])

    return result
def s3_data(expression, key):
    data = []
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
    return data

def s3_select(table_name, beg_t, end_t):
    times = [] # record the date used to retrieve data
    retrieve_file = []

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
                file_name = table_name + r"/%s_" % (i[0].strftime("%Y-%m-%d"))
                indexes = time_index(None,i[1])
                for index in indexes:
                    retrieve_file.append(file_name+str(index)+'.csv')
                
            else:
                file_name = table_name + r"/%s_" % (i[0].strftime("%Y-%m-%d"))
                indexes = time_index(i[0],None)
                for index in indexes:
                    retrieve_file.append(file_name+str(index)+'.csv')


    elif end_t.date() == beg_t.date():
        file_name = table_name + r"/%s_" % (beg_t.strftime("%Y-%m-%d"))
        indexes = time_index(beg_t,end_t)
        for index in indexes:
            retrieve_file.append(file_name+str(index)+'.csv')

    print(retrieve_file)
    # loop to retrieve the data from s3
   
    if len(retrieve_file) == 1:
        basic_exp = "SELECT * FROM s3object s where s.\"time\" between " # Base expression
        expression = basic_exp + "'%s' and '%s';" % (beg_t, end_t)
        key = retrieve_file[0]
        print(key)
        data = s3_data(expression, key)
        df = pd.DataFrame(data)
        df.to_csv('/home/postgres/CS_FYP/data/tmp.csv', index=False, header=False)
    else:
        after_expression = "SELECT * FROM s3object s where s.\"time\" > '%s';"%(beg_t)
        key = retrieve_file[0]
        data = s3_data(after_expression, key)
        # df = pd.DataFrame(data)
        # df.to_csv('/home/postgres/CS_FYP/data/tmp0.csv', index=False, header=False)
        for i in range(1,len(retrieve_file)-1):
            expression = "SELECT * FROM s3object s "
            key = retrieve_file[i]
            data = data + s3_data(expression, key)
            # df = pd.DataFrame(data)
            # df.to_csv('/home/postgres/CS_FYP/data/tmp0.csv', index=False, header=False)
            # state = os.system("aws s3 cp s3://csfyp2023/%s /home/postgres/CS_FYP/data/tmp%s.csv"%(retrieve_file[i],str(i)))
            # if state != 0:
            #     print("There is no data in " + retrieve_file[i])

        before_expression = "SELECT * FROM s3object s where s.\"time\" < '%s';"%(end_t)
        key = retrieve_file[len(retrieve_file)-1]   
        data = data + s3_data(before_expression, key)
        df = pd.DataFrame(data)
        df.to_csv('/home/postgres/CS_FYP/data/tmp.csv', index=False, header=False)

def find_id(node,cpu):
    state = os.system("aws s3 cp s3://csfyp2023/map_matrix /home/postgres/CS_FYP/data/map_matrix.csv")
    if state != 0:
        print("There is no map in csfyp2023")

    csv_file = csv.reader(open('/home/postgres/CS_FYP/data/map_matrix.csv', 'r'))


    content = []  # 用来存储整个文件的数据，存成一个列表，列表的每一个元素又是一个列表，表示的是文件的某一行

    for line in csv_file:
        content.append(line)

    content = list(map(int, content))




if __name__ == "__main__":
    # s3_select('1', '2023-02-20 02:01:54', '2023-02-20 06:05:54')
    find_id(1,3)





