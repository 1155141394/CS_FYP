import re
import psycopg2
import os
import time
import boto3
import time
from datetime import date, datetime, timedelta
import pandas as pd
s3 = boto3.client('s3')
# find s3 files by date
def s3_files(table, start, end):
    start = start[:10]
    end = end[:10]
    start_date = datetime.strptime(start, '%Y-%m-%d')
    end_date = datetime.strptime(end, '%Y-%m-%d')
    date_array = []
    while start_date < end_date:
        date_array.append(start_date.strftime('%Y-%m-%d'))
        start_date += timedelta(days=1)
    date_array.append(end_date.strftime('%Y-%m-%d'))

    s3_files = []
    for date in date_array:
        tempt = datetime.strptime(date, '%Y-%m-%d')
        next_date = tempt + timedelta(days=1)
        next = next_date.strftime('%Y-%m-%d')
        file_name = table + "_" + date + "-" + next + ".csv"
        s3_files.append(file_name)


    return s3_files

def s3_select(table_name, beg_t, end_t):
    times = [] # record the date used to retrieve data
    basic_exp = "SELECT * FROM s3object s where s.\"time\" between " # Base expression
    # table_name = input("Please input the table you want to search:") # Get table name from user
    # beg_t = input("Please input the start time:") # Get the start time
    # end_t = input("Please input the end time:") # Get the end time

    # Change the string to datetime type
    beg_t = datetime.strptime(beg_t, '%Y-%m-%d %H:%M:%S')
    end_t = datetime.strptime(end_t, '%Y-%m-%d %H:%M:%S')

    # Change the string to date type
    beg_t_date = beg_t.date()
    end_t_date = end_t.date()

    # Determine if the time interval is bigger than one day
    if end_t_date > beg_t_date:
        temp_date = beg_t_date + timedelta(days=1)
        times.append([beg_t, temp_date])
        while temp_date < end_t_date:
            times.append([temp_date, temp_date+timedelta(days=1)])
            temp_date = temp_date + timedelta(days=1)
        times.append([temp_date, end_t])
    elif end_t_date == beg_t_date:
        times.append([beg_t, end_t])

    data = []
    # loop to retrieve the data from s3
    for i in times:
        if i[0].strftime("%Y-%m-%d") == i[1].strftime("%Y-%m-%d"):
            temp_date = i[1] + timedelta(days=1)
            file_name = table_name + "_%s-%s.csv" % (i[0].strftime("%Y-%m-%d"), temp_date.strftime("%Y-%m-%d"))
        else:
            file_name = table_name + "_%s-%s.csv" % (i[0].strftime("%Y-%m-%d"), i[1].strftime("%Y-%m-%d"))
        expression = basic_exp + "'%s' and '%s';" % (i[0], i[1])
        key = 'benchmark' + r"/" + file_name
        print(key)
        resp = s3.select_object_content(
            Bucket='fypts',
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
    df = pd.DataFrame(data)
    file_name = 'tmp.csv'
    df.to_csv('/var/lib/postgresql/'+file_name, index=False, header=False)
    return(file_name)


if __name__ == "__main__":
    # Connect to Postgresql database
    conn = psycopg2.connect(database="benchmark", user="postgres", password="1234", host="localhost", port="5432")

    # Let user input command
    table_name = 'cpu'
    start_time = '2023-04-09 11:22:40'
    end_time = '2023-04-09 23:22:40'

    sql_select = "select * from %s where time > '%s' and time < '%s';"%(table_name, start_time, end_time)

    cur = conn.cursor()

    # cur.execute(sql_select)
    # conn.commit()
    # data = cur.fetchall()

    # if data:
    #     # Data in database
    #     print(data)
    # else:
    # Data is stored in S3. Retrieve data from S3
    print("Data is not in the TimescaleDB.\nSearch for data in S3.")
    # opt = input('Select your query method:')

    begin = time.time()
    # if opt == '1':
    #     s3 = s3_files(table_name, start_time, end_time)
        # Copy the s3 files into PostgresqlDB
        # for i in range(0, len(s3)):
        #     print("We need data from " + s3[i])
        #     state = os.system("aws s3 cp s3://csfyp2023/%s/%s /var/lib/postgresql/%s/tempt.csv"%(table_name,s3[i],table_name))
        #     if state != 0:
        #         print("There is no data in " + s3[i])
        #         continue
        #     else:
        #         sql_copy = "COPY hardware_usage from '/var/lib/postgresql/%s/tempt.csv' DELIMITER ',' CSV HEADER;" % (table_name)
        #         cur.execute(sql_copy)
        #         conn.commit()
        #
        # os.system("rm -rf /var/lib/postgresql/%s/tempt.csv"%(table_name))
    # elif opt == '2' :
    s3 = s3_select(table_name, start_time, end_time)
    sql_copy = "COPY cpu from '/var/lib/postgresql/%s' DELIMITER ',' CSV HEADER;"%(s3)
    cur.execute(sql_copy)
    conn.commit()
    os.system("rm -rf ./%s"%(s3))

    query_111 = '''SELECT time_bucket('300 seconds', time) AS minute,
        max(usage_user) as max_usage_user
        FROM cpu
        WHERE tags_id IN (SELECT id FROM tags WHERE hostname IN ('host_0')) AND time >= '%s' AND time < '%s'
        GROUP BY minute ORDER BY minute;'''%(start_time,end_time)

    query_181 = '''SELECT time_bucket('300 seconds', time) AS minute,
        max(usage_user) as max_usage_user
        FROM cpu
        WHERE tags_id IN (SELECT id FROM tags WHERE hostname IN ('host_9','host_43','host_75','host_19','host_39','host_35','host_15','host_41')) AND time >= '2023-04-09 09:22:40.646325 +0000' AND time < '2023-04-09 10:22:40.646325 +0000'
        GROUP BY minute ORDER BY minute ASC'''

    query_5112 = '''SELECT time_bucket('300 seconds', time) AS minute,
        max(usage_user) as max_usage_user, max(usage_system) as max_usage_system, max(usage_idle) as max_usage_idle, max(usage_nice) as max_usage_nice, max(usage_iowait) as max_usage_iowait
        FROM cpu
        WHERE tags_id IN (SELECT id FROM tags WHERE hostname IN ('host_9')) AND time >= '2023-04-09 11:22:40.646325' AND time < '2023-04-09 23:22:40.646325'
        GROUP BY minute ORDER BY minute ASC'''

    query_581 = '''SELECT time_bucket('300 seconds', time) AS minute,
        max(usage_user) as max_usage_user, max(usage_system) as max_usage_system, max(usage_idle) as max_usage_idle, max(usage_nice) as max_usage_nice, max(usage_iowait) as max_usage_iowait
        FROM cpu
        WHERE tags_id IN (SELECT id FROM tags WHERE hostname IN ('host_9','host_43','host_75','host_19','host_39','host_35','host_15','host_41')) AND time >= '2023-04-09 09:22:40.646325 +0000' AND time < '2023-04-09 10:22:40.646325 +0000'
        GROUP BY minute ORDER BY minute'''

    query_max_all_1 = '''SELECT time_bucket('3600 seconds', time) AS hour,
        max(usage_user) as max_usage_user, max(usage_system) as max_usage_system, max(usage_idle) as max_usage_idle, max(usage_nice) as max_usage_nice, max(usage_iowait) as max_usage_iowait, max(usage_irq) as max_usage_irq, max(usage_softirq) as max_usage_softirq, max(usage_steal) as max_usage_steal, max(usage_guest) as max_usage_guest, max(usage_guest_nice) as max_usage_guest_nice
        FROM cpu
        WHERE tags_id IN (SELECT id FROM tags WHERE hostname IN ('host_1')) AND time >= '2023-04-06 23:00:44.894865 +0000' AND time < '2023-04-07 07:00:44.894865 +0000'
        GROUP BY hour ORDER BY hour'''

    high_cpu_12 = '''SELECT * FROM cpu WHERE usage_user > 90.0 and time >= '2023-04-08 23:35:31.138978 +0000' AND time < '2023-04-09 11:35:31.138978 +0000' AND tags_id IN (SELECT id FROM tags WHERE hostname IN ('host_75'))'''

    lastpoint = '''SELECT DISTINCT ON (t.hostname) * FROM tags t INNER JOIN LATERAL(SELECT * FROM cpu c WHERE c.tags_id = t.id ORDER BY time DESC LIMIT 1) AS b ON true ORDER BY t.hostname, b.time DESC'''

    query_high_cpu_1 = '''SELECT usage_user FROM cpu WHERE usage_user > 80.0 and time >= '2023-04-06 11:35:31.138978 +0000' AND time < '2023-04-07 11:35:31.138978 +0000' AND tags_id = 76;'''

    cur.execute(query_111)

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
    sql_drop = "SELECT drop_chunks('%s', older_than => DATE '%s', newer_than => DATE '%s');"%(table_name, end_time, start_time[:10])
    cur.execute(sql_drop)
    conn.commit()
    #print(cur.fetchall())





