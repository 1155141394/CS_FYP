import re
import psycopg2
import os
import time
from datetime import date, datetime, timedelta
import pandas as pd
import random
import numpy as np

def generate_query(query_type):

    location = random.choice(['South', 'West', 'East','North'])

    sql_query = []
    sql_query.append("""SELECT t.name AS name, t.driver AS driver, r.*
                    FROM tags t INNER JOIN LATERAL
                            (SELECT longitude, latitude
                            FROM readings r
                            WHERE r.tags_id=t.id
                            ORDER BY time DESC LIMIT 1)  r ON true
                    WHERE t.name IS NOT NULL
                    AND t.fleet = '%s';"""%(location))

    sql_query.append("""SELECT t.name AS name, t.driver AS driver, d.*
                    FROM tags t INNER JOIN LATERAL
                            (SELECT fuel_state
                            FROM diagnostics d
                            WHERE d.tags_id=t.id
                            ORDER BY time DESC LIMIT 1) d ON true
                    WHERE t.name IS NOT NULL
                    AND d.fuel_state < 0.1
                    AND t.fleet = '%s';"""%(location))

    sql_query.append("""SELECT t.name AS name, t.driver AS driver, d.*
                    FROM tags t INNER JOIN LATERAL
                            (SELECT current_load
                            FROM diagnostics d
                            WHERE d.tags_id=t.id
                            ORDER BY time DESC LIMIT 1) d ON true
                    WHERE t.name IS NOT NULL
                    AND d.current_load/t.load_capacity > 0.9
                    AND t.fleet = '%s';"""%(location))

    sql_query.append("""SELECT t.name AS name, t.driver AS driver
                    FROM tags t
                    INNER JOIN readings r ON r.tags_id = t.id
                    WHERE time >= '2022-10-01 20:54:25.222186 +0000' AND time < '2022-10-01 21:04:25.222186 +0000'
                    AND t.name IS NOT NULL
                    AND t.fleet = '%s'
                    GROUP BY 1, 2
                    HAVING avg(r.velocity) < 1"""%(location))

    return sql_query[int(query_type)-1]

def s3(query_day, table):
    days = int(query_day)
    format = '2022-10-0'

    s3_files = []
    s3_tables = []
    begin = 1
    while begin <= days:
        file = table + '_' + format + str(begin) + '.csv'
        s3_files.append(file)
        s3_tables.append(table)

        begin += 1

    return s3_files,s3_tables

print('The query types:')
print('1.last-loc')
print('2.low-fuel')
print('3.high-load')
print('4.stationary-trucks')


query_type = input('Please enter the query type code: ')
query_day = input('Query day: ')
query_number = input('Number of queries: ')

conn = psycopg2.connect(database="benchmark", user="postgres", password="1234", host="localhost", port="5432")

time_cost = []

cur = conn.cursor()
freq = 1
while freq <= int(query_number):
    freq += 1
    table = ''
    sql_select = generate_query(query_type)
    if sql_select.find("readings") != -1:
        table = 'readings'
    else:
        table = 'diagnostics'

    begin_time = time.time()
    if query_day == '1':
        cur.execute(sql_select)
        conn.commit()
        data = cur.fetchall()
        finish_time = time.time()
        cost = finish_time - begin_time
        print("The query cost %f seconds"%(cost))
        # Data in database
        #print(data)
        time_cost.append(cost)


    else:
        s3_files,s3_tables = s3(query_day, table)
        # Copy the s3 files into PostgresqlDB
        for i in range(0, len(s3_files)):
            state = os.system("aws s3 cp s3://csfyp2023/benchmark/%s ../benchmark/tempt.csv"%(s3_files[i]))
            sql_copy = "COPY %s from '/var/lib/postgresql/benchmark/tempt.csv' DELIMITER ',' CSV HEADER;" %(s3_tables[i])
            cur.execute(sql_copy)
            conn.commit()

            #os.system("rm -rf /var/lib/postgresql/benchmark/tempt.csv")

        transfer_time = time.time()

        cur.execute(sql_select)
        conn.commit()
        data = cur.fetchall()


        finish_time = time.time()
        cost = finish_time - begin_time
        time_cost.append(cost)
        #print(data)
        print("The total query costs %f seconds"%(cost))
        #print("The transfer process costs %f seconds"%(transfer_time-begin_time))

        # drop the data that was inserted
        sql_drop = "SELECT drop_chunks('%s',newer_than => DATE '2022-10-02');"%(table)
        cur.execute(sql_drop)
        conn.commit()
        #print(cur.fetchall())

print(time_cost)
print('The mean is %s'%(np.average(time_cost)))
print('The var is %s'%(np.var(time_cost)))
