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
    return sql_query[int(query_type)-1]

def s3(query_day):
    days = int(query_day)
    format = '2022-10-0'
    
    readings_table = 'readings'

    s3_files = []
    s3_tables = []
    begin = 2
    while begin <= days:
        readings_file = readings_table + '_' + format + str(begin) + '.csv'
        s3_files.append(readings_file)
        s3_tables.append(readings_table)

        begin += 1

    return s3_files,s3_tables

print('The query types:')
print('1.last-loc')
print('2.low-fuel')

query_type = input('Please enter the query type code: ')
query_day = input('Query day: ')
query_number = input('Number of queries: ')

conn = psycopg2.connect(database="benchmark", user="postgres", password="1234", host="localhost", port="5432")

time_cost = []

cur = conn.cursor()
freq = 1
while freq <= int(query_number):
    freq += 1

    sql_select = generate_query(query_type)

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
        s3_files,s3_tables = s3(query_day)
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
        sql_drop = "SELECT drop_chunks('readings',newer_than => DATE '2022-10-02');"
        cur.execute(sql_drop)
        conn.commit()
        #print(cur.fetchall())

print(time_cost)
print('The mean is %s'%(np.average(time_cost)))
print('The var is %s'%(np.var(time_cost)))
