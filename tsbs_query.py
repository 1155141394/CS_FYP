import re
import datetime
import psycopg2
import os
import time
import datetime
from datetime import date, datetime, timedelta
import pandas as pd

def s3_files(query_type):
    days = int(query_type)
    format = '2022-10-0'
    database = 'benchmark'
    readings_table = 'readings'
    tags_table = 'tags'
    s3_files = []
    s3_tables = []
    begin = 2
    while begin <= days:
        readings_file = database + readings_table + '_' + format + str(begin) + '.csv'
        s3_files.append(readings_file)
        s3_tables.append(readings_table)

        tags_file = database + tags_table + '_' + format + str(begin) + '.csv'
        s3_files.append(tags_file)
        s3_tables.append(tags_table)
        begin += 1

    return s3_files,s3_tables

query_type = input("Please enter your query type: ")

conn = psycopg2.connect(database="benchmark", user="postgres", password="1234", host="localhost", port="5432")

sql_select = """SELECT t.name AS name, t.driver AS driver, r.*
                FROM tags t INNER JOIN LATERAL
                        (SELECT longitude, latitude
                        FROM readings r
                        WHERE r.tags_id=t.id
                        ORDER BY time DESC LIMIT 1)  r ON true
                WHERE t.name IS NOT NULL
                AND t.fleet = 'South';"""
begin_time = time.time()
cur = conn.cursor()

if query_type == '1':
    cur.execute(sql_select)
    conn.commit()
    data = cur.fetchall()
    finish_time = time.time()
    print("The query cost %f seconds"%(finish_time-begin_time))
    # Data in database
    print(data)


else:
    s3_files,s3_tables = s3_files(query_type)
    # Copy the s3 files into PostgresqlDB
    for i in range(0, len(s3_files)):
        state = os.system("aws s3 cp s3://csfyp2023/benchmark/%s ../benchmark/tempt.csv"%(s3_files[i]))
        sql_copy = "COPY %s from '/var/lib/postgresql/benchmark/tempt.csv' DELIMITER ',' CSV HEADER;" %(s3_tables)
        cur.execute(sql_copy)
        conn.commit()

        os.system("rm -rf /var/lib/postgresql/benchmark/tempt.csv")

    cur.execute(sql_select)
    conn.commit()
    data = cur.fetchall()


    finish_time = time.time()
    print(data)
    print("The query cost %f seconds"%(finish_time-begin_time))

    # drop the data that was inserted
    sql_drop = "SELECT drop_chunks('readings',newer_than => DATE '2022-10-01');"
    sql_drop = "SELECT drop_chunks('tags',newer_than => DATE '2022-10-01');"
    cur.execute(sql_drop)
    conn.commit()
    print(cur.fetchall())


