import re
from io import StringIO
import boto3
import psycopg2
import os
import time
from datetime import date, datetime, timedelta
import pandas as pd
import random
import numpy as np


def query_csv_s3(s3, bucket_name, filename, sql_exp, use_header):
    #  should we search by column name or column index
    if use_header:
        header = "Use"
    else:
        header = "None"

    resp = s3.select_object_content(
        Bucket=bucket_name,
        Key=filename,
        ExpressionType='SQL',
        Expression=sql_exp,
        InputSerialization={'CSV': {"FileHeaderInfo": header}},
        OutputSerialization={'CSV': {}},
    )

    records = []
    for event in resp['Payload']:
        if 'Records' in event:
            records.append(event['Records']['Payload'])

    file_str = ''.join(req.decode('utf-8') for req in records)
    return file_str


def s3_select_gen_csv(date, table):
    s3 = boto3.client('s3')
    bucket_name = 'csfyp2023'
    basic_filename = 'benchmark/'

    #  create SQL expression to query by date using column names
    sql_exp = "SELECT * FROM s3object s where s._1 >= '2022-10-03 13:09:14.823888 +0000' AND s._1 < '2022-10-03 13:19:14.823888 +0000';"

    #  should we use header names to filter
    use_header = False

    start_date = datetime.strptime(date, '%Y-%m-%d')
    return_path = "/var/lib/postgresql/benchmark/tmp.csv"
    filename = basic_filename + table + "_" + date + ".csv"
    print(filename)
    #  return CSV of unpacked data
    file_str = query_csv_s3(s3, bucket_name, filename, sql_exp, use_header)
    #  read CSV to dataframe
    df = pd.read_csv(StringIO(file_str))
    df.columns = ['time', 'tags_id', 'name', 'latitude', 'longitude', 'elevation', 'velocity', 'heading', 'grade',
                  'fuel_consumption', 'additional_tags']
    df.to_csv(return_path, index=False)
    return return_path

def s3_trans_csv(date, table):
    file_path = "/var/lib/postgresql/benchmark/tmp.csv"
    os.system("aws s3 cp s3://csfyp2023/benchmark/%s_%s %s" % (table, date, file_path))
    return file_path

def generate_query():
    location = random.choice(['South', 'West', 'East', 'North'])
    sql_query = ("""SELECT t.name AS name, t.driver AS driver
                    FROM tags t
                    INNER JOIN readings r ON r.tags_id = t.id
                    WHERE time >= '2022-10-03 13:09:14.823888 +0000' AND time < '2022-10-03 13:19:14.823888 +0000'
                    AND t.name IS NOT NULL
                    AND t.fleet = '%s'
                    GROUP BY 1, 2
                    HAVING avg(r.velocity) < 1""" % location)
    return sql_query

if __name__ == "__main__":
    conn = psycopg2.connect(database="benchmark", user="postgres", password="1234", host="localhost", port="5432")
    time_cost = []
    cur = conn.cursor()

    query_time = input('The number of running the query: ')
    query_type = input('Using Select or not(y/n): ')
    query_time = int(query_time)
    if query_type == 'y':
        query_type = True
    else:
        query_type = False

    while query_time > 0:
        begin_time = time.time()
        if query_type:
            file_path = s3_select_gen_csv("2022-10-03", "readings")
        else:
            file_path = s3_trans_csv("2022-10-03", "readings")

        sql_copy = "COPY diagnostics from '" + file_path + "' DELIMITER ',' CSV HEADER;"
        cur.execute(sql_copy)
        conn.commit()

        transfer_time = time.time()

        cur.execute(generate_query())
        conn.commit()
        data = cur.fetchall()

        finish_time = time.time()
        cost = finish_time - begin_time
        time_cost.append(cost)

        print("The total query costs %f seconds" % cost)

        sql_drop = "SELECT drop_chunks('%s',newer_than => DATE '2022-10-02');" % "readings"
        cur.execute(sql_drop)
        conn.commit()

        query_time -= 1

    print(time_cost)
    print('The mean is %s' % (np.average(time_cost)))
    print('The var is %s' % (np.var(time_cost)))


