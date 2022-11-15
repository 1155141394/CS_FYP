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


def s3_select_gen_csv(days, table):
    s3 = boto3.client('s3')
    days = int(days) - 1
    bucket_name = 'csfyp2023'
    basic_filename = 'benchmark/'

    #  create SQL expression to query by date using column names
    sql_exp = ("SELECT * FROM s3object s where s._4 < '0.1';")

    #  should we use header names to filter
    use_header = False
    start_date = datetime.strptime("2022-10-02 00:00:00", '%Y-%m-%d %H:%M:%S')
    start_date = start_date.date()
    frames = []
    return_path = "/var/lib/postgresql/benchmark/tmp.csv"
    if len(days) == 1:
        filename = basic_filename + "diagnostics_" + str(start_date) + ".csv"
        #  return CSV of unpacked data
        file_str = query_csv_s3(s3, bucket_name, filename, sql_exp, use_header)
        #  read CSV to dataframe
        df = pd.read_csv(StringIO(file_str))
        df.columns = ['time', 'tags_id', 'name', 'fuel_state', 'current_load', 'status', 'additional_tags']
        df.to_csv(return_path, index=False)

    else:
        for i in range(days):
            filename = basic_filename + "diagnostics_" + str(start_date) + ".csv"
            #  return CSV of unpacked data
            file_str = query_csv_s3(s3, bucket_name, filename, sql_exp, use_header)
            #  read CSV to dataframe
            df = pd.read_csv(StringIO(file_str))
            df.columns = ['time', 'tags_id', 'name', 'fuel_state', 'current_load', 'status', 'additional_tags']
            frames.append(df)
            start_date = start_date + timedelta(days=1)
        res = pd.concat(frames)
        res.to_csv(return_path, index=False)
    return return_path


def generate_query(query_type):
    location = random.choice(['South', 'West', 'East', 'North'])

    sql_query = []
    sql_query.append("""SELECT t.name AS name, t.driver AS driver, r.*
                    FROM tags t INNER JOIN LATERAL
                            (SELECT longitude, latitude
                            FROM readings r
                            WHERE r.tags_id=t.id
                            ORDER BY time DESC LIMIT 1)  r ON true
                    WHERE t.name IS NOT NULL
                    AND t.fleet = '%s';""" % (location))

    sql_query.append("""SELECT t.name AS name, t.driver AS driver, d.*
                    FROM tags t INNER JOIN LATERAL
                            (SELECT fuel_state
                            FROM diagnostics d
                            WHERE d.tags_id=t.id
                            ORDER BY time DESC LIMIT 1) d ON true
                    WHERE t.name IS NOT NULL
                    AND d.fuel_state < 0.1
                    AND t.fleet = '%s';""" % (location))

    sql_query.append("""SELECT t.name AS name, t.driver AS driver, d.*
                    FROM tags t INNER JOIN LATERAL
                            (SELECT current_load
                            FROM diagnostics d
                            WHERE d.tags_id=t.id
                            ORDER BY time DESC LIMIT 1) d ON true
                    WHERE t.name IS NOT NULL
                    AND d.current_load/t.load_capacity > 0.9
                    AND t.fleet = '%s';""" % (location))

    sql_query.append("""SELECT t.name AS name, t.driver AS driver
                    FROM tags t
                    INNER JOIN readings r ON r.tags_id = t.id
                    WHERE time >= '2022-01-03 13:09:14.823888 +0000' AND time < '2022-01-03 13:19:14.823888 +0000'
                    AND t.name IS NOT NULL
                    AND t.fleet = '%s'
                    GROUP BY 1, 2
                    HAVING avg(r.velocity) < 1""" % (location))

    return sql_query[int(query_type) - 1]


def s3(query_day, table):
    days = int(query_day)
    format = '2022-10-0'

    s3_files = []
    s3_tables = []
    begin = 2
    while begin <= days:
        file = table + '_' + format + str(begin) + '.csv'
        s3_files.append(file)
        s3_tables.append(table)

        begin += 1

    return s3_files, s3_tables


if __name__ == "__main__":
    print('The query types:')
    print('1.last-loc')
    print('2.low-fuel')
    print('3.high-load')
    print('4.stationary-trucks')

    query_type = input('Please enter the query type code: ')
    query_day = input('Query day: ')
    query_number = input('Number of queries: ')
    use_s3select = input('Use S3 select or not(y/n): ')
    if use_s3select == 'y':
        use_s3select = True
    else:
        use_s3select = False
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
            print("The query cost %f seconds" % (cost))
            # Data in database
            # print(data)
            time_cost.append(cost)


        else:
            if query_type == "2" and use_s3select:
                file_path = s3_select_gen_csv(query_day, table)
                sql_copy = "COPY diagnostics from '" + file_path + "' DELIMITER ',' CSV HEADER;"
                cur.execute(sql_copy)
                conn.commit()
            else:
                s3_files, s3_tables = s3(query_day, table)
                # Copy the s3 files into PostgresqlDB
                for i in range(0, len(s3_files)):
                    state = os.system("aws s3 cp s3://csfyp2023/benchmark/%s ../benchmark/tempt.csv" % (s3_files[i]))
                    sql_copy = "COPY %s from '/var/lib/postgresql/benchmark/tempt.csv' DELIMITER ',' CSV HEADER;" % (
                    s3_tables[i])
                    cur.execute(sql_copy)
                    conn.commit()

                # os.system("rm -rf /var/lib/postgresql/benchmark/tempt.csv")

            transfer_time = time.time()

            cur.execute(sql_select)
            conn.commit()
            data = cur.fetchall()

            finish_time = time.time()
            cost = finish_time - begin_time
            time_cost.append(cost)
            # print(data)
            print("The total query costs %f seconds" % (cost))
            # print("The transfer process costs %f seconds"%(transfer_time-begin_time))

            # drop the data that was inserted
            sql_drop = "SELECT drop_chunks('%s',newer_than => DATE '2022-10-02');" % (table)
            cur.execute(sql_drop)
            conn.commit()
            # print(cur.fetchall())

    print(time_cost)
    print('The mean is %s' % (np.average(time_cost)))
    print('The var is %s' % (np.var(time_cost)))
