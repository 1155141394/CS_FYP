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


def s3_select_gen_csv(days):
    s3 = boto3.client('s3')

    bucket_name = 'csfyp2023'
    basic_filename = 'benchmark/'

    #  create SQL expression to query by date using column names
    sql_exp = ("SELECT * FROM s3object s where s._4 < '0.1';")

    #  should we use header names to filter
    use_header = False
    start_date = datetime.strptime("2022-10-01 00:00:00", '%Y-%m-%d %H:%M:%S')
    start_date = start_date.date()
    frames = []
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
    res.to_csv("/var/lib/postgresql/benchmark/tmp.csv", index=False)
s3_select_gen_csv(2)
