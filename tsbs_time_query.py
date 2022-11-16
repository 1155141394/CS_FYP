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


if __name__ == "__main__":
    print(s3_select_gen_csv("2022-10-03", "readings"))
