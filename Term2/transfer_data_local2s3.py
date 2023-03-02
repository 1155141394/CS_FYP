import psycopg2
import os
import time
from datetime import date
import datetime
import pandas as pd
import numpy as np
from tools import *

conn = psycopg2.connect(host="localhost", port=5432, user="postgres", password="1234", database="example")

while True:
    now = datetime.datetime.now()
    # Set the date info
    today = date.today()
    # send data to s3 every two hours
    if now.hour & 1 == 0 and now.minute == 0:
        cur = conn.cursor()
        end_time = datetime.datetime.now()
        start_time = end_time + datetime.timedelta(hours=-2)
        curr_hour = start_time.hour

        # set the file name with data + date + hour
        cpu_file_name = "cpu_usage_" + str(today) + "_" + str(curr_hour) + ".csv"
        mem_file_name = "memory_usage_" + str(today) + "_" + str(curr_hour) + ".csv"

        # get the chunk name and data from the hypertable
        sql_get_chunks = r"SELECT show_chunks('hardware_usage', newer_than => now() - interval '3 hour');"
        cur.execute(sql_get_chunks)
        data = cur.fetchall()
        print(data)
        latest_chunk = data[0][0]

        # store the data into the tmp csv file
        tmp_filename = "hardware_usage_tmp.csv"
        sql_get_data = r"copy %s to" \
                       " '/home/postgres/tmp_file/%s' delimiter as ',' null as '' escape as '\"' CSV quote as '\"'" % (
                           latest_chunk, tmp_filename)
        print("File name is %s" % (tmp_filename))
        cur.execute(sql_get_data)

        # get the column name
        sql_get_fields = r"select column_name from information_schema.columns where table_schema='public' and table_name='hardware_usage';"
        cur.execute(sql_get_fields)
        fields = cur.fetchall()
        print(fields)
        file_fields = []
        for field in fields:
            file_fields.append(field[0])

        # convert the tmp file to two file
        tmp_data = np.loadtxt(open("/home/postgres/tmp_file/%s" % tmp_filename, "rb"), dtype=str, delimiter=',', unpack=True)
        cpu_data = pd.DataFrame({fields[0]: tmp_data[0], fields[1]: tmp_data[1]})
        mem_data = pd.DataFrame({fields[0]: tmp_data[0], fields[1]: tmp_data[2]})
        cpu_data.to_csv("/home/postgres/tmp_file/%s" % cpu_file_name, header=True, index=False)
        mem_data.to_csv("/home/postgres/tmp_file/%s" % mem_file_name, header=True, index=False)
        # csv = pd.read_csv(r'/var/lib/postgresql/%s' % (file_name), header=None, names=file_fields)
        # csv.to_csv('/var/lib/postgresql/%s' % (file_name), index=False)

        sql_delete_chunk = r"SELECT drop_chunks('hardware_usage', older_than => now());"
        cur.execute(sql_delete_chunk)
        # print(cur.fetchall())
        conn.commit()

        save_data_to_s3("csfyp2023", 1, start_time, end_time, "/home/postgres/tmp_file/%s" % cpu_file_name)


        # os.system("aws s3 cp /home/postgres/tmp_file/%s s3://csfyp2023/hardware_usage/%s" % (cpu_file_name, cpu_file_name))
        # os.system("aws s3 cp /home/postgres/tmp_file/%s s3://csfyp2023/hardware_usage/%s" % (mem_file_name, mem_file_name))

        os.system("rm /home/postgres/tmp_file/%s" % tmp_filename)
        os.system("rm /home/postgres/tmp_file/%s" % cpu_file_name)
        os.system("rm /home/postgres/tmp_file/%s" % mem_file_name)
        time.sleep(60)

conn.close()