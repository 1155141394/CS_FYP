import psycopg2
import os
import time
from datetime import date
import datetime
import pandas as pd 
import numpy as np

conn = psycopg2.connect(host="localhost", port=5432, user="postgres", password="1234", database="example")

while True:
    now = datetime.datetime.now()

    if now.hour == 0 and now.minute == 0 :
        cur = conn.cursor()
        today = date.today()
        yesterday = today - datetime.timedelta(days=1)
        dayBefYes = today - datetime.timedelta(days=2)

        file_name = "hardware_usage_" + str(dayBefYes) + "-" + str(yesterday) + ".csv"

        sql_get_chunks = r"SELECT show_chunks('hardware_usage', older_than => DATE '%s');"%(yesterday)
        cur.execute(sql_get_chunks)
        data = cur.fetchall()
        print(data)
        latest_chunk = data[0][0]


        #f = open(str(file_name), 'w')
        sql_get_data = r"copy %s to" \
              " '/var/lib/postgresql/%s' delimiter as ',' null as '' escape as '\"' CSV quote as '\"'" % (latest_chunk, file_name)
        print("File name is %s"%(file_name))
        cur.execute(sql_get_data)
        
        sql_get_fields = r"select column_name from information_schema.columns where table_schema='public' and table_name='hardware_usage';"
        cur.execute(sql_get_fields)
        fields = cur.fetchall()
        print(fields)
        file_fields = []
        for field in fields:
            file_fields.append(field[0])

        csv = pd.read_csv(r'/var/lib/postgresql/%s'%(file_name),header=None,names= file_fields)
        csv.to_csv('/var/lib/postgresql/%s'%(file_name),index=False)

        
        sql_delete_chunk = r"SELECT drop_chunks('hardware_usage', INTERVAL '24 hours');"
        cur.execute(sql_delete_chunk)
        # print(cur.fetchall())
        conn.commit()
        os.system("aws s3 cp ../%s s3://csfyp2023/hardware_usage/%s" % (file_name, file_name))

        os.system("rm -rf ../%s" % (file_name))
        time.sleep(60)
    time.sleep(1)
conn.close()
