import psycopg2
import os
import time
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np

# def generate_data(con, start_date, end_date, store_days):
# Change the string to datetime type
# start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
# end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')

# start_date_iso = start_date.isoformat()
# end_date_iso = end_date.isoformat()


os.system('tsbs_generate_data --use-case="iot" --seed=123 --scale=400 '
          '--timestamp-start="2022-10-01T00:00:00Z"'
          ' --timestamp-end="2022-10-05T00:00:00Z"'
          ' --log-interval="10s" --format="timescaledb" '
          '| gzip > ./timescaledb-data.gz')

os.system('cat /tmp/timescaledb-data.gz | gunzip | '
          'tsbs_load_timescaledb --postgres="sslmode=disable" --host="localhost" '
          '--port=5432 --pass="1234" --user="postgres" --admin-db-name=defaultdb --workers=8  '
          '--in-table-partition-tag=true --chunk-time=24h --write-profile= --field-index-count=1 '
          '--do-create-db=true --force-text-format=false --do-abort-on-exist=false --use-hypertable=true')

conn = psycopg2.connect(host="localhost", port=5432, user="postgres", password="1234", database="benchmark")
cur = conn.cursor()
# today = date.today()
beg_date = datetime.strptime("2022-10-01 00:00:00", '%Y-%m-%d %H:%M:%S')
for i in range(4):
    beg_time = time.time()
    end_date = beg_date + timedelta(days=1)
    beg_day = beg_date.date()
    file_name_1 = "diagnostics_" + str(beg_day) + ".csv"
    file_name_2 = "readings_" + str(beg_day) + ".csv"
    # file_name_3 = "tags_" + str(beg_day) + ".csv"

    # get data from diagnostics
    sql_get_chunks = r"SELECT show_chunks('diagnostics', older_than => DATE '%s');" % (end_date)
    cur.execute(sql_get_chunks)
    data = cur.fetchall()
    print(data)
    latest_chunk = data[0][0]

    sql_get_data = r"copy %s to" \
                   " '/var/lib/postgresql/%s' delimiter as ',' null as '' escape as '\"' CSV quote as '\"'" % (
                       latest_chunk, file_name_1)
    print("File name is %s" % (file_name_1))
    cur.execute(sql_get_data)

    sql_get_fields = r"select column_name from information_schema.columns where table_schema='public' and table_name='diagnostics';"
    cur.execute(sql_get_fields)
    fields = cur.fetchall()
    print(fields)
    file_fields = []
    for field in fields:
        file_fields.append(field[0])

    csv = pd.read_csv(r'/var/lib/postgresql/%s' % file_name_1, header=None, names=file_fields)
    csv.to_csv('/var/lib/postgresql/%s' % file_name_1, index=False)

    sql_delete_chunk = r"SELECT drop_chunks('diagnostics', older_than => DATE '%s');" % end_date
    cur.execute(sql_delete_chunk)
    # print(cur.fetchall())
    conn.commit()
    os.system("aws s3 cp ../%s s3://csfyp2023/benchmark/%s" % (file_name_1, file_name_1))
    os.system("rm -rf ../%s" % (file_name_1))

    # get data from readings
    sql_get_chunks = r"SELECT show_chunks('readings', older_than => DATE '%s');" % (end_date)
    cur.execute(sql_get_chunks)
    data = cur.fetchall()
    print(data)
    latest_chunk = data[0][0]

    sql_get_data = r"copy %s to" \
                   " '/var/lib/postgresql/%s' delimiter as ',' null as '' escape as '\"' CSV quote as '\"'" % (
                       latest_chunk, file_name_2)
    print("File name is %s" % file_name_2)
    cur.execute(sql_get_data)

    sql_get_fields = r"select column_name from information_schema.columns where table_schema='public' and table_name='readings';"
    cur.execute(sql_get_fields)
    fields = cur.fetchall()
    print(fields)
    file_fields = []
    for field in fields:
        file_fields.append(field[0])

    csv = pd.read_csv(r'/var/lib/postgresql/%s' % file_name_2, header=None, names=file_fields)
    csv.to_csv('/var/lib/postgresql/%s' % file_name_2, index=False)

    sql_delete_chunk = r"SELECT drop_chunks('readings', older_than => DATE '%s');" % end_date
    cur.execute(sql_delete_chunk)
    # print(cur.fetchall())
    conn.commit()
    os.system("aws s3 cp ../%s s3://csfyp2023/benchmark/%s" % (file_name_2, file_name_2))
    os.system("rm -rf ../%s" % (file_name_2))

    # # get data from tags
    # sql_get_chunks = r"SELECT show_chunks('tags', older_than => DATE '%s');" % (end_date)
    # cur.execute(sql_get_chunks)
    # data = cur.fetchall()
    # print(data)
    # latest_chunk = data[0][0]
    #
    # sql_get_data = r"copy %s to" \
    #                " '/var/lib/postgresql/%s' delimiter as ',' null as '' escape as '\"' CSV quote as '\"'" % (
    #                    latest_chunk, file_name_3)
    # print("File name is %s" % (file_name_3))
    # cur.execute(sql_get_data)
    #
    # sql_get_fields = r"select column_name from information_schema.columns where table_schema='public' and table_name='tags';"
    # cur.execute(sql_get_fields)
    # fields = cur.fetchall()
    # print(fields)
    # file_fields = []
    # for field in fields:
    #     file_fields.append(field[0])
    #
    # csv = pd.read_csv(r'/var/lib/postgresql/%s' % file_name_3, header=None, names=file_fields)
    # csv.to_csv('/var/lib/postgresql/%s' % file_name_3, index=False)
    #
    # sql_delete_chunk = r"SELECT drop_chunks('tags', older_than => DATE '%s');" % end_date
    # cur.execute(sql_delete_chunk)
    # # print(cur.fetchall())
    # conn.commit()
    # os.system("aws s3 cp ../%s s3://csfyp2023/benchmark/%s" % (file_name_3, file_name_3))
    # os.system("rm -rf ../%s" % (file_name_3))

    end_time = time.time()
    print("Time cost for one day data: %f" % (end_time-beg_time))
    beg_date = beg_date + timedelta(days=1)
conn.close()

# if __name__ == "__main__":
#     s_date =
#     e_date =
#     st_days =
#     conn = psycopg2.connect(host="localhost", port=5432, user="postgres", password="1234", database="example")
#
#     generate_data(con=conn, start_date=s_date, end_date=e_date, store_days=st_days)
#     conn.close()
