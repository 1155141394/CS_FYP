import psutil
import psycopg2
import time

conn = psycopg2.connect(host="localhost", port=5432, user="postgres", password="1234", database="example")

cur = conn.cursor()
sql1 = "CREATE TABLE hardware_usage (time TIMESTAMPTZ NOT NULL, cpu_percent REAL NOT NULL, memory_percent REAL NOT NULL);"
sql2 = "SELECT create_hypertable('hardware_usage','time', chunk_time_interval => INTERVAL '1 hour');"
cur.execute(sql1)
cur.execute(sql2)
print(cur.fetchall())
conn.commit()
conn.close()