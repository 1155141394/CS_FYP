import psutil
import psycopg2
import time

conn = psycopg2.connect(host="localhost", port=5432, user="postgres", password="1234", database="example")
while True:
    cpu_percent = psutil.cpu_percent()
    memory_percent = psutil.virtual_memory().percent
    cur = conn.cursor()
    sql = "INSERT INTO hardware_usage(time, cpu_percent, memory_percent) VALUES " \
          "(NOW(),'%.2f','%.2f');"%(cpu_percent,memory_percent)
    cur.execute(sql)
    # print(cur.fetchall())
    conn.commit()
    time.sleep(10)
    print(time.localtime(), cpu_percent, memory_percent)

conn.close()

