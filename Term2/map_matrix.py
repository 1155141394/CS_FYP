import psycopg2
import pandas as pd
import hashlib
import csv
import os
from hash import HashTable
# 建立连接

# change the string to char sum
def char_sum(str):
   res = 0
   count = 1
   for c in str:
      res += ord(c) * count
      count *= 256
   return res


# Use sha1 to get the index of tags
def index(index_map, tag1="", tag2=""):
   tag2 = char_sum(tag2)
   tag1 = char_sum(tag1)
   tag1_val =  index_map.get(tag1)
   tag2_val = index_map.get(tag2)
   res = [index_map.put(tag1, 1), index_map.put(tag2, 1)]
   if tag1_val and tag2_val:
      res.append(True)
   else:
      res.append(False)
      print("not existed")
   return res


def insert(tsid, time, val, columns=None):
   file_name = f"/home/postgres/CS_FYP/data/{tsid}.csv"
   if not os.path.exists(file_name):
      with open(file_name, "a") as f:
         csv_writer = csv.writer(f, delimiter=',')
         data = [str(time), str(val)]
         csv_writer.writerow(columns)
         csv_writer.writerow(data)
   else:
      with open(file_name, "a") as f:
         csv_writer = csv.writer(f, delimiter=',')
         data = [str(time), str(val)]
         csv_writer.writerow(data)
   print(f"Write data to {tsid}.csv successfully.")




conn = psycopg2.connect(
   database="example", user="postgres", password="1234", host="localhost", port="5432"
)
# 设置自动提交
conn.autocommit = True
# 使用cursor()方法创建游标对象
cursor = conn.cursor()
# 检索数据
cursor.execute('''SELECT * from cpu_usage''')
index_map = HashTable(5000)
#Fetching 1st row from the table
lines = cursor.fetchall();
des = cursor.description
attr = []
for item in des:
   attr.append(item[0])
map_matrix = []
for line in lines:
   node = line[attr.index("node")]
   cpu = line[attr.index("cpu")]
   time = line[attr.index("time")]
   cpu_usage = line[attr.index("cpu_usage")]
   node_index, cpu_index, is_exist = index(index_map, node, cpu)
   if is_exist:
      tsid = 0
      for i in range(len(map_matrix)):
         if map_matrix[i][node_index] == 1 and map_matrix[i][cpu_index] == 1:
            tsid = i
            break
      insert(tsid,time,cpu_usage,["time", "value"])
      continue
   else:
      new_TS = [0]*5000
      tsid = len(map_matrix)
      map_matrix.append(new_TS)
      map_matrix[tsid][cpu_index] = 1
      map_matrix[tsid][node_index] = 1
      insert(tsid,time,cpu_usage,["time", "value"])

# 提交数据
conn.commit()
# 关闭连接
conn.close()
df = pd.DataFrame(map_matrix)
df.to_csv('/home/postgres/CS_FYP/data/map_matrix', index=False, header=False)