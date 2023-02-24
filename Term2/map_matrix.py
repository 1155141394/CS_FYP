import psycopg2
import pandas as pd
import hashlib
import csv
import os
# 建立连接

# Use sha1 to get the index of tags
def index(tag1 = "", tag2 = ""):
   if tag1 and tag2:
      encoded_tag1 = hashlib.sha1(tag1.encode("utf-8")).hexdigest()
      encoded_tag2 = hashlib.sha1((tag1+tag2).encode("utf-8")).hexdigest()
      return encoded_tag1, encoded_tag2
   else:
      print("Lose arguments.")


def insert(tsid, time, val, columns=None):
   file_name = f"./{tsid}.csv"
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

#Fetching 1st row from the table
lines = cursor.fetchall();
des = cursor.description
attr = []
for item in des:
   attr.append(item[0])
index_map = {} # record the tags pair
map_matrix = []
for line in lines:
   node = line[attr.index("node")]
   cpu = line[attr.index("cpu")]
   time = line[attr.index("time")]
   cpu_usage = line[attr.index("cpu_usage")]
   node_index,cpu_index = index(node, cpu)
   is_exist = 1 if node_index in index_map.keys() and cpu_index in index_map.keys() else 0 # determine tags whether exist
   if is_exist:
      tsid = 0
      for i in range(len(map_matrix)):
         if map_matrix[i][node_index] == 1 and map_matrix[i][cpu_index] == 1:
            tsid = i
            break
      insert(tsid,time,cpu_usage,["time", "value"])
      continue
   else:
      if node_index not in index_map.keys():
         index_map[node_index] = 1
         index_map[cpu_index] = 1
      else:
         index_map[cpu_index] = 1
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