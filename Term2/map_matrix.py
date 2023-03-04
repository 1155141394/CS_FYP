import psycopg2
import pandas as pd
from tools import *
from hash import HashTable

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
cpu_node = set()
for line in lines:
   # 获得表中attr中的值
   node = line[attr.index("node")]
   cpu = line[attr.index("cpu")]
   time = line[attr.index("time")]
   cpu_usage = line[attr.index("cpu_usage")]
   # 获得cpu和node对应的hash值
   node_index, cpu_index = index(index_map, node, cpu)
   is_exist = 1 if cpu + '_' + node in cpu_node else 0
   if is_exist:
      tsid = 0
      for i in range(len(map_matrix)):
         if map_matrix[i][node_index] == 1 and map_matrix[i][cpu_index] == 1:
            tsid = i
            break
      insert(tsid,time,cpu_usage,["time", "value"])
      continue
   else:
      cpu_node.add(node + '_' + cpu)
      new_TS = [0]*5000
      tsid = len(map_matrix)
      map_matrix.append(new_TS)
      map_matrix[tsid][cpu_index] = 1
      map_matrix[tsid][node_index] = 1
      insert(tsid,time,cpu_usage,["time", "value"])

write_set_to_file(cpu_node,'/home/postgres/CS_FYP/data/query_set.txt')
index_map.save_hash('/home/postgres/CS_FYP/data/query_hash')
# 提交数据
conn.commit()
# 关闭连接
conn.close()
df = pd.DataFrame(map_matrix)
df.to_csv('/home/postgres/CS_FYP/data/map_matrix.csv', index=False, header=False)