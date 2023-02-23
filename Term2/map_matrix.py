import psycopg2
import pandas as pd
# 建立连接
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

map_matrix = []
for line in lines:
   node = line[attr.index("node")]
   cpu = line[attr.index("cpu")]
   time = line[attr.index("time")]
   cpu_usage = line[attr.index("cpu_usage")]
   node_index,cpu_index,is_exist = index(node,cpu)
   if is_exist:
      tsid = 0
      for i in range(len(map_matrix)):
         if map_matrix[i][node_index] == 1 and map_matrix[i][cpu_index] == 1:
            tsid = i
            break
      insert(tsid,time,cpu_usage)
      continue
   else:
      new_TS = [0]*5000
      tsid = len(map_matrix)
      map_matrix.append(new_TS)
      map_matrix[tsid][cpu_index] = 1
      map_matrix[tsid][node_index] = 1
      insert(tsid,time,cpu_usage)

# 提交数据
conn.commit()
# 关闭连接
conn.close()
df = pd.DataFrame(map_matrix)
df.to_csv('/home/postgres/CS_FYP/data/map_matrix', index=False, header=False)