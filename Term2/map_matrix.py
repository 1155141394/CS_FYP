import psycopg2
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
print(attr)
for line in lines:
   print(line[2])
# 提交数据
conn.commit()
# 关闭连接
conn.close()