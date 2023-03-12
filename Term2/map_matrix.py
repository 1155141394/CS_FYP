import psycopg2
import pandas as pd
from tools import *
from hash import HashTable
def data_mapping(tags_name,value_name,des,lines):
    attr = []
    for item in des:
        attr.append(item[0])
    map_matrix = []
    tags_pair_set = set()
    print("line of data: {}",len(lines))
    for line in lines:
        tags_value = []
        value = []
        index_list = []
        tags_str = ''
        for i in range(len(tags_name)):
            # 获得表中attr中的值
            tags_value.append(line[attr.index(tags_name[i])])

        for i in range(len(value_name)):
            value.append(line[attr.index(value_name[i])])

        for tag in tags_value:
            tag = str(tag)
            # 获得cpu和node对应的hash值
            index_list.append(index(index_map, tag))
            tags_str += tag + '_'

        is_exist = 1 if tags_str in tags_pair_set else 0
        if is_exist:
            tsid = 0
            for i in range(len(map_matrix)):
                flag = 1
                for indexes in index_list:
                    if map_matrix[i][indexes] != 1:
                        flag = 0
                        break
                if flag:
                    tsid = i
                    break
            insert(tsid, value, value_name)
            continue
        else:
            tags_pair_set.add(tags_str)
            new_TS = [0] * 5000
            tsid = len(map_matrix)
            map_matrix.append(new_TS)
            for indexes in index_list:
                map_matrix[tsid][indexes] = 1

            insert(tsid, value, value_name)

    write_set_to_file(tags_pair_set, '/home/postgres/CS_FYP/data/query_set.txt')
    index_map.save_hash('/home/postgres/CS_FYP/data/query_hash')
    # 提交数据
    conn.commit()
    # 关闭连接
    conn.close()
    compress_arr, shape = compress_array(map_matrix)
    print(compress_arr, shape)
    map_matrix = decompress_array(compress_arr, shape)
    df = pd.DataFrame(map_matrix)
    df.to_csv('/home/postgres/CS_FYP/data/map_matrix.csv', index=False, header=False)


conn = psycopg2.connect(
    database="benchmark", user="postgres", password="1234", host="localhost", port="5432"
)
# 设置自动提交
conn.autocommit = True
# 使用cursor()方法创建游标对象
cursor = conn.cursor()
# 检索数据
cursor.execute('''SELECT * from cpu''')
index_map = HashTable(length=5000)
# Fetching 1st row from the table
lines = cursor.fetchall()
des = cursor.description
tags_name =  ["tags_id", "hostname"]
value_name = ['time','usage_user','usage_system' ,'usage\
_idle' ,'usage_nice','usage_iowait','usage_irq' ,'usage_softirq','usage_steal','us\
age_guest','usage_guest_nice','additional_tags']
data_mapping(tags_name,value_name,des,lines)



