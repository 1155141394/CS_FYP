import psycopg2
import pandas as pd
from tools import *
import time
from hash import HashTable
import csv
import datetime
from tqdm import tqdm

def data_mapping(tags_name,value_name,des,lines,ts_name,map_matrix,tags_pair_set,index_map):
    attr = []
    for item in des:
        attr.append(item[0])
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

        # 将时间转换为时间戳

        tmp = str(value[0])[:19]
        time_tuple = time.strptime(tmp, '%Y-%m-%d %H:%M:%S')
        value[0] = str(int(time.mktime(time_tuple)))

        for tag in tags_value:
            tag = str(tag)
            # 获得cpu和node对应的hash值
            index_list.append(index(index_map, tag))
            tags_str += (tag + '_')
        index_list.append(index(index_map, ts_name))
        tags_str += ts_name

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

    write_set_to_file(tags_pair_set, '/home/postgres/CS_FYP/meta/query_set.txt')
    index_map.save_hash('/home/postgres/CS_FYP/meta/query_hash')
    # compress_arr, shape = compress_array(map_matrix)
    # print(compress_arr, shape)
    # map_matrix = decompress_array(compress_arr, shape)
    df = pd.DataFrame(map_matrix)
    df.to_csv('/home/postgres/CS_FYP/meta/map_matrix.csv', index=False, header=False)


def run_tsbs(conn, begin_t, end_t):
    # 设置自动提交
    conn.autocommit = True
    # 使用cursor()方法创建游标对象
    cursor = conn.cursor()
    # 检索数据
    cursor.execute('''SELECT * from cpu where time > '%s' and time < '%s';'''%(begin_t,end_t))

    # Fetching 1st row from the table
    lines = cursor.fetchall()
    des = cursor.description
    tags_name = ["tags_id", "hostname"]
    ts_names = ['usage_user', 'usage_system', 'usage_idle',
                'usage_nice', 'usage_iowait', 'usage_irq', 'usage_softirq', 'usage_steal',
                'usage_guest', 'usage_guest_nice', 'additional_tags']

    # 判断是否第一次跑
    if os.path.exists('/home/postgres/CS_FYP/meta/map_matrix.csv'):
        index_map = HashTable.read_hash('/home/postgres/CS_FYP/meta/query_hash')
        map_matrix = csv.reader(open('/home/postgres/CS_FYP/meta/map_matrix.csv', 'r'))
        tags_pair_set = read_set_from_file("/home/postgres/CS_FYP/meta/query_set.txt")
    else:
        index_map = HashTable(length=5000)
        map_matrix = []
        tags_pair_set = set()

    for ts_name in tqdm(ts_names):
        value_name = []
        value_name.append('time')
        value_name.append(ts_name)
        data_mapping(tags_name, value_name, des, lines, ts_name, map_matrix, tags_pair_set, index_map)

    csv_files = find_all_csv('/home/postgres/CS_FYP/data')
    begin_dt = datetime.datetime.strptime(begin_t, '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.datetime.strptime(end_t, '%Y-%m-%d %H:%M:%S')
    for csv_file in csv_files:
        save_data_to_s3('csfyp2023', begin_dt, end_dt, csv_file)

if __name__ == "__main__":
    conn = psycopg2.connect(
        database="benchmark", user="postgres", password="1234", host="localhost", port="5432"
    )
    run_tsbs(conn, '2023-01-01 08:00:00', '2023-01-01 10:00:00')
    # 提交数据
    conn.commit()
    # 关闭连接
    conn.close()




