import psycopg2
from tools import *
import time
from hash import HashTable
import json
import datetime
import sys
import gc
from multiprocessing import Pool


def multi_thread_save_s3(table_name, begin_dt, end_dt, csv_folder):
    p = Pool(25)
    for csv_file in csv_folder:
        p.apply_async(save_data_to_s3, args=(table_name, begin_dt, end_dt, csv_file,))
    p.close()
    p.join()
    # print('Finish transferring data to s3.')


def data_mapping(tags_name, value_name, des, lines, ts_name, map_matrix, tags_pair_set, index_map):
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

        is_exist = 1 if tags_str in tags_pair_set.keys() else 0
        if is_exist:
            tsid = tags_pair_set[tags_str]

            insert(tsid, value, value_name)
            continue
        else:
            # tags_pair_set.add(tags_str)
            new_TS = [0] * 5000
            tsid = len(map_matrix)
            tags_pair_set[tags_str] = tsid
            map_matrix.append(new_TS)
            for indexes in index_list:
                map_matrix[tsid][indexes] = 1

            insert(tsid, value, value_name)

    write_dict_to_file(tags_pair_set, META_FOLDER + 'query_set.txt')
    index_map.save_hash(META_FOLDER + 'query_hash')
    compress_arr = str(compress_array(map_matrix))
    compress_arr = json.dumps(compress_arr)
    f = open(META_FOLDER + 'map_matrix.txt', 'w')
    f.write(compress_arr)
    f.close()


def run_tsbs(table_name, conn, begin_t, end_t):
    # 设置自动提交
    # conn.autocommit = True
    # 使用cursor()方法创建游标对象
    cursor = conn.cursor()
    # 检索数据
    cursor.execute('''SELECT * from %s where time > '%s' and time < '%s';''' % (table_name, begin_t, end_t))

    # Fetching 1st row from the table
    lines = cursor.fetchall()
    des = cursor.description
    tags_name = ["tags_id", "hostname"]
    ts_names = get_col_name(conn, table_name)

    # 判断是否第一次跑
    if os.path.exists(META_FOLDER + 'map_matrix.txt'):
        # print('Not first time to run')
        index_map = HashTable.read_hash(META_FOLDER + 'query_hash')
        compress_arr = txt_to_list(META_FOLDER + 'map_matrix.txt')
        map_matrix = decompress_array(compress_arr)
        tags_pair_set = read_dict_from_file(META_FOLDER + 'query_set.txt')
    else:
        index_map = HashTable(length=5000)
        map_matrix = []
        tags_pair_set = {}
        # tags_pair_set = set()

    for ts_name in ts_names:
        value_name = []
        value_name.append('time')
        value_name.append(ts_name)
        data_mapping(tags_name, value_name, des, lines, ts_name, map_matrix, tags_pair_set, index_map)
        gc.collect()

    csv_folder = find_all_csv(META_FOLDER)
    begin_dt = datetime.datetime.strptime(begin_t, '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.datetime.strptime(end_t, '%Y-%m-%d %H:%M:%S')

    # print("Transfer files to S3.")
    gc.collect()
    multi_thread_save_s3(table_name, begin_dt, end_dt, csv_folder)


def transfer_to_s3():
    with open('/var/lib/postgresql/output.txt','w') as f:
        f.write('hello')
    while True:
        now = datetime.datetime.now()
        if now.hour & 1 == 0 and now.minute == 0:
            conn = psycopg2.connect(
                database="benchmark", user="postgres", password="1234", host="localhost", port="5432"
            )
            start_time = now + datetime.timedelta(hours=-2)
            end_time = now
            table_names = get_table_name(conn)
            for table_name in table_names:
                # print("Start transfer the data in table %s." % table_name)
                run_tsbs(table_name, conn, datetime.datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S"),
                         datetime.datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S"))

            # 提交数据
            conn.commit()
            # 关闭连接
            conn.close()


if __name__ == "__main__":
    inputs = sys.argv
    conn = psycopg2.connect(
        database="benchmark", user="postgres", password="1234", host="localhost", port="5432"
    )
    table_names = get_table_name(conn)
    # print(table_names)
    for table_name in table_names:
        # print("Start transfer the data in table %s." % table_name)
        run_tsbs(table_name, conn, "2023-01-01 22:00:00", "2023-01-02 00:00:00")
    # 提交数据
    conn.commit()
    # 关闭连接
    conn.close()
