import os
import numpy as np
import csv
from hash import *
import subprocess
import json
from datetime import date, datetime, timedelta
# import datetime
import re
import struct

META_FOLDER = '/var/lib/postgresql/CS_FYP/meta/'

# 将两个list合并
def list_combine(list1,list2):
    total_list = []
    if len(list1) != len(list2):
        return total_list
    for i in range(len(list1)):
        tmp_list = []
        tmp_list.append(list1[i])
        tmp_list.append(list2[i])
        total_list.append(tmp_list)
    return total_list




def group_by_mins(data):
    group_data = []
    group_max = []
    group_min = []
    group_avg = []
    group_time = []
    flag = int(data[0][0]) - 300
    for i in range(len(data)):
        if int(data[i][0]) > flag:
            group_data.append(float(data[i][1]))
        else:
            group_max.append(max(group_data))
            group_min.append(min(group_data))
            group_avg.append(sum(group_data)/len(group_data))
            group_time.append(data[i][0])
            flag -= 300
            group_data = []
            group_data.append(float(data[i][1]))
    if len(group_data) != 0:
        group_max.append(max(group_data))
        group_min.append(min(group_data))
        group_avg.append(sum(group_data) / len(group_data))
        group_time.append(data[i][0])

    return group_time, group_min, group_max, group_avg



def compress_array(arr):
    """将一个二维的数组压缩为一个一维的数组，并返回一个元组，包含压缩后的数组和压缩前后数组的行列数"""
    rows, cols = np.shape(arr)  # 获取数组的行列数
    arr = np.array(arr)
    flat_arr = arr.flatten()  # 将二维数组变成一维数组
    compressed_arr = []  # 用于存储压缩后的数组
    count = 0  # 计数器，用于记录连续的零的个数
    for i in range(len(flat_arr)):
        if flat_arr[i] == 0:
            count += 1  # 如果当前位置是零，计数器加一
        else:
            if count > 0:
                compressed_arr.append(-count)  # 如果当前位置是一，将之前的零的个数作为负数存储
                count = 0
            compressed_arr.append(flat_arr[i])  # 将当前位置的值存储到压缩数组中
    if count > 0:
        compressed_arr.append(-count)  # 处理最后一段连续的零
    compressed_arr.append(rows)
    compressed_arr.append(cols)
    return compressed_arr


def decompress_array(compressed_arr):
    compressed_arr = list(map(int, compressed_arr))
    """将一个压缩后的数组解压缩为原始的二维数组"""
    rows = compressed_arr[len(compressed_arr) - 2]  # 获取原始数组的行列数
    cols = compressed_arr[len(compressed_arr) - 1]
    compressed_arr = compressed_arr[:-2]
    decompressed_arr = np.zeros((rows, cols), dtype=int)  # 创建一个全零的二维数组
    i = 0
    j = 0
    for k in range(len(compressed_arr)):
        if compressed_arr[k] < 0:  # 如果当前位置是负数，将它转换成对应的零的个数
            j += abs(compressed_arr[k])
        else:
            decompressed_arr[i, j] = compressed_arr[k]  # 将当前位置的值存储到解压缩数组中
            j += 1
        if j >= cols:  # 如果当前行填满了，换到下一行
            i += 1
            j -= cols
    return decompressed_arr.tolist()


def txt_to_list(filename):
    f = open(filename, 'r')
    out = f.read()
    response = json.loads(out)
    res = response.strip('[')
    res = res.strip(']')
    res = res.split(',')
    return res


def time_index(start_t, end_t):
    hours = []
    if start_t == None:
        end_h = end_t.hour
        end_index = end_h // 2 + 1
        for i in range(1, end_index + 1):
            hours.append(i)
        return hours
    elif end_t == None:
        start_h = start_t.hour
        start_index = start_h // 2 + 1
        for i in range(start_index, 13):
            hours.append(i)
        return hours
    else:
        start_h = start_t.hour
        end_h = end_t.hour
        start_index = start_h // 2 + 1
        end_index = end_h // 2 + 1
        for i in range(start_index, end_index + 1):
            hours.append(i)
        return hours


def save_data_to_s3(table_name, time_start, time_end, data_path):
    csv_file = data_path.split('/')[-1]
    tsid = csv_file[:-4]
    generated_date = time_start.strftime("%Y-%m-%d")
    if time_end.hour == 0:
        index = time_index(time_start, None)[0]
    elif time_start.hour == 0:
        index = time_index(None, time_end)[0]
    else:
        index = time_index(time_start, time_end)[0]
    file_name = f"{generated_date}_{index}.csv"
    os.system("aws s3 cp %s s3://%s/%s/%s > /dev/null" % (data_path, "fypts", tsid, file_name))
    os.system("rm %s > /dev/null" % data_path)


# change the string to char sum
def char_sum(str):
    res = 0
    count = 1
    for c in str:
        res += ord(c) * count
        count *= 256
    return res


# Use sha1 to get the index of tags
def index(index_map, tag=""):
    tag = char_sum(tag)
    res = index_map.put(tag, 1)
    return res


def insert(tsid, vals, columns=None):
    file_name = META_FOLDER + f"{tsid}.csv"
    if not os.path.exists(file_name):
        with open(file_name, "a") as f:
            csv_writer = csv.writer(f, delimiter=',')
            data = []
            for val in vals:
                data.append(str(val))
            csv_writer.writerow(columns)
            csv_writer.writerow(data)
    else:
        with open(file_name, "a") as f:
            csv_writer = csv.writer(f, delimiter=',')
            data = []
            for val in vals:
                data.append(str(val))
            csv_writer.writerow(data)
    # print(f"Write data to {tsid}.csv successfully.")


# 将set写入文件
def write_set_to_file(input_set, output_file):
    with open(output_file, 'w') as f:
        for x in input_set:
            f.write(str(x) + '\n')


# 读取文件的set
def read_set_from_file(input_file):
    output_set = set()
    with open(input_file, 'r') as f:
        for line in f:
            output_set.add(line.strip())
    return output_set


def write_dict_to_file(dict, output_file):
    f = open(output_file, 'w')
    f.write(str(dict))
    f.close()


def read_dict_from_file(input_file):
    with open(input_file, 'r') as f:
        a = f.read()
        return eval(a)


# 将hashtable写入文件
def hash_to_file(hashtable, output_file):
    with open(output_file, "w") as f:
        f.write(f"{str(len(hashtable.slots))}\n")
        for indx, key in enumerate(hashtable.slots):
            f.write(f"{str(key)}:{str(hashtable.data[indx])}\n")


# 读取文件的hashtable
def read_hash_from_file(input_file):
    slots = []
    data = []
    hash_len = 0
    flag = 0
    with open(input_file, "r") as f:
        for line in f.readlines():
            line = line.strip()
            if not flag:
                flag = 1
                hash_len = int(line)
            else:
                line = line.split(":")
                slots.append(line[0])
                data.append(line[1])
    hashtable = HashTable(slots=slots, data=data, length=hash_len)
    return hashtable


# return csv_file list
def find_all_csv(dir):
    csv_files = []
    files = os.listdir(dir)
    for file in files:
        if ".csv" in file:
            csv_files.append(dir + file)
    return csv_files


# get all the column name given a conn and table name
def get_col_name(conn, table_name):
    res = []
    tags = ["time", "tags_id", "hostname"]
    cur = conn.cursor()
    sql = r"select column_name from information_schema.columns where table_schema='public' and table_name='%s';" % table_name
    cur.execute(sql)
    col_names = cur.fetchall()
    for col_name in col_names:
        if col_name[0] in tags:
            continue
        else:
            res.append(col_name[0])
    print(res)
    return res


def byte_to_time(byte_data):
    byte_data_new = []
    for i in byte_data:
        if i < 0:
            i += 256
        byte_data_new.append(i)
    binary_data = bytes(byte_data_new)
    microseconds = struct.unpack('q', binary_data)[0]

    # 计算 Unix 纪元的日期时间
    epoch = datetime(2000, 1, 1)
    timestamp = epoch + timedelta(hours=0, microseconds=microseconds)

    # 将日期时间格式化为字符串
    formatted_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')

    return formatted_timestamp


def byte_to_int(byte_data):
    byte_data_new = []
    for i in byte_data:
        if i < 0:
            i += 256
        byte_data_new.append(i)
    byte_stream = bytes(byte_data_new)
    value = struct.unpack("<q", byte_stream)[0]
    return value

def byte_to_str(byte_data):
    byte_data_new = []
    for i in byte_data:
        if i < 0:
            i += 256
        byte_data_new.append(i)
    result = ''.join(chr(int(num)) for num in byte_data_new if int(num) > 0)

    return result[1:]



def parse_query(attr, table, where_input):

    # parse attribute:
    attr_type = ''
    attrs = attr.split(',')
    attrs_res = []
    if len(attrs) > 1 and attrs[1] != '':
        for i in attrs:
            if i == 'tags_id' or i == 'hostname':
                continue
            else:
                attrs_res.append(i)
    else:
        attr = attrs[0]
        if "max" in attr:
            attr_type = 'max'
        elif "avg" in attr:
            attr_type = 'avg'
        elif "min" in attr:
            attr_type = 'min'
        attrs_res.append(attr[4:])


    # parse tabel

    cpu_col = {1: 'time', 2: 'tags_id', 3: 'hostname', 4: 'usage_user', 5: 'usage_system', 6: 'usage_idle',
               7: 'usage_nice',
               8: 'usage_iowait', 9: 'usage_irq', 10: 'usage_softirq', 11: 'usage_steal', 12: 'usage_guest',
               13: 'usage_guest_nice', 14: 'additional_tags'}


    # parse where part

    # operation dictionary (oid: operation)
    opno_dict = {'96': '=', '97': '<', '521': '>', '523': '<=', '525': '>=',  # int4
                 '1320': '=', '1322': '<', '1323': '<=', '1324': '>', '1325': '>=',  # timestamptz
                 '98': '=',  #text
                 '674': '>'
                 }

    # variable type dictionary
    var_oid_dict = {'23': 'int4', '1184': 'timestamptz', '25': 'text', '701': 'float8'}

    BoolEXPR = []
    opno_list = []
    col_indx_list = []
    vartype_list = []
    byte_list = []
    value_list = []

    split_res = where_input.split(' ')
    # print(split_res)
    for indx, word in enumerate(split_res):

        # get and / or
        if "BOOLEXPR" in word:
            BoolEXPR.append(split_res[indx + 2])

        # get operations in the where part
        if "opno" in word:
            opexpr = split_res[indx + 1]
            opno_list.append(opno_dict[opexpr])

        # get attribution in which column
        if "varattnosyn" in word:
            col = split_res[indx + 1]
            col_indx_list.append(int(col))

        # get variable type
        if "vartype" in word:
            vartype = split_res[indx + 1]
            vartype_list.append(var_oid_dict[vartype])

        # get value
        if "constvalue" in word:
            length = int(split_res[indx+1]) if int(split_res[indx+1]) > 8 else 8
            data = split_res[indx + 3: indx + 3 + length]
            # print(data)
            byte_list.append([int(i) for i in data])

    where_len = len(opno_list)

    for i in range(where_len):
        if vartype_list[i] == "timestamptz":
            value_list.append(byte_to_time(byte_list[i]))

        elif vartype_list[i] == "int4":
            value_list.append(byte_to_int(byte_list[i]))

        elif vartype_list[i] == 'text':
            value_list.append(byte_to_str(byte_list[i]))

        elif vartype_list[i] == 'float8':
            value_list.append(byte_to_int(byte_list[i]))


    # print(BoolEXPR)
    # print(opno_list)
    # print(col_indx_list)
    # print(vartype_list)
    # print(byte_list)
    # print(value_list)

    # put all the need things into a dictionary
    res = {'where_clause': [], 'conn': [], 'tags': [], 'attr': attrs_res, 'attr_type': attr_type}
    flag = 0
    for i in range(where_len):
        if cpu_col[col_indx_list[i]] == 'tags_id' or cpu_col[col_indx_list[i]] == 'hostname':
            res['tags'].append(str(value_list[i]))
            flag = 1
            # res['conn'].pop()
        else:
            if vartype_list[i] == "float8":
                tmp = "CAST("+ cpu_col[col_indx_list[i]] + ' AS FLOAT) ' + opno_list[i] + " " + str(value_list[i])
            else:
                tmp = cpu_col[col_indx_list[i]] + ' ' + opno_list[i] + " '" + str(value_list[i]) + "'"
            res['where_clause'].append(tmp)
            if flag == 1:
                flag = 0
            elif i < where_len - 1:
                res['conn'].append(BoolEXPR[0])


    # print(res['where_clause'])
    # print(res['conn'])
    # print(res['tags'])
    # print(res['attr'])
    return res


def get_table_name(conn):
    res = []
    cur = conn.cursor()
    sql = "select * from pg_tables where schemaname = 'public';"
    cur.execute(sql)
    data_lines = cur.fetchall()
    for data_line in data_lines:
        if data_line[1] == "tags":
            continue
        res.append(data_line[1])
    return res


# if __name__ == '__main__':
#     attr = 'time,tags_id,hostname,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice,additional_tags'
#     table = 'cpu'
#     input = "{BOOLEXPR :boolop and :args ({OPEXPR :opno 1324 :opfuncid 1157 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 1 :vartype 1184 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnosyn 1 :varattnosyn 1 :location 24} {CONST :consttype 1184 :consttypmod -1 :constcollid 0 :constlen 8 :constbyval true :constisnull false :location 31 :constvalue 8 [ 0 36 -8 -70 78 -101 2 0 ]}) :location 29} {OPEXPR :opno 1322 :opfuncid 1154 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 1 :vartype 1184 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnosyn 1 :varattnosyn 1 :location 57} {CONST :consttype 1184 :consttypmod -1 :constcollid 0 :constlen 8 :constbyval true :constisnull false :location 64 :constvalue 8 [ 0 40 99 -81 99 -101 2 0 ]}) :location 62} {OPEXPR :opno 98 :opfuncid 67 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 100 :args ({VAR :varno 1 :varattno 3 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnosyn 1 :varattnosyn 3 :location 90} {CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 :constbyval false :constisnull false :location 99 :constvalue 12 [ 48 0 0 0 104 111 115 116 95 49 51 48 ]}) :location 98} {OPEXPR :opno 96 :opfuncid 65 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnosyn 1 :varattnosyn 2 :location 114} {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 124 :constvalue 4 [ -127 0 0 0 0 0 0 0 ]}) :location 122} {OPEXPR :opno 674 :opfuncid 297 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 5 :vartype 701 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnosyn 1 :varattnosyn 5 :location 132} {FUNCEXPR :funcid 316 :funcresulttype 701 :funcretset false :funcvariadic false :funcformat 2 :funccollid 0 :inputcollid 0 :args ({CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 147 :constvalue 4 [ 9 0 0 0 0 0 0 0 ]}) :location -1}) :location 145}) :location 53}"
#     parse_query(attr, table, input)
