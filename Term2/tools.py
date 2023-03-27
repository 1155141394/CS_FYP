import os
import numpy as np
import csv
from hash import *
import subprocess
import json

META_FOLDER = '/home/postgres/CS_FYP/meta/'

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
    rows = compressed_arr[len(compressed_arr)-2] # 获取原始数组的行列数
    cols = compressed_arr[len(compressed_arr)-1]
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



