import os
import time
from datetime import date
import datetime
import sys
import hashlib
import numpy as np
import csv
from hash import HashTable

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
    return compressed_arr, (rows, cols)


def decompress_array(compressed_arr, shape):
    """将一个压缩后的数组解压缩为原始的二维数组"""
    rows,cols = shape  # 获取原始数组的行列数
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
    return decompressed_arr


def time_index(start_t, end_t):
    hours=[]
    if start_t == None:
        end_h = end_t.hour
        end_index = end_h//2 + 1
        for i in range(1,end_index+1):
                hours.append(i)
        return hours
    elif end_t == None:
        start_h = start_t.hour
        start_index = start_h//2 + 1
        for i in range(start_index,13):
            hours.append(i)
        return hours
    else:
        start_h = start_t.hour
        end_h = end_t.hour
        start_index = start_h//2 + 1
        end_index = end_h//2 + 1
        for i in range(start_index,end_index+1):
            hours.append(i)
        return hours


def save_data_to_s3(bucket, tsid, time_start, time_end, data_path):
    generated_date = time_start.strftime("%Y-%m-%d")
    index = time_index(time_start, time_end)[0]
    file_name = f"{tsid}_{generated_date}_{index}.csv"
    os.system("aws s3 cp %s s3://%s/%s" % (data_path, bucket, file_name))
    print("Save the file to S3 successfully.")


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
   res = [index_map.put(tag1, 1), index_map.put(tag2, 1)]
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


