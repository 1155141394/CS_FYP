import os
import time
from datetime import date
import datetime
import pandas as pd
import numpy as np
import sys
import psycopg2
import hashlib
import csv
from hash import HashTable

def list_to_int(array):
    value = 0
    length = len(array)
    for i in range(length):
        value += array[length-i-1]*pow(2,i)
    return value

def int_to_list(value):
    array = []
    temp = format(value,"b")
    temp = str(temp)
    for char in temp:
        array.append(int(char))
    return array

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
    if time_end.hour == 0:
        index = time_index(time_start, None)[0]
    elif time_start.hour == 0:
        index = time_index(None, time_end)[0]
    else:
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


