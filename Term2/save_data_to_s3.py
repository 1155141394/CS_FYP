import os
import time
from datetime import date
import datetime
import pandas as pd
import numpy as np
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
    file_name = "%d.csv" % index
    os.system("aws s3 cp %s s3://%s/%d/%s/%s" % (data_path, bucket, tsid, generated_date, file_name))
    print("Save the file to S3 successfully.")
