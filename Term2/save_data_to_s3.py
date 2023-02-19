import os
import time
from datetime import date
import datetime
import pandas as pd
import numpy as np

def save_data_to_s3(bucket, tsid, time_start, time_end, data_path):
    generated_date = time_start.strftime("%Y-%m-%d")
    time_start_str = time_start.strftime("%H")
    time_end_str = time_end.strftime("%H")
    file_name = "%s-%s" % (time_start_str, time_end_str)
    os.system("aws s3 cp %s s3://%s/%d/%s/%s" % (data_path, bucket, tsid, generated_date, file_name))
    print("Save the file to S3 successfully.")

