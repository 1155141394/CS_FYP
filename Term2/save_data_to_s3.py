import os
import time
from datetime import date
import datetime
import pandas as pd
import numpy as np

def save_data_to_s3(bucket, tsid, time_start, time_end, data_path):

    time_start_str = time_start.strftime("%Y-%m-%d:%H")
    time_end_str = time_end.strftime("%Y-%m-%d:%H")
    file_name = "%s-%s" % (time_start_str, time_end_str)
    file_name = "%d-%d" % (time_start, time_end)
    os.system("aws s3 cp %s s3://%s/%d/%s" % (data_path, bucket, tsid, file_name))
    print("Save the file to S3 successfully.")

