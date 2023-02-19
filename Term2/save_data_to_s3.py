import os
import time
from datetime import date
import datetime
import pandas as pd
import numpy as np

t = datetime.datetime(1970, 1, 1)


def save_data_to_s3(bucket, tsid, time_start, time_end, data_path):
    time_start = int((time_start-t).total_seconds())
    time_end = int((time_end-t).total_seconds())
    file_name = "%d-%d" % (time_start, time_end)
    os.system("aws s3 cp %s s3://%s/%d/%s" % (data_path, bucket, tsid, file_name))
    print("Save the file to S3 successfully.")

