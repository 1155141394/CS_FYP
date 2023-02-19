import os
import time
from datetime import date
import datetime
import pandas as pd
import numpy as np
from cpu_retrieve import time_index


def save_data_to_s3(bucket, tsid, time_start, time_end, data_path):
    generated_date = time_start.strftime("%Y-%m-%d")
    index = time_index(time_start, time_end)[0]
    file_name = "%s-%s" % (time_start, time_end)
    os.system("aws s3 cp %s s3://%s/%d/%s/%s" % (data_path, bucket, tsid, generated_date, file_name))
    print("Save the file to S3 successfully.")
