a = [0]*4
a[0] = 8
print(a)

import hashlib
import csv
import os
a = "cpu0"

print(hashlib.sha1(str.encode("utf-8")).hexdigest())
f = open("./tmp.txt", "a")
f.writelines("a")


def insert(tsid, time, val, columns=None):
   file_name = f"./{tsid}.csv"
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


if __name__ == '__main__':
    insert(1, 1, 1, ["time", "value"])
