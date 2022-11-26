import boto3
import time
from datetime import date, datetime, timedelta
import pandas as pd
s3 = boto3.client('s3')


times = [] # record the date used to retrieve data
basic_exp = "SELECT * FROM s3object s where s.\"time\" between " # Base expression
table_name = input("Please input the table you want to search:") # Get table name from user
beg_t = input("Please input the start time:") # Get the start time
end_t = input("Please input the end time:") # Get the end time

# Change the string to datetime type
beg_t = datetime.strptime(beg_t, '%Y-%m-%d %H:%M:%S')
end_t = datetime.strptime(end_t, '%Y-%m-%d %H:%M:%S')

# Change the string to date type
beg_t_date = beg_t.date()
end_t_date = end_t.date()

# Determine if the time interval is bigger than one day
if end_t_date > beg_t_date:
    temp_date = beg_t_date + timedelta(days=1)
    times.append([beg_t, temp_date])
    while temp_date < end_t_date:
        times.append([temp_date, temp_date+timedelta(days=1)])
        temp_date = temp_date + timedelta(days=1)
    times.append([temp_date, end_t])
elif end_t_date == beg_t_date:
    times.append([beg_t, end_t])

# Get the start time
pg_beg = time.time()
data = []
# loop to retrieve the data from s3
for i in times:
    if i[0].strftime("%Y-%m-%d") == i[1].strftime("%Y-%m-%d"):
        temp_date = i[1] + timedelta(days=1)
        file_name = table_name + "_%s-%s.csv" % (i[0].strftime("%Y-%m-%d"), temp_date.strftime("%Y-%m-%d"))
    else:
        file_name = table_name + "_%s-%s.csv" % (i[0].strftime("%Y-%m-%d"), i[1].strftime("%Y-%m-%d"))
    expression = basic_exp + "'%s' and '%s';" % (i[0], i[1])
    key = table_name + r"/" + file_name
    # print(expression)
    # print(key)
    resp = s3.select_object_content(
        Bucket='csfyp2023',
        Key=key,
        ExpressionType='SQL',
        Expression=expression,
        InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'},
        OutputSerialization = {'CSV': {}},
    )
    record = ""
    for event in resp['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            record = record + records
        #    for line in (records.splitlines(True)):
        #        print(line)
        #        data.append(line.split(","))

        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            print("Stats details bytesScanned: ")
            print(statsDetails['BytesScanned'])
            print("Stats details bytesProcessed: ")
            print(statsDetails['BytesProcessed'])
            print("Stats details bytesReturned: ")
            print(statsDetails['BytesReturned'])
    for line in (record.splitlines()):
        print(line)
        data.append(line.split(","))
df = pd.DataFrame(data)

df.to_csv('tmp.csv', index=False, header=False)
pg_end = time.time()
time_cost = pg_end - pg_beg
print("The time cost is %f" % time_cost)
