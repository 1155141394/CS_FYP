import re
import psycopg2
import boto3
import time
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np
from tools import *
from hash import HashTable

META_FOLDER = '/var/lib/postgresql/CS_FYP/meta/'
def find_rows(arr, index1, index2):
    rows = []
    for i, row in enumerate(arr):
        if index1 != -1 and index2 != -1:
            if row[index1] == 1 and row[index2] == 1:
                rows.append(i)
        elif index1 == -1 and index2 != -1:
            if row[index2] == 1:
                rows.append(i)
        elif index1 != -1 and index2 == -1:
            if row[index1] == 1:
                rows.append(i)
    return rows


def get_params_from_sql(sql_query):
    import re
    # 用于提取表名的正则表达式
    table_regex = r'from\s+`?(\w+)`?'
    # 用于提取其他参数的正则表达式
    params_regex = r'(select|from|where|order by|limit|group by)\s+`?(\w+)`?(.*?)(?=(select|from|where|order by|limit|group by|$))'

    result = {}

    # 提取表名
    table_name = re.search(table_regex, sql_query)
    if table_name:
        result['table_name'] = table_name.group(1)

    # 提取其他参数
    params = re.findall(params_regex, sql_query, re.IGNORECASE)
    for param in params:
        result[param[0]] = (param[1], param[2])

    return result


def s3_data(expression, key):
    data = []
    s3 = boto3.client('s3')
    resp = s3.select_object_content(
        Bucket='fypts',
        Key=key,
        ExpressionType='SQL',
        Expression=expression,
        InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'},
        OutputSerialization={'CSV': {}},
    )
    com_rec = ""
    for event in resp['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            com_rec = com_rec + records


        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            print("Stats details bytesScanned: ")
            print(statsDetails['BytesScanned'])
            print("Stats details bytesProcessed: ")
            print(statsDetails['BytesProcessed'])
            print("Stats details bytesReturned: ")
            print(statsDetails['BytesReturned'])
    for line in (com_rec.splitlines()):
        # print(line)
        data.append(line.split(","))
    return data


def s3_select(tsid, where_clause):
    beg_t = '2023-4-2 09:00:00'
    end_t = '2099-4-2 09:00:00'
    # 判断除了time还有没有其他的条件
    attr_con = ''
    for elem in where_clause:
        if 'time' in elem:
            pattern = r"'(.*?)'"
            if '>' in elem:
                # 找到单引号内的数据
                beg_t = re.findall(pattern, elem)[0]
                time_tuple = time.strptime(beg_t, '%Y-%m-%d %H:%M:%S')
                beg_t_str = str(int(time.mktime(time_tuple)))
                continue
            elif '<' in elem:
                end_t = re.findall(pattern, elem)[0]
                time_tuple = time.strptime(end_t, '%Y-%m-%d %H:%M:%S')
                end_t_str = str(int(time.mktime(time_tuple)))
        else:
            attr_con = elem

    times = []  # record the date used to retrieve data
    retrieve_file = []

    print(beg_t_str, end_t_str)

    # Change the string to datetime type
    beg_t = datetime.strptime(beg_t, '%Y-%m-%d %H:%M:%S')
    end_t = datetime.strptime(end_t, '%Y-%m-%d %H:%M:%S')

    # Determine if the time interval is bigger than one day
    if end_t.date() > beg_t.date():
        temp_t = beg_t + timedelta(days=1)
        times.append([beg_t, temp_t])
        while temp_t.date() < end_t.date():
            times.append([temp_t, temp_t + timedelta(days=1)])
            temp_t = temp_t + timedelta(days=1)
        times.append([temp_t, end_t])

        for i in times:
            if i[0].strftime("%Y-%m-%d") == i[1].strftime("%Y-%m-%d"):
                file_name = str(tsid) + r"/%s_" % (i[0].strftime("%Y-%m-%d"))
                indexes = time_index(None, i[1])
                for index in indexes:
                    retrieve_file.append(file_name + str(index) + '.csv')

            else:
                file_name = str(tsid) + r"/%s_" % (i[0].strftime("%Y-%m-%d"))
                indexes = time_index(i[0], None)
                for index in indexes:
                    retrieve_file.append(file_name + str(index) + '.csv')


    elif end_t.date() == beg_t.date():
        file_name = str(tsid) + r"/%s_" % (beg_t.strftime("%Y-%m-%d"))
        indexes = time_index(beg_t, end_t)
        for index in indexes:
            retrieve_file.append(file_name + str(index) + '.csv')

    print(retrieve_file)

    # loop to retrieve the data from s3
    basic_exp = "SELECT * FROM s3object s where "  # Base expression
    if attr_con != '':
        basic_exp += attr_con
        basic_exp += ' AND '
    if len(retrieve_file) == 1:
        expression = basic_exp + "s.\"time\" between '%s' and '%s';" % (beg_t_str, end_t_str)
        key = retrieve_file[0]
        print(key)
        data = s3_data(expression, key)
        df = pd.DataFrame(data)
        print(df)
        df.to_csv(f'/home/postgresql/CS_FYP/data/result.csv', index=False, header=False)
    else:
        after_expression = basic_exp + "s.\"time\" > '%s';" % (beg_t_str)
        key = retrieve_file[0]
        data = s3_data(after_expression, key)

        for i in range(1, len(retrieve_file) - 1):
            expression = "SELECT * FROM s3object s"
            if attr_con != '':
                basic_exp += ' WHERE '
                basic_exp += attr_con
            key = retrieve_file[i]
            data = data + s3_data(expression, key)

        before_expression = basic_exp + "s.\"time\" < '%s';" % (end_t_str)
        key = retrieve_file[len(retrieve_file) - 1]
        data = data + s3_data(before_expression, key)
        df = pd.DataFrame(data)
        return df


def find_id(tags_list,attr_list):
    # 到s3寻找map
    state = os.system(f"aws s3 cp s3://map_matrix.txt " + META_FOLDER + 'map_matrix.txt' + '--profile csfyp')
    if state != 0:
        print(f"There is no map in s3.")

    compress_arr = txt_to_list(META_FOLDER + 'map_matrix.txt')
    content = decompress_array(compress_arr)
    # 读取query_hash
    index_map = HashTable.read_hash(META_FOLDER + 'query_hash')
    tsid_list = []
    for i in range(len(content)):
        tsid_list.append(i)
    for tag in tags_list:
        tag_index = index(index_map,tag)
        tmp_list = find_rows(content,tag_index,-1)
        tsid_list = [i for i in tsid_list if i in tmp_list]

    if len(attr_list) != 0:
        attr_tsid = []
        for attr in attr_list:
            attr_index = index(index_map, attr)
            tmp_list = find_rows(content, attr_index, -1)
            attr_tsid += [i for i in tsid_list if i in tmp_list]
        return attr_tsid

    return tsid_list

if __name__ == "__main__":
    table_name = 'cpu'
    tsids = find_id(['1','host_0'], ['usage_system'])
    where_clause = []
    where_clause.append("time > '2023-4-2 08:01:00'")
    where_clause.append("time < '2023-4-2 09:00:00'")
    print(tsids)
    df_list = []

    for tsid in tsids:
        df = s3_select(tsid, where_clause)
        df_list.append(df)
    if len(df_list) > 2:
        result = pd.concat(df_list)
        print(result)
        result.to_csv(f'/home/postgresql/CS_FYP/data/result.csv')




