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

# 找到index1和index2列都为1的行
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


def s3_data(expression, key, attr_type):
    data = []
    s3 = boto3.client('s3')
    try:
        resp = s3.select_object_content(
            Bucket='fypts',
            Key=key,
            ExpressionType='SQL',
            Expression=expression,
            InputSerialization={'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'},
            OutputSerialization={'CSV': {}},
        )
    except Exception as e:
        return None

    com_rec = ""
    for event in resp['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            com_rec = com_rec + records


        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            # print("Stats details bytesScanned: ")
            # print(statsDetails['BytesScanned'])
            # print("Stats details bytesProcessed: ")
            # print(statsDetails['BytesProcessed'])
            print("Stats details bytesReturned: ")
            print(statsDetails['BytesReturned'])
    for line in (com_rec.splitlines()):
        data_line = line.split(",")
        data.append(data_line)
    # 需要进行分组处理一段时间的数据
    if attr_type != '':
        time_group, min_group, max_group, avg_group = group_by_mins(data)
        data = []
        if attr_type == 'max':
            data = list_combine(time_group,max_group)
        elif attr_type == 'min':
            data = list_combine(time_group,min_group)
        elif attr_type == 'avg':
            data = list_combine(time_group,avg_group)
    print(data)
    return data


def s3_select(tsid, where_clause, attr_type):
    beg_t = '2023-4-2 09:00:00'
    end_t = '2024-4-2 09:00:00'
    # 判断除了time还有没有其他的条件
    attr_con = ''
    for elem in where_clause:
        if 'time' in elem:
            pattern = r"'(.*?)'"
            if '>' in elem:
                # 找到单引号内的数据
                beg_t = re.findall(pattern, elem)[0]

            if '<' in elem:
                end_t = re.findall(pattern, elem)[0]

        else:
            attr_con = elem
    print(beg_t,end_t)
    time_tuple = time.strptime(beg_t, '%Y-%m-%d %H:%M:%S')
    beg_t_str = str(int(time.mktime(time_tuple)))
    time_tuple = time.strptime(end_t, '%Y-%m-%d %H:%M:%S')
    end_t_str = str(int(time.mktime(time_tuple)))

    times = []  # record the date used to retrieve data
    retrieve_file = []

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


    # loop to retrieve the data from s3
    basic_exp = "SELECT * FROM s3object s WHERE "  # Base expression
    if attr_con != '':
        basic_exp += attr_con
        basic_exp += ' AND '
    if len(retrieve_file) == 1:
        expression = basic_exp + "s.\"time\" > '%s' AND s.\"time\" < '%s';" % (beg_t_str, end_t_str)
        print(expression)
        key = retrieve_file[0]
        data = s3_data(expression, key, attr_type)
        df = pd.DataFrame(data)
        return df

    else:
        data = []
        after_expression = basic_exp + "s.\"time\" > '%s';" % (beg_t_str)
        key = retrieve_file[0]
        ret_data = s3_data(after_expression, key, attr_type)
        if ret_data is not None:
            data += ret_data

        for i in range(1, len(retrieve_file) - 1):
            expression = "SELECT * FROM s3object s"
            if attr_con != '':
                basic_exp += ' WHERE '
                basic_exp += attr_con
            key = retrieve_file[i]
            ret_data = s3_data(expression, key, attr_type)
            if ret_data is None:
                break
            data += ret_data

        before_expression = basic_exp + "s.\"time\" < '%s';" % (end_t_str)
        key = retrieve_file[len(retrieve_file) - 1]
        ret_data = s3_data(before_expression, key, attr_type)
        if ret_data is not None:
            data += ret_data
        df = pd.DataFrame(data)
        return df


def find_id(tags_list,attr_list):
    tsid_list = []
    if not os.path.exists(META_FOLDER + 'map_matrix.txt'):
        # 到s3寻找map
        state = os.system(f"aws s3 cp s3://map_matrix.txt " + META_FOLDER + 'map_matrix.txt' + '--profile csfyp')
        if state != 0:
            print(f"There is no map in s3.")
            return tsid_list


    compress_arr = txt_to_list(META_FOLDER + 'map_matrix.txt')
    content = decompress_array(compress_arr)
    # 读取query_hash
    index_map = HashTable.read_hash(META_FOLDER + 'query_hash')
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


def query(attr,table,input):
    query_dict = parse_query(attr,table,input)

    begin_time = time.time()

    where_clause = query_dict['where_clause']
    tsid = query_dict['tsid']
    attr = query_dict['attr']
    attr_type = query_dict['attr_type']

    findid_b = time.time()
    tsids = find_id(tsid, attr)
    findid_e = time.time()
    print(tsids)

    df_list = []
    df = pd.DataFrame([])
    for tsid in tsids:
        df = s3_select(tsid, where_clause, attr_type)
        df_list.append(df)
    end_time = time.time()
    total_cost = end_time - begin_time
    findid_cost = findid_e - findid_b
    # print(df_list)
    print(f'Find id cost:{findid_cost} sec')
    print(f'Query cost: {total_cost} sec')

    if len(df_list) < 2:
        df.to_csv(f'/var/lib/postgresql/CS_FYP/data/result.csv')
    else:
        df_list = pd.concat(df_list)
        df_list.to_csv(f'/var/lib/postgresql/CS_FYP/data/result.csv')


if __name__ == "__main__":
    attr = 'max_usage_system'
    table = 'cpu'
    input = 'BOOLEXPR :boolop and :args ({OPEXPR :opno 96 :opfuncid 65 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnosyn 1 :varattnosyn 2 :location 35} {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location 45 :constvalue 4 [ 1 0 0 0 0 0 0 0 ]}) :location 43} {OPEXPR :opno 98 :opfuncid 67 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 100 :args ({VAR :varno 1 :varattno 3 :vartype 25 :vartypmod -1 :varcollid 100 :varlevelsup 0 :varnosyn 1 :varattnosyn 3 :location 51} {CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 :constbyval false :constisnull false :location 62 :constvalue 10 [ 40 0 0 0 104 111 115 116 95 48 ]}) :location 60} {OPEXPR :opno 1324 :opfuncid 1157 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 1 :vartype 1184 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnosyn 1 :varattnosyn 1 :location 75} {CONST :consttype 1184 :consttypmod -1 :constcollid 0 :constlen 8 :constbyval true :constisnull false :location 82 :constvalue 8 [ -18 -76 36 78 85 -101 2 0 ]}) :location 80} {OPEXPR :opno 1322 :opfuncid 1154 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 1 :vartype 1184 :vartypmod -1 :varcollid 0 :varlevelsup 0 :varnosyn 1 :varattnosyn 1 :location 120} {CONST :consttype 1184 :consttypmod -1 :constcollid 0 :constlen 8 :constbyval true :constisnull false :location 127 :constvalue 8 [ -18 88 -72 36 86 -101 2 0 ]}) :location 125}) :location 47'
    query(attr,table,input)

