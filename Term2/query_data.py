import sys
import os
import time
from datetime import date
import datetime

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
    #用于提取表名的正则表达式
    table_regex = r'from\s+`?(\w+)`?'
    #用于提取其他参数的正则表达式
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

# # 测试一下
# sql_query = 'SELECT id,name FROM mytable WHERE country="China" ORDER BY age LIMIT 10'
#
# result = get_params_from_sql(sql_query)
# print(result)
arr = [
    [1, 0, 1],
    [0, 1, 0],
    [0, 0, 1],
    [1, 1, 0],
    [0, 1, 1]
]

print(find_rows(arr, 0, 2))  # [0, 2]
print(find_rows(arr, -1, 1))  # [0, 1, 3, 4]
print(find_rows(arr, 2, -1))  # [0, 4]

