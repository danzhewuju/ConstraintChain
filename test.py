#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/11/25 1:25 下午
# @Author  : Alex
# @Site    : 
# @File    : test.py
# @Software: PyCharm
import re

if __name__ == '__main__':
    path = "Table_info/customer.csv"
    name = re.findall(r'/(.+)\.csv', path)[0]
    print(name)

    sql_sentence = 'select count(*) from customer,orders where c_mktsegment = \'BUILDING\' and c_custkey = o_custkey;  #303959'
    # sql_sentence = 'select count(*) from orders ;   #150000'
    running_result = re.findall(r'#(.+)', sql_sentence)[0]  # 这个语句在真实数据库中的运行结果
    print(running_result)
    sql_sentence_real = re.findall(r'(.+)#', sql_sentence)[0]
    print(sql_sentence_real)

    if "where" in sql_sentence_real:
        select_table = re.findall(r'from (.+) where', sql_sentence_real)[0]
    else:
        select_table = re.findall(r'from (.+);', sql_sentence_real)[0]
    select_table = select_table.replace(' ', '')
    select_table = select_table.split(",")
    print(select_table)

    if "where" in sql_sentence_real:
        constraint_condition = re.findall(r'where(.+);', sql_sentence_real)[0]
        constraint_condition = constraint_condition.replace(' ', '')
        constraint_condition = constraint_condition.split("and")
    else:
        constraint_condition = []
    print(constraint_condition)
    sql = constraint_condition[0]
    if "'" in sql:
        print("T")
    else:
        print("False")

    op = list(c for c in sql if c in "<>=")[0]
    print(op)
    op_left = sql[:sql.index(op)]
    print(op_left)

    n = 1.234453
    round(n, 3)
    print(n)


    a = [1, 2, 3, 4]
    b = [1, 3, 4]
    c = list(set(a).difference(set(b)))[0]
    print(c)

    path = "./Table_info/orders.csv"
    name = path.split("/")[-1]
    name = re.findall(r'(.+).csv', name)[0]
    print(name)

