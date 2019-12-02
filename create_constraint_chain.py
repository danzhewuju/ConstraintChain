#!/usr/bin/python
import os
import numpy as np
import pandas as pd
import re


def get_full_path(dir_path):
    names = os.listdir(dir_path)
    full_path = [os.path.join(dir_path, x) for x in names]
    return full_path


class table_structure:
    def __init__(self, *kwg, **kwgl):
        self.all_table_structure = {}
        table_full_path = get_full_path(kwg[0])
        # 解析所有的表结构
        for f in table_full_path:
            self._parse_table(f)
        # 解析sql语句的结构
        self._parse_sql(kwg[1])

    def get_table_info(self, path):  # 读取表结构的原始数据
        data = pd.read_csv(path)
        return data

    def _parse_table(self, path_table_struct):
        # 对于单个表结构进行解析
        structure = self.get_table_info(path_table_struct)
        name = os.path.basename(path_table_struct)
        table_name = name.split(".")[0]

        columns = structure['name'].tolist()
        key_primary_ori = structure['key_primary'].tolist()
        foreign_key = structure['foreign_key'].tolist()
        columns = [str.lower(x) for x in columns]
        key_primary = {}
        key_foreign = {}
        for i in range(len(columns)):
            if key_primary_ori[i] == 1:
                key_primary[columns[i]] = 0  # 1.初始化主键， 同时
            if not pd.isnull(foreign_key[i]):
                key_foreign[columns[i]] = foreign_key[i]
        table_info = {"columns": columns, "key_primary": key_primary, "key_foreign": key_foreign}
        self.all_table_structure[table_name] = table_info

    def _parse_sql(self, path_sql):
        f = open(path_sql, 'r')
        sql_statement = []  # sql 语句的划分解析
        sql_sentence = ""
        for line in f:
            line = line.replace('\n', ' ')
            sql_sentence += line
            if ";" in sql_sentence:
                sql_statement.append(sql_sentence)
                sql_sentence = ""
        self.sql_statement = sql_statement
        f.close()

    # select count(*) from customer where c_mktsegment = 'BUILDING' ;  #30142
    def _sql_parser(self, sql_sentence):  # sql解析器，对于单一可执行的sql的语句进行解析
        parse_result = {}
        # 运行结果的解析
        running_result = int(re.findall(r'#(.+)', sql_sentence)[0])  # 这个语句在真实数据库中的运行结果
        # 可运行的sql语句的解析
        sql_sentence_real = re.findall(r'(.+)#', sql_sentence)[0]
        # 表的解析
        if "where" in sql_sentence_real:
            select_table = re.findall(r'from (.+) where', sql_sentence_real)[0]
        else:
            select_table = re.findall(r'from (.+);', sql_sentence_real)[0]
        select_table = select_table.replace(" ", "")
        select_table = select_table.split(",")  # 获得选择呢的列表

        # 约束条件的解析
        if "where" in sql_sentence_real:
            constraint_condition = re.findall(r'where(.+);', sql_sentence_real)[0]
            constraint_condition = constraint_condition.replace(' ', '')
            constraint_condition = constraint_condition.split("and")
        else:
            constraint_condition = []
        parse_result["running_result"] = running_result
        parse_result["select_table"] = select_table
        parse_result["constraint_condition"] = constraint_condition

        return parse_result

    def generating_constraint_chain(self):
        # sql 语法解析的核心算法
        # self.all_table_structure = {}和self._parse_sql(kwg[1])
        # 构造字符映射表
        char_table = {"c": "customer", "l": "lineitem", "n": "nation", "o": "orders", "p": "part", "ps": "partsupp",
                      "r": "region", "s": "supplier"}

        # 存储生成的约束链
        constraint_chain = {}
        # 记录上一次所有的约束条件
        # 当前增加的约束条件
        last_constraint_condition = []
        now_running_result = 0
        last_running_result = 0
        for sql_s in self.sql_statement:
            # 对于单个语句进行解析
            parse_result = self._sql_parser(sql_s)
            now_running_result = parse_result["running_result"]
            all_constraint_condition = parse_result["constraint_condition"]

            if len(parse_result["constraint_condition"]) == 0:
                # 此时没有约束条件
                # 此时只有一张表
                select_table = parse_result["select_table"]

                constraint_chain[select_table[0]] = "[{}]; ".format(select_table[0])
            else:
                # 新增加的约束条件
                add_constraint_condition = list(
                    set(all_constraint_condition).difference(set(last_constraint_condition)))[0]
                # 选择节点的解析
                if "'" in add_constraint_condition:
                    # 主节点的解析
                    op = list(c for c in add_constraint_condition if c in "<>=")[0]
                    probability = now_running_result / last_running_result
                    if probability == 1.0:
                        continue
                    probability = round(probability, 6)
                    op_left = add_constraint_condition[:add_constraint_condition.index(op)]
                    constraint_chain[char_table[add_constraint_condition.split("_")[0]]] += \
                        "[0, {}@{}, {}]; ".format(op_left, op, probability)
                else:
                    probability = now_running_result / last_running_result
                    if probability == 1.0:
                        continue
                    probability = round(probability, 6)
                    # 主键和外键的解析
                    op = list(c for c in add_constraint_condition if c in "<>=")[0]
                    op_left = add_constraint_condition[:add_constraint_condition.index(op)]  # o_custkey
                    op_right = add_constraint_condition[add_constraint_condition.index(op) + 1:]
                    left_table = char_table[op_left.split("_")[0]]
                    right_table = char_table[op_right.split("_")[0]]

                    # 解析 操作符左边的表

                    if op_left in self.all_table_structure[left_table]["key_foreign"].keys():
                        k = self.all_table_structure[right_table]["key_primary"][op_right]
                        m, n = int(pow(2, k)), int(pow(2, k + 1))
                        self.all_table_structure[left_table]["key_primary"][op_left] = 2 * (k + 1)

                        full_foreign_key = self.all_table_structure[left_table]["key_foreign"][op_left]
                        chain = "[2, {}, {}, {}, {}, {}]; ".format(op_left, probability, full_foreign_key, m, n)

                    else:
                        k = self.all_table_structure[left_table]["key_primary"][op_left]
                        m, n = int(pow(2, k)), int(pow(2, k + 1))
                        chain = "[1, {}, {}, {}]; ".format(op_left, m, n)
                    constraint_chain[left_table] += chain
                    # 解析右边的操作符
                    if op_right in self.all_table_structure[right_table]["key_foreign"].keys():
                        k = self.all_table_structure[left_table]["key_primary"][op_left]
                        m, n = int(pow(2, k)), int(pow(2, k + 1))
                        self.all_table_structure[right_table]["key_primary"][op_right] = 2 * (k + 1)
                        full_foreign_key = self.all_table_structure[right_table]["key_foreign"][op_right]
                        chain = "[2, {}, {}, {}, {}, {}]; ".format(op_right, probability, full_foreign_key, m, n)

                    else:
                        k = self.all_table_structure[right_table]["key_primary"][op_right]
                        m, n = int(pow(2, k)), int(pow(2, k + 1))
                        chain = "[1, {}, {}, {}]; ".format(op_right, m, n)
                    constraint_chain[right_table] += chain
            if len(all_constraint_condition) >= len(last_constraint_condition):
                last_constraint_condition = all_constraint_condition.copy()
            last_running_result = now_running_result
        return constraint_chain


def run(path_table="./Table_info", path_sql="SQL/sql_info"):
    T = table_structure(path_table, path_sql)
    constraint_chain = T.generating_constraint_chain()
    for k, p in constraint_chain.items():
        print(p)


if __name__ == '__main__':
    run()
