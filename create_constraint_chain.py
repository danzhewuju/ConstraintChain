#!/usr/bin/python
import os
import numpy as np
import pandas as pd
import re
import pickle


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

    def _parse_sql(self, data_sql):

        sql_statement = []  # sql 语句的划分解析
        sql_sentence = ""
        for d in data_sql:
            count = d['count']
            sql = d['sql']
            sql_sentence = "{} ##{}".format(sql, count)
            sql_statement.append(sql_sentence)
        self.sql_statement = sql_statement
        # f.close()

    # select count(*) from customer where c_mktsegment = 'BUILDING' ;  #30142
    def _sql_parser(self, sql_sentence):  # sql解析器，对于单一可执行的sql的语句进行解析
        special = [" between "]
        parse_result = {}
        # 运行结果的解析
        running_result = int(re.findall(r'##(.+)', sql_sentence)[0])  # 这个语句在真实数据库中的运行结果
        # 可运行的sql语句的解析
        sql_sentence_real = re.findall(r'(.+)##', sql_sentence)[0]
        # 表的解析
        if "where" in sql_sentence_real:
            select_table = re.findall(r'from (.+) where', sql_sentence_real)[0]
        else:
            select_table = re.findall(r'from (.+);', sql_sentence_real)[0]
        # 过滤掉 nation n2 这种情况
        # 不允许过滤，需要进行处理
        select_table = "_".join(select_table.split(" ")) if " " in select_table else select_table

        select_table = select_table.replace(" ", "")
        select_table = select_table.split(",")  # 获得选择呢的列表

        # 约束条件的解析
        if "where" in sql_sentence_real:
            del_strip = lambda x: x.lstrip().rstrip()
            constraint_condition = re.findall(r'where(.+);', sql_sentence_real)[0]
            # constraint_condition = constraint_condition.replace(' ', '')
            constraint_condition = constraint_condition.split(" and ")
            # 特殊模式的拼接
            constraint_condition_result = []
            i = 0
            while i < len(constraint_condition):
                if " between " in constraint_condition[i]:
                    result = constraint_condition[i] + " and " + "'" + constraint_condition[i + 1]
                    result = "{} between '{}'".format(result.split(" between ")[0], result.split(" between ")[-1])
                    i += 1
                    constraint_condition_result.append(result)
                else:
                    result = constraint_condition[i]
                    constraint_condition_result.append(result)
                i += 1
            constraint_condition_result = [del_strip(x) for x in constraint_condition_result]
            # constraint_condition.lstrip()
            # constraint_condition.rstrip()
        else:
            constraint_condition_result = []
        parse_result["running_result"] = running_result
        parse_result["select_table"] = select_table
        parse_result["constraint_condition"] = constraint_condition_result

        return parse_result

    def generating_constraint_chain(self):
        # sql 语法解析的核心算法
        # self.all_table_structure = {}和self._parse_sql(kwg[1])
        # 构造字符映射表
        char_table = {"c": "customer", "l": "lineitem", "n": "nation", "n1": "nation_n1", "n2": "nation_n2",
                      "o": "orders", "p": "part", "ps": "partsupp", "r": "region", "s": "supplier"}

        # 存储生成的约束链
        constraint_chain = {}
        # 记录上一次所有的约束条件
        # 当前增加的约束条件
        last_constraint_condition = []
        op_list = [" <= ", " >= ", " <> ", " < ", " > ", " = ", " not in ", " in ", " not like ", " like ", " between "]
        # now_running_result = 0
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
                # 有重复的表取名的情况
                if select_table[0] in constraint_chain.keys():
                    continue
                constraint_chain[select_table[0]] = "[{}]; ".format(select_table[0])
            else:
                # 新增加的约束条件
                add_constraint_condition = list(
                    set(all_constraint_condition).difference(set(last_constraint_condition)))[0]
                # 选择节点的解析

                # print(add_constraint_condition)
                op_p = list(filter(lambda x: x in add_constraint_condition, op_list))
                # print(op_p)
                op = op_p[0]
                # print(op)

                if op != ' = ' or "'" in add_constraint_condition:
                    # 主节点的解析
                    probability = now_running_result / last_running_result

                    probability = round(probability, 6)
                    op_left = add_constraint_condition.split(op)[0]
                    op_left_list = op_left.split(".")
                    if op_left.split(".")[0] in char_table.keys():
                        op_left = op_left_list[0] + "_" + op_left_list[-1]
                    else:
                        op_left = op_left.split('.')[-1] if "." in op_left else op_left
                    chain = "[0, {}@{}, {}]; ".format(op_left, op.lstrip().rstrip(), probability)
                    left_table = char_table[op_left.split("_")[0]]
                    if probability != 1.0:
                        constraint_chain[left_table] += chain
                else:

                    probability = now_running_result / last_running_result
                    probability = round(probability, 6)
                    # 主键和外键的解析
                    op_left = add_constraint_condition.split(op)[0]  # o_custkey
                    op_left_list = op_left.split(".")
                    if op_left.split(".")[0] in char_table.keys():
                        op_left = op_left_list[0] + "_" + op_left_list[-1]
                    else:
                        op_left = op_left.split('.')[-1] if "." in op_left else op_left
                    op_right = add_constraint_condition.split(op)[1]
                    op_right_list = op_right.split(".")
                    if op_right.split(".")[0] in char_table.keys():
                        op_right = op_right_list[0] + "_" + op_right_list[-1]
                    else:
                        op_right = op_right.split('.')[-1] if "." in op_right else op_right
                    # 解析 p_price = 5 类似的条件
                    if "_" not in op_right:
                        chain = "[0, {}@{}, {}]; ".format(op_left, op.lstrip().rstrip(), probability)
                        left_table = char_table[op_left.split("_")[0]]
                        if probability != 1.0:
                            constraint_chain[left_table] += chain
                    else:
                        # 解析 操作符左边的表
                        left_table = char_table[op_left.split("_")[0]]
                        right_table = char_table[op_right.split("_")[0]]
                        if op_left in self.all_table_structure[left_table]["key_foreign"].keys():
                            # print("{} {} {}".format(left_table, right_table, op_left))
                            k = 0
                            if op_right in self.all_table_structure[right_table]["key_primary"].keys():
                                k = self.all_table_structure[right_table]["key_primary"][op_right]
                            m, n = int(pow(2, k)), int(pow(2, k + 1))
                            if op_left in self.all_table_structure[left_table]["key_primary"].keys():
                                self.all_table_structure[left_table]["key_primary"][op_left] = 2 * (k + 1)

                            full_foreign_key = self.all_table_structure[left_table]["key_foreign"][op_left]
                            chain = "[2, {}, {}, {}, {}, {}]; ".format(op_left, probability, full_foreign_key, m, n)

                        else:
                            if op_left in self.all_table_structure[left_table]["key_primary"].keys():
                                k = self.all_table_structure[left_table]["key_primary"][op_left]
                                m, n = int(pow(2, k)), int(pow(2, k + 1))
                                chain = "[1, {}, {}, {}]; ".format(op_left, m, n)
                            else:
                                chain = "[0, {}@{}, {}]; ".format(op_left, op.lstrip().rstrip(), probability)
                        if probability != 1.0:
                            constraint_chain[left_table] += chain
                        # 解析右边的操作符
                        if op_right in self.all_table_structure[right_table]["key_foreign"].keys():
                            k = 0
                            if op_left in self.all_table_structure[left_table]["key_primary"].keys():
                                k = self.all_table_structure[left_table]["key_primary"][op_left]
                            m, n = int(pow(2, k)), int(pow(2, k + 1))
                            if op_right in self.all_table_structure[right_table]["key_primary"].keys():
                                self.all_table_structure[right_table]["key_primary"][op_right] = 2 * (k + 1)
                            full_foreign_key = self.all_table_structure[right_table]["key_foreign"][op_right]
                            chain = "[2, {}, {}, {}, {}, {}]; ".format(op_right, probability, full_foreign_key, m, n)

                        else:
                            if op_right in self.all_table_structure[right_table]["key_primary"].keys():
                                k = self.all_table_structure[right_table]["key_primary"][op_right]
                                m, n = int(pow(2, k)), int(pow(2, k + 1))
                                chain = "[1, {}, {}, {}]; ".format(op_right, m, n)
                            else:
                                chain = "[0, {}@{}, {}]; ".format(op_left, op.lstrip().rstrip(), probability)
                        if probability != 1.0:
                            constraint_chain[right_table] += chain

            if len(all_constraint_condition) >= len(last_constraint_condition):
                last_constraint_condition = all_constraint_condition.copy()
            last_running_result = now_running_result
        return constraint_chain


def run(path_table="./Table_info", path_sql="SQL/parse_result.pkl"):
    error = []
    allow = ["7.sql"]
    special = ["7.sql"]
    check = True
    if check:
        with open(path_sql, 'rb') as f:
            data = pickle.load(f)
            for k, v in data.items():
                if k in error:
                    continue
                # if k in allow_list:
                print(" The information of {}".format(k))
                print("SQL")
                for p in v:
                    print("{} #{}".format(p['sql'], p['count']))
                if k in special:
                    print("Constraint Chain-1:")
                    T = table_structure(path_table, v[:14])
                    constraint_chain = T.generating_constraint_chain()
                    for k, p in constraint_chain.items():
                        print(p)
                    # print("\n")
                    print("Constraint Chain-2:")
                    T = table_structure(path_table, v[14:])
                    constraint_chain = T.generating_constraint_chain()
                    for k, p in constraint_chain.items():
                        print(p)
                    print("\n")
                else:
                    print("Constraint Chain:")
                    T = table_structure(path_table, v)
                    constraint_chain = T.generating_constraint_chain()
                    for k, p in constraint_chain.items():
                        print(p)
                    print("\n")
    else:
        with open(path_sql, 'rb') as f:
            data = pickle.load(f)
            for k, v in data.items():
                if k in allow:
                    print(" The information of {}".format(k))
                    print("SQL")
                    for p in v:
                        print("{} #{}".format(p['sql'], p['count']))

                    if k in special:
                        print("Constraint Chain-1:")
                        T = table_structure(path_table, v[:14])
                        constraint_chain = T.generating_constraint_chain()
                        for k, p in constraint_chain.items():
                            print(p)
                        # print("\n")
                        print("Constraint Chain-2:")
                        T = table_structure(path_table, v[14:])
                        constraint_chain = T.generating_constraint_chain()
                        for k, p in constraint_chain.items():
                            print(p)
                        print("\n")
                    else:
                        print("Constraint Chain:")
                        T = table_structure(path_table, v)
                        constraint_chain = T.generating_constraint_chain()
                        for k, p in constraint_chain.items():
                            print(p)
                        print("\n")


if __name__ == '__main__':
    run()
