import json
import pymysql
import datetime
import random
import re
import jieba
import numpy as np
from collections import Counter
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, confusion_matrix
from sqlalchemy import create_engine, text, Column, String, MetaData, Table, Float
import urllib.parse
from loguru import logger
from const import constants
from download import user_upload_util
from download.user_upload_util import upload_to_server
from util.mysql_util import get_KA_gene_params, insert_ka_generalization, get_KA_identifier, get_KA_sensitive, \
    get_KA_gene_all_params, insert_ka_logistic_regression1, insert_ka_logistic_regression2, get_KA_info, \
    get_identifier_conf, \
    insert_ka_risk_assessment, get_download_file
from util.mysql_classification_util import get_db_params
from .callback_api import re_api, delete_temp_ka_tpl, append_re_api
import pandas as pd
import os
# from .alg_mask import anonymity
from download.remote_access import check_ftp, check_sftp
from download.sftp_util import ftp_download_file, sftp_download_file, ftp_upload_file, sftp_upload_file
from itertools import product
import uuid
from sklearn.preprocessing import OneHotEncoder

th = constants.THRESHOLD
down_path = constants.DOWN_PATH
col_rule = constants.COL_RULE

# 采样,data是dataframe或list,retain_percent是保留数据的百分比，取值为0-100
def sampling(data, retain_percent: float = 50, task_id=None):
    if retain_percent < 0 or retain_percent > 100:
        raise ValueError("retain_percent must be between 0 and 100")

    if isinstance(data, pd.DataFrame):
        # 处理 DataFrame 类型的数据
        mask = [random.uniform(0, 100) <= retain_percent for _ in range(len(data))]
        # 确保至少保留一条数据
        if not any(mask):  # 如果mask中没有True值，说明没有任何数据被选中
            random_index = random.randint(0, len(data) - 1)
            mask[random_index] = True
        return data[mask].copy()
    # else:
    #     return [row for row in data if random.uniform(0, 100) <= retain_percent]
    # elif all((isinstance(item, str) or pd.isna(item)) for item in data):
    #     # 处理字符串列表类型的数据
    #     return [row for row in data if random.uniform(0, 100) <= retain_percent]
    elif all(isinstance(item, str) or isinstance(item, float) or isinstance(item, int) or pd.isna(item) for item in
             data):
        # 处理字符串列表类型的数据
        return [row for row in data if random.uniform(0, 100) <= retain_percent]
    else:
        # info = {'message': '不支持的数据类型！', 'task_id': task_id, 'status': -1}
        # append_re_api(info)
        # return data
        raise TypeError("Unsupported data type. Only supports DataFrame or list of strings/float/int.")

    # if retain_percent<0:
    #     print("percent error")
    #     return Exception
    # if retain_percent>100:
    #     return Exception
    # if isinstance(data, pd.DataFrame):
    #     copy_data=data.copy()
    #     result=pd.DataFrame()
    #     for index,row in copy_data.iterrows():
    #         #0删除这条数据，1保留这条数据
    #         flag = random.uniform(0,100)
    #         if flag<=retain_percent:
    #             result=result.append(row, ignore_index=True)
    # else:
    #     result = []
    #     for row in data:
    #         #0删除这条数据，1保留这条数据
    #         flag = random.uniform(0,100)
    #         if flag<=retain_percent:
    #             result.append(row)
    # return result


# 聚合数值,data是[record]或[string],group是需要聚合的列[int],group_by是聚合时需要相同的列[string]。group_by为空时，除了group剩下的都需相同。
def aggregation(data, group, group_by, task_id=None):
    if group is None:
        if all((isinstance(item, int) or (isinstance(item, float)) or pd.isna(item)) for item in data):
            tmp = data[:]
            # 空值处填写平均值
            average = np.nanmean(tmp)
            if isinstance(average, np.float64) and average.is_integer():
                average = int(average)
            tmp = [average for _ in tmp]
            # sum_values = sum(tmp)
            # average = sum_values / len(tmp)
            # for index, _ in enumerate(tmp):
            #     tmp[index] = average
            return tmp
        else:
            # info = {'message': '不支持的数据类型！', 'task_id': task_id, 'status': -1}
            # append_re_api(info)
            # return data
            raise TypeError("Unsupported data type. Only supports list of strings.")
    else:
        if group_by is None:
            group_by = []
            for k in data[0]:
                if k not in group:
                    group_by.append(k)
        # 将字典列表转换为 DataFrame
        df = pd.DataFrame(data)
        group_means = df.groupby(group_by)[group].transform('mean')
        df[group] = group_means
        dict_list_back = df.to_dict(orient='records')
        return dict_list_back


# 抑制,data是list,threshold是阈值的百分数（阈值/总数），取值出现的百分比小于threshold则将这里的数据变为'-'。默认阈值是5（%）
def suppression(data, threshold_percent: float = 5, task_id=None):
    if threshold_percent < 0 or threshold_percent > 100:
        raise ValueError("threshold_percent must be between 0 and 100")
    if all(isinstance(item, str) or isinstance(item, float) or isinstance(item, int) or pd.isna(item) for item in data):
        tmp = data[:]
        counts = Counter(tmp)
        threshold = len(tmp) * threshold_percent * 0.01
        # 根据阈值筛选要保留的取值
        valid_values = [value if counts[value] >= threshold else '-' for value in tmp]
        return valid_values
    else:
        # info = {'message': '不支持的数据类型！', 'task_id': task_id, 'status': -1}
        # append_re_api(info)
        # return data
        raise TypeError("Unsupported data type. Only supports list of strings/float/int.")


# 屏蔽，data是[string],start和end是需要打星号的起始和结束位置,取值从0开始,symbol是屏蔽时使用的符号
def masking(data, start: int, end: int, symbol: str, task_id=None):
    start = start - 1
    end = end - 1
    if start > end:
        raise ValueError("The start value must be less than or equal to the end value.")
    if all((isinstance(item, str) or pd.isna(item)) for item in data):
        result = data[:]
        for i, row in enumerate(result):
            length = len(row)
            if start < length:
                if end < length:
                    size = end - start + 1
                    result[i] = row[:start] + symbol * size + row[end + 1:]
                else:
                    size = length - start
                    result[i] = row[:start] + symbol * size
        # for i, row in enumerate(result):
        #     if isinstance(row, str):
        #         length = len(row)
        #         if start < length:
        #             if end < length:
        #                 result[i] = row[:start] + symbol + row[end + 1:]
        #             else:
        #                 result[i] = row[:start] + symbol
        return result
    else:
        result = [str(x) for x in data]
        for i, row in enumerate(result):
            length = len(row)
            if start < length:
                if end < length:
                    result[i] = row[:start] + symbol + row[end + 1:]
                else:
                    result[i] = row[:start] + symbol
        return result


# 数值属性分类,data是[int/float]或,col取值是要分类的列,double是10的倍数，表示取值区间，例如double是20的话取值是[0，20)等
def categorization(data, double: int, task_id=None):
    result = data[:]
    if all((isinstance(item, int) or pd.isna(item) or isinstance(item, float)) for item in data):
        for i, row in enumerate(result):
            if isinstance(row, int) or isinstance(row, float):
                x = row // double
                a = x * double
                b = a + double
                result[i] = "[" + str(a) + ", " + str(b) + ")"
            else:
                result[i] = "N/A"
        return result
    else:
        # info = {'message': '不支持的数据类型！', 'task_id': task_id, 'status': -1}
        # append_re_api(info)
        # return data
        raise TypeError("Unsupported data type. Only supports list of integer.")


# 泛化，data是[string],start和end是需要打星号的起始和结束位置,取值从0开始
def generalization(data, start: int, end: int, task_id=None):
    if start > end:
        raise ValueError("The start value must be less than or equal to the end value.")
    result = data[:]
    if all((isinstance(item, str) or pd.isna(item)) for item in data):
        for i, row in enumerate(result):
            if re.search(r'\s', row):
                string_list = re.split(r"\s+", row)
            elif re.compile(u'[\u4e00-\u9fa5]+').search(row):
                words = jieba.cut(text)
                string_list = list(words)
            else:
                string_list = re.split(r"\s+", row)
            length = len(string_list)
            if start < length:
                tmp1 = " ".join(string_list[:start])
                if end < length:
                    size = end - start + 1
                    tmp2 = " ".join(string_list[end + 1:])
                    result[i] = tmp1 + " " + "*" * size + " " + tmp2
                else:
                    size = length - start
                    result[i] = tmp1 + " " + "*" * size + " "
                # tmp1=""
                # for i in range(start-1):
                #     tmp1=tmp1+string_list[i]+" "
                # tmp1=tmp1+string_list[start-1]
                # if end<length:
                #     size=end-start+1
                #     tmp2=""
                #     for i in range(end+1,length-1):
                #         tmp2=tmp2+string_list[i]+" "
                #     tmp2=tmp2+string_list[length-1]
                #     result[i]=tmp1+"*"*size+tmp2
                # else:
                #     size=length-start
                #     result[i]=tmp1+"*"*size
        return result
    else:
        # info = {'message': '不支持的数据类型！', 'task_id': task_id, 'status': -1}
        # append_re_api(info)
        # return data
        raise TypeError("Unsupported data type. Only supports list of strings.")


'''
col_name是需要匿名的列名，gene_type是泛化的类型，gene_params是泛化参数
'''


def anonymity(data, col_name: str, index: int, gene_params: list,task_id=None):
    tmp = pd.DataFrame()
    try:
        if index == 1:  # "sampling"
            tmp = sampling(data, gene_params[0], task_id)
        elif index == 3:  # "aggregation"
            t1 = data.copy()
            t2 = t1.to_dict(orient='records')
            t3 = aggregation(t2, [col_name], gene_params[0], task_id)
            tmp = pd.DataFrame(t3)
        elif index == 6:  # "suppression"
            tmp = data.copy()
            string_list = tmp[col_name].tolist()
            filtered_list = suppression(string_list, gene_params[0], task_id)
            tmp[col_name] = filtered_list
        elif index == 2:  # "masking"
            tmp = data.copy()
            for index, row in tmp.iterrows():
                tmp.loc[index, col_name] = masking([row[col_name]], gene_params[0], gene_params[1], gene_params[2], task_id)[0]
        elif index == 4:  # "categorization"
            tmp = data.copy()
            tmp[col_name] = tmp[col_name].apply(lambda x: str(categorization([x], gene_params[0], task_id)[0]))
        elif index == 5:  # "generalization"
            tmp = data.copy()
            for index, row in tmp.iterrows():
                tmp.loc[index, col_name] = generalization([row[col_name]], gene_params[0], gene_params[1], task_id)[0]
        return tmp
    except Exception as e:
        message = {'status': -100, 'col': col_name, 'index': index}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        print(f"Error in {str(col_name)}: {str(e)}")
        # raise ValueError(f"Error in {str(col_name)}: {str(e)}")

# 假设有个函数是anonymity(data,identifier_col,generalization_type,gene_params) return data_gene
# 将所有准标识符的组合存入字典，值为出现次数。
def group_data(testedSet, iden_name_list):
    grouped = testedSet.groupby(iden_name_list)
    quasiDict = {}
    for value, group in grouped:
        quasiDict[value] = len(group)
    # quasiDict = {}
    # for item in testedSet.itertuples():
    #     # --------------------------------------------------------------
    #     # 将准标识符转化为字符串
    #     item_statement = ''
    #     for label in iden_name_list:
    #         item_statement = item_statement + str(getattr(item, label)) + ' '
    #     # --------------------------------------------------------------
    #     # 如果该准标识符组合已经出现过了，则计数+1
    #     if item_statement in quasiDict.keys():
    #         quasiDict[item_statement] += 1
    #     # 如果该准标识符组合没有出现过，则新建记录
    #     else:
    #         quasiDict[item_statement] = 1
    # 返回字典
    return quasiDict


# 判断数据集testedSet是否满足k匿名，是则返回true，否则返回false
def if_k(testedSet, k: int, iden_name_list):
    # 对数据集进行分组，获得组合数量
    ans_dict = group_data(testedSet, iden_name_list)
    # -----------------------------------------------------------------
    # 展示准标识符组合
    print('')
    print(ans_dict)
    print('')
    # -----------------------------------------------------------------
    min_k = -1
    # 遍历分组字典，取出最小的重复个数，赋值给min_k
    for i in ans_dict:
        if min_k == -1 or ans_dict[i] < min_k:
            min_k = ans_dict[i]
    # 如果字典的最小k值大于等于给定的k值，则满足k匿名
    if min_k >= k:
        return True
    else:
        return False


def is_edit_distance_one(point1, point2):
    """
    检查两个点是否仅在一个位置上不同
    """
    difference_count = 0
    for i in range(len(point1)):
        if point1[i] != point2[i]:
            difference_count += abs(point1[i] - point2[i])
        if difference_count > 1:
            return False
    return difference_count == 1


def KA(task_id: str, data, k, identifier_list, mask_pool, user_id, sheet_name, ds_path):
    '''
    data是dataframe格式数据
    # identifier_list处理为 (标识列名称,data_type,tree_max_height,ka_tpl_id) 格式的列表。
    返回k-匿名结果以及flag。flag为1表示脱敏成功，flag为0表示脱敏失败，返回最大泛化结果
    '''

    if identifier_list is None:
        message = {'status': -105, 'file_url': ds_path}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        print("标识列信息获取报错")
        return ("标识列信息获取报错", None), 0, None

    iden_size = len(identifier_list)  # 标识列个数
    if iden_size == 0:
        message = {'status': -105, 'file_url': ds_path}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        print("无标识列信息")
        return ("无标识列信息", None), 0, None
    iden_name_list = []  # 存储标识列名称
    tree_height = []  # 存储标识列最大泛化高度
    sum_gene = 1  # 泛化组合高度=泛化次数+1

    for ident in identifier_list:
        iden_name_list.append(ident[0])
        tree_height.append(ident[2])
        sum_gene += ident[2]

    # 生成每个值的迭代器
    value_iterators = []
    for count in tree_height:
        value_iterators.append(range(count + 1))
    # 使用 product 生成所有可能的组合
    all_combinations = list(product(*value_iterators))

    # 创建每个点的父节点和子节点列表
    node_relations = {point: {'parents': [], 'children': []} for point in all_combinations}
    for point in all_combinations:
        node_relations[point] = {'uid': str(uuid.uuid4()), 'parents': [], 'children': []}
    # 计算每个点的父节点和子节点
    for point1 in all_combinations:
        for point2 in all_combinations:
            if is_edit_distance_one(point1, point2):
                if point1 < point2:
                    node_relations[point1]['children'].append(node_relations[point2]['uid'])
                    node_relations[point2]['parents'].append(node_relations[point1]['uid'])
    '''
    df_dict存储不同泛化组合的脱敏后数据。若这种组合满足k-匿名，则值为脱敏后数据，否则为None
    '''
    df_dict = {}
    ka_tpl_delete = []
    for combination in all_combinations:
        tmp_data = data.copy()
        # i表示标识列中第i列
        for i in range(iden_size):
            height = combination[i]  # 第i列可以泛化的高度
            ident = identifier_list[i]
            col = ident[0]  # 列名
            ka_tpl_id = ident[3]
            flag = ka_tpl_id.split('-')[0]
            if flag == 'Temp':  # 含聚合规则的临时树
                if ka_tpl_id not in ka_tpl_delete:
                    ka_tpl_delete.append(ka_tpl_id)
            for j in range(height):
                h = j + 1
                gene = get_KA_gene_params(mask_pool, ka_tpl_id, h)
                index = int(gene[0][0])  # gene[0]为dm_rule_id
                gene_params = gene[1:]
                print(tmp_data)
                try:
                    tmp_data = anonymity(tmp_data, col, index, gene_params,task_id)
                except Exception as e:
                    return (str(e), None), 0, None
        if if_k(tmp_data, k, iden_name_list) is True:
            df_dict[combination] = tmp_data
            insert_ka_generalization(mask_pool, task_id, sheet_name, '(' + ','.join(map(str, combination)) + ')', 1,
                                     node_relations[combination]['uid'], node_relations[combination]['parents'],
                                     node_relations[combination]['children'])
        else:
            df_dict[combination] = None
            insert_ka_generalization(mask_pool, task_id, sheet_name, '(' + ','.join(map(str, combination)) + ')', 0,
                                     node_relations[combination]['uid'], node_relations[combination]['parents'],
                                     node_relations[combination]['children'])

    min_frequency = -1
    min_df = None
    min_combination = None
    for combination, df in df_dict.items():
        if df is not None:
            frequency = sum(combination)
            if min_frequency == -1 or frequency < min_frequency:
                min_frequency = frequency
                min_df = df_dict[combination]
                min_combination = combination
    flag = 1
    if min_df is None:
        flag = 0
        min_df = tmp_data
        min_combination = all_combinations[-1]
    return (min_df, min_combination), flag, ka_tpl_delete

    # '''
    # 存储对应泛化次数下的泛化情况。id表示这种情况的编号（从1开始），flag为0表示满足k-匿名，1表示不满足
    # 可以根据编号在dataframe中查找修改后的数据
    #
    # 泛化次数
    # 0:[id,(0,0,0,0),flag]
    # 1:[[id,(1,0,0,0),flag],[id,(0,1,0,0),flag]]
    # '''
    # dataframe={}
    # id=0
    # tree=[]
    # for i in range(sum_gene):
    #     tree.append([])
    #
    # # index表示第几种泛化情况，combination表示泛化情况
    # for index,combination in enumerate(all_combinations):
    #     id+=1
    #     tmp_data=data.copy()
    #     #i表示标识列中第i列
    #     for i in range(iden_size):
    #         height=combination[i]   #第i列可以泛化的高度
    #         ident=identifier_list[i]
    #         col=ident[0]  #列名
    #         rule=ident[3]
    #         for j in range(height):
    #             gene=get_KA_gene_params(mask_pool, rule, j)
    #             gene_type = gene[0]
    #             gene_params = gene[1:]
    #             tmp_data=anonymity(tmp_data, col, gene_type, gene_params)

    #
    #
    # ident_height_list = [0] * iden_list_length  # 存储对应标识列当前泛化高度
    # while if_k(copy_data, k, iden_name_list) is False:
    #     for index in range(iden_list_length):
    #         # 如果已经到达了泛化顶点
    #         if ident_height_list[index] >= identifier_list[index][2]:
    #             continue
    #         identifier_col = identifier_list[index][0]
    #         ident_height_list[index] += 1
    #         gene = get_KA_gene_params(mask_pool, identifier_list[index][3], ident_height_list[index])
    #         gene_type = gene[0]
    #         gene_params = gene[1:]
    #         copy_data = anonymity(copy_data, identifier_col, gene_type, gene_params)
    #         sum_gene -= 1
    #         if if_k(copy_data, k, iden_name_list):
    #             flag = True
    #             break
    #     if flag == True:
    #         print("泛化成功！")
    #         break
    #     if sum_gene == 0:
    #         print("泛化失败！")
    #         break
    # print('当前泛化高度：')
    # for index in range(iden_list_length):
    #     print(identifier_list[index][0] + ':' + str(ident_height_list[index]))
    # print('当前k值为：', k)
    # prec = 0
    # for index in range(iden_list_length):
    #     prec += (ident_height_list[index]) / (identifier_list[index][2])
    # prec = 1 - (prec / iden_list_length)
    # print('精确度为：', prec)
    # gene_result = copy_data
    # print(gene_result)
    # return gene_result


def get_df_list(ds_path, sheet_name_list, full_file_path, dict_tmp, start, task_id, file_extension):
    try:
        df_list = []
        # 依序获得df_list
        for sheet_name in sheet_name_list:
            if file_extension == '.xlsx' or file_extension == '.xls':
                df = pd.read_excel(full_file_path, sheet_name=sheet_name, converters=dict_tmp[sheet_name])
                print(df.head())  # 打印 DataFrame 的前几行，以确认数据读取正确
                df_list.append(df)
            elif file_extension == '.csv':
                df = pd.read_csv(full_file_path, converters=dict_tmp[sheet_name])
                print(df.head())  # 打印 DataFrame 的前几行，以确认数据读取正确
                df_list.append(df)
            else:
                print("未知文件类型或不支持的文件类型")
                end = datetime.datetime.now()
                span = end - start
                print(span)
                message = {'status': -103, 'file_url': ds_path}
                info = {'message': message, 'task_id': task_id, 'status': -1}
                append_re_api(info)
                return None
    except FileNotFoundError:
        print(f" {ds_path}服务器本地文件未找到")
        end = datetime.datetime.now()
        span = end - start
        print(span)
        #data = {'message': f"'{ds_path}'服务器本地文件未找到", 'task_id': task_id, 'status': -1}
        message = {'status': -112, 'file_url': ds_path}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        return None
    except Exception as e:
        print(f"读取文件时发生错误: {str(e)}")
        end = datetime.datetime.now()
        span = end - start
        print(span)
        message = {'status': -112, 'file_url': ds_path}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        return None
    return df_list


def ka_process(df_list, task_id, k_list, sheet_name_list, identifier_info_list_dict, mask_pool, user_id, store_flag, ds_path):
    gene_result_list = []
    success_list = []
    for df, sheet_name, k in zip(df_list, sheet_name_list, k_list):
        identifier_list = identifier_info_list_dict.get(sheet_name)
        copy_data = pd.DataFrame(df)
        (gene_result, min_com), flag, ka_tpl_delete = KA(task_id, copy_data, k, identifier_list, mask_pool, user_id,
                                                         sheet_name, ds_path)
        # if isinstance(gene_result, str):
        #     print("K匿名处理过程报错")
        #     data = {'message': gene_result, 'task_id': task_id, 'status': -1}
        #     re_api(data)
        #     return None, None, None, None
        if flag == 0:
            print("k值太大，脱敏失败！")
            success_list.append(0)
            store_flag = 2
        else:
            success_list.append(1)
        gene_result_list.append(gene_result)
    return gene_result_list, success_list, store_flag, min_com, ka_tpl_delete


def get_all_df(excel_file):
    # 保存excel_file路径下的xlsx及xls除了sheet_name_list的sheet表的内容
    # 使用 ExcelFile 类打开 Excel 文件
    xls = pd.ExcelFile(excel_file)
    # 获取所有工作表的名称
    sheet_names = xls.sheet_names
    # 遍历所有工作表，读取数据到一个字典中
    all_sheets = {}
    for sheet_name in sheet_names:
        all_sheets[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name)
    # 现在 all_sheets 字典包含了 Excel 文件中所有工作表的数据，键是工作表名，值是对应的 DataFrame
    # # 可以按需操作每个工作表的数据
    # for sheet_name, df in all_sheets.items():
    #     print(f"工作表：{sheet_name}")
    #     print(df.head())  # 打印工作表的前几行数据
    #     print()
    return all_sheets


def save_file_to_local(base_name, file_extension, df_list, sheet_list, start, task_id, file_path, ds_path):
    # xlsx和xls传递file_path取读取原文件中所有的sheet表
    save_file_name = base_name + "_copy" + file_extension
    save_path = down_path + '/' + save_file_name
    if file_extension == '.xlsx' or file_extension == '.xls':
        all_sheets = get_all_df(file_path)
        for df, sheet_name in zip(df_list, sheet_list):
            if sheet_name in all_sheets.keys():
                all_sheets[sheet_name] = df
            else:
                message = {'status': -119, 'file_url': ds_path, "sheet_name": sheet_name}
                info = {'message': message, 'task_id': task_id, 'status': -1}
                append_re_api(info)
                continue
                # return False, f"‘{file_path}’文件中未找到工作表‘{sheet_name}’！"
    try:
        if file_extension == '.xlsx':
            with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
                for sheet_name, df in all_sheets.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
        elif file_extension == '.xls':
            with pd.ExcelWriter(save_path, engine='xlsxwriter') as writer:
                for sheet_name, df in all_sheets.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
        elif file_extension == '.csv':
            df = df_list[0]
            df.to_csv(save_path, index=False, encoding='utf-8-sig')
        else:
            message = {'status': -103, 'file_url': ds_path}
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
    except Exception as e:
        return None, str(e)
    return save_path, None


# def save_file_to_local(base_name, file_extension, df_list, sheet_list, start, task_id):
#     # xlsx和xls传递file_path取读取原文件中所有的sheet表
#     save_file_name = base_name + "_copy" + file_extension
#     save_path = down_path + '/' + save_file_name
#     try:
#         if file_extension == '.xlsx':
#             with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
#                 for df, sheet_name in zip(df_list, sheet_list):
#                     df.to_excel(writer, sheet_name=sheet_name, index=False)
#         elif file_extension == '.xls':
#             with pd.ExcelWriter(save_path, engine='xlsxwriter') as writer:
#                 for df, sheet_name in zip(df_list, sheet_list):
#                     df.to_excel(writer, sheet_name=sheet_name, index=False)
#         elif file_extension == '.csv':
#             df = df_list[0]
#             df.to_csv(save_path, index=False)
#         else:
#             raise Exception(f"未知文件类型或不支持的文件类型:{file_extension}")
#     except Exception as e:
#         return None, str(e)
#     return save_path, None


def k_anonymity(mask_pool, class_pool, task_id: str, user_id: str):
    '''
    #Pandas DataFrame 对象，数据格式是二维表格，类似于数据库中的表格。
    #identifier_list是[标识列名称,标识列类型,当前标识列构建泛化树的层数,当前标识列构建泛化树时的规则id]格式列表的列表。
    #sensitives_list是敏感名称的列表。
    table_name:如果数据库中access_protocol为MySQL，应该传一个表名

    若脱敏失败，取最大泛化结果为k-匿名结果。
    '''
    start = datetime.datetime.now()
    ds_id, ds_path, store_flag, authorization = get_KA_info(mask_pool, task_id, user_id)
    if ds_id is None:
        data = {'message': 'K匿名运行失败，获取数据源参数出错！', 'task_id': task_id, 'status': -1}
        append_re_api(data)
        return None
    elif not ds_id:
        message = {'status': -104}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        return None
    identifier_list_dict, sheet_name_list, k_list = get_KA_identifier(mask_pool, task_id)
    # identifier_list_dict:{
    #         sheet_name1:[(identifier,ka_tpl_id)],
    #         sheet_name2:[(identifier,ka_tpl_id)]
    #     }
    # 非Excel表的sheet_name_list只有一个值，为"default"或文件名/数据库表名

    sensitive_list_dict = get_KA_sensitive(mask_pool, task_id)
    print(sensitive_list_dict)
    # sensitive_list_dict:{
    #             sheet_name1:[sensitive],
    #             sheet_name2:[sensitive]
    #         }

    identifier_info_list_dict = get_identifier_conf(mask_pool, identifier_list_dict, user_id)
    # identifier_info_list_dict:{
    #         sheet_name1:[(identifier,data_type,tree_max_height,ka_tpl_id)],
    #         sheet_name2:[(identifier,data_type,tree_max_height,ka_tpl_id)]
    #     }
    if identifier_info_list_dict is None or identifier_list_dict is None or sensitive_list_dict is None:
        data = {'message': 'K匿名运行失败，读取列信息出错！', 'task_id': task_id, 'status': -1}
        append_re_api(data)
        return None
    elif not identifier_info_list_dict or not identifier_list_dict:
        message = {'status': -105, 'file_url': ds_path}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        return None
    elif not sensitive_list_dict:
        message = {'status': -106, 'file_url': ds_path}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)

    # 根据列col的第一层规则判断它是什么类型数据，存到dict_tmp[sheet_name][col] 字典
    # dict_tmp = {
    #   sheet_name1: {
    #       column1: 'int',
    #       column2: 'str'
    #   }
    #   sheet_name2:{}
    # }
    dict_tmp = {}
    identifier_name_list = {}
    for sheet_name in sheet_name_list:
        identifier_name_list[sheet_name] = []
        dict_tmp[sheet_name] = {}
        identifier_list = identifier_info_list_dict.get(sheet_name)
        for i in range(len(identifier_list)):
            ident = identifier_list[i]
            col = ident[0]  # 列名
            identifier_name_list[sheet_name].append(col)
            ka_tpl_id = ident[3]
            max_height = ident[2]
            gene = get_KA_gene_all_params(mask_pool, ka_tpl_id, max_height)
            if not gene:
                message = {'status': -107, 'file_url': ds_path, 'sheet_name': sheet_name}#K匿名运行失败，服务内部读取数据库报错：获取泛化规则出错
                info = {'message': message, 'task_id': task_id, 'status': -1}
                append_re_api(info)
                break
            flag = "1"
            for gene_info in gene:
                index = gene_info[0][0]
                if index in ["3", "4"]:
                    flag = "3"
                    break
            dict_tmp[sheet_name][col] = col_rule[flag]

    df_list = []

    if ds_id == constants.USER_UPLOAD_SERVER:  # ds_id为空的情况在API处理时已置为USER_UPLOAD_SERVER
        downloaded_file = get_download_file(mask_pool, task_id, user_id)
        # if not downloaded_file:
        #     data = {'message': '下载到本地的文件缺失', 'task_id': task_id, 'status': -1}
        #     re_api(data)
        #     return None

        file_name = ds_path.split('/')[-1]
        base_name, file_extension = os.path.splitext(file_name)
        file_extension = file_extension.lower()
        # 与sheet_name_list同序获得df_list
        # 原始数据：sheet_name_list,df_list:list
        df_list = get_df_list(ds_path, sheet_name_list, downloaded_file, dict_tmp, start, task_id, file_extension)
        if df_list is None:
            return  # 出错情况在函数内已经回调接口，此处直接return结束即可
        # gene_result_list为依sheet_name_list序处理的数据结果
        gene_result_list, success_list, store_flag, _, ka_tpl_delete = ka_process(df_list, task_id, k_list,
                                                                                  sheet_name_list,
                                                                                  identifier_info_list_dict, mask_pool,
                                                                                  user_id,
                                                                                  store_flag, ds_path)
        if gene_result_list is None:
            return

        if ka_tpl_delete:
            for ka_tpl_id in ka_tpl_delete:
                delete_temp_ka_tpl(ka_tpl_id, user_id)

        local_save_path, e = save_file_to_local(base_name, file_extension, gene_result_list, sheet_name_list, start,
                                                task_id, downloaded_file, ds_path)
        if local_save_path is None:
            message = {'status': -113, 'file_url': ds_path}
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
            return None
        # elif not local_save_path:
        #     data = {'message': e, 'task_id': task_id, 'status': -1} # 未找到工作表
        #     append_re_api(data)
        #     return None

        # os.remove(full_file_path)
        # @ TODO 上传local_save_path到指定远端路径
        upload_success, url = upload_to_server(authorization, local_save_path)
        if not upload_success:
            message = {'status': -116, 'download_url': ds_path} #上传失败
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
            return None
        data = {'message': {'result': 'ok'}, 'task_id': task_id, 'status': 2, 'resource': {"url": url}}
        append_re_api(data)
    else:
        result = get_db_params(class_pool, ds_id)
        if result is None:
            data = {'message': 'K匿名运行-获取数据源访问参数失败！', 'task_id': task_id, 'status': -1}
            append_re_api(data)
            return None
        elif not result:
            message = {'status': -109, 'ds_id': ds_id}
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
            return None
        (access_protocol, auth_ip, auth_port, auth_username, auth_pwd) = result
        if access_protocol == 'FTP':
            state, ftp = check_ftp(ip=auth_ip, port=auth_port, username=auth_username, pwd=auth_pwd)
            if state != 0:
                print("连接失败！")
                end = datetime.datetime.now()
                span = end - start
                print(span)
                message = {'status': -101, 'ip': auth_ip, 'port': auth_port}
                info = {'message': message, 'task_id': task_id, 'status': -1}
                append_re_api(info)
                return None
            else:
                print("连接成功")
                state2, full_file_path = ftp_download_file(ftp, remote_path=ds_path, local_path=down_path)
                if not state2:
                    message = {'status': -102, 'ip': auth_ip, 'port': auth_port, 'file_url': ds_path}
                    info = {'message': message, 'task_id': task_id, 'status': -1}
                    append_re_api(info)
                    return None
                ftp.close()  # 后续上传会新申请连接，可以关闭了

                file_name = ds_path.split('/')[-1]
                base_name, file_extension = os.path.splitext(file_name)
                file_extension = file_extension.lower()

                # 与sheet_name_list同序获得df_list
                # 原始数据：sheet_name_list,df_list:list
                df_list = get_df_list(ds_path, sheet_name_list, full_file_path, dict_tmp, start, task_id,
                                      file_extension)
                if df_list is None:
                    return  # 出错情况在函数内已经回调接口，此处直接return结束即可

                # gene_result_list为依sheet_name_list序处理的数据结果
                gene_result_list, success_list, store_flag, _, ka_tpl_delete = ka_process(df_list, task_id, k_list,
                                                                                          sheet_name_list,
                                                                                          identifier_info_list_dict,
                                                                                          mask_pool, user_id,
                                                                                          store_flag, ds_path)
                if gene_result_list is None:
                    return

                if ka_tpl_delete:
                    for ka_tpl_id in ka_tpl_delete:
                        delete_temp_ka_tpl(ka_tpl_id, user_id)

                local_save_path, e = save_file_to_local(base_name, file_extension, gene_result_list, sheet_name_list,
                                                        start, task_id, full_file_path, ds_path)
                if local_save_path is None:
                    message = {'status': -113, 'file_url': ds_path}
                    info = {'message': message, 'task_id': task_id, 'status': -1}
                    append_re_api(info)
                    return None

                os.remove(full_file_path)
                storage_conf = {"auth_ip": auth_ip, "auth_port": auth_port, "auth_username": auth_username,
                                "auth_pwd": auth_pwd, "resource_url": ds_path}
                upload_success = ftp_upload_file(local_save_path, store_flag, storage_conf)
                if upload_success:
                    data = {'message': {'ok, the data has been successfully processed.'}, 'task_id': task_id, 'status': 2}
                    append_re_api(data)
                else:
                    message = {'status': -120, 'file_url': ds_path}  # 上传失败
                    info = {'message': message, 'task_id': task_id, 'status': -1}
                    append_re_api(info)
                # state3, ftp = check_ftp(ip=auth_ip, port=auth_port, username=auth_username, pwd=auth_pwd)
                # if state3 == 0:
                #     print("连接成功")
                #     state4 = ftp_upload_file(ftp, local_path=save_path, remote_path=remote_file_path)
                #     if state4:
                #         print("文件上传成功")
                #         os.remove(save_path)
                #     else:
                #         print("文件上传失败")
        elif access_protocol == 'SFTP':
            state, (sftp, transport) = check_sftp(ip=auth_ip, port=auth_port, username=auth_username, pwd=auth_pwd)
            if state != 0:
                print("连接失败！")
                end = datetime.datetime.now()
                span = end - start
                print(span)
                sftp.close()
                transport.close()
                message = {'status': -101, 'ip': auth_ip, 'port': auth_port}
                info = {'message': message, 'task_id': task_id, 'status': -1}
                append_re_api(info)
                return None
            else:
                print("连接成功")
                state2, full_file_path = sftp_download_file(sftp, remote_path=ds_path, local_path=down_path)
                if state2 == False:
                    print('sftp下载失败')
                    end = datetime.datetime.now()
                    span = end - start
                    print(span)
                    sftp.close()
                    transport.close()
                    message = {'status': -102, 'ip': auth_ip, 'port': auth_port, 'file_url': ds_path}
                    info = {'message': message, 'task_id': task_id, 'status': -1}
                    append_re_api(info)
                    return None

                file_name = ds_path.split('/')[-1]
                base_name, file_extension = os.path.splitext(file_name)
                file_extension = file_extension.lower()
                df_list = get_df_list(ds_path, sheet_name_list, full_file_path, dict_tmp, start, task_id,
                                      file_extension)
                if df_list is None:
                    return  # 在函数内已经回调接口，此处直接return结束即可

                gene_result_list, success_list, store_flag, _, ka_tpl_delete = ka_process(df_list, task_id, k_list,
                                                                                          sheet_name_list,
                                                                                          identifier_info_list_dict,
                                                                                          mask_pool, user_id,
                                                                                          store_flag, ds_path)
                if gene_result_list is None:
                    return

                if ka_tpl_delete:
                    for ka_tpl_id in ka_tpl_delete:
                        delete_temp_ka_tpl(ka_tpl_id, user_id)

                local_save_path, e = save_file_to_local(base_name, file_extension, gene_result_list, sheet_name_list,
                                                        start, task_id, full_file_path, ds_path)
                if local_save_path is None:
                    message = {'status': -113, 'file_url': ds_path}
                    info = {'message': message, 'task_id': task_id, 'status': -1}
                    append_re_api(info)
                    return None

                os.remove(full_file_path)
                upload_success = sftp_upload_file(sftp, local_url=local_save_path, remote_url=ds_path,
                                                  storage_flag=store_flag)
                if upload_success:
                    print("文件上传成功")
                    data = {'message': {'result':'ok, the data has been successfully processed.'}, 'task_id': task_id,
                            'status': 2}
                    append_re_api(data)
                else:
                    print("文件上传失败")
                    message = {'status': -120, 'file_url': ds_path}  # 上传失败
                    info = {'message': message, 'task_id': task_id, 'status': -1}
                    append_re_api(info)
                sftp.close()
                transport.close()
        elif access_protocol == 'MySQL':
            print('mysql')
            l = ds_path.split(",")
            db = l[0]
            table = l[1]
            # 返回dataframe格式数据
            # df=mysql_api(ds_type,auth_ip,auth_port,auth_username,auth_pwd,ds_url)
            auth_username_encoded = urllib.parse.quote_plus(auth_username)
            auth_pwd_encoded = urllib.parse.quote_plus(auth_pwd)
            engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8'
                                   % (auth_username_encoded, auth_pwd_encoded, auth_ip, auth_port, db))
            sql = "SELECT * FROM " + str(table)
            try:
                df = pd.read_sql(sql, engine)
            except Exception as e:
                print(e)
                data = {'message': {'status': -122}, 'task_id': task_id, 'status': -1}
                append_re_api(data)
                return
            # 放到df_list列表中
            df_list.append(df)
            pd.set_option('display.unicode.ambiguous_as_wide', True)
            pd.set_option('display.unicode.east_asian_width', True)

            gene_result_list, success_list, store_flag, min_con, ka_tpl_delete = ka_process(df_list, task_id, k_list,
                                                                                            sheet_name_list,
                                                                                            identifier_info_list_dict,
                                                                                            mask_pool, user_id,
                                                                                            store_flag, ds_path)
            if gene_result_list is None:
                return

            str_change_columns = []
            float_change_columns = []
            for i in identifier_list_dict:
                temp = identifier_list_dict[i]
                for index2 in range(len(temp)):
                    col_name = temp[index2][0]
                    ka_tpl_id = temp[index2][1]
                    limit = int(min_con[index2])
                    gene = get_KA_gene_all_params(mask_pool, ka_tpl_id, limit)
                    if not gene:
                        message = {'status': -107, 'file_url': ds_path,
                                   'sheet_name': 'default'}  # K匿名运行失败，服务内部读取数据库报错：获取泛化规则出错
                        info = {'message': message, 'task_id': task_id, 'status': -1}
                        append_re_api(info)
                        #return None
                    for item in reversed(gene):
                        if item[0][0] in ["3"]:
                            float_change_columns.append(col_name)
                            break
                        elif item[0][0] in ["4", "6", "2"]:
                            str_change_columns.append(col_name)
                            break

            if ka_tpl_delete:
                for ka_tpl_id in ka_tpl_delete:
                    delete_temp_ka_tpl(ka_tpl_id, user_id)

            print("str_change_columns", str_change_columns)
            print("float_change_columns", float_change_columns)
            new_table = str(table) + '_copy'
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {new_table}"))
            metadata = MetaData()
            source_table = Table(table, metadata, autoload_with=engine)
            new_columns = []
            for column in source_table.columns:
                if column.name in str_change_columns:
                    new_column = Column(
                        column.name,
                        String(255),  # 修改数据类型为String
                        primary_key=column.primary_key,
                        nullable=column.nullable,
                        default=column.default,
                        comment=column.comment,
                        unique=column.unique
                    )
                elif column.name in float_change_columns:
                    new_column = Column(
                        column.name,
                        Float,  # 修改数据类型为String
                        primary_key=column.primary_key,
                        nullable=column.nullable,
                        default=column.default,
                        comment=column.comment,
                        unique=column.unique
                    )
                else:
                    new_column = Column(
                        column.name,
                        column.type,  # 保留原始数据类型
                        primary_key=column.primary_key,
                        nullable=column.nullable,
                        default=column.default,
                        comment=column.comment,
                        unique=column.unique
                    )
                new_columns.append(new_column)
            new_table1 = Table(new_table, metadata, *new_columns)
            new_table1.create(engine)
            gene_result_list[0].to_sql(new_table, engine, if_exists='append', index=False)

            if store_flag == 1:
                with engine.connect() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                with engine.connect() as conn:
                    conn.execute(text(f"ALTER TABLE {new_table} RENAME TO {table}"))
            # conf = {
            #     "db": ds_url,
            #     "host": auth_ip,
            #     "port": auth_port,
            #     "user": auth_username,
            #     "pwd": auth_pwd
            # }
            # # 创建一个 DB_mysql 类的实例，传入数据库连接配置信息
            # db_mysql_instance = DBTool.DB_mysql(conf)
            # data_list = data_list = [tuple(row) for row in gene_result.to_numpy()]
            # db_mysql_instance.form_duplicate(str(ds_path) + '_copy', ds_path, data_list)
            end = datetime.datetime.now()
            span = end - start
            print(span)
            data = {'message': {'result':'ok, the data has been successfully processed.'}, 'task_id': task_id, 'status': 2}
            append_re_api(data)
            return
        elif access_protocol == 'Local':
            file_name = ds_path.split('/')[-1]
            base_name, file_extension = os.path.splitext(file_name)
            file_extension = file_extension.lower()
            df_list = get_df_list(ds_path, sheet_name_list, ds_path, dict_tmp, start, task_id, file_extension)
            if df_list is None:
                return  # 在函数内已经回调接口，此处直接return结束即可

            gene_result_list, success_list, store_flag, _, ka_tpl_delete = ka_process(df_list, task_id, k_list,
                                                                                      sheet_name_list,
                                                                                      identifier_info_list_dict,
                                                                                      mask_pool, user_id,
                                                                                      store_flag, ds_path)
            if gene_result_list is None:
                return

            if ka_tpl_delete:
                for ka_tpl_id in ka_tpl_delete:
                    delete_temp_ka_tpl(ka_tpl_id, user_id)

            local_save_path, e = save_file_to_local(base_name, file_extension, gene_result_list, sheet_name_list, start,
                                                    task_id, ds_path, ds_path)
            if local_save_path is None:
                message = {'status': -113, 'file_url': ds_path}
                info = {'message': message, 'task_id': task_id, 'status': -1}
                append_re_api(info)
                return None

            if store_flag == 1:
                os.remove(ds_path)
                save_path = ds_path
            else:
                save_file_name = base_name + "_copy" + file_extension
                save_path = os.path.dirname(ds_path) + '/' + save_file_name

            os.rename(local_save_path, save_path)
            # os.remove(local_save_path)
            data = {'message': {'result':'ok, the data has been successfully processed.'}, 'task_id': task_id,
                    'status': 2}
            append_re_api(data)
        else:
            print("不支持的access类型")
            end = datetime.datetime.now()
            span = end - start
            print(span)
            message = {'status': -108, 'access_protocol': access_protocol}
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
            return None

    end = datetime.datetime.now()
    span = end - start
    print(span)
    data = {'message': {'result':'ok'}, 'task_id': task_id, 'status': 4}
    flag = 0
    status = 0
    # k匿名默认值environment_state: 1, control: 3, ability: 2,per: 0.9, m: 10, power: 3
    for df, result_df, sheet_name, success in zip(df_list, gene_result_list, sheet_name_list, success_list):
        ka_risk_assessment(mask_pool, task_id, sheet_name, df, result_df, identifier_name_list[sheet_name], 1, 3, 2, 0.9, 10, 3,
                           user_id)
        if sensitive_list_dict:
            if sheet_name in sensitive_list_dict.keys():
                sensitive_list = sensitive_list_dict[sheet_name]
                ka_availability_analysis(mask_pool, task_id, sheet_name, df, result_df, identifier_name_list[sheet_name],
                                         sensitive_list)
            else:
                print(sheet_name + "无敏感列！")
                logger.warning(f"任务{task_id}的工作表{sheet_name}缺失敏感列！")
                status = -2
                # data = {'message': 'sheet表无敏感列', 'task_id': task_id, 'status': -2}
        if success== 0:
            message = {'status': -110, 'file_url': ds_path, 'sheet_name': sheet_name}
            info = {'message': message, 'task_id': task_id, 'status': 4}
            append_re_api(info)
            # data = {'message': 'k值太大！不满足K匿名条件', 'task_id': task_id, 'status': 2}
    if status == -2:
        message = {'status': -106, 'file_url': ds_path}
        data = {'message': message, 'task_id': task_id, 'status': -2}
    # f=1
    # for success in success_list:
    #     if success==0:
    #         f=0
    # if f == 0:
    #     data = {'message': 'k值太大，脱敏失败！', 'task_id': task_id, 'status': -1}
    # elif f == 1:  # 有-1的情况，已调用回调接口，此处不要直接使用else，
    #     data = {'message': 'ok', 'task_id': task_id, 'status': 2}
    append_re_api(data)
    print("成功回调")
    return
    # dict_list_back = gene_result.to_dict(orient='records')
    # return dict_list_back


# def prosecutor_attack(raw_data, mask_data, identifier_list):
#     data = raw_data
#     sum1 = 0
#     min = -1
#     copy_data = pd.DataFrame(data)
#     n = len(copy_data)
#     grouped2 = copy_data.groupby(identifier_list)
#     J = len(grouped2)
#     for name, gene_data in grouped2:
#         len1 = len(gene_data)
#         if min == -1 or min > len1:
#             min = len1
#         if 1 / len1 > th:
#             sum1 += len1
#     # min!=-1
#     inspector1 = sum1 / n
#     inspector2 = 1 / min
#     inspector3 = J / n
#     print("原始数据的检察官攻击风险：")
#     print(f"重识别概率大于{th}的数据集记录占总体的比例:{inspector1}")
#     print(f"数据集所有记录中最大的重识别概率:{inspector2}")
#     print(f"平均重识别概率:{inspector3}")
#     data = mask_data
#     sum1 = 0
#     min = -1
#     copy_data = pd.DataFrame(data)
#     n = len(copy_data)
#     grouped2 = copy_data.groupby(identifier_list)
#     J = len(grouped2)
#     for name, gene_data in grouped2:
#         len1 = len(gene_data)
#         if min == -1 or min > len1:
#             min = len1
#         if 1 / len1 > th:
#             sum1 += len1
#     # min!=-1
#     inspector4 = sum1 / n
#     inspector5 = 1 / min
#     inspector6 = J / n
#     print("脱敏后数据的检察官攻击风险：")
#     print(f"重识别概率大于{th}的数据集记录占总体的比例:{inspector4}")
#     print(f"数据集所有记录中最大的重识别概率:{inspector5}")
#     print(f"平均重识别概率:{inspector6}")
#     combined_data = {
#         "inspector1": inspector1,
#         "inspector2": inspector2,
#         "inspector3": inspector3,
#         "inspector4": inspector4,
#         "inspector5": inspector5,
#         "inspector6": inspector6
#     }
#     return combined_data

def prosecutor_attack(data, identifier_list):
    '''
    data是dataframe,identifier_list是标识列名称
    '''
    sum1 = 0
    min = -1
    copy_data = data.copy()
    n = len(copy_data)
    grouped2 = copy_data.groupby(identifier_list)
    J = len(grouped2)
    for name, gene_data in grouped2:
        len1 = len(gene_data)
        if min == -1 or min > len1:
            min = len1
        if 1 / len1 > th:
            sum1 += len1
    # min!=-1
    inspector1 = sum1 / n
    inspector2 = 1 / min
    inspector3 = J / n
    print(f"重识别概率大于{th}的数据集记录占总体的比例:{inspector1}")
    print(f"数据集所有记录中最大的重识别概率:{inspector2}")
    print(f"平均重识别概率:{inspector3}")
    return inspector1, inspector2, inspector3


def ka_prosecutor_attack(raw_data, mask_data, identifier_list):
    '''
    raw_data是原始数据,mask_data是脱敏数据,这两者都是record
    identifier_list是标识列名称
    '''
    inspector1, inspector2, inspector3 = prosecutor_attack(raw_data.copy(), identifier_list)
    inspector4, inspector5, inspector6 = prosecutor_attack(mask_data.copy(), identifier_list)
    return inspector1, inspector2, inspector3, inspector4, inspector5, inspector6


def prosecutor_attack_api(data, identifier_list):
    '''
    raw_data是原始数据,mask_data是脱敏数据,这两者都是record
    identifier_list是标识列名称
    '''
    start = datetime.datetime.now()
    inspector1, inspector2, inspector3 = prosecutor_attack(pd.DataFrame(data), identifier_list)
    result = {
        "inspector1": inspector1,
        "inspector2": inspector2,
        "inspector3": inspector3,
    }
    end = datetime.datetime.now()
    span = end - start
    print("任务执行时间", span)
    return result


# def correspondent_attack(raw_data, mask_data, attacker_data, identifier_list):
#     data = pd.DataFrame(raw_data)
#     attacker_data = pd.DataFrame(attacker_data)
#     grouped1 = data.groupby(identifier_list)
#     grouped2 = attacker_data.groupby(identifier_list)
#     n = len(data)
#     min = -1
#     sum1 = 0
#     sum2 = 0
#     sum3 = 0
#     J = len(grouped1)
#     for name, gene_data in grouped2:
#         len1 = len(gene_data)
#         sum2 += len1
#         f = len(data[data[identifier_list].isin(gene_data[identifier_list])][identifier_list])
#         sum3 += f / len1
#         if min == -1 or min > len1:
#             min = len1
#         if 1 / len1 > th:
#             sum1 += f
#     correspondent1 = sum1 / n
#     correspondent2 = 1 / min
#     co1 = J / sum2
#     co2 = sum3 / n
#     correspondent3 = max(co1, co2)
#     print("原始数据的记者攻击风险：")
#     print(f"重识别概率大于{th}的数据集记录占总体的比例:{correspondent1}")
#     print(f"数据集所有记录中最大的重识别概率:{correspondent2}")
#     print(f"平均重识别概率:{correspondent3}")
#     data = pd.DataFrame(mask_data)
#     attacker_data = pd.DataFrame(attacker_data)
#     grouped1 = data.groupby(identifier_list)
#     grouped2 = attacker_data.groupby(identifier_list)
#     n = len(data)
#     min = -1
#     sum1 = 0
#     sum2 = 0
#     sum3 = 0
#     J = len(grouped1)
#     for name, gene_data in grouped2:
#         len1 = len(gene_data)
#         sum2 += len1
#         f = len(data[data[identifier_list].isin(gene_data[identifier_list])][identifier_list])
#         sum3 += f / len1
#         if min == -1 or min > len1:
#             min = len1
#         if 1 / len1 > th:
#             sum1 += f
#     correspondent4 = sum1 / n
#     correspondent5 = 1 / min
#     co1 = J / sum2
#     co2 = sum3 / n
#     correspondent6 = max(co1, co2)
#     print("脱敏后数据的记者攻击风险：")
#     print(f"重识别概率大于{th}的数据集记录占总体的比例:{correspondent4}")
#     print(f"数据集所有记录中最大的重识别概率:{correspondent5}")
#     print(f"平均重识别概率:{correspondent6}")
#     combined_data = {
#         "correspondent1": correspondent1,
#         "correspondent2": correspondent2,
#         "correspondent3": correspondent3,
#         "correspondent4": correspondent4,
#         "correspondent5": correspondent5,
#         "correspondent6": correspondent6
#     }
#     return combined_data

def correspondent_attack(data, attacker_data, identifier_list):
    '''
    data为dataframe
    attack_data为dataframe，攻击者数据
    identifier_list为标识列名称
    '''
    grouped1 = data.groupby(identifier_list)
    grouped2 = attacker_data.groupby(identifier_list)
    n = len(data)
    min = -1
    sum1 = 0
    sum2 = 0
    sum3 = 0
    J = len(grouped1)
    for name, gene_data in grouped2:
        len1 = len(gene_data)
        sum2 += len1
        f = len(data[data[identifier_list].isin(gene_data[identifier_list])][identifier_list])
        sum3 += f / len1
        if min == -1 or min > len1:
            min = len1
        if 1 / len1 > th:
            sum1 += f
    correspondent1 = sum1 / n
    correspondent2 = 1 / min
    co1 = J / sum2
    co2 = sum3 / n
    correspondent3 = max(co1, co2)
    print(f"重识别概率大于{th}的数据集记录占总体的比例:{correspondent1}")
    print(f"数据集所有记录中最大的重识别概率:{correspondent2}")
    print(f"平均重识别概率:{correspondent3}")
    return correspondent1, correspondent2, correspondent3


def ka_correspondent_attack(raw_data, mask_data, attacker_data, identifier_list):
    '''
    数据都是dataframe
    '''
    correspondent1, correspondent2, correspondent3 = correspondent_attack(raw_data.copy(), attacker_data.copy(),
                                                                          identifier_list)
    correspondent4, correspondent5, correspondent6 = correspondent_attack(mask_data.copy(),
                                                                          attacker_data.copy(), identifier_list)
    return correspondent1, correspondent2, correspondent3, correspondent4, correspondent5, correspondent6


def correspondent_attack_api(data, attacker_data, identifier_list):
    start = datetime.datetime.now()
    correspondent1, correspondent2, correspondent3 = correspondent_attack(pd.DataFrame(data),
                                                                          pd.DataFrame(attacker_data), identifier_list)
    result = {
        "correspondent1": correspondent1,
        "correspondent2": correspondent2,
        "correspondent3": correspondent3,
    }
    end = datetime.datetime.now()
    span = end - start
    print("任务执行时间", span)
    return result


# def marketer_attack(raw_data, mask_data, attacker_data, identifier_list):
#     data = pd.DataFrame(raw_data)
#     attacker_data = pd.DataFrame(attacker_data)
#     grouped1 = data.groupby(identifier_list)
#     grouped2 = attacker_data.groupby(identifier_list)
#     n = len(data)
#     J = len(grouped1)
#     N = len(attacker_data)
#     sum1 = 0
#     for name, gene_data in grouped2:
#         len1 = len(gene_data)
#         f = len(data[data[identifier_list].isin(gene_data[identifier_list])][identifier_list])
#         sum1 += f / len1
#     marketer1 = J / N
#     marketer2 = sum1 / n
#     print("原始数据的营销者攻击风险：")
#     print(f"身份数据集和发布数据集的个人信息主体完全相同情况下的平均重识别概率{marketer1}")
#     print(f"发布数据集是身份数据集的个人信息主体的一部分情况下的平均重识别概率{marketer2}")
#     data = pd.DataFrame(mask_data)
#     attacker_data = pd.DataFrame(attacker_data)
#     grouped1 = data.groupby(identifier_list)
#     grouped2 = attacker_data.groupby(identifier_list)
#     n = len(data)
#     J = len(grouped1)
#     N = len(attacker_data)
#     sum1 = 0
#     for name, gene_data in grouped2:
#         len1 = len(gene_data)
#         f = len(data[data[identifier_list].isin(gene_data[identifier_list])][identifier_list])
#         sum1 += f / len1
#     marketer3 = J / N
#     marketer4 = sum1 / n
#     print("脱敏后数据的营销者攻击风险：")
#     print(f"身份数据集和发布数据集的个人信息主体完全相同情况下的平均重识别概率{marketer3}")
#     print(f"发布数据集是身份数据集的个人信息主体的一部分情况下的平均重识别概率{marketer4}")
#     combined_data = {
#         "marketer1": marketer1,
#         "marketer2": marketer2,
#         "marketer3": marketer3,
#         "marketer4": marketer4
#     }
#     return combined_data

def marketer_attack(data, attacker_data, identifier_list):
    grouped1 = data.groupby(identifier_list)
    grouped2 = attacker_data.groupby(identifier_list)
    n = len(data)
    J = len(grouped1)
    N = len(attacker_data)
    sum1 = 0
    for name, gene_data in grouped2:
        len1 = len(gene_data)
        f = len(data[data[identifier_list].isin(gene_data[identifier_list])][identifier_list])
        sum1 += f / len1
    marketer1 = J / N
    marketer2 = sum1 / n
    print(f"身份数据集和发布数据集的个人信息主体完全相同情况下的平均重识别概率{marketer1}")
    print(f"发布数据集是身份数据集的个人信息主体的一部分情况下的平均重识别概率{marketer2}")
    return marketer1, marketer2


def ka_marketer_attack(raw_data, mask_data, attacker_data, identifier_list):
    marketer1, marketer2 = marketer_attack(raw_data.copy(), attacker_data.copy(), identifier_list)
    marketer3, marketer4 = marketer_attack(mask_data.copy(), attacker_data.copy(), identifier_list)
    return marketer1, marketer2, marketer3, marketer4


def marketer_attack_api(data, attacker_data, identifier_list):
    '''
    data, attacker_data是record
    '''
    start = datetime.datetime.now()
    marketer1, marketer2 = marketer_attack(pd.DataFrame(data), pd.DataFrame(attacker_data), identifier_list)
    result = {
        "marketer1": marketer1,
        "marketer2": marketer2,
    }
    end = datetime.datetime.now()
    span = end - start
    print("任务执行时间", span)
    return result


'''
environment_state:0表示完全公开共享数据发布，1表示受控公开共享数据发布，2表示领地公开共享数据发布。
    0时环境重标识攻击概率为1，1时取（内部故意攻击概率、数据集包含熟人概率、数据泄露概率）最大值.
        内部故意攻击概率：风险减缓控制水平control，动机和能力ability。control取值：{3：高，2：中，1：低}。ability取值：{3：高，2：中，1：低}
        数据集包含熟人概率:所有人中具有数据集中特征的个体的百分比per，接收者的熟人数m。
        数据泄露概率=数据接收方发生数据泄露的概率。数据泄露发生概率与接收方的数据安全和隐私控制的能力power分级(高，中，低)相关.power取值：{3：高，2：中。1：低}
    重标识总体风险:environment_state{0:threshold=0.05, 1:threshold=0.2, 2:threshold=1/3}

返回 等价类重标识风险最大值，等价类重标识风险平均值，环境重标识概率，重标识总体风险。
环境重标识概率，重标识总体风险 若不存在返回None
'''


def risk(data, identifier_list: list, environment_state: int, control: int, ability: int, per: float, m: int,
         power: int):
    print('准标识符:' + str(identifier_list))
    grouped = data.groupby(identifier_list)
    quasiDict = {}  # 标识符组合出现次数（等价类大小）
    re_identification = {}  # 等价类重标识风险
    for value, group in grouped:
        quasiDict[value] = len(group)
    J = len(quasiDict)  # 标识符等价类个数
    for key, value in quasiDict.items():
        re_identification[key] = 1 / value

    # 等价类重标识风险最大值
    max_re_identification = max(re_identification.values())

    # 等价类重标识风险平均值
    values = list(re_identification.values())
    average_re_identification = sum(values) / J

    # 环境重标识概率
    environment_attack = None
    if environment_state is not None:
        if environment_state == 0:
            environment_attack = 1
        elif environment_state == 1 or environment_state == 2:
            environment_attack1 = 0
            environment_attack2 = 0
            environment_attack3 = 0
            if (control is not None) and (ability is not None):
                if control == 3:
                    if ability == 1:
                        environment_attack1 = 0.05
                    elif ability == 2:
                        environment_attack1 = 0.1
                    elif ability == 3:
                        environment_attack1 = 0.2
                    else:
                        print("ability传参错误！")
                        return False, (1, ability)
                elif control == 2:
                    if ability == 1:
                        environment_attack1 = 0.2
                    elif ability == 2:
                        environment_attack1 = 0.3
                    elif ability == 3:
                        environment_attack1 = 0.4
                    else:
                        print("ability传参错误！")
                        return False, (1, ability)
                elif control == 1:
                    if ability == 1:
                        environment_attack1 = 0.4
                    elif ability == 2:
                        environment_attack1 = 0.5
                    elif ability == 3:
                        environment_attack1 = 0.6
                    else:
                        print("ability传参错误！")
                        return False, (1, ability)
                else:
                    print("control传参错误！")
                    return False, (2, control)
                if (per is not None) and (m is not None):
                    tmp = pow(1 - per, m)
                    environment_attack2 = 1 - tmp
                if power is not None:
                    if power == 3:
                        environment_attack3 = 0.14
                    elif power == 2:
                        environment_attack3 = 0.27
                    elif power == 1:
                        environment_attack3 = 0.55
                    else:
                        print("power传参错误！")
                        return False, (3, power)
                environment_attack = max(environment_attack1, environment_attack2, environment_attack3)
        else:
            print("environment_state传参错误！")
            return False, (4, environment_state)

    # 重标识总体风险
    all_re_identification = None
    if environment_state is not None:
        if environment_state == 0:
            threshold = 0.05
        elif environment_state == 1:
            threshold = 0.2
        elif environment_state == 2:
            threshold = 1 / 3
        else:
            print("environment_state传参错误！")
            return False, (4, environment_state)
        i = 0
        for tmp in re_identification.values():
            if tmp > threshold:
                i += 1
        # 等价类门限风险
        equal_risk = i / J
        if environment_state == 0:
            if equal_risk == 0:
                all_re_identification = max_re_identification * environment_attack
            else:
                all_re_identification = 1
        elif environment_state == 1 or environment_state == 2:
            if equal_risk == 0:
                all_re_identification = average_re_identification * environment_attack
            else:
                all_re_identification = 1
        else:
            print("environment_state传参错误！")
            return False, (4, environment_state)

    return (max_re_identification, average_re_identification, environment_attack, all_re_identification), (None, 0, 0)


'''
environment_state:0表示完全公开共享数据发布，1表示受控公开共享数据发布，2表示领地公开共享数据发布。
    0时环境重标识攻击概率为1，1时取（内部故意攻击概率、数据集包含熟人概率、数据泄露概率）最大值.
        内部故意攻击概率：风险减缓控制水平control，动机和能力ability。control取值：{3：高，2：中，1：低}。ability取值：{3：高，2：中，1：低}
        数据集包含熟人概率:所有人中具有数据集中特征的个体的百分比per，接收者的熟人数m。
        数据泄露概率=数据接收方发生数据泄露的概率。数据泄露发生概率与接收方的数据安全和隐私控制的能力power分级(高，中，低)相关.power取值：{3：高，2：中。1：低}
    重标识总体风险:environment_state{0:threshold=0.05, 1:threshold=0.2, 2:threshold=1/3}

返回 等价类重标识风险最大值，等价类重标识风险平均值，环境重标识概率，重标识总体风险。
环境重标识概率，重标识总体风险 若不存在返回None
k匿名默认值environment_state: 1, control: 3, ability: 2,per: 0.9, m: 10, power: 3
'''


def ka_risk_assessment(mask_pool, task_id, sheet_name, raw_data, modified_data, identifier_list: list,
                       environment_state: int, control: int, ability: int,
                       per: float, m: int, power: int, user_id: str):
    result, e = risk(
        raw_data.copy(),
        identifier_list,
        environment_state,
        control,
        ability, per,
        m, power)
    if result:
        max_re_identification1, average_re_identification1, environment_attack1, all_re_identification1 = result
    else:
        return None, e
    result, e = risk(
        modified_data.copy(),
        identifier_list,
        environment_state,
        control,
        ability, per,
        m, power)
    if result:
        max_re_identification2, average_re_identification2, environment_attack2, all_re_identification2 = result
    else:
        return None, e
    insert_ka_risk_assessment(mask_pool, task_id, sheet_name, max_re_identification1, average_re_identification1,
                              environment_attack1, all_re_identification1, max_re_identification2,
                              average_re_identification2, environment_attack2, all_re_identification2, user_id)
    return (max_re_identification1, average_re_identification1, environment_attack1, all_re_identification1,
            max_re_identification2, average_re_identification2, environment_attack2, all_re_identification2), None


# k匿名默认值environment_state: 1, control: 3, ability: 2,per: 0.9, m: 10, power: 3
def risk_assessment_api(mask_pool, task_id, local_path, ds_path, sheet_name, identifier_list: list, user_id: str,
                        environment_state: int = 1,
                        control: int = 3, ability: int = 2,
                        per: float = 0.9, m: int = 10, power: int = 3, flag=1):
    if flag == 1:
        file_name = local_path.split('/')[-1]
        base_name, file_extension = os.path.splitext(file_name)
        # 判断文件类型
        if file_extension == '.xlsx' or file_extension == '.xls':
            df = pd.read_excel(local_path, sheet_name=sheet_name)
            print(df.head())  # 打印 DataFrame 的前几行，以确认数据读取正确
        elif file_extension == '.csv':
            df = pd.read_csv(local_path)
            print(df.head())  # 打印 DataFrame 的前几行，以确认数据读取正确
        else:
            print("未知文件类型或不支持的文件类型")
            message = {'status': -103, 'file_url': ds_path}
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
            return
    else:
        db_conf = json.loads(local_path)["mysql"]
        auth_username_encoded = urllib.parse.quote_plus(db_conf["username"])
        auth_pwd_encoded = urllib.parse.quote_plus(db_conf["password"])
        engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8'
                               % (auth_username_encoded, auth_pwd_encoded, db_conf["host"], db_conf["port"],
                                  db_conf["database"]))
        sql = "SELECT * FROM " + db_conf["table"]
        try:
            df = pd.read_sql(sql, engine)
        except Exception as e:
            print(e)
            data = {'message': {'status':-122}, 'task_id': task_id, 'status': -1}
            append_re_api(data)
            return
    result, e = risk(
        df,
        identifier_list,
        environment_state,
        control,
        ability, per,
        m, power)
    if result:
        max_re_identification1, average_re_identification1, environment_attack1, all_re_identification1 = result
    else:
        param, value = e
        message = {'status': -123, 'param': param, 'value': value}
        data = {"message": message, "task_id": task_id, "status": -1}
        append_re_api(data)
        return
    insert_success = insert_ka_risk_assessment(mask_pool, task_id, sheet_name, max_re_identification1,
                                               average_re_identification1,
                                               environment_attack1, all_re_identification1, None, None, None, None,
                                               user_id)
    if not insert_success:
        data = {'message':{'status': -125}, 'task_id': task_id, 'status': -1}
    else:
        data = {'message': {'result':'ok'}, 'task_id': task_id, 'status': 2}
    append_re_api(data)
    os.remove(local_path)
    return


# def classification_analysis(data, feature_col, target_col):
#     '''
#     分类
#     data是dataframe
#     '''
#     # 分割训练集和测试集
#     X_train, X_test, y_train, y_test = train_test_split(data[feature_col], data[target_col], test_size=0.3,
#                                                         random_state=1)
#     # 初始化MLPClassifier模型
#     clf = MLPClassifier(solver='lbfgs', alpha=1e-5, hidden_layer_sizes=(5, 2), random_state=1)
#     # 拟合模型
#     clf.fit(X_train, y_train)
#     # 预测测试集
#     y_pred = clf.predict(X_test)
#     # 计算准确率
#     accuracy = accuracy_score(y_test, y_pred)
#     return accuracy


def value_analysis(data, target_col):
    '''
    data是dataframe
    '''
    return len(data[target_col].unique())


# def availability_analysis(raw_form, modified_form, feature_col, target_col):
#     start = datetime.datetime.now()
#     # 分类，使用raw_form,modified_form,feature_col,target_col
#     # 原始数据
#     form1 = pd.DataFrame(raw_form)
#     accuracy1 = classification_analysis(form1, feature_col, target_col)
#     print(f"Accuracy of raw_data: {accuracy1}")
#
#     # 修改后数据
#     form2 = pd.DataFrame(modified_form)
#     accuracy2 = classification_analysis(form2, feature_col, target_col)
#     print(f"Accuracy of raw_data: {accuracy2}")
#     print(f"Difference:{accuracy1 - accuracy2}")
#
#     # 类个数,使用raw_form,modified_form,target_col
#     form1 = pd.DataFrame(raw_form)
#     form2 = pd.DataFrame(modified_form)
#     unique_values1 = value_analysis(form1, target_col)
#     print(f"unique_values_size of raw_target_col:{unique_values1}")
#     unique_values2 = value_analysis(form2, target_col)
#     print(f"unique_values_size of modified_target_col:{unique_values2}")
#
#     combined_data = {
#         "Accuracy1": accuracy1,
#         "Accuracy2": accuracy2,
#         "Difference": accuracy1 - accuracy2,
#         "size1": unique_values1,
#         "size2": unique_values2,
#     }
#     end = datetime.datetime.now()
#     span = end - start
#     print("任务执行时间", span)
#     return combined_data


def logistic_regression1(data, feature_col, target_col):
    '''
    对dataframe数据的特征列及目标列进行逻辑回归分析，返回不同、基线准确率（baseline accuracy）、原始准确率（original accuracy）、相对准确率（relative accuracy）、以及Brier评分（Brier score）
    data是dataframe格式数据，feature_col、target_col是列表
    '''
    from sklearn.preprocessing import LabelEncoder
    label_encoder = LabelEncoder()
    X = label_encoder.fit_transform(data[feature_col].astype(str)).reshape(-1, 1)
    y = label_encoder.fit_transform(data[target_col].astype(str)).reshape(-1, 1)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1)
    # 训练逻辑回归模型
    model = LogisticRegression()
    model.fit(X_train, y_train)

    # 在测试集上进行预测
    y_pred = model.predict(X_test)

    # 计算各种评估指标
    # accuracy = accuracy_score(y_test, y_pred)
    # baseline_accuracy = max(y_test.mean(), 1 - y_test.mean())  # 基线准确率
    # original_accuracy = accuracy_score(y_test, y_test)  # 原始准确率（预测与真实一致）
    # relative_accuracy = accuracy / baseline_accuracy  # 相对准确率
    # if value_analysis(data, target_col) == 2:
    #     brier_score = brier_score_loss(y_test, y_pred)  # Brier评分
    # else:
    #     brier_score = None
    values = value_analysis(data, target_col)  #
    baseline_accuracy = max(y_test.mean(), 1 - y_test.mean())  # 基线准确率
    original_accuracy = accuracy_score(y_test, y_pred)  # 原始准确率（预测与真实一致）
    relative_accuracy = original_accuracy / baseline_accuracy  # 相对准确率
    if values == 2:
        brier_score = brier_score_loss(y_test, y_pred)  # Brier评分
    else:
        brier_score = None

    # 输出各种评估指标
    print(f"Values: {values}")
    print(f"Baseline Accuracy: {baseline_accuracy}")
    print(f"Original Accuracy: {original_accuracy}")
    print(f"Relative Accuracy: {relative_accuracy}")
    print(f"Brier Score: {brier_score}")
    return values, baseline_accuracy, original_accuracy, relative_accuracy, brier_score


def logistic_regression2(data, feature_col):
    '''
    data是dataframe
    对特征列中的每一个不同取值进行敏感性（sensitivity）、特异性（specificity）和 Brier 分数（brier_score）的分析
    '''
    # 初始化空列表来存储结果
    results = []

    # 获取特征列中的所有唯一取值
    unique_values = data[feature_col].unique()

    # 遍历每一个特征取值
    for value in unique_values:
        # 按照特征取值筛选数据
        subset = data[data[feature_col] == value]

        # 提取预测概率和真实标签
        predicted_prob = subset['predicted_prob']
        true_label = subset['true_label']

        # 计算混淆矩阵
        threshold = 0.5  # 二分类阈值，通常为0.5
        predicted_class = (predicted_prob >= threshold).astype(int)
        cm = confusion_matrix(true_label, predicted_class)

        # 计算敏感性和特异性
        TN, FP, FN, TP = cm.ravel()
        sensitivity = TP / (TP + FN)
        specificity = TN / (TN + FP)

        # 计算布里尔分数
        brier_score = brier_score_loss(true_label, predicted_prob)

        # 将结果存储到列表中
        results.append({
            'value': value,
            'sensitivity': sensitivity,
            'specificity': specificity,
            'brier_score': brier_score
        })

    # 将结果转换为DataFrame进行输出
    results_df = pd.DataFrame(results)
    print(results_df)
    return results_df


def ka_availability_analysis(mask_pool, task_id, sheet_name, raw_form, modified_form, identifier_col_list,
                             sensitive_col_list):
    '''
    raw_form, modified_form是dataframe格式数据
    1.在数据库中插入 k-匿名修改前后数据中每一标识列对敏感列（如果有多个敏感列，放在一起分析）的逻辑回归分析结果
    2.在数据库中插入k-匿名修改前后数据每一标识列的不同取值的逻辑回归分析结果
    '''
    form1 = raw_form.copy()
    form2 = modified_form.copy()
    for identifier_col in identifier_col_list:
        for sensitive_col in sensitive_col_list:
            # 任务1
            values, baseline_accuracy, original_accuracy, relative_accuracy, brier_score = logistic_regression1(form1,
                                                                                                                [
                                                                                                                    identifier_col],
                                                                                                                sensitive_col)
            insert_ka_logistic_regression1(mask_pool, task_id, sheet_name, 0, identifier_col, sensitive_col, values,
                                           baseline_accuracy, original_accuracy, relative_accuracy, brier_score)
            values, baseline_accuracy, original_accuracy, relative_accuracy, brier_score = logistic_regression1(form2,
                                                                                                                [
                                                                                                                    identifier_col],
                                                                                                                sensitive_col)
            insert_ka_logistic_regression1(mask_pool, task_id, sheet_name, 1, identifier_col, sensitive_col, values,
                                           baseline_accuracy, original_accuracy, relative_accuracy, brier_score)
        # # 任务2
        # result_df = logistic_regression2(form1, identifier_col)
        # for index, row in result_df.iterrows():
        #     insert_ka_logistic_regression2(mask_pool, task_id, 0, identifier_col, row['value'], row['sensitivity'],
        #                                    row['specificity'], row['brier_score'])
        # result_df = logistic_regression2(form2, identifier_col)
        # for index, row in result_df.iterrows():
        #     insert_ka_logistic_regression2(mask_pool, task_id, 1, identifier_col, row['value'], row['sensitivity'],
        #                                    row['specificity'], row['brier_score'])
        print("可用性分析完成！")


def availability_analysis_api(mask_pool, task_id, local_path, ds_path, sheet_name, identifier_col_list,
                              sensitive_col_list, user_id, flag=1):
    '''
    raw_form, modified_form是dataframe格式数据
    1.在数据库中插入 k-匿名修改前后数据中每一标识列对敏感列（如果有多个敏感列，放在一起分析）的逻辑回归分析结果
    2.在数据库中插入k-匿名修改前后数据每一标识列的不同取值的逻辑回归分析结果
    '''
    if flag == 1:
        file_name = local_path.split('/')[-1]
        base_name, file_extension = os.path.splitext(file_name)
        # 判断文件类型
        if file_extension == '.xlsx' or file_extension == '.xls':
            df = pd.read_excel(local_path, sheet_name=sheet_name)
            print(df.head())  # 打印 DataFrame 的前几行，以确认数据读取正确
        elif file_extension == '.csv':
            df = pd.read_csv(local_path)
            print(df.head())  # 打印 DataFrame 的前几行，以确认数据读取正确
        else:
            print("未知文件类型或不支持的文件类型")
            message = {'status': -103, 'file_url': ds_path}
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
            return
    else:
        db_conf = json.loads(local_path)["mysql"]
        auth_username_encoded = urllib.parse.quote_plus(db_conf["username"])
        auth_pwd_encoded = urllib.parse.quote_plus(db_conf["password"])
        engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8'
                               % (auth_username_encoded, auth_pwd_encoded, db_conf["host"], db_conf["port"],
                                  db_conf["database"]))
        sql = "SELECT * FROM " + db_conf["table"]
        try:
            df = pd.read_sql(sql, engine)
        except Exception as e:
            print(e)
            data = {'message': {'status':-122}, 'task_id': task_id, 'status': -1}
            append_re_api(data)
            return
    for identifier_col in identifier_col_list:
        for sensitive_col in sensitive_col_list:
            # 任务1
            (accuracy, baseline_accuracy, original_accuracy,
             relative_accuracy, brier_score) = logistic_regression1(df, [identifier_col], sensitive_col)
            insert_success = insert_ka_logistic_regression1(mask_pool, task_id, sheet_name, None, identifier_col,
                                                            sensitive_col,
                                                            accuracy, baseline_accuracy, original_accuracy,
                                                            relative_accuracy,
                                                            brier_score)

            if not insert_success:
                data = {'message': {'status': -125}, 'task_id': task_id, 'status': -1}
                append_re_api(data)
                os.remove(local_path)
                return
    data = {'message': {'result':'ok'}, 'task_id': task_id, 'status': 2}
    append_re_api(data)
    os.remove(local_path)
    return
