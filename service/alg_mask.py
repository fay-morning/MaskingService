import os
import random
import re
import time

import jieba
import pymysql
import json
import datetime
import pandas as pd
from collections import Counter
import numpy as np
import requests
from pyexpat.errors import messages
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Float

from download import sftp_util
from download import user_upload_util
from download.user_upload_util import upload_to_server
from util.mysql_classification_util import get_db_params, get_sensitive_db,get_sensitivedata_by_resource_batch,get_sensitivedata_by_taskid
from util.mysql_util import delete_aggregation_params, get_download_file
from util.mysql_util import get_ruleid_params
from util.mysql_util import get_generalization_rule
from util.mysql_util import update_data_volume, insert_masking_id_data_batch
from .callback_api import re_api, append_re_api
from download.remote_access import check_ftp, check_sftp
from download.sftp_util import ftp_download_file, sftp_download_file, ftp_upload_file, sftp_upload_file
import urllib.parse

from const import constants
down_path = constants.DOWN_PATH
download_path = constants.DOWNLOAD_PATH
col_rule = constants.COL_RULE
'''
注意，[string]中可能有空的情况
'''


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
        raise TypeError("Unsupported data type. Only supports DataFrame or list of strings/float/int.")
        # info = {'message': '不支持的数据类型！', 'task_id': task_id, 'status': -1}
        # append_re_api(info)
        # return data


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
    tmp = data.copy()
    try:
        if index == 1:  # "sampling"
            tmp = sampling(data, gene_params[0], task_id)
        elif index == 3:  # "aggregation"
            t2 = tmp.to_dict(orient='records')
            t3 = aggregation(t2, [col_name], gene_params[0], task_id)
            tmp = pd.DataFrame(t3)
        elif index == 6:  # "suppression"
            string_list = tmp[col_name].tolist()
            filtered_list = suppression(string_list, gene_params[0], task_id)
            tmp[col_name] = filtered_list
        elif index == 2:  # "masking"
            for index, row in tmp.iterrows():
                tmp.loc[index, col_name] = masking([row[col_name]], gene_params[0], gene_params[1], gene_params[2], task_id)[0]
        elif index == 4:  # "categorization"
            tmp[col_name] = tmp[col_name].apply(lambda x: str(categorization([x], gene_params[0], task_id)[0]))
        elif index == 5:  # "generalization"
            for index, row in tmp.iterrows():
                tmp.loc[index, col_name] = generalization([row[col_name]], gene_params[0], gene_params[1], task_id)[0]
        return tmp
    except Exception as e:
        if task_id is not None:
            message={'status':-100,'col':col_name,'index':index}
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
            return tmp
        else:
            print("不支持的数据类型！task_id is none")
            return tmp
        # raise ValueError(f"Error in {str(col_name)}: {str(e)}")



def mask_data(mask_pool, data, col, rule_id, user_id, task_id=None):
    '''
    data是dataframe格式数据，col是要小脱敏的列，rule_id是脱敏规则
    '''
    x = int(rule_id[0])
    gene_params = []
    if x == 1:
        params = get_generalization_rule(mask_pool, rule_id)
        gene_params = [params[0]]
    elif x == 2:
        params = get_generalization_rule(mask_pool, rule_id)
        gene_params = [params[0], params[1], params[2]]
    elif x == 3:
        params = get_generalization_rule(mask_pool, rule_id)
        delete_aggregation_params(mask_pool, rule_id, user_id)
        gene_params = [params[0]]
    elif x == 4:
        params = get_generalization_rule(mask_pool, rule_id)
        gene_params = [params[0]]
    elif x == 5:
        params = get_generalization_rule(mask_pool, rule_id)
        gene_params = [params[0], params[1]]
    elif x == 6:
        params = get_generalization_rule(mask_pool, rule_id)
        gene_params = [params[0]]
    else:
        print('匿名规则id不存在！')
        if task_id is not None:
            message={'status':-117,'rule_id':rule_id}
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
        else:
            print("不支持的数据类型！task_id is none")
        return data
    alerted_data = anonymity(data, col, x, gene_params,task_id)
    return alerted_data


def get_df_info(resource_url, full_file_path, task_id, rule_map_list, file_extension):
    df_list = []
    sheet_list = []
    try:
        for item in rule_map_list:
            sheet_name = item.get('name')
            rule_map = item.get('rule_map')
            dict_tmp = {}
            for col, rule in rule_map.items():
                # col_rule = {'1': str, '2': str, '3': float, '4': float, '5': str, '6': str}
                if rule[0] in col_rule.keys():
                    dict_tmp[col] = col_rule[rule[0]]
            # 判断文件类型
            if file_extension == '.xlsx' or file_extension == '.xls':
                df = pd.read_excel(full_file_path, sheet_name=sheet_name, converters=dict_tmp)
                print(df.head())  # 打印 DataFrame 的前几行，以确认数据读取正确
            elif file_extension == '.csv':
                df = pd.read_csv(full_file_path, converters=dict_tmp)
                print(df.head())  # 打印 DataFrame 的前几行，以确认数据读取正确
            else:
                print("未知文件类型或不支持的文件类型")
                print("masking_api")
                message={'status':-118,'extension':file_extension}
                info = {'message': message, 'task_id': task_id,
                        'status': -1}
                append_re_api(info)
                return None, None
            sheet_list.append(sheet_name)
            df_list.append(df)
    except FileNotFoundError:
        print(f"文件 {resource_url} 未找到")
        message={'status':-111,'file_url':resource_url}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        return None, None
    except Exception as e:
        print(f"读取文件时发生错误: {str(e)}")
        message={'status':-112,'file_url':resource_url}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        return None, None
    return df_list, sheet_list


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


def save_file_to_local(df_list, sheet_list, base_name, file_extension, file_path,task_id):
    save_file_name = base_name + "_copy" + file_extension
    save_path = down_path + '/' + save_file_name
    if file_extension == '.xlsx' or file_extension == '.xls':
        all_sheets = get_all_df(file_path)
        for df, sheet_name in zip(df_list, sheet_list):
            if sheet_name in all_sheets.keys():
                all_sheets[sheet_name] = df
            else:
                print("sheet_name读取完整文件时获取失败！")
                message={'status':-119,'file_url':file_path,'sheet_name':sheet_name}
                info={'message': message, 'task_id': task_id, 'status': -1}
                append_re_api(info)
                continue
                # return None, "sheet_name读取完整文件时获取失败！"
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
            print("masking_api")
            message = {'status': -118, 'extension': file_extension}
            info = {'message': message, 'task_id': task_id,
                    'status': -1}
            append_re_api(info)
            return None, None
    except Exception as e:
        return None, str(e)
    return save_path, None


# def save_file_to_local(df_list, sheet_list, base_name, file_extension):
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
#     return save_path,None


def alert_df_to_data(mask_pool, df_list, sheet_list, rule_map_list, user_id,task_id=None):
    alerted_data_list = []
    rule_map_dict = {item['name']: item['rule_map'] for item in rule_map_list}
    for df, sheet_name in zip(df_list, sheet_list):
        if sheet_name in rule_map_dict.keys():
            rule_map = rule_map_dict.get(sheet_name, None)  # rule_map应该一定存在
            alerted_data = df.copy()
            for col, rule in rule_map.items():
                alerted_data = mask_data(mask_pool, alerted_data, col, rule, user_id, task_id)
            alerted_data_list.append(alerted_data)
    return alerted_data_list


def alter_rule_map_list(rule_map_list):
    sheet_name = rule_map_list[0].get('name')
    rule_map = rule_map_list[0].get('rule_map')
    if not sheet_name:
        sheet_name = "default"
        rule_map_list = [{
            'name': sheet_name,
            'rule_map': rule_map
        }]
    return rule_map_list


# resource_url是文件原下载路径。full_file_path是下载后存在本地的路径，文件名带时间戳。返回脱敏后数据存储路径。
def process_file_with_rules(resource_url, rule_map_list, mask_pool, task_id, user_id, local_file_path):
    file_name = resource_url.split('/')[-1]
    base_name, file_extension = os.path.splitext(file_name)
    df_list, sheet_list = get_df_info(resource_url, local_file_path, task_id, rule_map_list,
                                      file_extension)
    if df_list is None:
        return None
    try:
        alerted_data_list = alert_df_to_data(mask_pool, df_list, sheet_list, rule_map_list, user_id,task_id)
    except Exception as e:
        print('匿名化处理出错: ' + str(e))
        # info = {'message': '匿名化处理出错: ' + str(e), 'task_id': task_id, 'status': -1}
        # append_re_api(info)
        return None
    local_save_path, e = save_file_to_local(alerted_data_list, sheet_list, base_name, file_extension, local_file_path,task_id)
    if local_save_path is None:
        print(e)
        message={'status':-113,'file_url':resource_url}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        return None
    return local_save_path


def unit(class_pool, mask_pool, task_id: str, class_run_task_id: str, resource: dict,
         tpl_id: str, user_id: str, rule_map_list: list, download_url: str, authorization: str, store_flag: int):
    '''
    class_pool, mask_pool, task_id都有
    第一种输入：class_run_task_id，tpl_id,user_id，resource可有可无
    第二种输入：resource,rule_map_list,store_flag,user_id
    第三种输入：download_url,user_id
    '''
    start_time = time.perf_counter()
    if tpl_id and user_id:
        # 查询 敏感规则到脱敏规则的映射，返回字典。
        dict_rule = get_ruleid_params(mask_pool, tpl_id, user_id)
    if class_run_task_id is not None:
        if resource:
            resource_ids = []
            resource_urls = []
            for resource_id, resource_url_list in resource.items():
                for resource_url in resource_url_list:
                    resource_ids.append(resource_id)
                    resource_urls.append(resource_url)
            sen_data = get_sensitivedata_by_resource_batch(class_pool, resource_ids, resource_urls, class_run_task_id)
        else:
            sen_data = get_sensitivedata_by_taskid(class_pool, class_run_task_id)
    # 用户根据    服务器（主机）-->数据源-->文件/数据表    确定最终脱敏文件。
    elif resource:
        rule_map_list = alter_rule_map_list(rule_map_list)
        for resource_id, resource_ulr_list in resource.items():
            for resource_url in resource_ulr_list:
                (access_protocol, auth_ip, auth_port, auth_username, auth_pwd) = get_db_params(class_pool,
                                                                                               resource_id)
                if access_protocol == 'MySQL':
                    l = resource_url.split(',')
                    database = l[0]
                    table = l[1]
                    auth_username_encoded = urllib.parse.quote_plus(auth_username)
                    auth_pwd_encoded = urllib.parse.quote_plus(auth_pwd)
                    engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8'
                                           % (auth_username_encoded, auth_pwd_encoded, auth_ip, auth_port, database))
                    sql = "SELECT * FROM " + str(table)
                    df = pd.read_sql(sql, engine)
                    # try:
                    #     connection = pymysql.connect(
                    #         host=auth_ip,
                    #         user=auth_username,
                    #         password=auth_pwd,
                    #         database=database,
                    #         port=auth_port
                    #     )
                    #     # 返回dataframe格式数据
                    #     # data_form=mysql_api(ds_type,auth_ip,auth_port,auth_username,auth_pwd,ds_url)
                    #     df = pd.read_sql(sql, connection)
                    # except pymysql.MySQLError as e:
                    #     raise ValueError(f"Error connecting to MySQL database: {e}")
                    # except pd.DatabaseError as e:
                    #     raise ValueError(f"Error reading data from MySQL database: {e}")
                    # finally:
                    #     # Closing the database connection in the final block
                    #     if connection:
                    #         connection.close()
                    alerted_data = df.copy()
                    rule_map = rule_map_list[0].get("rule_map")  # 目前单文件直接读第一个rule_map即可
                    str_change_columns = []
                    float_change_columns = []
                    for col, rule in rule_map.items():
                        try:
                            alerted_data = mask_data(mask_pool, alerted_data, col, rule, user_id,task_id)
                            if rule[0] in ['4', '6', '2']:
                                str_change_columns.append(col)
                            elif rule[0] in ['3']:
                                float_change_columns.append(col)
                        except Exception as e:
                            info = {'message': '匿名化处理出错: ' + str(e), 'task_id': task_id, 'status': -1}
                            append_re_api(info)
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
                    alerted_data.to_sql(new_table, engine, if_exists='append', index=False)
                    if store_flag == 1:
                        with engine.connect() as conn:
                            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                        with engine.connect() as conn:
                            conn.execute(text(f"ALTER TABLE {new_table} RENAME TO {table}"))
                    print('mysql测试成功！')
                    message = {'result':'ok'}
                    info = {'message': message, 'task_id': task_id, 'status': 2}
                    append_re_api(info)
                    return
                elif access_protocol == 'FTP':
                    state, ftp = check_ftp(ip=auth_ip, port=auth_port, username=auth_username, pwd=auth_pwd)
                    if state == 1:
                        print("ftp连接失败！")
                        message={'status':-101,'ip':auth_ip,'port':auth_port}
                        info = {'message': message, 'task_id': task_id, 'status': -1}
                        append_re_api(info)
                        return
                    else:
                        print("ftp连接成功")
                        state2, full_file_path = ftp_download_file(ftp, remote_path=resource_url, local_path=down_path)
                        if not state2:
                            message = {'status': -102, 'ip': auth_ip, 'port': auth_port,'file_url':resource_url}
                            info = {'message': message, 'task_id': task_id, 'status': -1}
                            append_re_api(info)
                            return
                        # resource_url是文件原下载路径。full_file_path是下载后存在本地的路径，文件名带时间戳。返回脱敏后数据存储路径。
                        local_save_path = process_file_with_rules(resource_url, rule_map_list, mask_pool, task_id,
                                                                  user_id, full_file_path)
                        if local_save_path is None:
                            return  # 错误情况已经在process_file_with_rules中调用回调接口了，此处直接返回即可

                        os.remove(full_file_path)
                        storage_conf = {"auth_ip": auth_ip, "auth_port": auth_port, "auth_username": auth_username,
                                        "auth_pwd": auth_pwd, "resource_url": resource_url}
                        upload_success = ftp_upload_file(local_save_path, store_flag, storage_conf)
                        if upload_success:
                            print('ftp测试完成,文件上传成功')
                            message = {'result': 'ok'}
                            info = {'message': message, 'task_id': task_id, 'status': 2}
                            append_re_api(info)
                        else:
                            print('脱敏完成，文件上传失败')
                            message={'status':-120,'file_url':resource_url}
                            info = {'message': message, 'task_id': task_id, 'status': -1}
                            append_re_api(info)
                        return
                        # state3 = ftp_upload_file(ftp, local_path=save_path, remote_path=remote_file_path)
                        # if state3:
                        #     print("文件上传成功")
                        # else:
                        #     print("文件上传失败")
                        #     data = {'message': '文件上传失败！', 'task_id': task_id,
                        #             'status': -1}
                        #     re_api(data)
                        #     return None
                        # ftp.close()
                        # os.remove(save_path)

                elif access_protocol == 'SFTP':
                    state, (sftp, transport) = check_sftp(ip=auth_ip, port=auth_port, username=auth_username,
                                                          pwd=auth_pwd)
                    if state == 1:
                        print("sftp连接失败！")
                        message = {'status': -101, 'ip': auth_ip, 'port': auth_port}
                        info = {'message': message, 'task_id': task_id, 'status': -1}
                        append_re_api(info)
                        return None
                    else:
                        print("sftp连接成功")
                        state2, full_file_path = sftp_download_file(sftp, remote_path=resource_url,
                                                                    local_path=down_path)
                        if not state2:
                            message = {'status': -102, 'ip': auth_ip, 'port': auth_port, 'file_url': resource_url}
                            info = {'message': message, 'task_id': task_id, 'status': -1}
                            append_re_api(info)
                            return None
                        local_save_path = process_file_with_rules(resource_url, rule_map_list, mask_pool, task_id,
                                                                  user_id, full_file_path)
                        if local_save_path is None:
                            return  # 错误情况已经在process_file_with_rules中调用回调接口了，此处直接返回即可

                        os.remove(full_file_path)
                        upload_success = sftp_upload_file(sftp, local_url=local_save_path, remote_url=resource_url,
                                                          storage_flag=store_flag)
                        if upload_success:
                            print('sftp测试完成,文件上传成功')
                            message = {'result': 'ok'}
                            info = {'message': message, 'task_id': task_id, 'status': 2}
                            append_re_api(info)
                        else:
                            print('脱敏完成，文件上传失败')
                            message={'status':-120,'file_url':resource_url}
                            info = {'message': message, 'task_id': task_id, 'status': -1}
                            append_re_api(info)
                        sftp.close()
                        transport.close()
                        return
                elif access_protocol == 'Local':
                    print("Local 测试")
                    local_save_path = process_file_with_rules(resource_url, rule_map_list, mask_pool, task_id, user_id,
                                                              resource_url)
                    if local_save_path is None:
                        return  # 错误情况已经在process_file_with_rules中调用回调接口了，此处直接返回即可

                    if store_flag == 1:
                        os.remove(resource_url)
                        save_path = resource_url
                    else:
                        file_path = os.path.dirname(resource_url)
                        save_file_name = os.path.basename(local_save_path)
                        save_path = file_path + '/' + save_file_name
                    os.rename(local_save_path, save_path)  # 移动文件并重命名
                    # os.remove(local_save_path)

                # raw_data = (pd.DataFrame(df)).to_json(orient='records')
                # json_data = gene_result.to_json(orient='records')
                # insert_maskData(mask_pool, task_id, raw_data, json_data)
                # update_data_volume(mask_pool, len(gene_result), task_id)
                message = {'result':'ok'}
                info = {'message': message, 'task_id': task_id, 'status': 2}
                append_re_api(info)
                return
    elif download_url:
        rule_map_list = alter_rule_map_list(rule_map_list)
        downloaded_file = get_download_file(mask_pool, task_id, user_id)
        # if not downloaded_file:
        #     data = {'message': '下载到本地的文件缺失', 'task_id': task_id, 'status': -1}
        #     append_re_api(data)
        #     return None

        local_save_path = process_file_with_rules(download_url, rule_map_list, mask_pool, task_id, user_id,
                                                  downloaded_file)
        if local_save_path is None:
            return  # 错误情况已经在process_file_with_rules中调用回调接口了，此处直接返回即可
        # 成功处理并得到处理后的文件local_save_path
        os.remove(downloaded_file)
        upload_success, url = upload_to_server(authorization, local_save_path)
        if not upload_success:
            message={'status':-116,'download_url':download_url}
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
            return None

        os.remove(local_save_path)
        # 成功上传，删除文件
        message = {'result': 'ok'}
        info = {'message': message, 'task_id': task_id, 'status': 2, 'resource': {"url": url}}
        append_re_api(info)
        return

    else:
        print("参数传递错误！")
        print("masking_api")
        message={'result':'false'}
        info = {'message': message, 'task_id': task_id, 'status': -1}
        append_re_api(info)
        return

    # sen_data:(分类分级id,敏感规则rule_id,需要脱敏的数据,resource_id,resource_url,resource_type)格式的列表。
    # 脱敏结果格式{"data_scan_result_id": [1,2,3,4] , "masking_data": [m_data1, m_data2, m_data3, m_data4]}
    data_scan_result_id_list = []
    masking_data = []
    raw_data = []
    for (class_id, rule_id, datad, resource_id, resource_url, resource_type) in sen_data:
        try:
            raw_data.append(datad)
            if rule_id in dict_rule.keys():
                mask_rule_id = dict_rule[rule_id]
                x = int(mask_rule_id[0])
                if x == 1:
                    params = get_generalization_rule(mask_pool, mask_rule_id)
                    new_data = sampling([datad], params[0], task_id)
                    data_scan_result_id_list.append(class_id)
                    if len(new_data) == 0:
                        new_data = ['']
                    masking_data.append(new_data[0])
                elif x == 2:
                    params = get_generalization_rule(mask_pool, mask_rule_id)
                    new_data = masking([datad], params[0], params[1], params[2], task_id)
                    data_scan_result_id_list.append(class_id)
                    masking_data.append(new_data[0])
                elif x == 3:
                    # params = get_generalization_rule(mask_pool, mask_rule_id)
                    new_data = aggregation([datad], None, None, task_id)
                    data_scan_result_id_list.append(class_id)
                    masking_data.append(new_data[0])
                elif x == 4:
                    params = get_generalization_rule(mask_pool, mask_rule_id)
                    new_data = categorization([datad], params[0], task_id)
                    data_scan_result_id_list.append(class_id)
                    masking_data.append(new_data[0])
                elif x == 5:
                    params = get_generalization_rule(mask_pool, mask_rule_id)
                    new_data = generalization([datad], params[0], params[1], task_id)
                    data_scan_result_id_list.append(class_id)
                    masking_data.append(new_data[0])
                elif x == 6:
                    params = get_generalization_rule(mask_pool, mask_rule_id)
                    new_data = suppression([datad], params[0], task_id)
                    data_scan_result_id_list.append(class_id)
                    masking_data.append(new_data[0])
                else:
                    print('匿名规则id不存在！')
                    message = {'status': -117, 'rule_id': rule_id}
                    info = {'message': message, 'task_id': task_id, 'status': -1}
                    append_re_api(info)
                    continue
            else:
                print('敏感规则rule_id不存在！' + rule_id)
                message = {'status': -117, 'rule_id': rule_id}
                info = {'message': message, 'task_id': task_id, 'status': -1}
                append_re_api(info)
                continue
        except Exception as e:
            print(f"脱敏处理出错: {str(e)}")
            message={'status':-121,'data':datad,'rule_id': mask_rule_id}
            info = {'message': message, 'task_id': task_id, 'status': -1}
            append_re_api(info)
            return None
    ret = {'data_scan_result_id': data_scan_result_id_list, 'masking_data': masking_data}
    # json_mask_data = json.dumps(masking_data)
    # insert_maskData(mask_pool, task_id, data_scan_result_id_list, json_mask_data)
    insert_masking_id_data_batch(mask_pool, task_id, data_scan_result_id_list, masking_data)
    update_data_volume(mask_pool, len(masking_data), task_id)
    # 初始化类
    # 调用类的函数
    # 失败时 resource_id，resource_url应该重写
    # connection不止一个？storage_conf
    # 错误时返回resource_id,resource_url
    # overwrite_instance = Overwrite(connection, data_scan_result_id_list, masking_data, storage_conf)
    # # Call the start method of Overwrite instance
    # overwrite_instance.start()
    print("文件脱敏运行成功")
    end_time = time.perf_counter()
    execution_time = end_time - start_time
    print("代码执行时间", execution_time, "秒")
    message = {'result': 'ok'}
    info = {'message': message, 'task_id': task_id, 'status': 2}
    # data['resource_id']=resource_id
    # data['resource_url']=resource_url
    append_re_api(info)
    return ret

# if __name__ == "__main__":
#     authorization = 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOjEsIm5pY2tOYW1lIjoi566h55CG5ZGYIiwibGFuZ0NvZGUiOiJ6aENOIiwidXNlck5hbWUiOiJkZXZlbG9wZXIiLCJ1c2VyVHlwZSI6IjAxIiwidGVuYW50SWQiOiIxIiwiaWF0IjoxNzIyMjM4Mjg4LCJleHAiOjE3MjIzMjQ2ODh9.AbUfN-p83qaskVoAo3wuunQOhjUWxOhltkrczw2bOgg'
#     local_save_path = '/home/hit/DataMasking/test2/data/m_sheets_copy.xlsx'
#     upload_success = upload_to_server(authorization, local_save_path)
#     if not upload_success:
#         print({'message': '上传文件失败', 'status': -1})

# data=[["张三一二",18,46,"ewdfr sfedsa ewfa"],["李四",23,45,"dfs edf edvf effs"],["王五", 20, 50,"efr efg e d f f"],["赵六", 22, 48,"df efg efr "],["钱七", 25, 47,""],["孙八", 19, 49,""],["周九", 21, 44,""],["吴十", 24, 42,""],["郑十一", 27, 43,""],["王十二", 26, 51,""],["冯十三", 28, 52,""],["陈十四", 30, 53,""],["楚十五", 29, 41,""],["魏十六", 32, 54,""],["蔡十七", 31, 55,""]]
# sampling(data,50)
# aggregation(data,2,3)
# masking(data,0,1,2)
# categorization_digit(data,1,20)
# generalization(data,3,1,2)
# suppression(data,5)
