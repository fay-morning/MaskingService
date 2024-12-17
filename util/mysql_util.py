import datetime
import json
import random
import time
from typing import Tuple, Optional
import mysql.connector.pooling
import pytz as pytz
from loguru import logger
from const import const_task
from service.callback_api import append_re_api
from util.server_util import get_config
import pymysql
from itertools import chain
from const import constants

length = 8


def get_mysql_connector_pool() -> mysql.connector.pooling.MySQLConnectionPool:
    server_config = get_config()
    db_config = {
        'host': server_config.get('Mysql_Masking', 'host'),
        'user': server_config.get('Mysql_Masking', 'user'),
        'password': server_config.get('Mysql_Masking', 'password'),
        'database': server_config.get('Mysql_Masking', 'database'),
        'port': server_config.get('Mysql_Masking', 'port'),
    }
    connection_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="main_pool", pool_size=32,
                                                                  **db_config)
    return connection_pool


def safe_get_connection(connection_pool: mysql.connector.pooling.MySQLConnectionPool):
    try:
        connection = connection_pool.get_connection()
        return connection
    except Exception as e:
        # logger.info("获取数据库连接出错，自动添加连接") 
        connection_pool.add_connection()
        connection = connection_pool.get_connection()
        return connection


def safe_close_connection(connection: mysql.connector.pooling.PooledMySQLConnection):
    try:
        connection.close()
    except Exception as e:
        logger.info("返还数据库连接出错，已自动抛弃1")
        return


def get_now_str():
    tz = pytz.timezone('Asia/Shanghai')  # 东八区
    t = datetime.datetime.fromtimestamp(int(time.time()), tz).strftime('%Y-%m-%d %H:%M:%S')
    return t


def insert_into_hit_log(connection_pool: mysql.connector.pooling.MySQLConnectionPool,
                        values: Tuple[str, str, str, str, int, int]):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    insert_query = "INSERT INTO `hit_log` (ip, filePath, line, hitText, hitNum, task_id) VALUES (%s, %s, %s, %s, %s, %s)"
    try:
        cursor.execute(insert_query, values)
        connection.commit()
    except Exception as e:
        connection.rollback()
        logger.error("Error inserting data: {}".format(e))
    finally:
        cursor.close()
        safe_close_connection(connection)


def check_rule_id(connection_pool, rule_id: str) -> (Optional[bool], str):
    connection = safe_get_connection(connection_pool)
    if not rule_id or len(rule_id) == 0:
        return False, None  # 如果 rule_id 为空，则返回 False
    index = int(rule_id.split('_')[0])
    if not (1 <= index <= 6):
        return False, None  # 如果第一个字段不是 1 到 6，则返回 False
    table_map = {
        1: 'rule_sampling',
        2: 'rule_masking',
        3: 'rule_aggregation',
        4: 'rule_categorization',
        5: 'rule_generalization',
        6: 'rule_suppression'
    }
    table_name = table_map[index]

    cursor = connection.cursor()
    query = f"SELECT COUNT(1) From {table_name} WHERE rule_id = %s"
    try:
        cursor.execute(query, (rule_id,))
        result = cursor.fetchone()
        cursor.close()
        return result[0] > 0, table_name
    except Exception as e:
        print(f"Error: {e}")
        return None, table_name
    finally:
        safe_close_connection(connection)


def get_rule_by_rule_id(connection_pool, rule_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    is_exist, table = check_rule_id(connection_pool, rule_id)
    if not is_exist:
        return None
    query = f"SELECT * FROM {table} WHERE rule_id = '{rule_id}'"

    try:
        cursor.execute(query, ())
        result = cursor.fetchone()
        cursor.close()
        return result
    except Exception as e:
        connection.rollback()
        print("Error:", e)
        return None
    finally:
        safe_close_connection(connection)


def get_aggregation_params(connection_pool: mysql.connector.pooling.MySQLConnectionPool, rule_id):
    '''
    返回聚合方法的参数列表：[[groupby]]
    '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    # query1 = "select group from rule_aggregation_group where rule_id=%s"
    query2 = "select groupby from rule_aggregation_groupby where rule_id=%s"
    try:
        # cursor.execute(query1, (rule_id,))
        # group_tmp = cursor.fetchall()
        # group = list(chain.from_iterable(group_tmp))
        cursor.execute(query2, (rule_id,))
        groupby_tmp = cursor.fetchall()
        groupby = list(chain.from_iterable(groupby_tmp))
        cursor.close()
    except Exception as e:
        connection.rollback()
    finally:
        safe_close_connection(connection)
    result = []
    # result.append(group)
    result.append(groupby)
    return result


def get_generalization_rule(connection_pool: mysql.connector.pooling.MySQLConnectionPool, rule_id):
    '''
    返回泛化参数的列表
    '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    x = int(rule_id[0])
    if x != 3:  # != "aggregation"
        try:
            result = get_rule_by_rule_id(connection_pool, rule_id)
            cursor.close()
        except Exception as e:
            print("An exception occurred:", e)
            connection.rollback()
            result = []
        finally:
            safe_close_connection(connection)
        return result[2:]
    else:
        return get_aggregation_params(connection_pool, rule_id)


def get_KA_gene_all_params(connection_pool: mysql.connector.pooling.MySQLConnectionPool, identifier_rule_id, limit):
    '''
    返回结果:[标识列泛化所有泛化方法，泛化方法参数]格式的列表。
    '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "select dm_rule_id from task_k_tpl_rule_map where ka_tpl_id = %s and layer_index <= %s ORDER BY layer_index"
    try:
        cursor.execute(query, (identifier_rule_id, limit))
        v = cursor.fetchall()
        cursor.close()
        return v
    except Exception as e:
        print("An exception occurred:", e)
        connection.rollback()
        return None
    finally:
        safe_close_connection(connection)


def get_KA_gene_params(connection_pool: mysql.connector.pooling.MySQLConnectionPool, identifier_rule_id, layer_index):
    '''
    返回结果:[标识列泛化第height层泛化方法，泛化方法参数]格式的列表。
    '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "select dm_rule_id from task_k_tpl_rule_map where ka_tpl_id = %s and layer_index = %s"
    try:
        print(identifier_rule_id, layer_index)
        cursor.execute(query, (identifier_rule_id, layer_index))
        v = cursor.fetchone()
        result = [v[0]]
        params = get_generalization_rule(connection_pool, v[0])
        for p in params:
            result.append(p)
        cursor.close()
        return result
    except Exception as e:
        print("An exception occurred:", e)
        connection.rollback()
    finally:
        safe_close_connection(connection)


def get_KA_dsid(connection_pool: mysql.connector.pooling.MySQLConnectionPool, task_id):
    '''
    返回需要处理的数据id
    '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "select ds_id,ds_path from task_k where task_id=%s"
    try:
        cursor.execute(query, (task_id,))
        v = cursor.fetchone()
        print(v)
        cursor.close()
    except Exception as e:
        print("An exception occurred:", e)
        connection.rollback()
    finally:
        safe_close_connection(connection)
    return v


def get_KA_identifier(connection_pool: mysql.connector.pooling.MySQLConnectionPool, task_id):
    '''
    返回结果:字典
    {
        sheet_name1:[(identifier,ka_tpl_id)],
        sheet_name2:[(identifier,ka_tpl_id)]
    }
    '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "select identifier,ka_tpl_id,sheet_name,k from task_k_params_identifier where task_id=%s"
    try:
        cursor.execute(query, (task_id,))
        v1 = cursor.fetchall()
        if v1:
            result_dict = {}
            sheet_name_list = []
            k_list = []
            for identifier, ka_tpl_id, sheet_name, k in v1:
                if sheet_name not in result_dict:
                    result_dict[sheet_name] = []
                if sheet_name not in sheet_name_list:
                    sheet_name_list.append(sheet_name)
                    k_list.append(k)
                result_dict[sheet_name].append((identifier, ka_tpl_id))
            cursor.close()
            return result_dict, sheet_name_list, k_list
        else:
            cursor.close()
            info = {}
            append_re_api(info)
            raise Exception("No identifier info found for the task.")
    except Exception as e:
        connection.rollback()
        logger.warning(e)
        return None, []
    finally:
        safe_close_connection(connection)


def get_KA_sensitive(connection_pool: mysql.connector.pooling.MySQLConnectionPool, task_id):
    '''
        返回结果:字典
        {
            sheet_name1:[sensitive],
            sheet_name2:[sensitive]
        }
        '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "select sheet_name,`sensitive` from task_k_sensitive where task_id=%s"
    try:
        cursor.execute(query, (task_id,))
        v1 = cursor.fetchall()
        result_dict = {}
        if v1:
            for sheet_name, sensitive in v1:
                if sheet_name not in result_dict:
                    result_dict[sheet_name] = []
                result_dict[sheet_name].append(sensitive)
            cursor.close()
            return result_dict
        else:
            cursor.close()
            info = {}
            append_re_api(info)
            return result_dict
            # raise Exception("No identifier info found for the task.")
    except Exception as e:
        connection.rollback()
        logger.warning(e)
        return {}
    finally:
        safe_close_connection(connection)


def get_KA_info(connection_pool: mysql.connector.pooling.MySQLConnectionPool, task_id, user_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "SELECT ds_id,ds_path,store_flag,`authorization` FROM task_k WHERE user_id = %s and task_id = %s"
    try:
        cursor.execute(query, (user_id, task_id))
        result = cursor.fetchone()
        if result:
            cursor.close()
            return result
        else:
            cursor.close()
            return False, None, None, None, None
    except Exception as e:
        print("get_KA_k error:", e)
        connection.rollback()
        return None, None, None, None, None
    finally:
        safe_close_connection(connection)


def alert_seq(connection_pool: mysql.connector.pooling.MySQLConnectionPool, index):
    # index: 1-samping,2-masking,3-aggregation,4-categorization,5-generalization,6-suppression,7-common-task,8-KA,9-tpl
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    v_query = "select v from seq_id WHERE id=%s"
    seq_query = "UPDATE seq_id SET v=%s WHERE id=%s"
    try:
        cursor.execute(v_query, (index,))
        v = cursor.fetchone()
        new_value = v[0] + 1
        cursor.execute(seq_query, (new_value, index))
        connection.commit()
        print("test", v, new_value)
        cursor.close()
        return new_value
    except Exception as e:
        print("Error:", e)
        connection.rollback()
        return None
    finally:
        safe_close_connection(connection)


def insert_sampling(connection_pool: mysql.connector.pooling.MySQLConnectionPool, retain_percent, rule_name, user_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    try:
        v = alert_seq(connection_pool, constants.SAMPLING_RULE_FLAG)
        if v is None:
            raise Exception
        time_dt = datetime.datetime.now()
        time_str = time_dt.strftime('%Y%m%d')
        rule_id = '1_sampling' + time_str + str(v).zfill(length)
        update_seq_query = "UPDATE seq_id SET update_at=%s WHERE tag='sampling'"
        insert_query = "INSERT INTO rule_sampling (rule_id, retain_percent, rule_name, user_id) VALUES (%s, %s, %s, %s)"
        cursor.execute(update_seq_query, (time_str,))
        cursor.execute(insert_query, (rule_id, retain_percent, rule_name, user_id))
        connection.commit()
        cursor.close()
        return rule_id  # 返回生成的 rule_id
    except Exception as e:
        connection.rollback()
        print(f"Error: {e}")
        return None  # 返回 None 表示插入失败
    finally:
        safe_close_connection(connection)


def insert_masking(connection_pool: mysql.connector.pooling.MySQLConnectionPool, start, end, symbol, rule_name,
                   user_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    try:
        v = alert_seq(connection_pool, constants.MASKING_RULE_FLAG)
        if v is None:
            raise Exception
        time_dt = datetime.datetime.now()
        time_str = time_dt.strftime('%Y%m%d')
        rule_id = '2_masking' + time_str + str(v).zfill(length)
        insert_query = ("INSERT INTO rule_masking (rule_id,  start, end, symbol, rule_name, user_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s)")
        cursor.execute(insert_query, (rule_id, start, end, symbol, rule_name, user_id))
        connection.commit()
        cursor.close()
        return rule_id
    except Exception as e:
        connection.rollback()
        return None
    finally:
        safe_close_connection(connection)


def insert_aggregation(connection_pool: mysql.connector.pooling.MySQLConnectionPool, group, group_by, user_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    try:
        v = alert_seq(connection_pool, constants.AGGREGATION_RULE_FLAG)
        if v is None:
            raise Exception
        time_dt = datetime.datetime.now()
        time_str = time_dt.strftime('%Y%m%d')
        rule_id = '3_aggregation' + time_str + str(v).zfill(length)
        query = "INSERT INTO rule_aggregation (rule_id, user_id) VALUES (%s, %s)"
        query_group = "INSERT INTO rule_aggregation_group (rule_id, `group`) VALUES (%s, %s)"
        query_group_by = "INSERT INTO rule_aggregation_groupby (rule_id, `groupby`) VALUES (%s, %s)"
        cursor.execute(query, (rule_id, user_id))
        # group为字符串（列名）或空字符串（未传）
        if group:
            cursor.execute(query_group, (rule_id, group))
        for item in group_by:
            cursor.execute(query_group_by, (rule_id, item))
        connection.commit()
        cursor.close()
        return rule_id
    except Exception as e:
        print("Error:", e)
        connection.rollback()
        return None
    finally:
        safe_close_connection(connection)


def insert_categorization(connection_pool: mysql.connector.pooling.MySQLConnectionPool, double, rule_name, user_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    try:
        v = alert_seq(connection_pool, constants.CATEGORIZATION_RULE_FLAG)
        if v is None:
            raise Exception
        time_dt = datetime.datetime.now()
        time_str = time_dt.strftime('%Y%m%d')
        rule_id = '4_categorization' + time_str + str(v).zfill(length)
        insert_query = "INSERT INTO rule_categorization (rule_id, `double`, rule_name, user_id) VALUES (%s, %s, %s, %s)"
        cursor.execute(insert_query, (rule_id, double, rule_name, user_id))
        connection.commit()
        cursor.close()
        return rule_id
    except Exception as e:
        print("Error:", e)
        connection.rollback()
        return None
    finally:
        safe_close_connection(connection)


def insert_generalization(connection_pool: mysql.connector.pooling.MySQLConnectionPool, start, end, rule_name, user_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    try:
        v = alert_seq(connection_pool, constants.GENERALIZATION_RULE_FLAG)
        if v is None:
            raise Exception
        time_dt = datetime.datetime.now()
        time_str = time_dt.strftime('%Y%m%d')
        rule_id = '5_generalization' + time_str + str(v).zfill(length)
        insert_query = ("INSERT INTO rule_generalization (rule_id, start, end, rule_name, user_id) "
                        "VALUES (%s, %s, %s, %s, %s)")
        cursor.execute(insert_query, (rule_id, start, end, rule_name, user_id))
        connection.commit()
        cursor.close()
        return rule_id
    except Exception as e:
        connection.rollback()
        return None
    finally:
        safe_close_connection(connection)


def insert_suppression(connection_pool: mysql.connector.pooling.MySQLConnectionPool, threshold_percent, rule_name,
                       user_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    try:
        v = alert_seq(connection_pool, constants.SUPPRESSION_RULE_FLAG)
        if v is None:
            raise Exception
        time_dt = datetime.datetime.now()
        time_str = time_dt.strftime('%Y%m%d')
        rule_id = '6_suppression' + time_str + str(v).zfill(length)
        insert_query = ("INSERT INTO rule_suppression (rule_id, threshold_percent, rule_name, user_id) "
                        "VALUES (%s, %s, %s, %s)")
        cursor.execute(insert_query, (rule_id, threshold_percent, rule_name, user_id))
        connection.commit()
        cursor.close()
        return rule_id
    except Exception as e:
        connection.rollback()
        print(e)
        return None
    finally:
        safe_close_connection(connection)


# def alert_seq(connection_pool: mysql.connector.pooling.MySQLConnectionPool, generalization_type):
#     connection = safe_get_connection(connection_pool)
#     cursor = connection.cursor()
#     v_query = "select v from seq_id WHERE tag=%s"
#     seq_query = "UPDATE seq_id SET v=%s WHERE tag=%s"
#     try:
#         cursor.execute(v_query, (generalization_type,))
#         v = cursor.fetchone()
#         new_value = v[0] + 1
#         cursor.execute(seq_query, (new_value, generalization_type))
#         connection.commit()
#     except Exception as e:
#         print("Error:", e)
#         connection.rollback()
#     finally:
#         cursor.close()
#         safe_close_connection(connection)
#     return new_value
#
#
# def insert_sampling(connection_pool: mysql.connector.pooling.MySQLConnectionPool, retain_percent):
#     connection = safe_get_connection(connection_pool)
#     cursor = connection.cursor()
#     v = alert_seq(connection_pool, "sampling")
#     time = datetime.datetime.now().strftime('%Y%m%d')
#     rule_id = '1_sampling' + time + str(v).zfill(length)
#     time = datetime.datetime.now().strftime('%Y%m%d')
#     update_seq_query = "UPDATE seq_id SET update_at=%s WHERE tag='sampling'"
#     insert_query = "INSERT INTO rule_sampling (rule_id, retain_percent) VALUES (%s, %s)"
#     try:
#         cursor.execute(update_seq_query, (time,))
#         cursor.execute(insert_query, (rule_id, retain_percent))
#         connection.commit()
#         return rule_id  # 返回生成的 rule_id
#     except Exception as e:
#         connection.rollback()
#         print(f"Error: {e}")
#         return None  # 返回 None 表示插入失败
#     finally:
#         cursor.close()
#         safe_close_connection(connection)
#
#
# def insert_masking(connection_pool: mysql.connector.pooling.MySQLConnectionPool, start, end, symbol):
#     connection = safe_get_connection(connection_pool)
#     cursor = connection.cursor()
#     v = alert_seq(connection_pool, "masking")
#     time = datetime.datetime.now().strftime('%Y%m%d')
#     rule_id = '2_masking' + time + str(v).zfill(length)
#     insert_query = "INSERT INTO rule_masking (rule_id,  start, end, symbol) VALUES (%s, %s, %s, %s)"
#     try:
#         cursor.execute(insert_query, (rule_id, start, end, symbol))
#         connection.commit()
#     except Exception as e:
#         connection.rollback()
#     finally:
#         cursor.close()
#         safe_close_connection(connection)
#     return rule_id
#
#
# # def insert_aggregation(connection_pool: mysql.connector.pooling.MySQLConnectionPool, group, group_by):
# #     connection = safe_get_connection(connection_pool)
# #     cursor = connection.cursor()
# #     v = alert_seq(connection_pool,"aggregation")
# #     time = datetime.datetime.now().strftime('%Y%m%d')
# #     rule_id = '3_aggregation' + time + str(v).zfill(length)
# #     insert_query = "INSERT INTO rule_aggregation_group (rule_id, group) VALUES (%s, %s)"
# #     try:
# #         cursor.execute(insert_query,(rule_id,group))
# #         connection.commit()
# #     except Exception as e:
# #         print("Error:", e)
# #         connection.rollback()
# #     finally:
# #         cursor.close()
# #         safe_close_connection(connection)
# #     insert_query = "INSERT INTO rule_aggregation_groupby (rule_id, groupby) VALUES (%s, %s)"
# #     try:
# #         cursor.execute(insert_query, (rule_id, group_by))
# #         connection.commit()
# #     except Exception as e:
# #         print("Error:", e)
# #         connection.rollback()
# #     finally:
# #         cursor.close()
# #         safe_close_connection(connection)
# #     return rule_id
#
# def insert_categorization(connection_pool: mysql.connector.pooling.MySQLConnectionPool, double):
#     connection = safe_get_connection(connection_pool)
#     cursor = connection.cursor()
#     v = alert_seq(connection_pool, "categorization")
#     time = datetime.datetime.now().strftime('%Y%m%d')
#     rule_id = '4_categorization' + time + str(v).zfill(length)
#     insert_query = "INSERT INTO rule_categorization (rule_id, `double`) VALUES (%s, %s)"
#     try:
#         cursor.execute(insert_query, (rule_id, double))
#         connection.commit()
#     except Exception as e:
#         print("Error:", e)
#         connection.rollback()
#     finally:
#         cursor.close()
#         safe_close_connection(connection)
#     return rule_id
#
#
# def insert_generalization(connection_pool: mysql.connector.pooling.MySQLConnectionPool, start, end):
#     connection = safe_get_connection(connection_pool)
#     cursor = connection.cursor()
#     v = alert_seq(connection_pool, "generalization")
#     time = datetime.datetime.now().strftime('%Y%m%d')
#     rule_id = '5_generalization' + time + str(v).zfill(length)
#     insert_query = "INSERT INTO rule_generalization (rule_id, start, end) VALUES (%s, %s, %s)"
#     try:
#         cursor.execute(insert_query, (rule_id, start, end))
#         connection.commit()
#     except Exception as e:
#         connection.rollback()
#     finally:
#         cursor.close()
#         safe_close_connection(connection)
#     return rule_id
#
#
# def insert_suppression(connection_pool: mysql.connector.pooling.MySQLConnectionPool, threshold_percent):
#     connection = safe_get_connection(connection_pool)
#     cursor = connection.cursor()
#     v = alert_seq(connection_pool, "suppression")
#     time = datetime.datetime.now().strftime('%Y%m%d')
#     rule_id = '6_suppression' + time + str(v).zfill(length)
#     insert_query = "INSERT INTO rule_suppression (rule_id, threshold_percent) VALUES (%s, %s)"
#     try:
#         cursor.execute(insert_query, (rule_id, threshold_percent))
#         connection.commit()
#     except Exception as e:
#         connection.rollback()
#     finally:
#         cursor.close()
#         safe_close_connection(connection)
#     return rule_id
#
#
# def insert_default(connection_pool: mysql.connector.pooling.MySQLConnectionPool, data_type, rule_id):
#     connection = safe_get_connection(connection_pool)
#     cursor = connection.cursor()
#     insert_query = "INSERT INTO default_gene_params (data_rule_id, rule_id) VALUES (%s, %s)"
#     try:
#         cursor.execute(insert_query, (data_type, rule_id))
#         connection.commit()
#     except Exception as e:
#         connection.rollback()
#     finally:
#         cursor.close()
#         safe_close_connection(connection)
#     return rule_id
#
#
# def get_default_params(connection_pool: mysql.connector.pooling.MySQLConnectionPool):
#     '''
#     返回结果{'敏感规则rule_id':脱敏规则rule_id}
#     '''
#     connection = safe_get_connection(connection_pool)
#     cursor = connection.cursor()
#     query = "select data_rule_id,rule_id from default_gene_params"
#     dict = {}
#     try:
#         cursor.execute(query)
#         v1 = cursor.fetchall()
#         r = [list(t) for t in v1]
#         for (a, b) in r:
#             dict[a] = b
#     except Exception as e:
#         connection.rollback()
#     finally:
#         cursor.close()
#     return dict


def get_ruleid_params(connection_pool: mysql.connector.pooling.MySQLConnectionPool, tpl_id, user_id):
    '''
    返回结果{'敏感规则rule_id':脱敏规则rule_id}
    '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "select DC_rule_id,DM_rule_id from tpl_rule_map where tpl_id=%s and user_id=%s"
    rule_dict = {}
    try:
        cursor.execute(query, (tpl_id, user_id))
        results = cursor.fetchall()
        for dc_rule_id, dm_rule_id in results:
            # if dc_rule_id in rule_dict:
            #     rule_dict[dc_rule_id].append(dm_rule_id)
            # else:
            #     rule_dict[dc_rule_id] = [dm_rule_id]
            rule_dict[dc_rule_id] = [dm_rule_id]
        cursor.close()
    except Exception as e:
        connection.rollback()
    finally:
        safe_close_connection(connection)
    return rule_dict


def update_data_volume(connection_pool: mysql.connector.pooling.MySQLConnectionPool, data_volume: int, task_id: str):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    update_seq_query = "UPDATE task SET data_volume=%s WHERE task_id=%s"
    try:
        cursor.execute(update_seq_query, (data_volume, task_id))
        connection.commit()
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error: {e}")
        return None  # 返回 None 表示插入失败
    finally:
        safe_close_connection(connection)


# def insert_maskData(connection_pool: mysql.connector.pooling.MySQLConnectionPool, task_id: str, raw_data: json,
#                     mask_data: json):
#     connection = safe_get_connection(connection_pool)
#     cursor = connection.cursor()
#     insert_query = "INSERT INTO mask_data (task_id, raw_data, masking_data) VALUES (%s, %s, %s)"
#     try:
#         cursor.execute(insert_query, (task_id, raw_data, mask_data))
#         connection.commit()
#     except Exception as e:
#         connection.rollback()
#     finally:
#         cursor.close()
#         safe_close_connection(connection)


def insert_masking_id_data_batch(connection_pool: mysql.connector.pooling.MySQLConnectionPool, task_id: str,
                                 data_scan_result_id_list: list, masking_data_list: list):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    insert_query = "INSERT INTO masking_data (task_id, data_scan_result_id, masking_data) VALUES (%s, %s, %s)"

    try:
        data_to_insert = [
            (task_id, data_scan_result_id_list[i], masking_data_list[i])
            for i in range(len(data_scan_result_id_list))
        ]
        cursor.executemany(insert_query, data_to_insert)
        connection.commit()
        cursor.close()
    except Exception as e:
        connection.rollback()
        print(f"Error occurred: {e}")
    finally:
        safe_close_connection(connection)


def insert_ka_generalization(connection_pool: mysql.connector.pooling.MySQLConnectionPool, task_id: str, sheet_name: str,
                             generalization_combination: str, flag: int, uid: str, parent_node: list, child_node: list):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    print(task_id)
    print(generalization_combination)
    print(flag)
    print(sheet_name)
    insert_query = ("INSERT INTO ka_generalization_analysis (task_id, sheet_name, generalization_combination, flag, "
                    "combination_uid, parent_node, child_node)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)")
    try:
        cursor.execute(insert_query, (task_id, sheet_name, generalization_combination, flag, uid, json.dumps(parent_node), json.dumps(child_node)))
        connection.commit()
        cursor.close()
    except Exception as e:
        connection.rollback()
    finally:
        safe_close_connection(connection)


def insert_ka_logistic_regression1(connection_pool: mysql.connector.pooling.MySQLConnectionPool, task_id: str,
                                   sheet_name: str,
                                   flag, identifier_col: str, sensitive_col: str, values: int,
                                   baseline_accuracy: float,
                                   original_accuracy: float, relative_accuracy: float, brier_score: float):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    insert_query = ("INSERT INTO ka_logistic_regression1 (task_id,sheet_name,flag,identifier_col,sensitive_col,"
                    "`values`,baseline_accuracy,original_accuracy,relative_accuracy,brier_skill_score) VALUES (%s, "
                    "%s,%s, %s,%s, %s,%s, %s,%s,%s)")
    try:
        cursor.execute(insert_query, (
            task_id, sheet_name, flag, identifier_col, sensitive_col, values, baseline_accuracy, original_accuracy,
            relative_accuracy,
            brier_score))
        connection.commit()
        cursor.close()
        return True
    except Exception as e:
        connection.rollback()
        return False
    finally:
        safe_close_connection(connection)


def insert_ka_logistic_regression2(connection_pool: mysql.connector.pooling.MySQLConnectionPool, task_id: str,
                                   sheet_name: str,
                                   flag: int, identifier_col: str, value, sensitivity: float, specificity: float,
                                   brier_score: float):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    insert_query = ("INSERT INTO ka_logistic_regression2 (task_id,sheet_name,flag,identifier_col,value,sensitivity,"
                    "specificity,brier_score) VALUES (%s, %s,%s, %s,%s, %s,%s,%s)")
    try:
        cursor.execute(insert_query,
                       (task_id, sheet_name, flag, identifier_col, value, sensitivity, specificity, brier_score))
        connection.commit()
        cursor.close()
    except Exception as e:
        connection.rollback()
    finally:
        safe_close_connection(connection)


def insert_ka_risk_assessment(connection_pool: mysql.connector.pooling.MySQLConnectionPool, task_id: str,
                              sheet_name: str,
                              max_re_identification1: float, average_re_identification1: float,
                              environment_attack1: float, all_re_identification1: float, max_re_identification2,
                              average_re_identification2, environment_attack2,
                              all_re_identification2, user_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    insert_query = ("INSERT INTO ka_risk_assessment (task_id,sheet_name,max_re_identification1, "
                    "`average_re_identification1`, environment_attack1, all_re_identification1,max_re_identification2, "
                    "average_re_identification2, environment_attack2, all_re_identification2, user_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    try:
        cursor.execute(insert_query, (
            task_id, sheet_name, max_re_identification1, average_re_identification1, environment_attack1,
            all_re_identification1, max_re_identification2, average_re_identification2, environment_attack2,
            all_re_identification2, user_id))
        connection.commit()
        cursor.close()
        return True
    except Exception as e:
        connection.rollback()
        return False
    finally:
        safe_close_connection(connection)


def get_identifier_conf(connection_pool, identifier_list_dict, user_id):
    # identifier_info_list_dict:{
    #         sheet_name1:[(identifier,data_type,tree_max_height,ka_tpl_id)],
    #         sheet_name2:[(identifier,data_type,tree_max_height,ka_tpl_id)]
    #     }
    # identifier_list_dict:{
    #         sheet_name1:[(identifier,ka_tpl_id)],
    #         sheet_name2:[(identifier,ka_tpl_id)]
    #     }
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "SELECT data_type,tree_max_height FROM task_k_tpl WHERE ka_tpl_id = %s and (user_id = %s or user_id ='default')"
    # query = "SELECT data_type,tree_max_height FROM task_k_tpl WHERE ka_tpl_id = %s and user_id = %s"
    identifier_info_list_dict = {}
    try:
        for sheet_name, identifier_list in identifier_list_dict.items():
            identifier_info_list = []
            for item in identifier_list:
                identifier_name, ka_tpl_id = item
                cursor.execute(query, (ka_tpl_id, user_id))
                result = cursor.fetchone()
                if result:
                    item_info = (identifier_name, result[0], result[1], ka_tpl_id)
                    identifier_info_list.append(item_info)
                else:
                    info = {f"未找到工作表{sheet_name}的列{identifier_name}对应的KA_TPL设置:'{ka_tpl_id}'"}
                    append_re_api(info)
                    # raise Exception(f"No info found for identifier '{identifier_name}' of sheet '{sheet_name}'.")
            identifier_info_list_dict[sheet_name] = identifier_info_list
        cursor.close()
        return identifier_info_list_dict
    except Exception as e:
        print("get_identifier_conf error:", e)
        connection.rollback()
        return None
    finally:
        safe_close_connection(connection)


def delete_aggregation_params(connection_pool, rule_id, user_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    delete_query = "DELETE FROM rule_aggregation WHERE rule_id = %s AND user_id = %s"
    try:
        cursor.execute(delete_query, (rule_id, user_id))
        connection.commit()
        print("Deletion successful.")
        cursor.close()
    except Exception as e:
        print("Error deleting records:", e)
        connection.rollback()
    finally:
        safe_close_connection(connection)

def get_download_file(connection_pool, task_id, user_id):
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "SELECT download_path FROM download_file_path WHERE task_id = %s AND user_id = %s"
    try:
        cursor.execute(query, (task_id, user_id))
        result = cursor.fetchone()
        if result:
            cursor.close()
            return result[0]
    except Exception as e:
        print("get_download_file: ", e)
        connection.rollback()
        return False
    finally:
        safe_close_connection(connection)