import datetime
import random
import time
from typing import Tuple
import mysql.connector.pooling
import pytz as pytz
from loguru import logger
from const import const_task
from util.server_util import get_config
import pymysql
from itertools import chain

length=8

def get_mysql_connector_pool() -> mysql.connector.pooling.MySQLConnectionPool:
    server_config = get_config()
    db_config = {
        'host': server_config.get('Mysql_Classification', 'host'),
        'user': server_config.get('Mysql_Classification', 'user'),
        'password': server_config.get('Mysql_Classification', 'password'),
        'database': server_config.get('Mysql_Classification', 'database'),
        'port': server_config.get('Mysql_Classification', 'port'),
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

def get_sensitivedata_by_taskid(connection_pool: mysql.connector.pooling.MySQLConnectionPool,run_task_id:str):
    '''
    run_task_id:任务id
    返回结果:[分类分级id,敏感规则rule_id,需要脱敏的数据,resource_id,resource_url,resource_type]格式列表的列表。
    '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "select id,data_feature_id,rule_hit_text,resource_id,resource_url,resource_type from data_scan_result where run_task_id=%s"
    try:
        cursor.execute(query,(run_task_id,))
        results = cursor.fetchall()
        data = [list(t) for t in results]
        cursor.close()
    except Exception as e:
        connection.rollback()
        data = []
    finally:
        safe_close_connection(connection)
    return data

def get_sensitive_data2(connection_pool: mysql.connector.pooling.MySQLConnectionPool,data_scan_id):
    '''
    data_scan_id:data_scan_result表中的id
    返回结果:[分类分级id,敏感规则rule_id,需要脱敏的数据,resource_id,resource_url,resource_type]格式列表的列表。
    '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "select id,data_feature_id,rule_hit_text,resource_id,resource_url,resource_type from data_scan_result where id=%s"
    v=[]
    try:
        for id in data_scan_id:
            cursor.execute(query,(id,))
            v1=cursor.fetchone()
            if v1:
                v.append(v1)
            else:
                print(id+'不存在!')
        cursor.close()
    except Exception as e:
        connection.rollback()
    finally:
        safe_close_connection(connection)
    return v


def get_sensitivedata_by_resource_batch(connection_pool: mysql.connector.pooling.MySQLConnectionPool, resource_ids: list[str],
                             resource_urls: list[str], run_task_id: str):
    '''
    批量查询数据，返回结果：[分类分级id,敏感规则rule_id,需要脱敏的数据,resource_id,resource_url,resource_type] 格式列表的列表。
    '''
    if not resource_ids or not resource_urls:
        return []
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()

    # 生成查询字符串
    format_strings = ','.join(['(%s, %s)'] * len(resource_ids))
    query = f"""
    SELECT id, data_feature_id, rule_hit_text, resource_id, resource_url, resource_type
    FROM data_scan_result
    WHERE run_task_id = %s AND (resource_id, resource_url) IN ({format_strings})
    """

    # 准备查询参数
    params = [run_task_id] + [item for sublist in zip(resource_ids, resource_urls) for item in sublist]

    try:
        cursor.execute(query, params)
        results = cursor.fetchall()
        data = [list(t) for t in results]
        cursor.close()
    except Exception as e:
        connection.rollback()
        data = []
    finally:
        safe_close_connection(connection)
    return data

def get_db_params(connection_pool: mysql.connector.pooling.MySQLConnectionPool,ds_id:str):
    '''
    返回结果:(access_protocol,auth_ip,auth_port,auth_username,auth_pwd,ds_url)的元组
    '''
    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "select access_protocol,auth_ip,auth_port,auth_username,auth_pwd from resource_datasource where ds_id=%s"
    try:
        cursor.execute(query,(ds_id, ))
        r=cursor.fetchone()
        cursor.close()
        return r
    except Exception as e:
        connection.rollback()
        return None
    finally:
        safe_close_connection(connection)


# def get_db_by_resourceid(connection_pool: mysql.connector.pooling.MySQLConnectionPool,resource_id:str):
#     '''
#         resource_id:
#         返回结果:((access_protocol,auth_ip,auth_port,auth_username,auth_pwd,ds_url),location)格式的列表。
#     '''
#
#     connection = safe_get_connection(connection_pool)
#     cursor = connection.cursor()
#     query = "select resource_id,location from data_scan_result where resource_id=%s"
#     resource_id = ''
#     location = ''
#     try:
#         cursor.execute(query, (resource_id,))
#         (resource_id, location) = cursor.fetchone()
#     except Exception as e:
#         connection.rollback()
#     finally:
#         cursor.close()
#     db = get_db_params(connection_pool, resource_id)
#     return (db, location)

def get_sensitive_db(connection_pool: mysql.connector.pooling.MySQLConnectionPool,data_scan_result_id:str):
    '''
    run_task_id:任务id
    返回结果:((access_protocol,auth_ip,auth_port,auth_username,auth_pwd,ds_url),location,rule_id)格式的列表。
    '''

    connection = safe_get_connection(connection_pool)
    cursor = connection.cursor()
    query = "select resource_id,location,data_feature_id from data_scan_result where id=%s"
    resource_id=''
    location=''
    rule_id=''
    try:
        cursor.execute(query,(data_scan_result_id,))
        (resource_id,location,rule_id)=cursor.fetchone()
        cursor.close()
    except Exception as e:
        connection.rollback()
    finally:
        safe_close_connection(connection)
    db=get_db_params(connection_pool,resource_id)
    return (db,location,rule_id)