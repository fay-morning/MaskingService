import json
import os
from flask import Flask, request, jsonify
from datetime import datetime
import threading

from loguru import logger
from service import alg_mask
from service import k_anonymity
from util import mysql_util
from util.server_util import get_config
from util.mysql_classification_util import get_mysql_connector_pool as get_class_connector_pool
from util.mysql_util import get_mysql_connector_pool as get_mask_connector_pool

class_pool = get_class_connector_pool()
mask_pool = get_mask_connector_pool()
app = Flask(__name__)

# 添加输出到文件的处理器
logger.add(
    sink=os.path.join(os.path.dirname(os.path.abspath(__file__)), './logs/server.log'),
    format="{time} {level} {message}",
    enqueue=True,
    rotation="1 week",
    retention="7 days",
    compression="zip"
)

# 使用默认配置创建一个日志记录器对象
logger = logger.opt()

server_config = get_config()


# connection_pool = mysql_util.get_mysql_connector_pool()


@app.route('/MaskingService/hello/get', methods=['GET'])
def hello_get():
    data = request.args
    return jsonify({"message": "ok", "code": 0, "data": data}), 200


@app.route('/MaskingService/hello/post', methods=['POST'])
def hello_post():
    data = request.get_json()
    return jsonify({"message": "ok", "code": 0, "data": data}), 200


@app.route('/MaskingService/mask/unit', methods=['POST'])
def mask_unit():
    """
    对识别出的数据库中的敏感数据做数据匿名（文件脱敏）
    三种输入：

    1. {"run_task_id": 1, "user_id":"", "tpl_id":""} 只传run_task_id
    2. {"run_task_id":1, "resource":{resource_id:[resource_url1, resource_url2]}, "user_id":"", "tpl_id":""}
    3. {
        "user_id": user_id,
        "resource": resource,
        "store_flag": store_flag,//可能不需要
        "flag": type_flag,
        "rule_map": {column_name:dm_rule_id,...}
        "task_id": task_id
        }
    flag=1时表示数据为数据库。读取class_run_task_id的id，
    根据data_scan_result_id得到需要连接的数据库（根据resource_id）和需要修改的表、列信息（根据location）

    参数格式：
    data = {
        "run_task_id": str,
        "resource":{resource_id:[resource_url]},
        "data_scan_result_id":[string],
        "task_id": str,
        "flag": int,
        "tpl_id": str,
        "user_id": str,
        "store_flag":int
    }
    """
    data = request.get_json()

    class_run_task_id = data.get("run_task_id", None)
    resource = data.get("resource", None)
    # data_scan_result_id = data.get("data_scan_result_id", None)
    # flag = data.get("flag", None)
    task_id = data.get("task_id", None)
    tpl_id = data.get("tpl_id", None)
    user_id = data.get("user_id", None)
    rule_map_list = data.get("rule_map_list", None)
    download_url = data.get("download_url", None)
    store_flag = data.get("store_flag", None)
    authorization = data.get('authorization', None)

    resp_list = []
    if task_id is None:
        return jsonify({"message": "task_id is None", "code": 0, "data": resp_list}), 400
    t = threading.Thread(target=alg_mask.unit, args=(
        class_pool, mask_pool, task_id, class_run_task_id, resource, tpl_id, user_id,
        rule_map_list, download_url, authorization, store_flag))
    t.start()

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/mask/masking', methods=['POST'])
def mask_masking():
    """
    对给定数据进行屏蔽操作

    参数格式：
    data = {
        "start": int,
        "end": int,
        "symbol": char,
        "target_list": [string],
    }
    """
    data = request.get_json()
    target_list = data.get('target_list', None)
    start = data.get('start', None)
    end = data.get('end', None)
    symbol = data.get('symbol', None)

    resp_list = alg_mask.masking(target_list, start, end, symbol,None)

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/mask/aggregation', methods=['POST'])
def mask_aggregation():
    """
    对给定数据进行聚合操作

    record = {
        age: int,
        sex: str,
        zip: str,
        weight: int,
        diagnosis: str
    }

    参数格式：
    data = {
        "group": [str, str],
        "group_by": [str],
        "target_list": [record],
    }
    """
    data = request.get_json()
    # target_list = [int(x) for x in data["target_list"]]
    target_list = data.get('target_list', None)
    # group = int(data["group"])
    group = data.get('group', None)
    group_by = data.get('group_by', None)
    if group_by is None:
        group_by = []
        for k in target_list[0]:
            if k not in group:
                group_by.append(k)

    resp_list = alg_mask.aggregation(target_list, group, group_by,None)

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/mask/categorization', methods=['POST'])
def mask_categorization():
    """
    对给定数据进行分类操作

    参数格式：
    data = {
        "double": int,
        "target_list": [int],
    }
    """
    data = request.get_json()
    target_list = data.get('target_list', None)
    double = data.get('double', None)

    resp_list = alg_mask.categorization(target_list, double,None)

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/mask/generalization', methods=['POST'])
def mask_generalization():
    """
    对给定数据进行泛化操作

    参数格式：
    data = {
        "start": int,
        "end": int,
        "target_list": [string],
    }
    """
    data = request.get_json()
    target_list = data.get('target_list', None)
    start = data.get('start', None)
    end = data.get('end', None)

    resp_list = alg_mask.generalization(target_list, start, end,None)

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/mask/sampling', methods=['POST'])
def mask_sampling():
    """
    对给定数据进行采样操作

    参数格式：
    data = {
        "retain_percent": float,
        "target_list": [string],
    }
    """
    data = request.get_json()
    target_list = data.get('target_list', None)
    retain_percent = data.get('retain_percent', None)

    resp_list = alg_mask.sampling(target_list, retain_percent,None)

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/mask/suppression', methods=['POST'])
def mask_suppression():
    """
    对给定数据进行抑制操作

    参数格式：
    data = {
        "threshold_percent": float,
        "target_list": [string],
    }
    """
    data = request.get_json()
    target_list = data.get('target_list', None)
    threshold_percent = data.get('threshold_percent', None)

    resp_list = alg_mask.suppression(target_list, threshold_percent,None)

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/mask/k_anonymity', methods=['POST'])
def mask_k_anonymity():
    """
    对给定数据进行k-匿名处理
    参数格式：
    data = {
        "task_id":string
        "user_id":string
    }
    """
    data = request.get_json()
    user_id = data.get("user_id", None)
    task_id = data.get("task_id", None)
    t = threading.Thread(target=k_anonymity.k_anonymity, args=(mask_pool, class_pool, task_id, user_id))
    t.start()
    resp_list = []

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/risk_assessment/prosecutor_attack', methods=['POST'])
def prosecutor_risk():
    """
    对处理后的数据进行检察官攻击风险评估

    record = {
        age: int,
        sex: str,
        zip: str,
        weight: int,
        diagnosis: str
    }

    参数格式：
    data = {
        "raw_data": [record],
        "identifier_list":[string]
    }
    """
    data = request.get_json()
    raw_data = data.get('raw_data', None)
    identifier_list = data.get('identifier_list', None)
    resp_list = k_anonymity.prosecutor_attack_api(raw_data, identifier_list)

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/risk_assessment/correspondent_attack', methods=['POST'])
def correspondent_risk():
    """
    对处理后的数据进行记者攻击风险评估

    record = {
        age: int,
        sex: str,
        zip: str,
        weight: int,
        diagnosis: str
    }

    参数格式：
    data = {
        "raw_data": [record],
        "attack_form":[record]
        "identifier_list":[string]
    }
    """
    data = request.get_json()
    raw_data = data.get('raw_data', None)
    attack_form = data.get('attack_form', None)
    identifier_list = data.get('identifier_list', None)

    resp_list = k_anonymity.correspondent_attack_api(raw_data, attack_form, identifier_list)

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/risk_assessment/marketer_attack', methods=['POST'])
def marketer_risk():
    """
    对处理后的数据进行营销者攻击风险评估

    record = {
        age: int,
        sex: str,
        zip: str,
        weight: int,
        diagnosis: str
    }

    参数格式：
    data = {
        "raw_data": [record],
        "attack_form":[record]
        "identifier_list":[string]
    }
    """
    data = request.get_json()
    raw_data = data.get('raw_data', None)
    attack_form = data.get('attack_form', None)
    identifier_list = data.get('identifier_list', None)

    resp_list = k_anonymity.marketer_attack_api(raw_data, attack_form, identifier_list)

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/risk_assessment/risk_assessment', methods=['POST'])
def risk_assessment():
    """
    对处理后的数据进行风险评估
    record = {
        age: int,
        sex: str,
        zip: str,
        weight: int,
        diagnosis: str
    }
    参数格式：
    data = {
        "raw_data": [record],
        "modified_data": [record],
        "identifier_list":[string],
        "environment_state": int,
        "control": int,
        "ability": int,
        "per": float,
        "m": int,
        "power": int
    }
    """
    data = request.get_json()
    task_id = data.get('task_id', None)
    local_path = data.get('local_path', None)
    ds_path = data.get('ds_path', None)
    sheet_name = data.get('sheet_name', None)
    identifier_list = data.get('identifier_list', None)
    user_id = data.get('user_id',None)
    flag = data.get('flag', 1)

    environment_state = data.get('environment_state', 1)
    control = data.get('control', 3)
    ability = data.get('ability', 2)
    per = data.get('per', 0.9)
    m = data.get('m', 10)
    power = data.get('power', 3)
    t = threading.Thread(target=k_anonymity.risk_assessment_api,
                         args=(mask_pool, task_id, local_path, ds_path, sheet_name, identifier_list, user_id,
                               environment_state, control,
                               ability, per, m, power, flag))
    t.start()
    resp_list = {}
    # resp_list = k_anonymity.risk_assessment_api(mask_pool, task_id, local_path, sheet_name, identifier_list,
    #                                             environment_state, control,
    #                                             ability, per, m, power)

    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


@app.route('/MaskingService/availability/availability', methods=['POST'])
def availability_analysis():
    """
    对处理后的数据进行可用性评估

    record = {
        age: int,
        sex: str,
        zip: str,
        weight: int,
        diagnosis: str
    }

    参数格式：
    data = {
        "raw_data":[record],
        "modified_data":[record],
        "feature_col":[str],
        "target_col":str
    }
    """
    data = request.get_json()
    task_id = data.get('task_id', None)
    local_path = data.get('local_path', None)
    ds_path = data.get('ds_path', None)
    sheet_name = data.get('sheet_name', None)
    identifier_col_list = data.get('identifier_col_list', None)
    sensitive_col_list = data.get('sensitive_col_list', None)
    user_id = data.get('user_id', None)
    flag = data.get('flag', 1)
    t = threading.Thread(target=k_anonymity.availability_analysis_api,
                         args=(mask_pool, task_id, local_path, ds_path, sheet_name, identifier_col_list,
                               sensitive_col_list, user_id,flag))
    t.start()
    # resp_list = k_anonymity.availability_analysis_api(mask_pool, task_id, local_path, sheet_name, identifier_col_list,
    #                                                   sensitive_col_list)
    resp_list = {}
    return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


#
# @app.route('/MaskingService/availability/availability2', methods=['POST'])
# def availability_analysis2():
#     """
#     对处理后的数据进行可用性评估
#     record = {
#         age: int,
#         sex: str,
#         zip: str,
#         weight: int,
#         diagnosis: str
#     }
#
#     参数格式：
#     data = {
#         "task_id":str,
#         "raw_data":[record],
#         "modified_data":[record],
#         "identifier_col_list":[str],
#         "sensitive_col_list":[str]
#     }
#     """
#     data = request.get_json()
#     task_id = data.get('task_id', None)
#     raw_data = data.get('raw_data', None)
#     modified_data = data.get('modified_data', None)
#     identifier_col_list = data.get('identifier_col_list', None)
#     sensitive_col_list = data.get('sensitive_col_list', None)
#
#     resp_list = k_anonymity.availability_analysis(mask_pool, task_id, raw_data, modified_data, identifier_col_list,
#                                                      sensitive_col_list)
#
#     return jsonify({"message": "ok", "code": 0, "data": resp_list}), 200


if __name__ == '__main__':
    port = server_config.get("Server", "port")
    logger.info("listen at: %s" % port)
    app.run(debug=True, host='0.0.0.0', port=port)
