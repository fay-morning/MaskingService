# MaskingAPI提供的接口调用
import requests

import util.server_util

# from app import server_config
import util.server_util
server_config = util.server_util.get_config()

def re_api(params):
    '''
    params = {'message': '未知文件类型或不支持的文件类型！' + file_extension, 'task_id': task_id,
                        'status': -1}
    '''
    service_host = server_config.get("Masking_API", "host")
    service_port = server_config.get("Masking_API", "port")
    service_url = f"http://{service_host}:{service_port}"
    path = server_config.get("Masking_API", "path")  # MaskingService提供的url路径
    full_url = f"{service_url}/{path}"
    ret = requests.post(full_url, json=params)

def append_re_api(params):
    service_host = server_config.get("Masking_API", "host")
    service_port = server_config.get("Masking_API", "port")
    service_url = f"http://{service_host}:{service_port}"
    path = server_config.get("Masking_API", "append_path")  # MaskingService提供的url路径
    full_url = f"{service_url}/{path}"
    ret = requests.post(full_url, json=params)


def delete_temp_ka_tpl(ka_tpl_id, user_id):
    service_host = server_config.get("Masking_API", "host")
    service_port = server_config.get("Masking_API", "port")
    path = server_config.get("Masking_API", "delete_temp_ktpl_url")  # MaskingService提供的url路径
    full_url = f"http://{service_host}:{service_port}/{path}"
    params = {
        "org_id": user_id,
        "ka_tpl_id": ka_tpl_id
    }
    ret = requests.post(full_url, json=params)
    # print(ret)

def set_ka_template(org_id, ka_tpl_id, tpl_name, tree_height, rule_map):
    '''
    {
    "org_id": "1",//用户id
    "ka_tpl_id": "", //可为空或不传，此时会新建一个模板，否则就是在该模板id对应的模板上更新
    "tpl_name": "身份证号默认配置",
    "tree_height": 3,
    "rule_map": {
        "1": "2_masking2024070100000009",
        "2": "2_masking2024070100000010",
        "3": "2_masking2024070100000011",
    } //键即指定的层数，值为数据脱敏规则ID
}
    '''
    service_host = server_config.get("Masking_API", "host")
    service_port = server_config.get("Masking_API", "port")
    path = server_config.get("Masking_API", "set_ka_template")  # MaskingService提供的url路径
    full_url = f"http://{service_host}:{service_port}/{path}"
    params = {
        "org_id": org_id,
        "ka_tpl_id": ka_tpl_id,
        "tpl_name": tpl_name,
        "tree_height": tree_height,
        "rule_map": rule_map,
        "store_flag": 0
    }
    response = requests.post(full_url, json=params)
    status_code = response.status_code
    headers = response.headers
    content = response.text  # or response.json() if the response is JSON

    print("Status Code:", status_code)
    # print("Headers:", headers)
    print("Content:", content)
