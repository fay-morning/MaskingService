import requests
import os
from download import sftp_util
# from app import server_config
import util.server_util
server_config = util.server_util.get_config()

def download_file_by_url(url, ds_path, down_path):
    response = requests.get(url, stream=True)
    # 检查请求是否成功
    if response.status_code == 200:
        # 下载文件并保存到本地
        file_name = "%s_%s" % (sftp_util.generate_random_file_name(), os.path.basename(ds_path))
        full_path = f"{down_path}/{file_name}"
        with open(full_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
        return full_path, None
    else:
        e = f"下载失败，请求下载状态码：{response.status_code}，错误信息：{response.text}"
        return None, e


def download_user_upload_file(authorization, url, down_path):
    if authorization:
        headers = {
            'Authorization': authorization
        }
        url_response = requests.get(url, headers=headers, stream=True)

        if url_response.status_code == 200:
            response_data = url_response.json()  # 解析 JSON 响应
            download_url = response_data.get('result')  # 获取 'result' 字段的值
            if download_url is None:
                e = "未获取到私有文件下载链接"
                return None, e
            print("download_url: ", download_url)
            result = download_file_by_url(download_url, url, down_path)
            return result
        else:
            e = f"获取私有文件下载链接失败，状态码： {url_response.status_code}，错误信息：{url_response.text}"
            return None, e
    else:
        # url = constants.UPLOAD_PUBLIC_URL_PREFIX + url
        result = download_file_by_url(url, url, down_path)
        return result

def upload_to_server(authorization, file_path):
# 设置请求的 URL
    url = server_config.get('Masking_File','upload_url')

    # 设置其他表单数据
    data = {
        'private': 'true',  # 是否为私有文件，设置为 'true' 或 'false'
        'bztype': 'Form'
    }

    headers = {
        'Authorization': authorization
    }

    with open(file_path, "rb") as f:
        files = {
            "file": f
        }
        # 发起 POST 请求
        response = requests.post(url, files=files, data=data, headers=headers)

    # 处理响应
    if response.status_code == 200:
        try:
            response_json = response.json()
            print("成功响应:", response_json)
            return True, response_json["result"][0]["url"]
        except ValueError:
            print("响应内容不是有效的 JSON")
            return False, None
    else:
        print(f"请求失败，状态码: {response.status_code}, 响应内容: {response.text}")
        return False, None
