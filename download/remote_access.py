import paramiko
from . import sftp_util 
from loguru import logger


def check_ftp(ip, port, username=None, pwd=None, extra=None):
    try:
        ftp = sftp_util.FTPHost(ip, username, pwd, port=port, session_factory=sftp_util.MySession)
    except Exception as e:
        return 1, e.__str__()
    return 0, ftp


def check_sftp(ip, port, username=None, pwd=None, extra=None):
    try:
        transport = paramiko.Transport((ip, port))
        transport.connect(username=username, password=pwd)
        sftp = paramiko.SFTPClient.from_transport(transport)
    except Exception as e:
        return 1, e.__str__()
    return 0, (sftp, transport)

# logger.warning("check sftp:", remote_check_access("ftp", "127.0.0.1", 21, "dc_001", "123456"))
# logger.warning("check sftp:", remote_check_access("sftp", "127.0.0.1", 990, "dc_001", "123456"))
# logger.warning("check mysql:", remote_check_access("mysql", "127.0.0.1", 3306, "root", "123456"))
# logger.warning("check http:", remote_check_access("http", "127.0.0.1", 8000, "admin", "12356"))
# logger.warning("check https:", remote_check_access("https", "www.baidu.com", 443))
# logger.warning("check redis:", remote_check_access("redis", "10.10.15.151", 6379, None, "hitcs2020!"))
# logger.warning("check mongodb:", remote_check_access("mongodb", "10.10.15.151", 27017, "root", "hitcs2020!"))
