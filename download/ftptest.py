from remote_access import check_ftp, check_sftp
from sftp_util import ftp_download_file, sftp_download_file

state, ftp = check_ftp(ip='127.0.0.1', port=21, username="dc_001", pwd="123456")

if state == 0:
    print("连接成功")
    ftp_download_file(ftp, remote_path="/data/code/中文.txt", local_path="/data/code")



# state, sftp, transport = check_sftp(ip='127.0.0.1', port=21, username="dc_001", pwd="123456")
#
# if state == 0:
#     print("连接成功")
#     sftp_download_file(sftp, remote_path="/data/code/中文.txt", local_path="/data/code")