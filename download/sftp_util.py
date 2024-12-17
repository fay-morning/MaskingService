import datetime
import ftplib
import os
import random

import ftputil
import paramiko
from loguru import logger
from . import remote_access

try:
    import ssl
except ImportError:
    _SSLSocket = None
else:
    _SSLSocket = ssl.SSLSocket


class MySession(ftplib.FTP):
    def __init__(self, host, user, password, port):
        """Act like ftplib.FTP's constructor but connect to another port."""
        self.conn = ftplib.FTP.__init__(self)
        self.connect(host, port, 5)
        self.login(user, password)
        self.set_pasv(True)
        self.encoding = "utf-8"

    def retrlines(self, cmd, callback=None):
        """Retrieve data in line mode.  A new port is created for you.

        Args:
          cmd: A RETR, LIST, or NLST command.
          callback: An optional single parameter callable that is called
                    for each line with the trailing CRLF stripped.
                    [default: print_line()]

        Returns:
          The response code.
        在编码时添加异常处理
        """
        resp = self.sendcmd('TYPE A')
        with self.transfercmd(cmd) as conn, \
                conn.makefile('rb') as fp:
            lines = fp.read().split(b'\n')
            for line in lines:
                try:
                    line = line.decode(self.encoding)
                except Exception as e:
                    continue
                if len(line) > self.maxline:
                    raise Exception("got more than %d bytes" % self.maxline)
                if self.debugging > 2:
                    print('*retr*', repr(line))
                if not line:
                    break
                if line[-1:] == '\r':
                    line = line[:-1]
                callback(line)
            # shutdown ssl layer
            if _SSLSocket is not None and isinstance(conn, _SSLSocket):
                conn.unwrap()
        return self.voidresp()


class FTPHost(ftputil.FTPHost):
    def _dir(self, path):
        """
        Return a directory listing as made by FTP's `LIST` command as a list of
        strings.
        """

        # Don't use `self.path.isdir` in this method because that would cause a
        # call of `(l)stat` and thus a call to `_dir`, so we would end up with
        # an infinite recursion.
        # 在编码时添加异常处理
        def _FTPHost_dir_command(self, path):
            """Callback function."""
            lines = []

            def callback(line):
                """Callback function."""
                try:
                    lines.append(ftputil.tool.as_str(line, encoding=self._encoding))
                except Exception:
                    pass

            with ftputil.error.ftplib_error_to_ftp_os_error:
                if self.use_list_a_option:
                    self._session.dir("-a", path, callback)
                else:
                    self._session.dir(path, callback)
            return lines

        lines = self._robust_ftp_command(
            _FTPHost_dir_command, path, descend_deeply=True
        )
        return lines

    def listdir(self, path):
        """
        Return a list of directories, files etc. in the directory named `path`.

        If the directory listing from the server can't be parsed with any of
        the available parsers raise a `ParserError`.
        在编码时添加异常处理
        """
        ftputil.tool.raise_for_empty_path(path)
        original_path = path
        path = ftputil.tool.as_str_path(path, encoding=self._encoding)
        items = self._stat._listdir(path)
        filelist = []
        for item in items:
            try:
                filelist.append(ftputil.tool.same_string_type_as(original_path, item, self._encoding))
            except Exception as e:
                continue
        return filelist


class SFTPClient(paramiko.SFTPClient):

    def listdir_attr(self, path="."):
        """
        Return a list containing `.SFTPAttributes` objects corresponding to
        files in the given ``path``.  The list is in arbitrary order.  It does
        not include the special entries ``'.'`` and ``'..'`` even if they are
        present in the folder.

        The returned `.SFTPAttributes` objects will each have an additional
        field: ``longname``, which may contain a formatted string of the file's
        attributes, in unix format.  The content of this string will probably
        depend on the SFTP server implementation.

        :param str path: path to list (defaults to ``'.'``)
        :return: list of `.SFTPAttributes` objects

        .. versionadded:: 1.2
        在编码时添加异常处理
        """
        path = self._adjust_cwd(path)
        self._log(paramiko.common.DEBUG, "listdir({!r})".format(path))
        t, msg = self._request(paramiko.sftp.CMD_OPENDIR, path)
        if t != paramiko.sftp.CMD_HANDLE:
            raise Exception("Expected handle")
        handle = msg.get_binary()
        filelist = []
        while True:
            try:
                t, msg = self._request(paramiko.sftp.CMD_READDIR, handle)
            except EOFError:
                # done with handle
                break
            if t != paramiko.sftp.CMD_NAME:
                raise Exception("Expected name response")
            count = msg.get_int()
            for i in range(count):
                try:
                    try:
                        filename = msg.get_text()
                    except Exception:
                        filename = None
                    try:
                        longname = msg.get_text()
                    except Exception:
                        longname = None
                    # filename = msg.get_string()
                    # longname = msg.get_string()
                    # print(filename, longname)
                    attr = paramiko.sftp_attr.SFTPAttributes._from_msg(msg, filename, longname)
                    if (filename != ".") and (filename != "..") and (filename is not None):
                        filelist.append(attr)
                except Exception as e:
                    logger.error(e)
                    continue
        self._request(paramiko.sftp.CMD_CLOSE, handle)
        return filelist


def generate_random_file_name():
    now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    num = random.randint(1, 10000)

    return "%s%05d" % (now, num)


# print("%s_%s" % (generate_random_file_name(), os.path.basename("/data/code/中文.txt")))

# 存储文件名修改为时间戳
def sftp_download_file(sftp, remote_path, local_path):
    file_name = "%s_%s" % (generate_random_file_name(), os.path.basename(remote_path))
    full_path = f"{local_path}/{file_name}"
    try:
        sftp.get(remote_path, full_path)
    except Exception as e:
        logger.error("sftp下载失败" + str(e))
        return False, None
    return True, full_path


# 存储文件名修改为时间戳
def ftp_download_file(host, remote_path, local_path):
    file_name = "%s_%s" % (generate_random_file_name(), os.path.basename(remote_path))
    full_path = f"{local_path}/{file_name}"
    try:
        host.download(remote_path, full_path)
    except Exception as e:
        logger.error("ftp下载失败" + str(e))
        return False, None
    return True, full_path


def sftp_upload_file(sftp, local_url, remote_url, storage_flag):
    try:
        remote_path = os.path.dirname(remote_url)
        remote_name = os.path.basename(remote_url)
        if storage_flag == 1 and sftp.stat(remote_url):
            sftp.remove(remote_url)
            upload_url = remote_url
        else:
            upload_url = remote_path +'/'+ os.path.basename(local_url)
        print(upload_url)
        print(local_url)
        # 上传文件
        sftp.put(local_url, upload_url)

        print(f"文件 '{local_url}' 已成功上传到 SFTP 服务器的 '{remote_url}'")
        os.remove(local_url)
        return True
    except FileNotFoundError:
        print(f"本地文件 '{local_url}' 未找到")
        return False
    except Exception as e:
        print(f"上传文件时出错: {str(e)}")
        return False


# def ftp_upload_file(host, local_path, remote_path):
#     try:
#         # 打开本地文件
#         with open(local_path, 'rb') as file:
#             # 上传文件到 FTP 服务器
#             host.storbinary(f'STOR {remote_path}', file)
#
#         print(f"文件 '{local_path}' 已成功上传到 FTP 服务器的 '{remote_path}'")
#         return True
#
#     except FileNotFoundError:
#         print(f"本地文件 '{local_path}' 未找到")
#         return False
#
#     except Exception as e:
#         print(f"上传文件时出错: {str(e)}")
#         return False

def ftp_upload_file(copy_path, storage_flag, storage_conf):
    _, ftp = remote_access.check_ftp(storage_conf["auth_ip"], storage_conf["auth_port"], storage_conf["auth_username"],
                       storage_conf["auth_pwd"])
    remote_path = os.path.dirname(storage_conf["resource_url"])
    remote_name = os.path.basename(storage_conf["resource_url"])
    ftp.chdir(remote_path)

    if storage_flag == 1 and ftp.path.exists(remote_name):
        ftp.remove(remote_name)
        file_name = remote_name
    else:
        file_name = os.path.basename(copy_path)
    print(copy_path)
    print(file_name)
    try:
        ftp.upload(copy_path, file_name)
        print("File uploaded successfully.")
        os.remove(copy_path)
        ftp.chmod(file_name, 0o644)
        return True
    except Exception as e:
        print(f"Failed to upload file: {e}")
        return False



def delete_if_exists(path):
    if path is None:
        return
    if os.path.isfile(path):
        try:
            os.remove(path)
        except Exception as e:
            logger.error(f"Error deleting the file {path}. Error: {e}")
