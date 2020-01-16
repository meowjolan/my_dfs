# -*- encoding: utf-8 -*-
"""
@File    : clientServer.py.py
@Time    : 1/15/20 9:32 AM
@Author  : Guo Junnan
@Email   : 529931457@qq.com
@Software: PyCharm
"""

import sys
import os
import re
import time
import base64
import service_pb2
import service_pb2_grpc
import grpc

# 命令匹配正则表达式
OPEN_REGEX = "open [a-zA-Z0-9_]*."
CLOSE_REGEX = "close [a-zA-Z0-9_]*."
DELETE_REGEX = "delete [a-zA-Z0-9_]*."


class Client:
    HOST = "0.0.0.0"
    DIR_PORT = 9001
    LOCK_PORT = 9002
    DIR_HOST = HOST
    LOCK_HOST = HOST
    CLIENT_ROOT = os.getcwd()
    BUCKET_NAME = "ClientFiles"
    BUCKET_LOCATION = os.path.join(CLIENT_ROOT, BUCKET_NAME)

    def __init__(self):
        # 初始化函数
        # 记录打开状态的文件
        self.open_files = {}

    def open(self, filename):
        # 文件打开操作，从服务器上下载文件
        if filename not in self.open_files.keys():
            # 没有缓存，则下载文件
            request = self._get_directory(filename)
            # 找到目录
            params = request.text.splitlines()
            server = params[0].split()[1]
            port = int(params[1].split()[1])
            open_file = params[2].split()[1]
            # 下载过程要加锁
            self._lock_file(filename)
            file_downloaded = self._download_file("%s:%s" % (server, port),
                                                  open_file)
            self._unlock_file(filename)
            if file_downloaded:
                self.open_files[filename] = open_file
            else:
                return False
        return True

    def close(self, filename):
        # 上传并关闭文件
        if filename in self.open_files.keys():
            # 文件已打开
            request = self._get_directory(filename)
            # 找到目录
            params = request.text.splitlines()
            server = params[0].split()[1]
            port = int(params[1].split()[1])
            open_file = params[2].split()[1]
            # 加索并上传文件
            self._lock_file(filename)
            file_uploaded = self._upload_file("%s:%s" % (server, port),
                                              open_file)
            self._unlock_file(filename)
            if file_uploaded:
                # 上传成功，清除本地信息
                path = os.path.join(self.CLIENT_ROOT, self.BUCKET_NAME)
                path = os.path.join(path, self.open_files[filename])
                if os.path.exists(path):
                    os.remove(path)
                del self.open_files[filename]
            else:
                return False
        return True

    def delete(self, filename):
        # 文件删除操作，移除各个副本
        request = self._get_directory(filename)
        # 找到目录
        params = request.text.splitlines()
        server = params[0].split()[1]
        port = int(params[1].split()[1])
        open_file = params[2].split()[1]
        slaves = (params[3].split()[1:])
        # 删除过程要加锁
        self._lock_file(filename)
        self._remove_file("%s:%s" % (server, port), open_file)
        for slave in slaves:
            self._remove_file(slave, open_file)
        self._unlock_file(filename)
        return True

    def read(self, filename):
        # 读取打开的文件
        if filename in self.open_files.keys():
            local_name = self.open_files[filename]
            path = os.path.join(self.BUCKET_LOCATION, local_name)
            with open(path, "r") as f:
                data = f.read()
            return data
        else:
            return None

    def write(self, filename, data):
        # 写入打开的文件
        if filename in self.open_files.keys():
            local_name = self.open_files[filename]
            path = os.path.join(self.BUCKET_LOCATION, local_name)
            with open(path, "wb+") as f:
                f.write(data)
            return True
        else:
            return False

    def _remove_file(self, server, filename):
        # 移除某个文件副本
        channel = grpc.insecure_channel(server)
        stub = service_pb2_grpc.fileServiceStub(channel)
        reply = stub.Delete(service_pb2.SimpleRequest(text=filename))
        return reply

    def _upload_file(self, server, filename):
        # 发送上传请求
        path = os.path.join(self.BUCKET_LOCATION, filename)
        with open(path, 'r') as f:
            data = f.read()
        channel = grpc.insecure_channel(server)
        stub = service_pb2_grpc.fileServiceStub(channel)
        reply = stub.Upload(service_pb2.DataRequest(text=filename, data=data))
        return reply

    def _download_file(self, server, filename):
        # 发送下载请求
        path = os.path.join(self.BUCKET_LOCATION, filename)

        channel = grpc.insecure_channel(server)
        stub = service_pb2_grpc.fileServiceStub(channel)
        reply = stub.Download(service_pb2.SimpleRequest(text=filename))

        if reply.flag:
            with open(path, "w") as f:
                f.write(reply.data)

        return reply.flag

    def _get_directory(self, filename):
        # 请求文件目录
        channel = grpc.insecure_channel('%s:%s' % (self.DIR_HOST, self.DIR_PORT))
        stub = service_pb2_grpc.directoryServiceStub(channel)
        reply = stub.GetFileServer(service_pb2.SimpleRequest(text=filename))

        return reply

    def _lock_file(self, filename):
        # 锁定一个文件
        channel = grpc.insecure_channel('%s:%s' % (self.LOCK_HOST, self.LOCK_PORT))
        stub = service_pb2_grpc.lockServiceStub(channel)
        while True:
            reply = stub.Lock(service_pb2.SimpleRequest(text=filename))
            if reply.flag:
                # 上锁成功
                return reply.text
            else:
                # 上锁失败，延迟一段时间
                request_data = reply.text.splitlines()
                wait_time = float(request_data[1].split()[1])
                time.sleep(wait_time)

    def _unlock_file(self, filename):
        # 解锁一个文件
        channel = grpc.insecure_channel('%s:%s' % (self.LOCK_HOST, self.LOCK_PORT))
        stub = service_pb2_grpc.lockServiceStub(channel)
        reply = stub.Unlock(service_pb2.SimpleRequest(text=filename))
        return reply


def main():
    # 客户端主程序
    client = Client()
    while True:
        user_input = input(">> ")
        if user_input.lower() == "exit":
            return
        elif re.match(OPEN_REGEX, user_input.lower()):
            # 打开文件
            request = user_input.lower()
            file_name = request.split()[1]
            if client.open(file_name):
                # 打开成功
                print('Opening file done!')
            else:
                print('Opening file failed!')
        elif re.match(CLOSE_REGEX, user_input.lower()):
            request = user_input.lower()
            file_name = request.split()[1]
            if client.close(file_name):
                # 打开成功
                print('Closing file done!')
            else:
                print('Closing file failed!')
        elif re.match(DELETE_REGEX, user_input.lower()):
            request = user_input.lower()
            file_name = request.split()[1]
            if client.delete(file_name):
                # 删除成功
                print('Deleting file done!')
            else:
                print('Deleting file failed!')
            pass
        else:
            os.system(user_input)


if __name__ == "__main__":
    main()
