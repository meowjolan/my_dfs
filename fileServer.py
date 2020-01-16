# -*- encoding: utf-8 -*-
"""
@File    : fileServer.py
@Time    : 1/15/20 9:31 AM
@Author  : Guo Junnan
@Email   : 529931457@qq.com
@Software: PyCharm
"""


# 导入相关模块
import os
import sys
import grpc
import service_pb2
import service_pb2_grpc
from concurrent import futures


# 实现文件服务器
class FileServer(service_pb2_grpc.fileServiceServicer):
    UPLOAD_DONE = "Upload file successfully!\n"
    UPLOAD_FAIL = "Uploading failed!\n"
    DOWNLOAD_DONE = "Download file successfully!\n"
    DOWNLOAD_FAIL = "Downloading failed!\n"
    UPDATE_DONE = "Update file successfully!\n"
    UPDATE_FAIL = "Updating failed!\n"
    DELETE_DONE = "Delete file successfully!\n"
    SERVER_ROOT = os.getcwd()
    BUCKET_LOCATION = SERVER_ROOT
    HOST = "0.0.0.0"
    PORT = 9003
    DIR_HOST = "0.0.0.0"
    DIR_PORT = 9001

    def __init__(self, port=None):
        # 初始化函数
        if port:
            self.PORT = port

    def Upload(self, request, context):
        # 客户端上传文件到当前文件服务器，并同步更新其他节点
        self._write_file(request.text, request.data)
        self._update_slaves(request)

        return service_pb2.SimpleReply(flag=True, text=self.UPLOAD_DONE)

    def Download(self, request, context):
        # 从服务器上下载文件
        path = os.path.join(self.BUCKET_LOCATION, request.text)
        try:
            with open(path, 'w+') as f:
                data = f.read()
            return service_pb2.DataReply(flag=True, text=self.DOWNLOAD_DONE,
                                         data=data)
        except IOError:
            return service_pb2.DataReply(flag=False, text=self.DOWNLOAD_FAIL,
                                         data=None)

    def Update(self, request, context):
        # 更新服务器上的某个文件
        self._write_file(request.text, request.data)
        return service_pb2.SimpleReply(flag=True, text=self.UPDATE_DONE)

    def Delete(self, request, context):
        # 删除服务器上的文件
        path = os.path.join(self.BUCKET_LOCATION, request.text)
        os.system('rm %s' % path)
        return service_pb2.SimpleReply(flag=True, text=self.DELETE_DONE)

    def _write_file(self, filename, data):
        # 给定文件名，将数据写入
        path = os.path.join(self.BUCKET_LOCATION, filename)
        with open(path, 'w+') as f:
            f.write(data)

    def _update_slaves(self, request):
        # 将更新请求转发给其他文件服务器节点
        slaves = self._get_slaves()
        for address in slaves:
            channel = grpc.insecure_channel(address)
            stub = service_pb2_grpc.fileServiceStub(channel)
            stub.Update(request)

    def _get_slaves(self):
        # 获取其余所有文件服务器节点位置
        channel = grpc.insecure_channel('{}:{}'.format(self.DIR_HOST, self.DIR_PORT))
        stub = service_pb2_grpc.directoryServiceStub(channel)
        response = stub.GetSlaves(service_pb2.SimpleRequest(
            text='{}:{}'.format(self.HOST, self.PORT)))

        return response.text.split()


def main():
    # 获取端口参数（如果有的话）
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        port = int(sys.argv[1])
    else:
        port = 9003

    # 开启服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_fileServiceServicer_to_server(FileServer(port), server)
    server.add_insecure_port('0.0.0.0:{}'.format(port))
    server.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
