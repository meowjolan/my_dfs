# -*- encoding: utf-8 -*-
"""
@File    : directoryServer.py
@Time    : 1/15/20 9:32 AM
@Author  : Guo Junnan
@Email   : 529931457@qq.com
@Software: PyCharm
"""


# 导入相关模块
import sys
import os
import hashlib
import random
import grpc
import service_pb2
import service_pb2_grpc
import sqlite3 as db
from concurrent import futures


class DirectoryServer(service_pb2_grpc.directoryServiceServicer):
    GET_RESPONSE = "PRIMARY_SERVER: %s\nPORT: %s\nFILENAME: %s\nSLAVE_STRING: %s\n"
    SLAVE_HEADER = "%s:%s "
    DATABASE = "Database/directories.db"
    PORT = 9001
    HOST = "0.0.0.0"

    def __init__(self, port=None):
        # 初始化函数
        if port:
            self.PORT = port
        self._create_tables()

    def GetFileServer(self, request, context):
        # 根据文件名找到相应的服务器，如果不存在，则返回随机一个
        # 解析路径
        path, file = os.path.split(request.text)
        name, ext = os.path.splitext(file)
        filename = hashlib.sha256(request.text.encode('utf-8')).hexdigest() + ext
        host, port = self._find_host(path)

        if not host:
            # 不存在则创建
            server_id = self._pick_random_host()
            self._create_dir(path, server_id)
            host, port = self._find_host(path)
            flag = False
        else:
            flag = True

        # 获取有备份的其余文件服务器
        slave_string = self._get_slave_string(host, port)
        reply = self.GET_RESPONSE % (host, port, filename, slave_string)
        return service_pb2.SimpleReply(flag=flag, text=reply)

    def GetSlaves(self, request, context):
        # 获取其他文件服务器节点
        host, port = request.text.split(':')
        reply = self._get_slave_string(host, port)
        return service_pb2.SimpleReply(flag=True, text=reply)

    def _find_host(self, path):
        # 根据路径找到相应的文件服务器
        return_host = (False, False)
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("SELECT Server FROM Directories WHERE Path = ?", (path,))
            server = cur.fetchone()
            if server:
                server_id = server[0]
                cur = con.cursor()
                cur.execute("SELECT Server, Port FROM Servers WHERE Id = ?", (server_id,))
                return_host = cur.fetchone()

        if return_host:
            return return_host
        else:
            return None, None

    def _pick_random_host(self):
        # 随机挑选一个文件服务器
        return_host = False
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("SELECT Id FROM Servers")
            servers = cur.fetchall()
            if servers:
                return_host = random.choice(servers)[0]
        return return_host

    def _get_slave_string(self, host, port):
        # 找到其余文件服务器
        return_string = ""
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("SELECT Server, Port FROM Servers WHERE NOT (Server=? AND Port=?)", (host, port,))
            servers = cur.fetchall()
        if servers:
            for (host, port) in servers:
                header = self.SLAVE_HEADER % (host, port)
                return_string = return_string + header
        return return_string + '\n'

    def _create_dir(self, path, host):
        # 添加目录
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Directories (Path, Server) VALUES (?, ?)", (path, host,))
        con.commit()
        con.close()

    def _add_server(self, host, port):
        # 添加文件服务器
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("INSERT INTO Servers (Server, Port) VALUES (?, ?)", (host, port,))
        con.commit()
        con.close()

    def _remove_dir(self, path):
        # 删除目录
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM Directories WHERE Path = ?", (path,))
        con.commit()
        con.close()

    def _remove_server(self, server):
        # 删除文件服务器
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("DELETE FROM Servers WHERE Server = ?", (server,))
        con.commit()
        con.close()

    def _create_tables(self):
        # 创建表来存储文件服务器信息以及目录信息
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS Servers(Id INTEGER PRIMARY KEY, Server TEXT, Port TEXT)")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS SERVS ON Servers(Server, Port)")
            cur.execute("CREATE TABLE IF NOT EXISTS Directories(Id INTEGER PRIMARY KEY, Path TEXT, Server INTEGER, FOREIGN KEY(Server) REFERENCES Servers(Id))")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS DIRS ON Directories(Path)")


def main():
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        port = int(sys.argv[1])
    else:
        port = 9001

    # 开启服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_directoryServiceServicer_to_server(DirectoryServer(port), server)
    server.add_insecure_port('0.0.0.0:{}'.format(port))
    server.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
