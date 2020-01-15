# -*- encoding: utf-8 -*-
"""
@File    : lockServer.py
@Time    : 1/15/20 9:32 AM
@Author  : Guo Junnan
@Email   : 529931457@qq.com
@Software: PyCharm
"""


# 导入相关模块
import sys
import time
import grpc
import service_pb2
import service_pb2_grpc
import sqlite3 as db
from concurrent import futures


# 实现锁服务器
class LockServer(service_pb2_grpc.lockServiceServicer):
    LOCK_RESPONSE = "LOCK_RESPONSE: \nFILENAME: %s\nTIME: %d\n\n"
    FAIL_RESPONSE = "ERROR: %d\nMESSAGE: %s\n\n"
    DATABASE = 'Database/locking.db'
    HOST = "0.0.0.0"
    PORT = 9002
    DEFAULT_LOCK_TIME = 10

    def __init__(self, port=None):
        # 初始化函数
        if port:
            self.PORT = port
        self._create_table()

    def Lock(self, request, context):
        # 为文件加锁
        lock_time = self._lock_file(request.text, self.DEFAULT_LOCK_TIME)
        if lock_time:
            reply = self.LOCK_RESPONSE % (request.text, lock_time)
            return service_pb2.SimpleReply(text=reply, flag=True)
        else:
            reply = self.FAIL_RESPONSE % (0, str(self.DEFAULT_LOCK_TIME))
            return service_pb2.SimpleReply(text=reply, flag=False)

    def Unlock(self, request, context):
        # 给文件解锁
        lock_time = self._unlock_file(request.text)
        reply = self.LOCK_RESPONSE % (request.text, lock_time)
        return service_pb2.SimpleReply(text=reply, flag=True)

    def _lock_file(self, path, duration):
        # 尝试为文件加锁
        # 连接数据库
        con = db.connect(self.DATABASE)
        # 隔离等级为EXCLUSIVE
        con.isolation_level = 'EXCLUSIVE'
        con.execute('BEGIN EXCLUSIVE')
        current_time = int(time.time())
        end_time = current_time + duration
        # 检查是否可加锁
        cur = con.cursor()
        cur.execute("SELECT count(*) FROM Locks WHERE Path = ? AND Time > ?", (path, current_time))
        count = cur.fetchone()[0]
        if count is 0:
            cur.execute("INSERT INTO Locks (Path, Time) VALUES (?, ?)", (path, end_time))
            return_time = end_time
        else:
            return_time = False
        # 提交事务并关闭连接
        con.commit()
        con.close()
        return return_time

    def _unlock_file(self, path):
        # 给文件解锁
        # 连接数据库
        con = db.connect(self.DATABASE)
        # 隔离等级为EXCLUSIVE
        con.isolation_level = 'EXCLUSIVE'
        con.execute('BEGIN EXCLUSIVE')
        current_time = int(time.time())
        # 检查是否存在锁
        cur = con.cursor()
        cur.execute("SELECT count(*) FROM Locks WHERE Path = ? AND Time > ?", (path, current_time))
        count = cur.fetchone()[0]
        # 如果有锁则将锁置为过期
        if count is 0:
            cur.execute("UPDATE Locks SET Time=? WHERE Path = ? AND Time > ?", (current_time, path, current_time))
        # 提交事务并关闭连接
        con.commit()
        con.close()
        return current_time

    def _create_table(self):
        # 创建表来存储锁
        con = db.connect(self.DATABASE)
        with con:
            cur = con.cursor()
            cur.execute("CREATE TABLE IF NOT EXISTS Locks(Id INTEGER PRIMARY KEY, Path TEXT, Time INT)")
            cur.execute("CREATE INDEX IF NOT EXISTS PATHS ON Locks(Path)")


def main():
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        port = int(sys.argv[1])
    else:
        port = 9002

    # 开启服务器
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_lockServiceServicer_to_server(LockServer(port), server)
    server.add_insecure_port('0.0.0.0:{}'.format(port))
    server.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    main()
