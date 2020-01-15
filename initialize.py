# -*- encoding: utf-8 -*-
"""
@File    : initialize.py
@Time    : 1/16/20 4:27 AM
@Author  : Guo Junnan
@Email   : 529931457@qq.com
@Software: PyCharm
"""

import sqlite3 as db

DATABASE = "Database/directories.db"
con = db.connect(DATABASE)
with con:
    cur = con.cursor()
    cur.execute('INSERT INTO Servers (Server, Port) VALUES ("0.0.0.0", 10001)')
    cur.execute('INSERT INTO Servers (Server, Port) VALUES ("0.0.0.0", 10002)')
    cur.execute('INSERT INTO Servers (Server, Port) VALUES ("0.0.0.0", 10003)')
    con.commit()
    cur.close()
