# -*- coding:utf-8 -*-
import MySQLdb
import time
import sys

USER_NAME = 'root'
PSSWRD = 'kai123'


class DbManager(object):
    def __init__(self, create_table, create_db, insert_db):
        reload(sys)
        sys.setdefaultencoding('utf-8')

        self.create_db_ = create_db
        self.create_table_ = create_table
        self.insert_db_ = insert_db

        try:
            self.conn = MySQLdb.connect(host='127.0.0.1', user=USER_NAME, passwd=PSSWRD, connect_timeout=10,
                                        charset='utf8')
            # 使用cursor()方法获取操作游标
            self.cursor = self.conn.cursor()
            self.cursor.execute('SET NAMES utf8')
        except MySQLdb.Error, e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])

    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def create_db(self):
        self.create_db_(self.conn, self.cursor)

    def create_table(self):
        self.create_table_(self.cursor)

    def insert_db(self, row):
        self.insert_db_(self.cursor, self.conn, row)
