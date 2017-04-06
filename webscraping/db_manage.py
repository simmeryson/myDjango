# -*- coding:utf-8 -*-
import MySQLdb
import sys

USER_NAME = 'root'
PSSWRD = 'kai123'


class DbManager(object):
    def __init__(self):
        reload(sys)
        sys.setdefaultencoding('utf-8')

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

    def create_db(self, db_name):
        self.cursor.execute("CREATE DATABASE IF NOT EXISTS %s" % db_name)
        self.conn.select_db(db_name)
        self.cursor.execute("SELECT VERSION()")
        data = self.cursor.fetchone()
        print "Database version : %s " % data

    def create_table(self, sql):
        self.cursor.execute(sql)

    def drop_table(self, table_name):
        drop_sql = "DROP TABLE IF EXISTS `%s`" % table_name
        self.cursor.execute(drop_sql)

    def insert_db(self, sql):
        self.cursor.execute(sql)
        self.conn.commit()

    def insert_db_values(self, sql, values):
        self.cursor.execute(sql, values)
        self.conn.commit()

    def query(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()
