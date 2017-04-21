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

    def select_db(self, name):
        self.conn.select_db(name)
        self.cursor.execute("set character_set_server='utf8'")
        self.cursor.execute("set character_set_database='utf8'")
        self.cursor.execute("SELECT VERSION()")
        data = self.cursor.fetchone()
        print "Database version : %s " % data

    def create_db(self, db_name):
        self.cursor.execute("CREATE DATABASE IF NOT EXISTS %s"
                            # " CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'"
                            % db_name)
        self.select_db(db_name)

    def drop_db(self, db_name):
        drop_sql = "DROP DATABASE IF EXISTS `%s`" % db_name
        self.cursor.execute(drop_sql)

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

    def insert_tables_list(self, sql_list):
        for (sql, value) in sql_list:
            self.cursor.execute(sql, value)
        self.conn.commit()

    def query(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()
