# -*- coding:utf-8 -*-
# fang.com 小区配套设施, 日查询.
import re
import sys
from HTMLParser import HTMLParseError
from threading import Thread

import MySQLdb
import bs4

from webscraping.db_manage import DbManager

sys.path.append('../../..')
sys.path.append('../..')

table_name = 'fangcomCourtFacility'
db_name = 'XianHousePrice'


class CourtFacility(Thread):
    rex = re.compile(ur'配套设施')

    rex_list = [
        ur'供.*水',
        ur'供.*暖',
        ur'供.*电',
        ur'燃.*气',
        ur'通讯设备',
        ur'电梯服务',
        ur'安全管理',
        ur'卫生服务',
        ur'停 车 位',
        ur'小区入口',
    ]

    def __init__(self, html, name_id):
        self.html = html
        self.name_id = name_id
        super(CourtFacility, self).__init__()

    def run(self):
        self.parse_html(self.html, self.name_id)

    def create_table(self):
        sql = "CREATE TABLE IF NOT EXISTS fangcomCourtFacility" \
              "(id int(11) NOT NULL AUTO_INCREMENT," \
              "`供水` VARCHAR (100), " \
              "`供暖` VARCHAR (100), " \
              "`供电` VARCHAR (100), " \
              "`燃气` VARCHAR (100), " \
              "`通讯设备` VARCHAR (100), " \
              "`电梯服务` VARCHAR (100), " \
              "`安全管理` VARCHAR (100), " \
              "`卫生服务` VARCHAR (100), " \
              "`停 车 位` VARCHAR (100), " \
              "`小区入口` VARCHAR (100), " \
              "nameId INT NOT NULL , " \
              "FOREIGN Key(nameId) REFERENCES  fangcomSecondHandCourtName(id) on delete cascade on update cascade, " \
              "PRIMARY KEY(id)," \
              "UNIQUE KEY(nameId)" \
              ")" \
              "ENGINE=InnoDB " \
              "AUTO_INCREMENT=1 " \
              "DEFAULT CHARSET=utf8"
        return sql

    def insert_table_sql(self):
        sql = "insert into fangcomCourtFacility " \
              "(`供水` , " \
              "`供暖` , " \
              "`供电` , " \
              "`燃气` ," \
              "`通讯设备` , " \
              "`电梯服务` , " \
              "`安全管理` , " \
              "`卫生服务` , " \
              "`停 车 位` , " \
              "`小区入口` , " \
              "nameId  " \
              ")" \
              "VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s," \
              "%s)"

        return sql

    def insert_table_value(self, row):
        return (row[0], row[1] or None, row[2] or None, row[3] or None,
                row[4] or None, row[5] or None, row[6] or None, row[7] or None,
                row[8] or None, row[9] or None, row[10]
                )

    def parse_html(self, html, name_id):
        global db
        try:
            db = DbManager()
            db.select_db(db_name)
            # db.drop_table(table_name)
            db.create_table(self.create_table())

            div = html.find(string=self.rex).parent.parent.find_next_sibling('div')
            row = []
            for rx in self.rex_list:
                rex_ = re.compile(rx)
                element = div.find(string=rex_)
                row.append(element.parent.nextSibling.encode('utf-8').strip() if element else None)
            row.append(name_id)
            if len(row) == 11:
                db.insert_db_values(self.insert_table_sql(), self.insert_table_value(row))
            else:
                print "wrong row size: " + " ".join(row)
        except MySQLdb.Error, e:
            print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], table_name)
        except HTMLParseError, e:
            print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], table_name)
        finally:
            print "%s 抓取完成" % table_name
            db.close_db()
