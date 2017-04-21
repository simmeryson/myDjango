# -*- coding:utf-8 -*-
# fang.com 小区基本信息, 日查询.
import re
import sys
from HTMLParser import HTMLParseError
from threading import Thread

import MySQLdb
import bs4

from webscraping.db_manage import DbManager

sys.path.append('../../..')
sys.path.append('../..')

table_name = 'fangcomCourtBasicInfo'
db_name = 'XianHousePrice'


class CourtBasicInfo(Thread):
    rex = re.compile(ur'基本信息')

    rex_list = [ur'小区地址',
                ur'所属区域',
                ur'邮.*编',
                ur'环线位置',
                ur'产权描述',
                ur'物业类别',
                ur'竣工时间',
                ur'开 发 商',
                ur'建筑类别',
                ur'建筑面积',
                ur'占地面积',
                ur'当期户数',
                ur'总 户 数',
                ur'绿 化 率',
                ur'容 积 率',
                ur'物业办公电话',
                ur'物 业 费',
                ur'附加信息',
                ur'物业办公地点', ]

    def __init__(self, html, name_id):
        super(CourtBasicInfo, self).__init__()
        self.html = html
        self.name_id = name_id

    def run(self):
        self.parse_html(self.html, self.name_id)

    def create_table(self):
        sql = "CREATE TABLE IF NOT EXISTS fangcomCourtBasicInfo" \
              "(id int(11) NOT NULL AUTO_INCREMENT," \
              "`小区地址` VARCHAR (200), " \
              "`所属区域` VARCHAR (100), " \
              "`邮编` INT (10), " \
              "`环线位置` VARCHAR (30), " \
              "`产权描述` VARCHAR (100), " \
              "`物业类别` VARCHAR (20), " \
              "`竣工时间` VARCHAR (20), " \
              "`开 发 商` VARCHAR (100), " \
              "`建筑类别` VARCHAR (100), " \
              "`建筑面积` VARCHAR (30), " \
              "`占地面积` VARCHAR (50), " \
              "`当期户数` VARCHAR (20), " \
              "`总 户 数` VARCHAR (20), " \
              "`绿 化 率` VARCHAR (10), " \
              "`容 积 率` FLOAT , " \
              "`物业办公电话` VARCHAR (30), " \
              "`物 业 费` VARCHAR (10), " \
              "`附加信息` VARCHAR (50), " \
              "`物业办公地点` VARCHAR (50), " \
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
        sql = "insert into fangcomCourtBasicInfo " \
              "(`小区地址`, " \
              "`所属区域` , " \
              "`邮编` , " \
              "`环线位置` , " \
              "`产权描述` , " \
              "`物业类别` , " \
              "`竣工时间` , " \
              "`开 发 商`, " \
              "`建筑类别` , " \
              "`建筑面积` , " \
              "`占地面积` , " \
              "`当期户数` , " \
              "`总 户 数`, " \
              "`绿 化 率`, " \
              "`容 积 率`," \
              "`物业办公电话`," \
              "`物 业 费`, " \
              "`附加信息` , " \
              "`物业办公地点` , " \
              "nameId  " \
              ")" \
              "VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s," \
              "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        return sql

    def insert_table_value(self, row):
        return (row[0], row[1] or None, row[2] or None, row[3] or None,
                row[4] or None, row[5] or None, row[6] or None, row[7] or None,
                row[8] or None, row[9] or None, row[10] or None, row[11] or None,
                row[12] or None, row[13] or None, row[14] or None, row[15] or None,
                row[16] or None, row[17] or None, row[18] or None, row[19]
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
            if len(row) == 20:
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
