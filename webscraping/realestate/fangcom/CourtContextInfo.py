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

table_name = 'fangcomCourtContextInfo'
db_name = 'XianHousePrice'


class CourtContextInfo(Thread):
    rex = re.compile(ur'周边信息')
    commuting = re.compile(ur'交通状况')
    rex_list = {
        r'幼儿园': None,
        r'中小学': None,
        r'大学': None,
        r'商场': None,
        r'医院': None,
        r'邮局': None,
        r'银行': None,
        r'其他': None,
        r'小区内部配套': None,
    }

    def __init__(self, html, name_id):
        super(CourtContextInfo, self).__init__()
        self.html = html
        self.name_id = name_id

    def run(self):
        self.parse_html(self.html, self.name_id)

    def create_table(self):
        sql = "CREATE TABLE IF NOT EXISTS fangcomCourtContextInfo" \
              "(id int(11) NOT NULL AUTO_INCREMENT," \
              "`幼儿园` VARCHAR (200), " \
              "`中小学` VARCHAR (200), " \
              "`大学` VARCHAR (200), " \
              "`商场` VARCHAR (200), " \
              "`医院` VARCHAR (200), " \
              "`邮局` VARCHAR (200), " \
              "`银行` VARCHAR (200), " \
              "`其他` VARCHAR (200), " \
              "`小区内部配套` VARCHAR (200), " \
              "`交通状况` VARCHAR (500), " \
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
        sql = "insert into fangcomCourtContextInfo " \
              "(`幼儿园` , " \
              "`中小学` , " \
              "`大学` , " \
              "`商场` , " \
              "`医院` , " \
              "`邮局` , " \
              "`银行` , " \
              "`其他` , " \
              "`小区内部配套` , " \
              "`交通状况` , " \
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
            db.drop_table(table_name)
            db.create_table(self.create_table())

            div = html.find(string=self.rex).parent.parent.find_next_sibling('div')
            for dt in div.find_all('dt'):
                pare = dt.string.encode('utf-8').strip().split('：')
                pare = pare if len(pare) == 2 else dt.string.encode('utf-8').strip().split(':')
                self.rex_list[pare[0]] = pare[1]

            row = [self.rex_list[r'幼儿园'], self.rex_list[r'中小学'], self.rex_list[r'大学'], self.rex_list[r'商场'],
                   self.rex_list[r'医院'], self.rex_list[r'邮局'], self.rex_list[r'银行'], self.rex_list[r'其他'],
                   self.rex_list[r'小区内部配套']]

            commut_div = html.find(string=self.commuting)
            jiaotong = commut_div.parent.parent.find_next_sibling('div').dl.dt if commut_div else []
            string_line = []
            for string in jiaotong.contents:
                if type(string) == bs4.element.NavigableString:
                    string_line.append(string.encode('utf-8').strip())
            row.append('\r\n'.join(string_line) if len(string_line) > 0 else None)
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
