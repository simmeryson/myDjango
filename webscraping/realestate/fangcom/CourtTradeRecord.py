# -*- coding:utf-8 -*-
# fang.com 小区数据, 日查询.
import re
import sys

import bs4

from CourtBasicInfo import CourtBasicInfo
from CourtFacility import CourtFacility
from CourtContextInfo import CourtContextInfo

sys.path.append('../../..')
sys.path.append('../..')

from HTMLParser import HTMLParseError

import MySQLdb
import time

from scraping import Scraping
from webscraping.db_manage import DbManager

reload(sys)
sys.setdefaultencoding('utf8')

# 住宅 小区
trade_url = 'chengjiao/-p11-t11/'
rent_url = 'chengjiao/-p11-t12/'

city_list = [('xa', 'XianHousePrice'),
             # ('bj', 'BeijingHousePrice'), ('sh', 'ShanghaiHousePrice'),
             # ('sz', 'ShenzhenHousePrice'), ('gz', 'GuangzhouHousePrice'), ('cd', 'ChengduHousePrice'),
             # ('hz', 'HangzhouHousePrice'),
             ]

table_name = 'fangcomCourtTradeRecord'


#
def drop_tables(db):
    db.drop_table(table_name)


# 总数据
# creprice.cn的房价数据
def create_fangcomCourtTradeRecord_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS fangcomCourtTradeRecord" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          " DATE , " \
          "CurPrice INT (10), " \
          "MoM VARCHAR (20), " \
          "YoY VARCHAR (20), " \
          "nameId INT NOT NULL , " \
          "FOREIGN Key(nameId) REFERENCES  fangcomSecondHandCourtName(id) on delete cascade on update cascade, " \
          "PRIMARY KEY(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_fangcomCourtTradeRecord_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into fangcomCourtTradeRecord " \
          "(date," \
          "CurPrice," \
          "MoM," \
          "YoY," \
          "nameId" \
          ") " \
          "VALUES (%s, %s, %s, %s, %s)"
    return sql


def insert_fangcomCourtTradeRecord_value(row):
    return (row[0], row[1] or None, row[2] or None, row[3] or None,
            row[4]
            )


def parse_trade_html(html, today, court_id, db):
    try:
        div = html.find('div', class_='box detaiLtop mt20 clearfix')
        row = [today]
        for dl in div.find_all('dl'):
            row.append(dl.dd.span.string.encode('utf-8').strip())
        row.append(court_id)
        if len(row) == 5:
            db.insert_db_values(insert_fangcomCourtTradeRecord_sql_values(),
                                insert_fangcomCourtTradeRecord_value(row))
        else:
            print "wrong row size: " + " ".join(row)
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], table_name)
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], table_name)
    finally:
        print "%s 抓取完成" % table_name


def scrap_detail(db):
    drop_tables(db)
    db.create_table(create_fangcomCourtTradeRecord_table_sql())
    court_list = db.query('select * from fangcomSecondHandCourtName')
    for (court_id, court_name, court_url) in court_list:
        print '小区:' + court_name + "  开始获取详情"
        url = court_url.replace('esf/', 'xiangqing/')
        scraper = Scraping(url)
        scraper.make_header_para({'Referer': court_url})
        html = scraper.send_request_get()
        today = str(scraper.get_today_date())
        parse_trade_html(html, today, court_id, db)

        time.sleep(0.5)


def scraping_today():
    db = DbManager()

    for city, db_name in city_list:
        db.create_db(db_name)
        scrap_detail(db)

    db.close_db()


scraping_today()
