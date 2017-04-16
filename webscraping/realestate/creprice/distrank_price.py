# -*- coding:utf-8 -*-
# creprice 城市各区县房价, 月查询.
import re
import sys

import bs4

sys.path.append('../../..')

from HTMLParser import HTMLParseError

import MySQLdb
import time

from scraping import Scraping
from webscraping.db_manage import DbManager

reload(sys)
sys.setdefaultencoding('utf8')

url = "http://www.creprice.cn/market/distrank/city/%s.html?flag=1"

city_list = [('xa', 'XianHousePrice'), ('bj', 'BeijingHousePrice'), ('sh', 'ShanghaiHousePrice'),
             ('sz', 'ShenzhenHousePrice'), ('gz', 'GuangzhouHousePrice'), ('cd', 'ChengduHousePrice'),
             ('hz', 'HangzhouHousePrice'),
             ]


def create_crepriceSecondHandDistRank_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS crepriceSecondHandDistRank " \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date VARCHAR (20) NOT NULL , " \
          "DistrictName VARCHAR (20)," \
          "AveragePrice VARCHAR (20)," \
          "MoM VARCHAR (20)," \
          "YoY VARCHAR (20)," \
          "PRIMARY KEY(id)," \
          "Unique Key(date)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"

    return sql


def insert_crepriceSecondHandDistRank_value(row):
    return (row[0], row[1] or None, row[2] or None, row[3] or None,
            row[4] or None
            )


def insert_crepriceSecondHandDistRank_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into crepriceSecondHandDistRank (" \
          "date, " \
          "DistrictName, " \
          "AveragePrice, " \
          "MoM, " \
          "YoY)" \
          " VALUES (%s, %s, %s, %s, %s)"
    return sql


def parse_html(html, insert_db_values):
    try:
        date_h2 = html.find('h2', class_='ranktit')
        date = date_h2.string.encode('utf-8').strip()
        for tr in date_h2.parent.find_next_sibling('div').find('tbody', attrs={'id': 'order_f'}).find_all('tr'):
            row_ = []
            for td in tr.find_all('td'):
                string = td.string.encode('utf-8').strip()
                row_.append(string if string != '--' else None)
            row = [date]
            row.extend(row_[1:])
            if len(row) == 5:
                insert_db_values(insert_crepriceSecondHandDistRank_sql_values(),
                                 insert_crepriceSecondHandDistRank_value(row))
            else:
                print "wrong row size: " + " ".join(row)
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], 'crepriceSecondHandDistRank')
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], 'crepriceSecondHandDistRank')
    finally:
        print "%s 抓取完成" % 'crepriceSecondHandDistRank'


def scrap_data(db, scraper, city):
    # db.drop_table('crepriceSecondHandDistRank')
    db.create_table(create_crepriceSecondHandDistRank_table_sql())
    scraper.url = url % city
    html = scraper.send_request_get({})
    parse_html(html, db.insert_db_values)

    time.sleep(2)


# 抓取当天数据
def scraping_today():
    scraper = Scraping(url)
    db = DbManager()

    for city, db_name in city_list:
        db.create_db(db_name)
        scrap_data(db, scraper, city)

    db.close_db()


scraping_today()
