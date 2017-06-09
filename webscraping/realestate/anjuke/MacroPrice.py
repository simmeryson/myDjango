# -*- coding:utf-8 -*-
# fang.com 小区数据, 日查询.
import re
import sys
import json

import bs4
from concurrent import futures

from webscraping.proxy.ProxyProvider import ProxyProvider

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

city_list = [('xa', 'XianHousePrice', 'https://xa.anjuke.com/market/'),
             ]

table_name = 'anjukeDistricts'
quyu_rx = re.compile(ur'^区域\s')
price_rx = re.compile(ur"(id:'regionChart',[\s\S]*?ydata:)(?P<first>\[(.*?)\])")


# proxyProvider = ProxyProvider()


#
def drop_tables(db):
    db.drop_table(table_name)


# 总数据
# creprice.cn的房价数据
def create_anjukeDistricts_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS anjukeDistricts" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "`name` VARCHAR (50) NOT NULL , " \
          "`belong` VARCHAR (50)," \
          "PRIMARY KEY(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_anjukeDistricts_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "replace into anjukeDistricts " \
          "(`name` , " \
          "`belong`" \
          ") " \
          "VALUES (%s, %s)"
    return sql


def insert_anjukeDistricts_value(row):
    return (row[0], row[1] or None
            )


def insert_anjukeDistricts_dic(dic):
    return (dic['name'], dic['belong']
            )


def create_anjukePriceOfDate_table_sql:
    sql = "CREATE TABLE IF NOT EXISTS anjukePriceOfDate" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "`date` DATE NOT NULL , " \
          "`price` INT (8), " \
          "`district` VARCHAR (50) NOT NULL ," \
          "PRIMARY KEY(id)," \
          "FOREIGN KEY (`district`) REFERENCES anjukeDistricts(`name`)on delete cascade on update cascade, " \
          "UNIQUE KEY (`date`,`district`)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_anjukePriceOfDate_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "replace into anjukePriceOfDate " \
          "(`date` , " \
          "`price`," \
          "`district`" \
          ") " \
          "VALUES (%s, %s, %s)"
    return sql


def insert_anjukePriceOfDate_value(row):
    return (row[0], row[1] or None, row[2]
            )


def insert_anjukePriceOfDate_dic(dic):
    return (dic['date'], dic['price'], dic['district']
            )


def district_price(dist_dic, scraper):
    for (name, url) in dist_dic:
        scraper.url = url
        # html = scraper.send_request_get()
        html = scraper.send_get_return_text()
        match = re.search(price_rx, html.encode('utf-8'))
        price = match.group('first') + "}"
        data = json.loads(price[1:] if price.startswith('[') else price)
        data_list = data['data']


def getDistricts(db, url):
    drop_tables(db)
    db.create_table(create_anjukeDistricts_table_sql())
    scraper = Scraping(url)
    html = scraper.send_request_get()
    quyu = html.find_all(string=quyu_rx)
    if len(quyu) < 1:
        return
    span = quyu[0].parent
    dist_dic = []
    for a in span.find_next_siblings('a'):
        key = a.string.encode('utf-8').strip()
        val = a['href']
        dist_dic.append((key, val))
        row = [key, None]
        db.insert_db_values(insert_anjukeDistricts_sql_values(), insert_anjukeDistricts_value(row))
    district_price(dist_dic, scraper)


def scraping_today():
    db = DbManager()

    for city, db_name, url in city_list:
        db.create_db(db_name)
        getDistricts(db, url)

    db.close_db()


scraping_today()
