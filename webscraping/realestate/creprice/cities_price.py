# -*- coding:utf-8 -*-
#
import sys

sys.path.append('../../..')

from HTMLParser import HTMLParseError

import MySQLdb
import time

from scraping import Scraping
from webscraping.db_manage import DbManager

reload(sys)
sys.setdefaultencoding('utf8')

DB_NAME = 'XianHousePrice'

url = "http://www.creprice.cn/market/chartsdata.html"

post_para = {'city': 'xa', 'proptype': 11, 'district': '', 'sinceyear': 1, 'flag': 1, 'matchrand': 'a0b92382',
             'based': 'price', 'dtype': 'line'
             }

city_list = [('xa', 'XianHousePrice'), ('bj', 'BeijingHousePrice'), ('sh', 'ShanghaiHousePrice'),
             ('sz', 'ShenzhenHousePrice'), ('gz', 'GuangzhouHousePrice'), ('cd', 'ChengduHousePrice'),
             ('hz', 'HangzhouHousePrice'),
             ]

mounth_date = ""


# 创建表crepriceMonthPrice
# creprice.cn的房价数据
def create_crepriceMonthPrice_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS crepriceMonthPrice" \
          "(date VARCHAR (20) NOT NULL , " \
          "SupplyPrice INT (10)," \
          "FocusPrice INT (10)," \
          "PRIMARY KEY(date)" \
          ")" \
          "ENGINE=InnoDB "

    return sql


def insert_crepriceMonthPrice_value(row):
    return (row[0], row[1] or None, row[2] or None
            )


def insert_crepriceMonthPrice_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into crepriceMonthPrice (date, SupplyPrice, FocusPrice)" \
          " VALUES (%s, %s, %s)"
    return sql


# 解析月度房价数据
def parse_price_html(html, save_row, insert_into):
    try:
        data_lines = html.find('body').string.encode('utf-8').split('\n')

        start = data_lines.index('#CHARTSDATA') + 1
        end = data_lines.index('#CHARTSDATAEND')

        for line in data_lines[start:end]:
            l = line.split(',')
            row = [s for s in l]
            if len(row) == 3:
                insert_into(save_row(), insert_crepriceMonthPrice_value(row))
            else:
                print "wrong row size: " + " ".join(row)
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], 'crepriceMonthPrice')
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], 'crepriceMonthPrice')
    finally:
        print "%s 抓取完成" % 'crepriceMonthPrice'


# 创建表crepricePriceHistogram
# creprice.cn的房价的价格分布
def create_crepricePriceHistogram_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS crepricePriceHistogram" \
          "(date VARCHAR (20) NOT NULL , " \
          "PriceRange VARCHAR (30) NOT NULL ," \
          "SupplyProportion FLOAT ," \
          "FocusProportion FLOAT ," \
          "PRIMARY KEY(date, PriceRange)" \
          ")" \
          "ENGINE=InnoDB "
    return sql


def insert_crepricePriceHistogram_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into crepricePriceHistogram (date, PriceRange, SupplyProportion, FocusProportion)" \
          " VALUES (%s, %s, %s, %s)"
    return sql


def insert_crepricePriceHistogram_value(row):
    return (row[0], row[1], row[2] or None, row[3] or None
            )


def parse_histogram_html(html, save_row, insert_into):
    global mounth_date
    try:
        data_lines = html.find('body').string.encode('utf-8').split('\n')
        start = data_lines.index('#CHARTSDATA') + 1
        end = data_lines.index('#CHARTSDATAEND')
        for line in data_lines[start:end]:
            l = line.split(',')
            row = [s for s in l]
            row.insert(0, mounth_date)
            if len(row) == 4:
                insert_into(save_row(), insert_crepricePriceHistogram_value(row))
            else:
                print "wrong row size: " + " ".join(row)
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], 'crepriceMonthPrice')
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], 'crepriceMonthPrice')
    finally:
        print "%s 抓取完成" % 'crepricePriceHistogram'


# 抓取曲线数据
def scrap_data(db, scraper, city):
    post_para['city'] = city
    post_para['dtype'] = 'line'
    # db.drop_table('crepriceMonthPrice')
    db.create_table(create_crepriceMonthPrice_table_sql())
    price_html = scraper.send_request_get(post_para)
    parse_price_html(price_html, insert_crepriceMonthPrice_sql_values, db.insert_db_values)

    global mounth_date
    mounth_date = db.query('select date from crepriceMonthPrice')[-1][0]

    # db.drop_table('crepricePriceHistogram')
    db.create_table(create_crepricePriceHistogram_table_sql())
    post_para['dtype'] = 'bar'
    histogram_html = scraper.send_request_get(post_para)
    parse_histogram_html(histogram_html, insert_crepricePriceHistogram_sql_values, db.insert_db_values)
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
