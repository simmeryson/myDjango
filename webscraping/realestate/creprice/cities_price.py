# -*- coding:utf-8 -*-
# creprice 房价数据, 日查询.
import re
import sys

import bs4

sys.path.append('../../..')
sys.path.append('../..')

from HTMLParser import HTMLParseError

import MySQLdb
import time

from scraping import Scraping
from webscraping.db_manage import DbManager

reload(sys)
sys.setdefaultencoding('utf8')

DB_NAME = 'XianHousePrice'

url = "http://www.creprice.cn/market/chartsdata.html"

page_url = "http://www.creprice.cn/market/xa/forsale/ALL/11.html"

post_para = {'city': 'xa', 'proptype': 11, 'district': '', 'sinceyear': 1, 'flag': 1, 'matchrand': 'a0b92382',
             'based': 'price', 'dtype': 'line'
             }

city_list = [('xa', 'XianHousePrice'), ('bj', 'BeijingHousePrice'), ('sh', 'ShanghaiHousePrice'),
             ('sz', 'ShenzhenHousePrice'), ('gz', 'GuangzhouHousePrice'), ('cd', 'ChengduHousePrice'),
             ('hz', 'HangzhouHousePrice'),
             ]

mounth_date = ""


# 创建表crepriceSecondHandMonthPrice
# creprice.cn的房价数据
def create_crepriceSecondHandMonthPrice_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS crepriceSecondHandMonthPrice" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date VARCHAR (20) NOT NULL , " \
          "SupplyPrice INT (10)," \
          "FocusPrice INT (10)," \
          "PRIMARY KEY(id)," \
          "Unique Key(date)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"

    return sql


def insert_crepriceSecondHandMonthPrice_value(row):
    return (row[0], row[1] or None, row[2] or None
            )


def insert_crepriceSecondHandMonthPrice_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into crepriceSecondHandMonthPrice (date, SupplyPrice, FocusPrice)" \
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
                insert_into(save_row(), insert_crepriceSecondHandMonthPrice_value(row))
            else:
                print "wrong row size: " + " ".join(row)
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], 'crepriceSecondHandMonthPrice')
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], 'crepriceSecondHandMonthPrice')
    finally:
        print "%s 抓取完成" % 'crepriceSecondHandMonthPrice'


# 创建表crepriceSecondHandPriceHistogram
# creprice.cn的房价的价格分布
def create_crepriceSecondHandPriceHistogram_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS crepriceSecondHandPriceHistogram" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date VARCHAR (20) NOT NULL , " \
          "PriceRange INT (10) NOT NULL ," \
          "SupplyProportion FLOAT ," \
          "FocusProportion FLOAT ," \
          "Unique KEY(date, PriceRange)," \
          "PRIMARY Key(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_crepriceSecondHandPriceHistogram_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into crepriceSecondHandPriceHistogram (date, PriceRange, SupplyProportion, FocusProportion)" \
          " VALUES (%s, %s, %s, %s)"
    return sql


def insert_crepriceSecondHandPriceHistogram_value(row):
    return (row[0] or None, row[1] or None, row[2] or None, row[3] or None
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
                insert_into(save_row(), insert_crepriceSecondHandPriceHistogram_value(row))
            else:
                print "wrong row size: " + " ".join(row)
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], 'crepriceSecondHandPriceHistogram')
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], 'crepriceSecondHandPriceHistogram')
    finally:
        print "%s 抓取完成" % 'crepriceSecondHandPriceHistogram'


# 总数据
# creprice.cn的房价数据
def create_crepriceSecondHandMacroPrice_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS crepriceSecondHandMacroPrice" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date DATE NOT NULL , " \
          "TodayPrice VARCHAR (20)," \
          "DoD VARCHAR (10)," \
          "LastMonthPrice VARCHAR (20)," \
          "YoY VARCHAR (10)," \
          "CurrentMonthPrice VARCHAR (20)," \
          "MoM VARCHAR (10) ," \
          "TodayNewSupplyNumber VARCHAR (30)," \
          "SellNumber VARCHAR (20)," \
          "SellValue VARCHAR (20)," \
          "EstateGardenNumber INT (10)," \
          "TradeAmount VARCHAR (20)," \
          "AveragePrice INT (10)," \
          "LastNewSupplyNumber VARCHAR (20)," \
          "AverageArea INT (10)," \
          "PRIMARY KEY(id)," \
          "Unique KEY(date)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_crepriceSecondHandMacroPrice_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into crepriceSecondHandMacroPrice " \
          "(date, " \
          "TodayPrice, " \
          "DoD, " \
          "LastMonthPrice, " \
          "YoY," \
          "CurrentMonthPrice," \
          "MoM," \
          "TodayNewSupplyNumber, " \
          "SellNumber, " \
          "SellValue, " \
          "EstateGardenNumber," \
          "TradeAmount," \
          "AveragePrice," \
          "LastNewSupplyNumber," \
          "AverageArea) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    return sql


def insert_crepriceSecondHandMacroPrice_value(row):
    return (row[0], row[1] or None, row[2] or None, row[3] or None,
            row[4] or None, row[5] or None, row[6] or None,
            row[7] or None, row[8] or None, row[9] or None,
            row[10] or None, row[11] or None, row[12] or None,
            row[13] or None, row[14] or None
            )


def parse_macro_html(html, save_row, insert_into, today):
    try:

        price_div = html.find('div', class_='cityprice_sy1 city-price clearfix')
        row = [today]
        # TodayPrice 当日房价
        div = price_div.find('span', class_='value green')
        if div is not None:
            v = div.string.encode('utf-8').strip()
            row.append(v)
            dod_div = div.parent.find_next_sibling('div')
            dod = dod_div.find('span', class_='vfloat dw')
            v = dod.string.encode('utf-8').strip()
            row.append(v)
            # v = dod_div.find('span', class_='ask')['title'].encode('utf-8').strip()
            # row.append(v)
        else:
            row.extend(['', ''])
        # 上月房价
        div = price_div.find('span', class_='value red')
        v = div.string.encode('utf-8').strip()
        row.append(v)
        yoy_div = div.parent.find_next_sibling('div')
        yoy = yoy_div.find('span', class_='vfloat up')
        v = yoy.string.encode('utf-8').strip()
        row.append(v)
        # v = yoy_div.find('span', class_='ask')['title'].encode('utf-8').strip()
        # row.append(v)

        # 当月房价
        today_div = price_div.find('div', attrs={'class': 'price40'})
        v = today_div.find('span', class_='mr5 numr').string.encode('utf-8').strip()
        row.append(v)
        mom = today_div.find('span', class_='vfloat up')
        v = mom.string.encode('utf-8').strip()
        row.append(v)
        # v = mom['title'].encode('utf-8').strip()
        # row.append(v)

        v = today_div.find_next_sibling('p').span.string.encode('utf-8').strip()
        row.append(v)

        # SellNumber 出售数量
        for span in html.find('ul', class_='gbox clearfix').find_all('span'):
            v = span.string.encode('utf-8').strip()
            row.append(v)

        # EstateGardenNumber 楼盘小区个数
        numbs = re.compile(ur'\b-?\d+\.?\d*\b')
        tao = re.compile(ur'万')
        div = html.find('div', class_='tips-sy1 mb5')
        for s in div.contents:
            if type(s) == bs4.element.NavigableString:
                v = numbs.findall(s.strip(), 0)
                v = v[0] if len(v) > 0 else ''
                b = tao.findall(s.strip(), 0)
                v = v if len(b) == 0 else v + b[0]
                row.append(v)

        # AveragePrice平均总价
        avg_row = []
        for span in div.parent.find_all('span', class_='pricedata'):
            v = span.span.span.string.encode('utf-8').strip()
            avg_row.append(v)
        row.extend(avg_row[1:])

        if len(row) == 15:
            insert_into(save_row(), insert_crepriceSecondHandMacroPrice_value(row))
        else:
            print "wrong row size: " + " ".join(row)

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], 'crepriceSecondHandMacroPrice')
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], 'crepriceSecondHandMacroPrice')
    finally:
        print "%s 抓取完成" % 'crepriceSecondHandMacroPrice'


# 抓取曲线数据
def scrap_data(db, scraper, city):
    post_para['city'] = city
    post_para['dtype'] = 'line'

    # 月均价
    db.create_table(create_crepriceSecondHandMonthPrice_table_sql())
    price_html = scraper.send_request_get(post_para)
    parse_price_html(price_html, insert_crepriceSecondHandMonthPrice_sql_values, db.insert_db_values)

    global mounth_date
    mounth_date = db.query('select date from crepriceSecondHandMonthPrice')[-1][0]

    # 月价格分布
    db.create_table(create_crepriceSecondHandPriceHistogram_table_sql())
    post_para['dtype'] = 'bar'
    histogram_html = scraper.send_request_get(post_para)
    parse_histogram_html(histogram_html, insert_crepriceSecondHandPriceHistogram_sql_values, db.insert_db_values)

    # 房价总体数据
    today = scraper.get_today_date()
    scraper.url = page_url
    db.create_table(create_crepriceSecondHandMacroPrice_table_sql())
    macro_html = scraper.send_request_get({})
    parse_macro_html(macro_html,
                     insert_crepriceSecondHandMacroPrice_sql_values,
                     db.insert_db_values,
                     today)
    scraper.url = url

    time.sleep(2)


#
def drop_tables(db):
    db.drop_table('crepriceSecondHandMonthPrice')
    db.drop_table('crepriceSecondHandPriceHistogram')
    db.drop_table('crepriceSecondHandMacroPrice')


# 抓取当天数据
def scraping_today():
    scraper = Scraping(url)
    db = DbManager()

    for city, db_name in city_list:
        db.create_db(db_name)
        scrap_data(db, scraper, city)

    db.close_db()


scraping_today()
