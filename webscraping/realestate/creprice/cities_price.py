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
          "AUTO_INCREMENT=1 "

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
          "AUTO_INCREMENT=1 "
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
          "DoDDesc VARCHAR (30)," \
          "LastMonthPrice VARCHAR (20)," \
          "YoY VARCHAR (10)," \
          "YoYDesc VARCHAR (30)," \
          "CurrentMonthPrice VARCHAR (20)," \
          "MoM VARCHAR (10) ," \
          "MoMDesc VARCHAR (30)," \
          "TodayNewSupplyNumber VARCHAR (30)," \
          "SellNumber VARCHAR (20)," \
          "SellValue VARCHAR (20)," \
          "EstateGardenNumber INT (10)," \
          "TradeAmount VARCHAR (20)," \
          "AveragePrice INT (10)," \
          "AverageArea INT (10)," \
          "LastNewSupplyNumber INT (15)," \
          "PRIMARY KEY(id)," \
          "Unique KEY(date)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 "
    return sql


def insert_crepriceSecondHandMacroPrice_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = """insert into crepriceSecondHandMacroPrice (date, TodayPrice, DoD, ,DoDDesc,LastMonthPrice,
    YoY,YoYDesc, CurrentMonthPrice,MoM,MoMDesc,TodayNewSupplyNumber, SellNumber, SellValue, EstateGardenNumber,TradeAmount,
    AveragePrice,AverageArea,LastNewSupplyNumber)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s)"""
    return sql


def insert_crepriceSecondHandMacroPrice_value(row):
    return (row[0], row[1] or None, row[2] or None, row[3] or None,
            row[4] or None, row[5] or None, row[6] or None,
            row[7] or None, row[8] or None, row[9] or None,
            row[10] or None, row[11] or None, row[12] or None,
            row[13] or None, row[14] or None, row[15] or None,
            row[16] or None, row[17] or None
            )


def parse_macro_html(html, save_row, insert_into, today):
    try:

        price_div = html.find('div', class_='cityprice_sy1 city-price clearfix')

        # TodayPrice 当日房价
        v = price_div.find('span', class_='value green').string.encode('utf-8').strip()
        row = [today, v]
        dod = price_div.find('span', class_='vfloat dw')
        v = dod.string.encode('utf-8').strip()
        row.append(v)
        v = dod.find_next_sibling('span').string.encode('utf-8').strip()
        row.append(v)

        # 上月房价
        v = price_div.find('span', class_='value red').string.encode('utf-8').strip()
        row = [v]
        yoy = price_div.find('span', class_='vfloat up')
        v = yoy.string.encode('utf-8').strip()
        row.append(v)
        v = yoy.find_next_sibling('span').string.encode('utf-8').strip()
        row.append(v)

        # 当月房价
        today_div = price_div.find('div', attrs={'class': 'price40'})
        v = today_div.find('span', class_='mr5 numr').string.encode('utf-8').strip()
        row.append(v)
        mom = today_div.find('span', class_='vfloat up')
        v = mom.string.encode('utf-8').strip()
        row.append(v)
        v = mom['title'].encode('utf-8').strip()
        row.append(v)

        v = today_div.find_next_sibling('p').span.string.encode('utf-8').strip()
        row.append(v)

        # SellNumber 出售数量
        for span in html.find('ul', class_='gbox clearfix').find_all('span'):
            v = span.string.encode('utf-8').strip()
            row.append(v)

        # EstateGardenNumber 楼盘小区个数
        div = html.find('div', class_='tips-sy1 mb5')






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


# 抓取曲线数据
def scrap_data(db, scraper, city):
    post_para['city'] = city
    post_para['dtype'] = 'line'
    # # db.drop_table('crepriceSecondHandMonthPrice')
    # db.create_table(create_crepriceSecondHandMonthPrice_table_sql())
    # price_html = scraper.send_request_get(post_para)
    # parse_price_html(price_html, insert_crepriceSecondHandMonthPrice_sql_values, db.insert_db_values)
    #
    # global mounth_date
    # mounth_date = db.query('select date from crepriceSecondHandMonthPrice')[-1][0]
    #
    # # db.drop_table('crepriceSecondHandPriceHistogram')
    # db.create_table(create_crepriceSecondHandPriceHistogram_table_sql())
    # post_para['dtype'] = 'bar'
    # histogram_html = scraper.send_request_get(post_para)
    # parse_histogram_html(histogram_html, insert_crepriceSecondHandPriceHistogram_sql_values, db.insert_db_values)

    today = scraper.get_today_date()
    scraper.url = page_url
    db.drop_table('crepriceSecondHandMacroPrice')
    db.create_table(create_crepriceSecondHandMacroPrice_table_sql())
    macro_html = scraper.send_request_get({})
    parse_macro_html(macro_html,
                     insert_crepriceSecondHandMacroPrice_sql_values,
                     db.insert_db_values,
                     today)
    scraper.url = url
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
