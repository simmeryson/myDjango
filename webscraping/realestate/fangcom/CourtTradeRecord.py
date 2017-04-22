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

num_re = re.compile(ur'^-?\d+\.?\d*')
exclude_re = re.compile(ur'-')


#
def drop_tables(db):
    db.drop_table(table_name)


# 总数据
# creprice.cn的房价数据
def create_fangcomCourtTradeRecord_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS fangcomCourtTradeRecord" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "RoomNum VARCHAR (20), " \
          "FloorInfo VARCHAR (20), " \
          "Direction VARCHAR (10), " \
          "Area FLOAT , " \
          "TradeDate DATE , " \
          "TradePrice INT , " \
          "PricePerMeter INT , " \
          "nameId INT NOT NULL , " \
          "FOREIGN Key(nameId) REFERENCES  fangcomSecondHandCourtName(id) on delete cascade on update cascade, " \
          "PRIMARY KEY(id)," \
          "UNIQUE KEY (Area, TradeDate, TradePrice, PricePerMeter)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_fangcomCourtTradeRecord_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into fangcomCourtTradeRecord " \
          "(RoomNum , " \
          "FloorInfo, " \
          "Direction , " \
          "Area , " \
          "TradeDate  , " \
          "TradePrice , " \
          "PricePerMeter , " \
          "nameId" \
          ") " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    return sql


def insert_fangcomCourtTradeRecord_value(row):
    return (row[0], row[1] or None, row[2] or None, row[3] or None,
            row[4] or None, row[5] or None, row[6] or None,
            row[7] or None
            )


def parse_trade_html(html, court_id, db):
    try:
        record_list = html.find('div', class_='dealSent sentwrap').find_all('tr')
        for line in record_list[1:]:
            row = []
            roomInfo = line.find('td', class_='firsttd')
            number = roomInfo.div.find('a', attrs={'target': '_blank'})
            row.append(number.string.encode('utf-8').strip())
            for floor in number.parent.parent.find_next_siblings('p'):
                row.append(floor.string.encode('utf-8').strip() if floor and floor.string else None)
            for trading in roomInfo.find_next_siblings('td')[:-1]:
                string = trading.b.string.encode('utf-8').strip() if trading.b \
                    else trading.string.encode('utf-8').strip()
                ex = exclude_re.findall(string, 0)
                if len(ex) > 0:
                    row.append(string)
                else:
                    b = num_re.findall(string, 0)
                    row.append(b[0] if len(b) > 0 else '0')

            row.append(court_id)
            if len(row) == 8:
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
    # drop_tables(db)
    db.create_table(create_fangcomCourtTradeRecord_table_sql())
    court_list = db.query('select id, name, DetailUrl from fangcomSecondHandCourtName ')
    for (court_id, court_name, court_url) in court_list:
        print '小区:' + court_name + "  开始获取详情"
        url = court_url.replace('esf/', trade_url) if court_url.find('esf') != -1 else court_url + trade_url
        if url.find('house-xm') != -1:
            continue
        scraper = Scraping(url)
        try:
            send_request(court_id, court_url, db, url, scraper)
        except:
            print "error ->> id:" + str(court_id) + "  " + str(court_url)


def send_request(court_id, court_url, db, url, scraper):
    scraper.url = url
    scraper.make_header_para({'Referer': court_url})
    html = scraper.send_request_get()
    parse_trade_html(html, court_id, db)
    time.sleep(0.5)
    next_page = html.find('a', id='ctl00_hlk_next')
    if next_page:
        next_url = next_page['href']
        send_request(court_id, url, db, next_url, scraper)


def scraping_today():
    db = DbManager()

    for city, db_name in city_list:
        db.create_db(db_name)
        scrap_detail(db)

    db.close_db()


scraping_today()
