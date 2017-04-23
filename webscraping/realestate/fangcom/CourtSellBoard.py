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
trade_url = 'chushou/list/-h332-j340/'

city_list = [('xa', 'XianHousePrice'),
             # ('bj', 'BeijingHousePrice'), ('sh', 'ShanghaiHousePrice'),
             # ('sz', 'ShenzhenHousePrice'), ('gz', 'GuangzhouHousePrice'), ('cd', 'ChengduHousePrice'),
             # ('hz', 'HangzhouHousePrice'),
             ]

table_name = 'fangcomCourtSellingInfo'

num_re = re.compile(ur'\d*\b')
date_rx = re.compile(ur'\d{4}/\d+/\d+')
maner_re = re.compile(ur'满二')
manwu_re = re.compile(ur'满五')
tejia_re = re.compile(ur'特价')
yezhu_re = re.compile(ur'业主发布')

re_list = [manwu_re, maner_re, tejia_re, yezhu_re]


#
def drop_tables(db):
    db.drop_table(table_name)


# 总数据
# creprice.cn的房价数据
def create_fangcomCourtSellingInfo_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS fangcomCourtSellingInfo" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "HouseNum INT (10), " \
          "PublishDate DATE NOT NULL , " \
          "SellingPrice INT , " \
          "RoomNum VARCHAR (20), " \
          "Area INT (10) NOT NULL , " \
          "FloorInfo VARCHAR (20), " \
          "Direction VARCHAR (10), " \
          "Decor VARCHAR (10), " \
          "HouseHistory VARCHAR (10), " \
          "Telephone VARCHAR (30), " \
          "`满五唯一` TINYINT (1), " \
          "`满二` TINYINT (1), " \
          "`特价房` TINYINT (1)," \
          "`业主发布` TINYINT (1)," \
          "nameId INT NOT NULL ," \
          "FOREIGN Key(nameId) REFERENCES  fangcomSecondHandCourtName(id) on delete cascade on update cascade, " \
          "PRIMARY KEY(id)," \
          "UNIQUE KEY (Area, RoomNum, FloorInfo, nameId)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_fangcomCourtSellingInfo_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "replace into fangcomCourtSellingInfo " \
          "(HouseNum , " \
          "PublishDate , " \
          "SellingPrice , " \
          "RoomNum , " \
          "Area , " \
          "FloorInfo , " \
          "Direction , " \
          "Decor , " \
          "HouseHistory , " \
          "Telephone , " \
          "`满五唯一` TINYINT (1), " \
          "`满二` TINYINT (1), " \
          "`特价房` TINYINT (1)," \
          "`业主发布` TINYINT (1)," \
          "nameId ," \
          ") " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)"
    return sql


def insert_fangcomCourtSellingInfo_value(row):
    return (row[0], row[1] or None, row[2] or None, row[3] or None,
            row[4] or None, row[5] or None, row[6] or None,
            row[7] or None, row[8] or None, row[9] or None,
            row[10] or None, row[11] or None, row[12] or None,
            row[13] or None, row[14]
            )


def parse_trade_html(html, court_id, db, scraper):
    try:
        for p in html.find_all('p', class_='fangTitle'):
            # 请求房源详细信息
            url = p.a['href']
            scraper.make_header_para({'Referer': scraper.url})
            scraper.url = url
            html_detail = scraper.send_request_get()
            row = []
            spans = html_detail.find('p', class_='gray9 titleAdd').find_all('span')
            house_num = num_re.findall(spans[-2].string.encode('utf-8').strip())
            row.append(house_num[0] if len(house_num) > 0 else None)
            publish = date_rx.findall(spans[-1].string.encode('utf-8').strip())
            row.append(publish[0] if len(publish) > 0 else None)

            # 标签
            for span in spans[:-2]:
                for rx in re_list:
                    if len(rx.findall(span.string.encode('utf-8').strip())) > 0:
                        row.append(1)

            row.append(court_id)
            if len(row) == 8:
                db.insert_db_values(insert_fangcomCourtSellingInfo_sql_values(),
                                    insert_fangcomCourtSellingInfo_value(row))
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
    db.create_table(create_fangcomCourtSellingInfo_table_sql())
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
    parse_trade_html(html, court_id, db, scraper)
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
