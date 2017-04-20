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
start_url = "http://esf.xian.fang.com/housing/__1_0_0_0_1_0_0/"
base_url = 'http://esf.xian.fang.com'

city_list = [('xa', 'XianHousePrice'),
             # ('bj', 'BeijingHousePrice'), ('sh', 'ShanghaiHousePrice'),
             # ('sz', 'ShenzhenHousePrice'), ('gz', 'GuangzhouHousePrice'), ('cd', 'ChengduHousePrice'),
             # ('hz', 'HangzhouHousePrice'),
             ]


# 总数据
# creprice.cn的房价数据
def create_fangcomSecondHandCourtName_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS fangcomSecondHandCourtName" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "name VARCHAR (30), " \
          "DetailUrl VARCHAR (100), " \
          "PRIMARY KEY(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_fangcomSecondHandCourtName_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into fangcomSecondHandCourtName " \
          "(name," \
          "DetailUrl) " \
          "VALUES (%s, %s)"
    return sql


def insert_fangcomSecondHandCourtName_value(row):
    return (row[0], row[1] or None
            )


# 抓取曲线数据
def parse_name_list(html, insert_db_values):
    try:

        for a in html.find_all('dl', class_='plotListwrap clearfix'):
            a = a.dd.p.a
            info_url = a['href'].encode('utf-8').strip()
            name = a.string.encode('utf-8').strip()
            row = [name, info_url]
            insert_db_values(insert_fangcomSecondHandCourtName_sql_values(),
                             insert_fangcomSecondHandCourtName_value(row))
        div = html.find('div', attrs={'id': 'houselist_B14_01'})
        now_page = div.find('a', class_='pageNow').string.encode('utf-8').strip()
        print 'now_page:' + now_page
        return None if (now_page == 100 or not div) else div.find('a', attrs={'id': 'PageControl1_hlk_next'})['href']
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], 'fangcomSecondHandCourtName')
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], 'fangcomSecondHandCourtName')
    finally:
        print "%s 抓取完成" % 'fangcomSecondHandCourtName'


# 递归抓取所有分页
def scrap_data(db, scraper, city):
    html = scraper.send_request_get({})
    next_page = parse_name_list(html, db.insert_db_values)
    if not next_page:
        return
    # time.sleep(1)
    scraper.url = base_url + next_page
    scrap_data(db, scraper, city)


#
def drop_tables(db):
    # db.drop_table('fangcomSecondHandCourtName')
    db.drop_table('fangcomSecondHandCourtDetail')


# 总数据
# creprice.cn的房价数据
def create_fangcomSecondHandCourtDetail_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS fangcomSecondHandCourtDetail" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date DATE , " \
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


def insert_fangcomSecondHandCourtDetail_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into fangcomSecondHandCourtDetail " \
          "(date," \
          "CurPrice," \
          "MoM," \
          "YoY," \
          "nameId" \
          ") " \
          "VALUES (%s, %s, %s, %s, %s)"
    return sql


def insert_fangcomSecondHandCourtDetail_value(row):
    return (row[0], row[1] or None, row[2] or None, row[3] or None,
            row[4]
            )


# 抓取当天数据
def parse_basic_info(html, court_id):
    CourtBasicInfo(html, court_id).start()
    CourtFacility(html, court_id).start()
    CourtContextInfo(html, court_id).start()


def parse_detail_html(html, today, court_id, db):
    try:
        div = html.find('div', class_='box detaiLtop mt20 clearfix')
        row = [today]
        for dl in div.find_all('dl'):
            row.append(dl.dd.span.string.encode('utf-8').strip())
        row.append(court_id)
        if len(row) == 5:
            db.insert_db_values(insert_fangcomSecondHandCourtDetail_sql_values(),
                                insert_fangcomSecondHandCourtDetail_value(row))
        else:
            print "wrong row size: " + " ".join(row)
        parse_basic_info(html, court_id)
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], 'fangcomSecondHandCourtDetail')
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], 'fangcomSecondHandCourtDetail')
    finally:
        print "%s 抓取完成" % 'fangcomSecondHandCourtDetail'


def scrap_detail(db):
    drop_tables(db)
    db.create_table(create_fangcomSecondHandCourtDetail_table_sql())
    court_list = db.query('select * from fangcomSecondHandCourtName')
    for (court_id, court_name, court_url) in court_list:
        print '小区:' + court_name + "  开始获取详情"
        url = court_url.replace('esf/', 'xiangqing/')
        scraper = Scraping(url)
        scraper.make_header_para({'Referer': court_url})
        html = scraper.send_request_get()
        today = str(scraper.get_today_date())
        parse_detail_html(html, today, court_id, db)

        time.sleep(0.5)


def scraping_today():
    scraper = Scraping(start_url)
    db = DbManager()

    for city, db_name in city_list:
        db.create_db(db_name)
        # drop_tables(db)
        # db.create_table(create_fangcomSecondHandCourtName_table_sql())
        # scrap_data(db, scraper, city)

        scrap_detail(db)

    db.close_db()


scraping_today()
