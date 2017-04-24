# -*- coding:utf-8 -*-
# fang.com 西安小区数据, 月查询.
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
url = 'http://esf.xian.fang.com/school/'
base_url = 'http://esf.xian.fang.com'

city_list = [('xa', 'XianHousePrice'),
             # ('bj', 'BeijingHousePrice'), ('sh', 'ShanghaiHousePrice'),
             # ('sz', 'ShenzhenHousePrice'), ('gz', 'GuangzhouHousePrice'), ('cd', 'ChengduHousePrice'),
             # ('hz', 'HangzhouHousePrice'),
             ]

table_name = 'fangcomXianSchool'

num_rx = re.compile(ur'^\d*')


#
def drop_tables(db):
    db.drop_table(table_name)


# 总数据
# creprice.cn的房价数据
def create_fangcomXianSchool_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS fangcomXianSchool" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "SchoolName VARCHAR (30) NOT NULL , " \
          "CourtNum INT , " \
          "Address VARCHAR (50), " \
          "TagSet SET ('双语','小班教学','寄宿制','名校附属','语文类','外语类','体育类','科技类','艺术类','专业学科类','特殊教育'), " \
          "PointTag ENUM ('普通','区重点','市重点','省重点') NOT NULL , " \
          "SellingHouseNum INT , " \
          "PRIMARY KEY(id)," \
          "UNIQUE KEY (SchoolName)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_fangcomXianSchool_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "replace into fangcomXianSchool " \
          "(SchoolName , " \
          "CourtNum , " \
          "Address , " \
          "TagSet , " \
          "PointTag ," \
          "SellingHouseNum " \
          ") " \
          "VALUES (%s, %s, %s, %s, %s, %s)"
    return sql


def insert_fangcomXianSchool_value(row):
    return (row[0], row[1] or None, row[2] or None, row[3] or None,
            row[4] or None, row[5] or None
            )


def parse_school_html(html, db):
    try:
        for dl in html.find('div', class_='schoollist').find_all('dl'):
            dd = dl.dd
            row = [dd.find('p', class_='title').a.string.encode('utf-8').strip()]
            court = num_rx.findall(dd.find('p', class_='mt13 bold').a.string.encode('utf-8').strip(), 0)
            row.append(court[0] if len(court) > 0 else 0)
            row.append(dd.find('p', class_='gray6 mt13').span.string.encode('utf-8').strip())
            tags = dd.find('p', class_='mt15').span
            tag_list = []
            for tag in tags.find_next_siblings('span'):
                tag_list.append(tag.string.encode('utf-8').strip())
            row.append(','.join(tag_list))
            row.append(tags.string.encode('utf-8').strip())
            row.append(dd.find('a', class_='hsNum blue alignR').strong.string.strip())
            if len(row) == 6:
                db.insert_db_values(insert_fangcomXianSchool_sql_values(), insert_fangcomXianSchool_value(row))
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
    db.create_table(create_fangcomXianSchool_table_sql())
    scraper = Scraping(url)
    try:
        send_request(db, url, scraper)
    except:
        print "error ->> :" + scraper.url


def send_request(db, _url, scraper):
    scraper.make_header_para({'Referer': scraper.url})
    scraper.url = _url
    html = scraper.send_request_get()
    parse_school_html(html, db)
    time.sleep(0.5)
    next_page = html.find('a', id='PageControl1_hlk_next')
    if next_page:
        next_url = base_url + next_page['href']
        send_request(db, next_url, scraper)


def scraping_today():
    db = DbManager()

    for city, db_name in city_list:
        db.create_db(db_name)
        scrap_detail(db)

    db.close_db()


scraping_today()
