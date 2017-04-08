# -*- coding:utf-8 -*-
# 保存 利率互换行情曲线.按月查询
import re
from HTMLParser import HTMLParseError

import MySQLdb
import bs4
import time

from webscraping.chinamoney.scraping import Scraping
from webscraping.db_manage import DbManager

import sys

reload(sys)
sys.setdefaultencoding('utf8')

DB_NAME = 'chinamoney'
TABLE_NAME = 'IRSMktCurve'

url = "http://www.chinamoney.com.cn/fe-c/irsMktCurveHisQuery.do"

get_data = {'beginDate': '', 'endDate': '',
            'calcTimeHMS': 'ALL', 'curveId': 'ALL'
            }

ssingleDay = [('2017-04-05', '2017-04-05')]

date_list = [
    ('2014-01-01', '2014-12-31'),
    ('2015-01-01', '2015-12-31'), ('2016-01-01', '2016-12-31'), ('2017-01-01', '2017-04-08')]


# 创建表TABLE_NAME
def create_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS %s" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "`日期` DATE NOT NULL , " \
          "`时刻` TIME NOT NULL ," \
          "`曲线名称` VARCHAR (30) NOT NULL ," \
          "`期限` VARCHAR (20)," \
          "`报买` FLOAT ," \
          "`报卖` FLOAT ," \
          "PRIMARY KEY(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1" \
          % TABLE_NAME

    return sql


def insert_db_value(row):
    return (row[0], row[1], row[2], row[3], row[4] or None, row[5] or None,
            )


def insert_db_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into IRSMktCurve (`日期`,`时刻`,`曲线名称`,`期限`,`报买`,`报卖`)" \
          " VALUES (%s, %s, %s, %s, %s, %s)"
    return sql


# 解析曲线数据
def parse_html(html, save_row, insert_into):
    try:
        trs = html.find('td', attrs={'class': 'dreport-title'}).parent.find_next_siblings('tr')
        if len(trs) == 0:
            begin_Date = html.find('input', attrs={'name': 'beginDate', 'id': 'beginDate'})
            endDate = html.find('input', attrs={'name': 'endDate', 'id': 'endDate'})
            begin = begin_Date['value'].encode('utf-8').replace("\xc2\xa0", "").strip() if begin_Date.has_attr('value') else ''
            end = endDate['value'].encode('utf-8').replace("\xc2\xa0", "").strip() if endDate.has_attr('value') else ''
            print begin + " --- " + end + " no data!"
            return
        for tr in trs:
            row = []
            for td in tr.find_all('td'):
                string = td.string.encode('utf-8').replace("\xc2\xa0", "").strip()
                row.append(string if string != '---' else None)
            if len(row) == 6:
                insert_into(save_row(), insert_db_value(row))
            else:
                print "wrong row size: " + " ".join(row)

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], TABLE_NAME)
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], TABLE_NAME)
    finally:
        print "%s 抓取完成" % TABLE_NAME


def do_scraping(dates_list):
    scraper = Scraping(url)
    db_name = DbManager()
    db_name.create_db(DB_NAME)
    # db_name.drop_table(TABLE_NAME)
    db_name.create_table(create_table_sql())

    for dates in dates_list:
        month_list = scraper.gen_month_list(dates[0], dates[1])
        scrap_data(month_list, db_name, scraper)

    db_name.close_db()


# 抓取曲线数据
def scrap_data(dates_list, db, scraper):
    for date in dates_list:
        para = {'beginDate': date[0], 'endDate': date[1]}
        html = scraper.send_request_get(para)
        parse_html(html, insert_db_sql_values, db.insert_db_values)
        time.sleep(2)


# do_scraping(date_list)


# 抓取当天数据
def scraping_today():
    scraper = Scraping(url)
    db_name = DbManager()
    db_name.create_db(DB_NAME)
    # db_name.drop_table(TABLE_NAME)
    db_name.create_table(create_table_sql())

    today = str(scraper.get_today_date())
    day_list = [(today, today)]

    scrap_data(day_list, db_name, scraper)

    db_name.close_db()

scraping_today()
