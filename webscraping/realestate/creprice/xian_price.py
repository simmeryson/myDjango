# -*- coding:utf-8 -*-
# 保存 利率互换日报.按日查询
import sys
sys.path.append('../..')

import re
from HTMLParser import HTMLParseError

import MySQLdb
import bs4
import time

from scraping import Scraping
from webscraping.db_manage import DbManager

reload(sys)
sys.setdefaultencoding('utf8')

DB_NAME = 'chinamoney'
TABLE_NAME = 'BillMarketDaily'

url = "http://www.chinamoney.com.cn/fe-c/cpDailySearchAction.do?method=searchData"

post_para = {'searchDate': ''
             }

singleDay = [('2017-03-07', '2017-03-07')]

date_list = [('2017-03-08', '2017-04-08')]


# 创建表TABLE_NAME
def create_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS %s" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date DATE NOT NULL , " \
          "`species` VARCHAR (30) NOT NULL ," \
          "`品种` VARCHAR (20) NOT NULL ," \
          "`方向` VARCHAR (20)," \
          "`金额(亿)` FLOAT ," \
          "`笔数` INT (10)," \
          "`最高利率` FLOAT ," \
          "`最低利率` FLOAT ," \
          "`平均利率` FLOAT ," \
          "`较昨日(BP)` FLOAT ," \
          "PRIMARY KEY(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1" \
          % TABLE_NAME

    return sql


def insert_db_value(row):
    return (row[0], row[1], row[2] or None, row[3] or None, row[4] or None, row[5] or None,
            row[6] or None, row[7] or None, row[8] or None, row[9] or None
            )


def insert_db_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into BillMarketDaily (date,`species`,`品种`,`方向`,`金额(亿)`,`笔数`,`最高利率`,`最低利率`,`平均利率`,`较昨日(BP)`)" \
          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    return sql


# 解析曲线数据
def parse_html(html, save_row, insert_into):
    try:
        date = html.find("input", attrs={"class": "input-date", "id": "mbmDaily"})['value'].encode('utf-8').replace(
            "\xc2\xa0", "").strip()

        re_words = re.compile(ur"品种")
        species = ''
        bill_type = ''
        data = html.find_all(string=re_words)
        if len(data) == 0:
            print date + " no data!"
            return

        for tr in data[0].parent.parent.find_next_siblings('tr'):
            row = [date]
            tds = tr.find_all('td')
            if len(tds) < 5:
                continue
            elif len(tds) == 7:
                row.append(species)
                row.append(bill_type)
            elif len(tds) == 8:
                row.append(species)

            for td in tr.find_all('td'):
                string = td.string.encode('utf-8').replace("\xc2\xa0", "").strip()
                if td.has_attr('rowspan') and td['rowspan'] == u'4':
                    species = string
                elif td.has_attr('rowspan') and td['rowspan'] == u'2':
                    bill_type = string
                row.append(string if string != '--' else None)
            if len(row) == 10:
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
        day_list = scraper.gen_day_list(dates[0], dates[1])
        scrap_data(day_list, db_name, scraper)

    db_name.close_db()


# 抓取曲线数据
def scrap_data(dates_list, db, scraper):
    for date in dates_list:
        para = {'searchDate': date}
        scraper.make_post_para(para)
        html = scraper.send_request_post()
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
    day_list = [today]

    scrap_data(day_list, db_name, scraper)

    db_name.close_db()


scraping_today()
