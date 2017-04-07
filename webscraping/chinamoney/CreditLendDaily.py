# -*- coding:utf-8 -*-
# 保存 同业拆借日.按日查询
import re

import MySQLdb
import bs4
import time

from webscraping.chinamoney.scraping import Scraping
from webscraping.db_manage import DbManager

DB_NAME = 'chinamoney'
TABLE_NAME = 'CreditLendDaily'

url = "http://www.chinamoney.com.cn/fe-c/CreditLendDailySearchAction.do"

post_para = {'searchDate': '', 'remarkBondType': '261'
             }

singleDay = [('2017-03-07', '2017-03-07')]

date_list = [('2017-03-08', '2017-04-08')]


# 创建表TABLE_NAME
def create_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS CreditLendDaily " \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date DATE NOT NULL , " \
          "`品种` VARCHAR (20) NOT NULL ," \
          "`开盘利率` FLOAT ," \
          "`收盘利率` FLOAT ," \
          "`加权利率` FLOAT ," \
          "`升降(基点)` FLOAT ," \
          "`平均拆借期限(天)` FLOAT ," \
          "`成交笔数(笔)` INT (10)," \
          "`成交金额(亿元)` FLOAT ," \
          "`增减(亿元)` FLOAT ," \
          "PRIMARY KEY(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1"
    return sql


def insert_db_value(row):
    return (row[0], row[1], row[2] or None, row[3] or None, row[4] or None, row[5] or None,
            row[6] or None, row[7] or None, row[8] or None, row[9] or None
            )


def insert_db_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into CreditLendDaily (date,`品种`,`开盘利率`,`收盘利率`,`加权利率`, `升降(基点)`," \
          "`平均拆借期限(天)`,`成交笔数(笔)`,`成交金额(亿元)`,`增减(亿元)`)" \
          " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    return sql


# 解析曲线数据
def parse_html(html, save_row, insert_into):
    date = html.find("img", attrs={"class": "icon-calendar"}).next.string.encode('utf-8')
    trs = html.find("td", attrs={"class": "dreport-title-1", "height": "28", "width": "8%"}).parent.find_next_siblings(
        'tr')
    try:
        for tr in trs:
            row = [date]
            for td in tr.find_all('td'):
                if td.string:
                    str_ = td.string.encode('utf-8').replace("\xc2\xa0", "").strip()
                    row.append(str_ if str_ != '---' else None)
            if len(row) == 10:
                insert_into(save_row(), insert_db_value(row))
            else:
                print "wrong row size: " + " ".join(row)
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    except:
        print "%s no data!" % TABLE_NAME
    finally:
        print "%s 抓取完成" % TABLE_NAME


def do_scraping(dates_list):
    scraper = Scraping(url)
    db_name = DbManager()
    db_name.create_db(DB_NAME)

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
