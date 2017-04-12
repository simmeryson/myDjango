# -*- coding:utf-8 -*-
# 保存 利率互换日报.按日查询
import re
import sys
sys.path.append('../..')

import MySQLdb
import time

from webscraping.chinamoney.scraping import Scraping
from webscraping.db_manage import DbManager

DB_NAME = 'chinamoney'
TABLE_NAME = 'InterestRateSwapDaily'

url = "http://www.chinamoney.com.cn/fe-c/InterestRateSwapDailySearchAction.do"

post_para = {'searchDate': '', 'langCode': 'ZH'
             }

singleDay = [('2017-03-07', '2017-03-07')]

date_list = [('2017-03-08', '2017-04-08')]


# 创建表TABLE_NAME
def create_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS %s" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date DATE NOT NULL , " \
          "`参考利率` VARCHAR (30) NOT NULL ," \
          "`期限` VARCHAR (20)," \
          "`固定利率` FLOAT ," \
          "PRIMARY KEY(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1" \
          % TABLE_NAME

    return sql


def insert_db_value(row):
    return (row[0], row[1], row[2] or None, row[3] or None
            )


def insert_db_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "insert into InterestRateSwapDaily (date,`参考利率`,`期限`,`固定利率`)" \
          " VALUES (%s, %s, %s, %s)"
    return sql


# 解析曲线数据
def parse_html(html, save_row, insert_into):
    date = html.find("img", attrs={"class": "icon-calendar"}). \
        next.string.encode('utf-8').replace("\xc2\xa0", "").strip()
    try:
        tds = html.find_all('td', attrs={'class': 'dreport-row2-1 tdline'})
        for td in tds:
            field = td.string.encode('utf-8').replace("\xc2\xa0", "").strip()
            trs = td.find_next_sibling('td').find_all('tr')
            parse_trs(date, insert_into, save_row, trs, field)

        trs = html.find(string=re.compile('Shibor_3M')).parent.find_next_sibling('td').find_all('tr')
        parse_trs(date, insert_into, save_row, trs, 'Shibor_3M')

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s on Table %s" % (e.args[0], e.args[1], TABLE_NAME)
    except:
        print "%s no data!" % TABLE_NAME
    finally:
        print "%s 抓取完成" % TABLE_NAME


# 组合每行的数据并插入
def parse_trs(date, insert_into, save_row, trs, field_name):
    for tr in trs:
        row = [date, field_name]
        for td in tr.find_all('td'):
            if td.string:
                str_ = td.string.encode('utf-8').replace("\xc2\xa0", "").strip()
                row.append(str_ if str_ != '---' else None)
        if len(row) == 4:
            insert_into(save_row(), insert_db_value(row))
        else:
            print "wrong row size: " + " ".join(row)


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
