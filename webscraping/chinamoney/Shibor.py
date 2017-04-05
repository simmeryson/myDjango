# -*- coding:utf-8 -*-
import bs4
import time

from webscraping.chinamoney.scraping import Scraping
from webscraping.db_manage import DbManager

DB_NAME = 'chinamoney'
TABLE_NAME = 'Shibor'

url = "http://www.chinamoney.com.cn/fe-c/shiborHistoricalDataAction.do"

post_data = {'startDate': '', 'endDate': '', 'message': ''}

singleDay = [('2017-03-01', '2017-03-01')]

date_list = [('2006-01-01', '2006-12-31'), ('2007-01-01', '2007-12-31'), ('2008-01-01', '2008-12-31'),
             ('2009-01-01', '2009-12-31'), ('2010-01-01', '2010-12-31'), ('2011-01-01', '2011-12-31'),
             ('2012-01-01', '2012-12-31'), ('2013-01-01', '2013-12-31'), ('2014-01-01', '2014-12-31'),
             ('2015-01-01', '2015-12-31'), ('2016-01-01', '2016-12-31'), ('2017-01-01', '2017-03-31')]


def create_table_sql():
    # 创建表
    sql = "CREATE TABLE IF NOT EXISTS %s " \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date DATE NOT NULL , " \
          "`O/N` FLOAT ," \
          "1W FLOAT ," \
          "2W FLOAT ," \
          "1M FLOAT ," \
          "3M FLOAT ," \
          "6M FLOAT ," \
          "9M FLOAT ," \
          "1Y FLOAT ," \
          "PRIMARY KEY(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1" \
          % TABLE_NAME
    return sql


def insert_db_sql(row):
    # 引号坑死人
    sql = "insert into %s (date,`O/N`,1W,2W,1M,3M,6M,9M,1Y) " \
          "VALUES ('%s', %s, %s, %s, %s, %s, %s, %s, %s)" \
          % (
              TABLE_NAME, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]
          )
    return sql


def parse_html(html, save_row):
    tbody = html.find("td", {"class": "dreport-title"}).parent.parent.parent.find_next_sibling('div').table.tbody
    try:
        for tr in tbody.find_all("tr"):
            if type(tr) == bs4.element.Tag:
                row = []
                for td in tr.find_all("td"):
                    row.append(td.string.encode("utf-8"))
                save_row(insert_db_sql(row))
    except:
        print "no data!"


def query_from_datelist(dates_list, insert_db, scraper):
    for date in dates_list:
        scraper.make_post_para({'startDate': date[0], 'endDate': date[1]})
        html = scraper.send_request()
        parse_html(html, insert_db)
        time.sleep(2)


def do_scraping():
    scraper = Scraping(url, post_data)
    db = DbManager()
    db.create_db(DB_NAME)
    db.drop_table(TABLE_NAME)
    db.create_drop_table(create_table_sql())
    query_from_datelist(date_list, db.insert_db, scraper)
    db.close_db()


do_scraping()
