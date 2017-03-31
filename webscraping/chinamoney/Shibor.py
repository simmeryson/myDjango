# -*- coding:utf-8 -*-
import bs4

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


def create_table(cursor):
    # 删除表
    # cursor.execute("DROP TABLE IF EXISTS %s" % TABLE_NAME)
    # 创建表
    cursor.execute(
        'CREATE TABLE IF NOT EXISTS %s '
        '(id int(11) NOT NULL AUTO_INCREMENT,'
        'date DATE NOT NULL , '
        '`O/N` FLOAT ,'
        '1W FLOAT ,'
        '2W FLOAT ,'
        '1M FLOAT ,'
        '3M FLOAT ,'
        '6M FLOAT ,'
        '9M FLOAT ,'
        '1Y FLOAT ,'
        'PRIMARY KEY(id)'
        ')'
        'ENGINE=InnoDB '
        'AUTO_INCREMENT=1'
        % TABLE_NAME)


def insert_db(cursor, conn, row):
    # 引号坑死人
    sql = "insert into %s (date,`O/N`,1W,2W,1M,3M,6M,9M,1Y) " \
          "VALUES ('%s', %s, %s, %s, %s, %s, %s, %s, %s)" \
          % (
              TABLE_NAME, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]
          )
    cursor.execute(sql)
    conn.commit()


def create_db(conn, cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS %s" % DB_NAME)
    conn.select_db(DB_NAME)
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print "Database version : %s " % data


def parse_html(html, save_row):
    tbody = html.find("td", {"class": "dreport-title"}).parent.parent.parent.find_next_sibling('div').table.tbody
    try:
        for tr in tbody.find_all("tr"):
            if type(tr) == bs4.element.Tag:
                row = []
                for td in tr.find_all("td"):
                    row.append(td.string.encode("utf-8"))
                save_row(row)
    except:
        print "no data!"


def do_scraping():
    scraper = Scraping(url, post_data)
    db = DbManager(create_table, create_db, insert_db)
    db.create_db()
    db.create_table()
    scraper.query_from_datelist(date_list, parse_html, db.insert_db)
    # scraper.query_today(parse_html, db.insert_db)
    db.close_db()


do_scraping()
