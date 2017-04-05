# -*- coding:utf-8 -*-
# 保存 外汇掉期曲线,对美元.最大按月查询
import MySQLdb
import bs4
import time

from webscraping.chinamoney.scraping import Scraping
from webscraping.db_manage import DbManager

DB_NAME = 'chinamoney'
TABLE_NAME = 'FxSwapsCurve'

url = "http://www.chinamoney.com.cn/fe-c/fxSwapsCurveHistoryAction.do"

post_data = {'startDate': '', 'endDate': '',
             'message': ''
             }

singleDay = [('2017-03-01', '2017-03-01')]

date_list = [('2006-01-01', '2006-12-31'), ('2007-01-01', '2007-12-31'), ('2008-01-01', '2008-12-31'),
             ('2009-01-01', '2009-12-31'), ('2010-01-01', '2010-12-31'), ('2011-01-01', '2011-12-31'),
             ('2012-01-01', '2012-12-31'), ('2013-01-01', '2013-12-31'), ('2014-01-01', '2014-12-31'),
             ('2015-01-01', '2015-12-31'), ('2016-01-01', '2016-12-31'), ('2017-01-01', '2017-03-31')]


# 创建表TABLE_NAME
def create_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS %s " \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date DATE NOT NULL , " \
          "`期限品种` VARCHAR(20)," \
          "`掉期点(Pips)` FLOAT ," \
          "`掉期点涨跌(Pips)` FLOAT ," \
          "`掉期点数据源` VARCHAR (30)," \
          "`全价汇率` FLOAT ," \
          "`全价汇率涨跌` FLOAT ," \
          "`远端起息日` DATE," \
          "PRIMARY KEY(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1" \
          % TABLE_NAME
    return sql


# 插入外汇掉期曲线数据
def insert_db_sql(row):
    # 引号坑死人
    sql = "insert into %s (date,`期限品种`,`掉期点(Pips)`,`掉期点涨跌(Pips)`,`掉期点数据源`, `全价汇率`, `全价汇率涨跌`,`远端起息日`) " \
          "VALUES ('%s', '%s', %s, %s, '%s', %s, %s, '%s')" \
          % (
              TABLE_NAME, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]
          )
    return sql


# 解析曲线数据
def parse_html(html, save_row, insert_into):
    trs = html.find("td", {'class': 'dreport-title'}).parent.find_next_siblings('tr')
    try:
        for tr in trs:
            row = []
            for td in tr.find_all('td'):
                if td.string:
                    row.append(td.string.encode('utf-8').replace("\xc2\xa0", "").strip())
            if len(row) == 8:
                insert_into(save_row(row))
            else:
                print "wrong row size: " + " ".join(row)
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    except:
        print "%s no data!" % TABLE_NAME
    finally:
        print "%s 抓取完成" % TABLE_NAME


def do_scraping(dates_list):
    scraper = Scraping(url, post_data)
    db_name = DbManager()
    db_name.create_db(DB_NAME)

    scrap_data(dates_list, db_name, scraper)

    db_name.close_db()


# 抓取曲线数据
def scrap_data(dates_list, db, scraper):
    # db.drop_table(TABLE_NAME)
    db.create_table(create_table_sql())
    for date in dates_list:
        scraper.make_post_para({'startDate': date[0], 'endDate': date[1]})
        html = scraper.send_request()
        parse_html(html, insert_db_sql, db.insert_db)
        time.sleep(2)


do_scraping(singleDay)
