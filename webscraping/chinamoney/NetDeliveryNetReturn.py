# -*- coding:utf-8 -*-
# 保存 货币净投放.按周计算.最大年度查询
import MySQLdb
import bs4
import time

from webscraping.chinamoney.scraping import Scraping
from webscraping.db_manage import DbManager

DB_NAME = 'chinamoney'
TABLE_NAME = 'NetDeliveryNetReturn'

url = "http://www.chinamoney.com.cn/fe/jsp/CN/chinamoney/notice/ticketPutAndBackStatByWeekList.jsp"

get_para = {'beginWeek': '', 'endWeek': ''
            }

singleDay = [('2017-01', '2017-14')]

date_list = [('2002-01', '2002-52'),
             ('2003-01', '2003-52'), ('2004-01', '2004-52'), ('2005-01', '2005-52'),
             ('2006-01', '2006-53'), ('2007-01', '2007-52'), ('2008-01', '2008-52'),
             ('2009-01', '2009-52'), ('2010-01', '2010-52'), ('2011-01', '2011-52'),
             ('2012-01', '2012-53'), ('2013-01', '2013-52'), ('2014-01', '2014-52'),
             ('2015-01', '2015-52'), ('2016-01', '2016-52'), ('2017-01', '2017-14')]


# 创建表TABLE_NAME
def create_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS %s " \
          "(date VARCHAR (20) NOT NULL , " \
          "`开始日期` DATE ," \
          "`结束日期` DATE ," \
          "`投放量(亿)` INT (10)," \
          "`回笼量(亿)` INT (10)," \
          "`净投放(亿)` INT (10)," \
          "PRIMARY KEY(date)" \
          ")" \
          "ENGINE=InnoDB " \
          % TABLE_NAME
    return sql


def insert_db_value(row):
    return (row[0], row[1], row[2] or None, row[3] or None, row[4] or None, row[5] or None
            )


def insert_db_sql_values():
    # 引号坑死人
    sql = "insert into NetDeliveryNetReturn (date,`开始日期`,`结束日期`,`投放量(亿)`,`回笼量(亿)`, `净投放(亿)`) " \
          "VALUES (%s, %s, %s, %s, %s, %s)"
    return sql


# 解析曲线数据
def parse_html(html, save_row, insert_into):
    trs = html.find("td", {'class': 'dreport-title'}).parent.find_next_siblings('tr')
    try:
        for tr in trs:
            row = []
            for td in tr.find_all('td'):
                if td.string:
                    str_ = td.string.encode('utf-8').replace("\xc2\xa0", "").strip()
                    row.append(str_ if str_ != '---' else None)
            if len(row) == 6:
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

    scrap_data(dates_list, db_name, scraper)

    db_name.close_db()


# 抓取曲线数据
def scrap_data(dates_list, db, scraper):
    # db.drop_table(TABLE_NAME)
    db.create_table(create_table_sql())
    for date in dates_list:
        para = {'beginWeek': date[0], 'endWeek': date[1]}
        html = scraper.send_request_get(para)
        parse_html(html, insert_db_sql_values, db.insert_db_values)
        time.sleep(2)


# do_scraping(date_list)
