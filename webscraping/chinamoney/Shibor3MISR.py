# -*- coding:utf-8 -*-
# 保存 Shibor3M利率互换收盘/定盘曲线 . 最大按月查询
import MySQLdb
import bs4
import time

from webscraping.chinamoney.scraping import Scraping
from webscraping.db_manage import DbManager

DB_NAME = 'chinamoney'
TABLE_NAME = 'Shibor3MISR'

url = "http://www.chinamoney.com.cn/fe-c/interestRateSwapCurve3MHistoryAction.do?lan=cn"

post_data = {'startDate': '', 'endDate': '', 'bidAskType': '', 'bigthType': 'Shibor3M',
             'interestRateType': '', 'message': ''
             }

singleDay = [('2016-03-01', '2016-03-01')]

date_list = [('2013-01-01', '2013-12-31'), ('2014-01-01', '2014-12-31'),
             ('2015-01-01', '2015-12-31'), ('2016-01-01', '2016-12-31'), ('2017-01-01', '2017-03-31')]


# 创建表TABLE_NAME
def create_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS %s " \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date DATE NOT NULL , " \
          "curve_name VARCHAR(40)," \
          "price_type VARCHAR(10)," \
          "6M FLOAT ," \
          "9M FLOAT ," \
          "1Y FLOAT ," \
          "2Y FLOAT ," \
          "3Y FLOAT ," \
          "4Y FLOAT ," \
          "5Y FLOAT ," \
          "7Y FLOAT ," \
          "10Y FLOAT," \
          "PRIMARY KEY(id)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1" \
          % TABLE_NAME
    return sql


# 插入每种债券类型收益率数据
def insert_db_sql(row):
    # 引号坑死人
    sql = "insert into `Shibor3MISR` (date,`curve_name`,`price_type`,`6M`,`9M`, 1Y, 2Y,3Y,4Y,5Y,7Y,10Y) " \
          "VALUES ('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
          % (
              row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]
          )
    return sql


# 解析收益率数据
def parse_html(html, save_row, insert_into):
    trs = html.find("td", {'class': 'dreport-title'}).parent.find_next_siblings('tr')
    try:
        for tr in trs:
            row = []
            for td in tr.find_all('td'):
                if td.string:
                    row.append(td.string.encode('utf-8').replace("\xc2\xa0", "").strip())
            if len(row) == 12:
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
    for date in dates_list:
        month_list = scraper.gen_month_list(date[0], date[1])
        scrap_data(month_list, db_name, scraper)
    db_name.close_db()


# 抓取债券收益率数据
def scrap_data(dates_list, db, scraper):
    # db.drop_table(TABLE_NAME)
    db.create_table(create_table_sql())
    for date in dates_list:
        scraper.make_post_para({'startDate': date[0], 'endDate': date[1]})
        html = scraper.send_request_post()
        parse_html(html, insert_db_sql, db.insert_db)
        time.sleep(2)

# do_scraping(date_list)
