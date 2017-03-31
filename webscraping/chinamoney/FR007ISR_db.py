# -*- coding:utf-8 -*-
import MySQLdb
import time
import bs4
from bs4 import BeautifulSoup
import requests
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

USER_NAME = 'root'
PSSWRD = 'kai123'
DB_NAME = 'chinamoney'
TABLE_NAME = 'FR007ISR'

url = "http://www.chinamoney.com.cn/fe-c/interestRateSwapCurve3MHistoryAction.do?lan=cn"

form_data = "startDate=%s&endDate=%s&bidAskType=&bigthType=FR007&interestRateType=&message="

date_list = [('2017-03-01', '2017-03-29'), ('2017-02-01', '2017-02-28'), ('2017-01-01', '2017-01-31'),
             ('2016-12-01', '2016-12-31'), ('2016-11-01', '2016-11-30'), ('2016-10-01', '2016-10-31'),
             ('2016-09-01', '2016-09-30'), ('2016-08-01', '2016-08-31')]

singleDay = [('2017-03-01', '2017-03-01')]


def create_table(cursor):
    # 删除表
    # cursor.execute("DROP TABLE IF EXISTS FR007ISR")
    # 创建表
    cursor.execute(
        'CREATE TABLE IF NOT EXISTS %s '
        '(id int(11) NOT NULL AUTO_INCREMENT,'
        'date DATE NOT NULL , '
        'curve_name VARCHAR(40),'
        'price_type VARCHAR(10),'
        '1M FLOAT ,'
        '3M FLOAT ,'
        '6M FLOAT ,'
        '9M FLOAT ,'
        '1Y FLOAT ,'
        '2Y FLOAT ,'
        '3Y FLOAT ,'
        '4Y FLOAT ,'
        '5Y FLOAT ,'
        '7Y FLOAT ,'
        '10Y FLOAT,'
        'PRIMARY KEY(id)'
        ')'
        'ENGINE=InnoDB '
        'AUTO_INCREMENT=1'
        % TABLE_NAME)


def create_db(conn, cursor):
    cursor.execute("CREATE DATABASE IF NOT EXISTS %s" % DB_NAME)
    conn.select_db(DB_NAME)
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print "Database version : %s " % data


def insert_db(cursor, conn, row):
    # sql = "insert into %s (date,curve_name) VALUES ('%s','%s') " % (TABLE_NAME, row[0],'aaa')
    # 引号坑死人
    sql = "insert into %s (date, curve_name, price_type,1M,3M,6M,9M,1Y,2Y,3Y,4Y,5Y,7Y,10Y) " \
          "VALUES ('%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
          % (
          TABLE_NAME, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11],
          row[12], row[13])
    # print sql
    cursor.execute(sql)
    conn.commit()


def query_from_datelist(datelist, cursor, conn):
    for data in datelist:
        html = send_request(data)
        # 解析出 行数据
        tr_list = html.find("td", {"class": "dreport-title"}).parent.next_siblings
        for tr in tr_list:
            if type(tr) == bs4.element.Tag:
                row = []
                for td in tr.find_all("td"):
                    row.append(td.string.encode("utf-8"))
                insert_db(cursor, conn, row)
        time.sleep(3)


# 解析出表头
def parse_title(html):
    title_list = []
    for title in html.find_all("td", {"class": "dreport-title"}):
        title_list.append(title.get_text().encode("utf8"))
    return title_list


# post发送请求
def send_request(date):
    post_data = {'startDate': date[0], 'endDate': date[1],
                 'bidAskType': '', 'bigthType': 'FR007',
                 'interestRateType': '', 'message': ''}
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }
    response = session.post(url, post_data, headers=headers)
    html = BeautifulSoup(response.text, "html5lib")
    return html


def connect_db():
    conn = MySQLdb.connect(host='127.0.0.1', user=USER_NAME, passwd=PSSWRD, connect_timeout=10, charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    cursor.execute('SET NAMES utf8')
    try:
        create_db(conn, cursor)

        create_table(cursor)

        query_from_datelist(date_list, cursor, conn)

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    finally:
        cursor.close()
        conn.close()


# 从日期列表中获取数据
connect_db()
