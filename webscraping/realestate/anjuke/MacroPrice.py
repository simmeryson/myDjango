# -*- coding:utf-8 -*-
# fang.com 小区数据, 日查询.
import re
import sys
import json

import bs4
from concurrent import futures

# from webscraping.proxy.ProxyProvider import ProxyProvider

sys.path.append('../../..')
sys.path.append('../..')

from HTMLParser import HTMLParseError

import MySQLdb
import time

from scraping import Scraping
from webscraping.db_manage import DbManager

reload(sys)
sys.setdefaultencoding('utf8')

# 住宅 小区

city_list = [('xa', 'XianHousePrice', 'https://xa.anjuke.com/market/'),
             ('bj', 'BeijingHousePrice', 'https://beijing.anjuke.com/market/'),
             ('sh', 'ShanghaiHousePrice', 'https://shanghai.anjuke.com/market/'),
             ('hz', 'HangzhouHousePrice', 'https://hangzhou.anjuke.com/market/'),
             ('sz', 'ShenzhenHousePrice', 'https://shenzhen.anjuke.com/market/'),
             ('cd', 'ChengduHousePrice', 'https://chengdu.anjuke.com/market/'),
             ('gz', 'GuangzhouHousePrice', 'https://guangzhou.anjuke.com/market/')
             ]

table_name = 'anjukePriceOfDistrict'
quyu_rx = re.compile(ur'^区域\s')
price_rx = re.compile(ur"(id:'regionChart',[\s\S]*?ydata:)(?P<first>\[(.*?)\])")

num_rx = re.compile(ur'\d+')


# proxyProvider = ProxyProvider()


#
def drop_tables(db):
    db.drop_table(table_name)


def create_anjukePriceOfDistrict_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS anjukePriceOfDistrict" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "`date` VARCHAR (20) NOT NULL , " \
          "`price` INT (8), " \
          "`district` VARCHAR (50) NOT NULL ," \
          "`belong` VARCHAR (50)  NOT NULL," \
          "PRIMARY KEY(id)," \
          "UNIQUE KEY (`date`,`district`, `belong`)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_anjukePriceOfDistrict_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "replace into anjukePriceOfDistrict " \
          "(`date` , " \
          "`price`," \
          "`district`," \
          "`belong`" \
          ") " \
          "VALUES (%s, %s, %s, %s)"
    return sql


def insert_anjukePriceOfDistrict_value(row):
    return (row[0], row[1] or None, row[2], row[3]
            )


def insert_anjukePriceOfDistrict_dic(dic):
    return (dic['date'], dic['price'], dic['district'], dic['belong']
            )


def parse_html(scraper, name, db):
    html = scraper.get_html_response()
    dist = html.find_all('div', class_='smallArea')
    if not dist:
        save_line(db, html, name, scraper)

    elif len(dist) > 0:
        for a in dist[0].find_all('a'):
            with futures.ThreadPoolExecutor(max_workers=20) as executor:  # 多线程
                executor.submit(second_district, a, db, name, scraper)


def second_district(a, db, name, scraper):
    url = a['href'] if a.get('href') else None
    if url:
        second_name = a.string.encode('utf-8').strip()
        scraper.url = url
        scraper.send_get_return_text()
        second_html = scraper.get_html_response()
        save_line(db, second_html, second_name, scraper, name)


def save_line(db, html, name, scraper, belong=None):
    data_list = parse_data_list(scraper.get_html_text())
    h2 = html.find('h2', class_='highLight')
    if h2:
        price = h2.em.string.encode('utf-8').strip()
        if price == str(data_list[-1]):
            string = h2.next.encode('utf-8').strip()
            month = re.findall(num_rx, string)[0]
            month_list = scraper.get_month_list_with_length(len(data_list) - 1, month)
            data_list = data_list[::-1]
            for i in range(len(month_list)):
                if not belong:
                    belong = name
                dic = {'date': month_list[i], 'price': data_list[i], 'district': name, 'belong': belong}
                # row = [month_list[i], data_list[i], name, None]
                db.insert_db_values(insert_anjukePriceOfDistrict_sql_values(),
                                    insert_anjukePriceOfDistrict_dic(dic))


def district_price(name, url, scraper, db):
    # for (name, url) in dist_dic:
    scraper.url = url
    scraper.send_get_return_text()
    parse_html(scraper, name, db)


def parse_data_list(html_text):
    match = re.search(price_rx, html_text.encode('utf-8'))
    price = match.group('first') + "}"
    data = json.loads(price[1:] if price.startswith('[') else price)
    data_list = data['data']
    return data_list


def getDistricts(db, url):
    # drop_tables(db)
    db.create_table(create_anjukePriceOfDistrict_table_sql())
    scraper = Scraping(url)
    html = scraper.send_request_get()
    quyu = html.find_all(string=quyu_rx)
    if len(quyu) < 1:
        return
    span = quyu[0].parent
    dist_dic = []
    for a in span.find_next_siblings('a'):
        key = a.string.encode('utf-8').strip()
        val = a['href']
        dist_dic.append((key, val))
        # row = [key, None]
        # db.insert_db_values(insert_anjukeDistricts_sql_values(), insert_anjukeDistricts_value(row))
    # district_price(dist_dic, scraper, db)
    for (name, url) in dist_dic:
        with futures.ThreadPoolExecutor(max_workers=20) as executor:  # 多线程
            executor.submit(district_price, name, url, scraper, db)


def scraping_today():
    start = time.time()
    db = DbManager()
    try:
        for city, db_name, url in city_list:
            print city
            db.create_db(db_name)
            getDistricts(db, url)

        db.close_db()
    except Exception as ex:
        print ex

    end = time.time()
    print end - start


scraping_today()
