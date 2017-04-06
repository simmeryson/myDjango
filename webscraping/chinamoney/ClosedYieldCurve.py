# -*- coding:utf-8 -*-
# 保存 国债收益率,央行票据,政策性金融债,超短期融资券,政府支持机构债券,短期融资券,中期票据,企业债,同业存单  等债权收益率
# 最大按年查询
import MySQLdb
import bs4
import time

from webscraping.chinamoney.scraping import Scraping
from webscraping.db_manage import DbManager

DB_NAME = 'chinamoney'
TABLE_NAME = 'ClosedYieldCurve'

url = "http://www.chinamoney.com.cn/fe-c/closedYieldCurveHistoryQueryAction.do"

post_data = {'startDateTool': '', 'endDateTool': '', 'showKey': '1,', 'termId': 1.0,
             'bondType': '', 'start': '', 'end': '', 'bondTypeTemp': '', 'reference': 1,
             'termIdTemp': 1.0
             }

singleDay = [('2017-03-01', '2017-03-01')]

date_list = [('2006-01-01', '2006-12-31'), ('2007-01-01', '2007-12-31'), ('2008-01-01', '2008-12-31'),
             ('2009-01-01', '2009-12-31'), ('2010-01-01', '2010-12-31'), ('2011-01-01', '2011-12-31'),
             ('2012-01-01', '2012-12-31'), ('2013-01-01', '2013-12-31'), ('2014-01-01', '2014-12-31'),
             ('2015-01-01', '2015-12-31'), ('2016-01-01', '2016-12-31'), ('2017-01-01', '2017-03-31')]


# 创建表ClosedYieldCurve
def create_table_name_sql():
    sql = "CREATE TABLE IF NOT EXISTS ClosedYieldCurve " \
          "(id int(11) NOT NULL," \
          "name VARCHAR (30) NOT NULL," \
          "PRIMARY KEY(id))" \
          "ENGINE=InnoDB "
    return sql


# 根据债券id创建表
def create_table_sql(table_name):
    sql = "CREATE TABLE IF NOT EXISTS `%s` " \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "date DATE NOT NULL ," \
          "`期限` FLOAT ," \
          "`到期收益率` FLOAT ," \
          "PRIMARY KEY(id)) " \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          % table_name
    return sql


# 插入债券类型 id和name
def insert_db_name_sql(row):
    # 引号坑死人
    sql = "insert into %s (id, name) " \
          "VALUES (%s,'%s')" \
          % (
              TABLE_NAME, row[0], row[1]
          )
    return sql


# 插入每种债券类型收益率数据
def insert_db_sql(table_name, row):
    # 引号坑死人
    sql = "insert into `%s` (date,`期限`,`到期收益率`) " \
          "VALUES ('%s', %s, %s)" \
          % (
              table_name, row[0], row[1], row[2]
          )
    return sql


# 解析债券类型
def parse_html_name(html, save_row, insert_into):
    curve_list = {}
    name_list = html.find("select", {"name": "bondTypeTemp", "id": "bondTypeTemp"})
    for option in name_list.find_all("option"):
        curve_list[option['value']] = option.string.encode("utf-8")

    for (d, x) in curve_list.items():
        print "%s --- %s" % (d, x)
        insert_into(save_row([d, x]))


# 解析收益率数据
def parse_html(html, table_name, save_row, insert_into):
    try:
        trs = html.find("td", {'class': 'dreport-title'}).parent.find_next_siblings('tr')
        for tr in trs:
            row = []
            for td in tr.find_all('td'):
                if td.string:
                    row.append(td.string.encode('utf-8').replace("\xc2\xa0", "").strip())
            if len(row) == 3:
                insert_into(save_row(table_name, row))
    except MySQLdb.Error, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    except:
        print "%s no data!" % table_name
    finally:
        print "%s 抓取完成" % table_name


def do_scraping(dates_list):
    scraper = Scraping(url, post_data)
    db_name = DbManager()
    db_name.create_db(DB_NAME)

    # scrap_names(dates_list[0], db_name, scraper)

    id_list = db_name.query("select id from %s" % TABLE_NAME)

    # scrap_yield_data(dates_list, db_name, id_list, scraper)

    db_name.close_db()


# 抓取债券收益率数据
def scrap_yield_data(dates_list, db_name, id_list, scraper):
    for bond_id in id_list:
        scraper.make_post_para({'bondType': bond_id[0], 'bondTypeTemp': bond_id[0]})
        # db_name.drop_table(bond_id[0])
        db_name.create_table(create_table_sql(bond_id[0]))
        for date in dates_list:
            scraper.make_post_para({'startDateTool': date[0], 'endDateTool': date[1], 'start': date[0], 'end': date[1]})
            html = scraper.send_request_post()
            parse_html(html, bond_id[0], insert_db_sql, db_name.insert_db)
            time.sleep(2)


# 抓取各债券类型和id
def scrap_names(date, db_name, scraper):
    # db_name.drop_table(TABLE_NAME)
    db_name.create_table(create_table_name_sql())

    scraper.make_post_para(
        {'startDateTool': date[0], 'endDateTool': date[1], 'start': date[0], 'end': date[1], 'bondType': 100001,
         'bondTypeTemp': 100001})
    html = scraper.send_request_post()
    # 解析出 行数据
    parse_html_name(html, insert_db_name_sql, db_name.insert_db)
    print "抓取各债券类型和id 完成";
    time.sleep(3)

# do_scraping(date_list)
