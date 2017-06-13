# -*- coding:utf-8 -*-
# fang.com 小区数据, 日查询.
import re
import sys

import bs4
from concurrent import futures

from CourtBasicInfo import CourtBasicInfo
from CourtFacility import CourtFacility
from CourtContextInfo import CourtContextInfo
from webscraping.proxy.ProxyProvider import ProxyProvider

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
trade_url = 'chushou/list/-h332-j340/'

city_list = [('xa', 'XianHousePrice'),
             # ('bj', 'BeijingHousePrice'), ('sh', 'ShanghaiHousePrice'),
             # ('sz', 'ShenzhenHousePrice'), ('gz', 'GuangzhouHousePrice'), ('cd', 'ChengduHousePrice'),
             # ('hz', 'HangzhouHousePrice'),
             ]

table_name = 'fangcomCourtSellingOnBoard'

num_re = re.compile(ur'\w*-?\d+\.?\d*')
date_rx = re.compile(ur'\d{4}[/-]?\d+[/-]?\d+')
time_tx = re.compile(ur'\w*前更新')

huxing_rx = re.compile(ur'^户型\w*')
shiting_rx = re.compile(ur'\d室\d厅\w*')
mianji_rx = re.compile(ur'\w*面积\w*')
louceng_rx = re.compile(ur'^楼层\w*')
chaoxiang_rx = re.compile(ur'^朝向\w*')
zhuangxiu_rx = re.compile(ur'^装修\w*')
niandai_rx = re.compile(ur'\w*年代\w*')
jiegou_rx = re.compile(ur'\w*结构\w*')
zhuzhai_rx = re.compile(ur'\w*住宅类别\w*')
jianzhu_rx = re.compile(ur'\w*建筑类别\w*')
chanquan_rx = re.compile(ur'\w*产权\w*')

fangyuan_rx = re.compile(ur'\w*房源编号\w*')

proxyProvider = ProxyProvider()


#
def drop_tables(db):
    db.drop_table(table_name)


# 总数据
# creprice.cn的房价数据
def create_fangcomCourtSellingOnBoard_table_sql():
    sql = "CREATE TABLE IF NOT EXISTS fangcomCourtSellingOnBoard" \
          "(id int(11) NOT NULL AUTO_INCREMENT," \
          "HouseNum INT (10), " \
          "PublishDate DATE NOT NULL , " \
          "SellingPrice INT , " \
          "RoomType VARCHAR (20), " \
          "Area FLOAT NOT NULL , " \
          "FloorInfo VARCHAR (20), " \
          "Direction VARCHAR (10), " \
          "Decor VARCHAR (10), " \
          "Structure VARCHAR (20), " \
          "HouseCategory VARCHAR (20), " \
          "BuildingCategory VARCHAR (20), " \
          "PropertyRight VARCHAR (20), " \
          "HouseHistory INT , " \
          "SchoolId INT , " \
          "Telephone VARCHAR (30), " \
          "TagSet SET ('满五唯一','满二','优质教育','特价房','业主自评','地铁', '钥匙', '不限购') , " \
          "TrainNote VARCHAR (50)," \
          "nameId INT NOT NULL ," \
          "FOREIGN Key(nameId) REFERENCES  fangcomSecondHandCourtName(id) on delete cascade on update cascade, " \
          "FOREIGN Key(SchoolId) REFERENCES  fangcomXianSchool(id) on delete cascade on update cascade, " \
          "PRIMARY KEY(id)," \
          "UNIQUE KEY (Area, RoomType, FloorInfo, Decor, HouseHistory, nameId)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8"
    return sql


def insert_fangcomCourtSellingOnBoard_sql_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "replace into fangcomCourtSellingOnBoard " \
          "(HouseNum , " \
          "PublishDate  , " \
          "SellingPrice  , " \
          "RoomType , " \
          "Area , " \
          "FloorInfo , " \
          "Direction , " \
          "Decor , " \
          "Structure , " \
          "HouseCategory , " \
          "BuildingCategory , " \
          "PropertyRight , " \
          "HouseHistory , " \
          "SchoolId , " \
          "Telephone , " \
          "TagSet , " \
          "TrainNote ," \
          "nameId " \
          ") " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    return sql


def insert_fangcomCourtSellingOnBoard_value(row):
    return (row[0], row[1] or None, row[2] or None, row[3] or None,
            row[4] or None, row[5] or None, row[6] or None,
            row[7] or None, row[8] or None, row[9] or None,
            row[10] or None, row[11] or None, row[12] or None,
            row[13] or None, row[14] or None, row[15] or None,
            row[16] or None, row[17]
            )


def query_court_id(court, db):
    ids = db.query('select id from fangcomSecondHandCourtName where name= %s' % court)
    return ids[0] if len(ids) > 0 else None


def query_school_id(schoolname, db):
    ids = db.query('select id from fangcomXianSchool where SchoolName= %s' % schoolname)
    return ids[0] if len(ids) > 0 else None


def parse_trade_html(html, court_id, db, scraper):
    try:
        with futures.ThreadPoolExecutor(max_workers=45) as executor:  # 多线程
            for p in html.find_all('p', class_='fangTitle'):
                executor.submit(save_line, court_id, p)

    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], table_name)


def save_line(court_id, p):
    if not p.a and not p.a.get('href'):
        return
    url = p.a['href']
    scraper = Scraping(url)
    while True:
        proxy = proxyProvider.pick()
        try:
            scraper.make_header_para({'Referer': scraper.url})
            html_detail = scraper.send_request('GET', None, {"https": proxy.url}, 5, False)
            parse_detai_html(html_detail, court_id, scraper, url)
            break
        except Exception as ex:
            proxy.fatal_error()


def parse_detai_html(html_detail, court_id, scraper, url):
    try:
        row = []
        fangyuan = html_detail.find_all(string=fangyuan_rx)
        if len(fangyuan) > 0:
            div = fangyuan[0].parent.parent
        if not div:
            div = html_detail.find('p', class_='gray9 titleAdd') or html_detail.find('p', class_='gray9')
        if not div:
            return

        db = DbManager()
        db.create_db('XianHousePrice')

        spans = div.find_all('span')
        tag_list = []
        # 标签和发布时间
        train_note = None
        house_num = None
        publish_date = None
        for span in spans:
            if span.get('class'):
                if span['class'][0] == 'colorPink':
                    tag_list.append(span.string.encode('utf-8').strip())
                elif span['class'][0] == 'train':
                    train_note = span.string.encode('utf-8').strip()
                elif span['class'][0] == 'mr10':
                    str_ = num_re.findall(span.string.encode('utf-8').strip())
                    if len(str_) > 0 and not house_num:
                        house_num = str_[0]

            elif not publish_date and not span.get('class'):
                publish = date_rx.findall(span.string.encode('utf-8').strip())
                if len(publish) == 0:
                    publish = time_tx.findall(span.string.encode('utf-8').strip())
                    if len(publish) > 0:
                        publish_date = scraper.get_today_date()
                else:
                    publish_date = publish[0].replace('/', '-') if len(publish) > 0 else None

                    # 具体信息
        school_id = None
        selling_price = None
        details = html_detail.find('div', class_='inforTxt')
        for span in details.find_all('span', class_='padl27'):
            span.extract()

        for dt in details.find_all('dt'):
            for span in dt.find_all('span'):
                if span.get('class') and span['class'][0] == 'red20b':
                    selling_price = span.string.encode('utf-8').strip()
            if dt.get('class') and dt['class'][0] == 'esftjftop':
                tag_list.append(dt.span.string.encode('utf-8').strip())
            elif dt.find('div', id='schoolname'):
                schoolname = dt.find('div', id='schoolname').a.string.encode('utf-8').strip()
                school_id = query_school_id(schoolname, db)
                # 房屋属性
        type_list = {'RoomType': None, 'Area': None, 'FloorInfo': None,
                     'Direction': None, 'Decor': None, 'Structure': None,
                     'HouseCategory': None, 'BuildingCategory': None,
                     'PropertyRight': None, 'HouseHistory': None}
        for dd in details.find_all('dd'):
            if len(dd.find_all(string=shiting_rx)) > 0:
                if len(dd.find_all(string=huxing_rx)) > 0:
                    string = dd.find_all(string=huxing_rx)[0].string.encode('utf-8').strip().split('：')[1].strip()
                else:
                    string = dd.find_all(string=shiting_rx)[0].encode('utf-8').strip()
                type_list['RoomType'] = string
            elif len(dd.find_all(string=mianji_rx)) > 0:
                mianji = dd.find_all(string=mianji_rx)[0].parent.span
                if mianji:
                    area = num_re.findall(mianji.string.encode('utf-8').strip())[0]
                else:
                    string = dd.find_all(string=mianji_rx)[0].string.encode('utf-8').strip()
                    area = num_re.findall(string)[0]
                type_list['Area'] = area
            elif len(dd.find_all(string=louceng_rx)) > 0:
                tag = dd.find_all(string=louceng_rx)
                string = dd.find_all(string=louceng_rx)[0].string.encode('utf-8').strip()
                type_list['FloorInfo'] = string.split('：')[1].strip()
            elif len(dd.find_all(string=chaoxiang_rx)) > 0:
                span = dd.find_all(string=chaoxiang_rx)[0].parent
                if not span.span:
                    string = dd.find_all(string=chaoxiang_rx)[0].string.encode('utf-8').strip().split('：')[
                        1].strip()
                else:
                    string = span.string.encode('utf-8').strip()
                type_list['Direction'] = string
            elif len(dd.find_all(string=zhuangxiu_rx)) > 0:
                string = dd.find_all(string=zhuangxiu_rx)[0].string.encode('utf-8').strip()
                type_list['Decor'] = string.split('：')[1].strip()
            elif len(dd.find_all(string=zhuzhai_rx)) > 0:
                string = dd.find_all(string=zhuzhai_rx)[0].parent
                if string.parent:
                    type_list['HouseCategory'] = string.parent.text.split('：')[1].strip()
            elif len(dd.find_all(string=jianzhu_rx)) > 0:
                string = dd.find_all(string=jianzhu_rx)[0].parent
                if string.parent:
                    type_list['BuildingCategory'] = string.parent.text.split('：')[1].strip()
            elif len(dd.find_all(string=chanquan_rx)) > 0:
                string = dd.find_all(string=chanquan_rx)[0].parent
                if string.parent:
                    type_list['PropertyRight'] = string.parent.text.split('：')[1].strip()
            elif len(dd.find_all(string=niandai_rx)) > 0:
                if dd.span:
                    if dd.span.string:
                        string = num_re.findall(dd.span.string.encode('utf-8').strip())
                    else:
                        for content in dd.contents:
                            if content == bs4.element.NavigableString and len(content.strip()) > 0:
                                string = content.encode('utf-8').strip()
                else:
                    _string = dd.find_all(string=niandai_rx)[0].string.encode('utf-8').strip()
                    string = num_re.findall(_string)[0]
                type_list['HouseHistory'] = string
            elif len(dd.find_all(string=jiegou_rx)) > 0:
                string = dd.find_all(string=zhuangxiu_rx)[0].string.encode('utf-8').strip()
                type_list['Structure'] = string.split('：')[1].strip()
            else:
                if dd.span:
                    if len(niandai_rx.findall(dd.span.string if dd.span.string else dd.span.text)) > 0:
                        for content in dd.contents:
                            if type(content) == bs4.element.NavigableString:
                                type_list['HouseHistory'] = num_re.findall(content)[0] if len(
                                    num_re.findall(content)) > 0 else None
                    elif len(zhuangxiu_rx.findall(dd.span.string if dd.span.string else dd.span.text)) > 0:
                        for content in dd.contents:
                            if type(content) == bs4.element.NavigableString and len(content.strip()) > 0:
                                type_list['Decor'] = content.encode('utf-8').strip()
                    elif len(chaoxiang_rx.findall(dd.span.string if dd.span.string else dd.span.text)) > 0:
                        for content in dd.contents:
                            if type(content) == bs4.element.NavigableString and len(content.strip()) > 0:
                                type_list['Direction'] = content.encode('utf-8').strip()
                    elif len(louceng_rx.findall(dd.span.string if dd.span.string else dd.span.text)) > 0:
                        for content in dd.contents:
                            if type(content) == bs4.element.NavigableString and len(content.strip()) > 0:
                                type_list['FloorInfo'] = content.encode('utf-8').strip()
                    elif len(mianji_rx.findall(dd.span.string if dd.span.string else dd.span.text)) > 0:
                        for content in dd.contents:
                            if type(content) == bs4.element.NavigableString and len(content.strip()) > 0:
                                type_list['Area'] = num_re.findall(content)[0] if len(
                                    num_re.findall(content)) > 0 else None
                    elif len(huxing_rx.findall(dd.span.string if dd.span.string else dd.span.text)) > 0:
                        for content in dd.contents:
                            if type(content) == bs4.element.NavigableString and len(content.strip()) > 0:
                                type_list['RoomType'] = content.encode('utf-8').strip()
                    elif len(jiegou_rx.findall(dd.span.string if dd.span.string else dd.span.text)) > 0:
                        for content in dd.contents:
                            if type(content) == bs4.element.NavigableString and len(content.strip()) > 0:
                                type_list['Structure'] = content.encode('utf-8').strip()
                    elif len(zhuzhai_rx.findall(dd.span.string if dd.span.string else dd.span.text)) > 0:
                        for content in dd.contents:
                            if type(content) == bs4.element.NavigableString and len(content.strip()) > 0:
                                type_list['HouseCategory'] = content.encode('utf-8').strip()
                    elif len(jianzhu_rx.findall(dd.span.string if dd.span.string else dd.span.text)) > 0:
                        for content in dd.contents:
                            if type(content) == bs4.element.NavigableString and len(content.strip()) > 0:
                                type_list['BuildingCategory'] = content.encode('utf-8').strip()
                    elif len(chanquan_rx.findall(dd.span.string if dd.span.string else dd.span.text)) > 0:
                        for content in dd.contents:
                            if type(content) == bs4.element.NavigableString and len(content.strip()) > 0:
                                type_list['PropertyRight'] = content.encode('utf-8').strip()
        telephone = []
        tel_div = details.find('span', class_='tel')
        if tel_div:
            for child in tel_div.children:
                telephone.append(child.string.encode('utf-8').strip() if child.string else '')
        row.append(house_num)
        row.append(publish_date)
        row.append(selling_price)
        row.append(type_list['RoomType'])
        row.append(type_list['Area'])
        row.append(type_list['FloorInfo'])
        row.append(type_list['Direction'])
        row.append(type_list['Decor'])
        row.append(type_list['Structure'])
        row.append(type_list['HouseCategory'])
        row.append(type_list['BuildingCategory'])
        row.append(type_list['PropertyRight'])
        row.append(type_list['HouseHistory'])
        row.append(school_id)
        row.append(''.join(telephone))
        row.append(','.join(tag_list))
        row.append(train_note)
        row.append(court_id)
        if len(row) == 18:
            db.insert_db_values(insert_fangcomCourtSellingOnBoard_sql_values(),
                                insert_fangcomCourtSellingOnBoard_value(row))
        else:
            print "wrong row size: " + " ".join(row)

    except MySQLdb.Error, e:
        print "Mysql Error %d: %s  on Table:%s" % (e.args[0], e.args[1], table_name)
    except HTMLParseError, e:
        print "HTMLParseError %d: %s! on Table:%s" % (e.args[0], e.args[1], table_name)
    except Exception as ex:
        print "error ->>court_id:" + court_id + "   msg:" + ex
    finally:
        db.close_db()
        return "%s 抓取完成" % url


def scrap_detail(db):
    drop_tables(db)
    db.create_table(create_fangcomCourtSellingOnBoard_table_sql())
    court_list = db.query('select id, name, DetailUrl from fangcomSecondHandCourtName ')
    for (court_id, court_name, court_url) in court_list:
        print '小区:' + court_name + "  开始获取详情"
        url = court_url.replace('esf/', trade_url) if court_url.find('esf') != -1 else court_url + trade_url
        if url.find('house-xm') != -1:
            continue
        scraper = Scraping(url)
        try:
            send_request(court_id, court_url, db, url, scraper)
        except Exception as ex:
            print "error ->> court id:" + str(court_id) + "  " + str(url) +  "   msg:" + ex


def send_request(court_id, court_url, db, url, scraper):
    scraper.url = url
    scraper.make_header_para({'Referer': court_url})
    html = scraper.send_request_get()
    parse_trade_html(html, court_id, db, scraper)
    next_page = html.find('a', id='ctl00_hlk_next') or html.find('a', id='PageControl1_hlk_next')

    if next_page and next_page.get('href'):
        next_url = next_page['href']
        send_request(court_id, url, db, next_url, scraper)


def scraping_today():
    db = DbManager()

    for city, db_name in city_list:
        db.create_db(db_name)
        scrap_detail(db)

    db.close_db()


scraping_today()
