# -*- coding:utf-8 -*-
import csv

import time

import bs4
from bs4 import BeautifulSoup
import requests
import os

url = "http://www.chinamoney.com.cn/fe-c/interestRateSwapCurve3MHistoryAction.do?lan=cn"

formdata = "startDate=%s&endDate=%s&bidAskType=&bigthType=FR007&interestRateType=&message="

date_list = [('2017-03-01', '2017-03-29'), ('2017-02-01', '2017-02-28'), ('2017-01-01', '2017-01-31'),
             ('2016-12-01', '2016-12-31'), ('2016-11-01', '2016-11-30'), ('2016-10-01', '2016-10-31'),
             ('2016-09-01', '2016-09-30'), ('2016-08-01', '2016-08-31')]

singleDay = [('2017-03-01', '2017-03-01')]


def query_from_datelist(datelist):
    for data in datelist:
        file_path = "%s - %s.csv" % (data[0], data[1])
        if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
            print file_path + " exists"
            continue
        else:
            csv_file = open(file_path, "wb")
            csv_writer = csv.writer(csv_file, delimiter=',')
            html = send_request(data)

            title_list = parse_title(html)
            if len(title_list) > 0:
                csv_writer.writerow(title_list)

            # 解析出 行数据
            tr_list = html.find("td", {"class": "dreport-title"}).parent.next_siblings
            for tr in tr_list:
                if type(tr) == bs4.element.Tag:
                    row = []
                    for td in tr.find_all("td"):
                        row.append(td.string.encode("utf-8"))
                    csv_writer.writerow(row)

            print (file_path + " created!")
            csv_file.close()

            time.sleep(3)


# 解析出表头
def parse_title(html):
    title_list = []
    for title in html.find_all("td", {"class": "dreport-title"}):
        title_list.append(title.get_text().encode("utf8"))
    return title_list


# post发送请求
def send_request(data):
    post_data = {'startDate': data[0], 'endDate': data[1],
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

# 从日期列表中获取数据
query_from_datelist(singleDay)
