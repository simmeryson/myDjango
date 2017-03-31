# -*- coding:utf-8 -*-
import time
import bs4
from bs4 import BeautifulSoup
import requests
import datetime


class Scraping(object):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }

    iso_format = '%Y-%m-%d'  # 设置输出格式

    def __init__(self, url, form_data):
        self.url = url
        self.form_data = form_data

    # 获得今天的日期
    def get_today_date(self):
        today = datetime.date.today()
        return today.strftime(self.iso_format)

    # 按天间隔生成日期列表
    def gen_date_list(self, start, end):
        _start = datetime.datetime.strptime(start + "", self.iso_format)
        _end = datetime.datetime.strptime(end + "", self.iso_format)
        deltadays = datetime.timedelta(days=1)  # 确定日期差额，如前天 days=2,最多取到2

        gen_list = []
        if _start > _end:
            return gen_list

        s = _start.strftime(self.iso_format)
        gen_list.append((s, s))
        tomorrow = _start + deltadays  # 获取差额日期，明天
        while tomorrow <> _end:
            s = tomorrow.strftime(self.iso_format)
            gen_list.append((s, s))
            tomorrow += deltadays
        s = tomorrow.strftime(self.iso_format)
        gen_list.append((s, s))
        return gen_list

    # 解析出表头
    @staticmethod
    def parse_title(html):
        title_list = []
        for title in html.find_all("td", {"class": "dreport-title"}):
            title_list.append(title.get_text().encode("utf8"))
        return title_list

    # post发送请求
    def send_request(self, date):
        self.form_data['startDate'] = date[0]
        self.form_data['endDate'] = date[1]
        session = requests.Session()
        response = session.post(self.url, self.form_data, headers=self.headers)
        html = BeautifulSoup(response.text, "html5lib")
        return html

    def query_from_datelist(self, datelist, parse_html, save_row):
        for date in datelist:
            html = self.send_request(date)
            # 解析出 行数据
            parse_html(html, save_row)
            time.sleep(3)

    # 查询今天的数据
    def query_today(self, parse_html, save_row):
        today = self.get_today_date() + ""
        single_day = [(today, today)]
        self.query_from_datelist(single_day, parse_html, save_row)
