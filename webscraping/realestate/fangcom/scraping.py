# -*- coding:utf-8 -*-
import time
import bs4
from bs4 import BeautifulSoup
import requests
import datetime


class Scraping(object):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Cookie": "cityredata=cb50dfaede3c93fc3a6ed905bdbbd24b; cityurl=11b7a3958e53db0; UM_distinctid=15b6267bfcd7e0-0db0b0a89dbf5-396e7807-fa000-15b6267bfcf41e; city=xa; __asc=b4c66e6515b62b426dec7238aa6; __auc=ba09ef2115b62685ed0cd954fcf; _ga=GA1.2.1704712836.1492004618; CNZZDATA1253686598=968704982-1491999749-%7C1492005152",
        "Referer": "http://esf.xian.fang.com/housing/"
    }

    iso_format = '%Y-%m-%d'  # 设置输出格式

    def __init__(self, url=None, form_data=None):
        if form_data is None:
            form_data = {}
        self.url = url
        self.form_data = form_data

    # 获得今天的日期
    def get_today_date(self):
        today = datetime.date.today()
        return today.strftime(self.iso_format)

    # 按天间隔生成日期列表
    def gen_day_tuple(self, start, end):
        _start = datetime.datetime.strptime(start + "", self.iso_format)
        _end = datetime.datetime.strptime(end + "", self.iso_format)
        deltadays = datetime.timedelta(days=1)  # 确定日期差额，如前天 days=2,最多取到2
        gen_list = []
        if _start > _end:
            return gen_list
        if _start == _end:
            return gen_list.append((_start.strftime(self.iso_format), _end.strftime(self.iso_format)))

        s = _start.strftime(self.iso_format)
        gen_list.append((s, s))
        tomorrow = _start + deltadays  # 获取差额日期，明天
        while tomorrow <> _end:
            print tomorrow.month
            s = tomorrow.strftime(self.iso_format)
            gen_list.append((s, s))
            tomorrow += deltadays
        s = tomorrow.strftime(self.iso_format)
        gen_list.append((s, s))
        return gen_list

    # 按天间隔生成日期列表
    def gen_day_list(self, start, end):
        _start = datetime.datetime.strptime(start + "", self.iso_format)
        _end = datetime.datetime.strptime(end + "", self.iso_format)
        deltadays = datetime.timedelta(days=1)  # 确定日期差额，如前天 days=2,最多取到2
        gen_list = []
        if _start > _end:
            return gen_list
        if _start == _end:
            k = _start.strftime(self.iso_format)
            gen_list.append(k)
            return gen_list
        s = _start.strftime(self.iso_format)
        gen_list.append((s, s))
        tomorrow = _start + deltadays  # 获取差额日期，明天
        while tomorrow <> _end:
            s = tomorrow.strftime(self.iso_format)
            gen_list.append(s)
            tomorrow += deltadays
        s = tomorrow.strftime(self.iso_format)
        gen_list.append(s)
        return gen_list

    # 生成月列表
    def gen_month_list(self, start, end):
        _start = datetime.datetime.strptime(start + "", self.iso_format)
        _end = datetime.datetime.strptime(end + "", self.iso_format)
        deltadays = datetime.timedelta(days=1)  # 确定日期差额，如前天 days=2,最多取到2
        gen_list = []
        if _start > _end:
            return gen_list
        if _start == _end:
            k = _start.strftime(self.iso_format)
            v = _end.strftime(self.iso_format)
            gen_list.append((k, v))
            return gen_list

        tomorrow = _start + deltadays  # 获取差额日期，明天
        while tomorrow <> _end:
            tomorrow += deltadays
            if tomorrow.month > _start.month:
                s = _start.strftime(self.iso_format)
                t = (tomorrow - deltadays).strftime(self.iso_format)
                gen_list.append((s, t))
                _start = tomorrow
        s = _start.strftime(self.iso_format)
        t = tomorrow.strftime(self.iso_format)
        gen_list.append((s, t))
        return gen_list

    # post发送请求
    def send_request_post(self):
        session = requests.Session()
        response = session.post(self.url, self.form_data, headers=self.headers)
        html = BeautifulSoup(response.text, "html5lib", from_encoding="utf-8")
        return html

    # get发送请求
    def send_request_get(self, payload={}):
        response = requests.get(self.url, params=payload, headers=self.headers)
        response.encoding = response.apparent_encoding
        html = BeautifulSoup(response.text, "html5lib")
        return html

    def make_post_para(self, paras):
        for (k, v) in paras.iteritems():
            self.form_data[k] = v

    def make_header_para(self, paras):
        for (k, v) in paras.iteritems():
            self.headers[k] = v
