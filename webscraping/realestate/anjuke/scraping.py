# -*- coding:utf-8 -*-
import time
import bs4
from bs4 import BeautifulSoup
import requests
import datetime, calendar


class Scraping(object):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Cookie": "sessid=5617FF67-F48A-2597-D866-D63EBFA14295;",
        "Referer": "https://xa.anjuke.com/market/chengbeic/"
    }

    iso_format = '%Y-%m-%d'  # 设置输出格式
    mon_format = '%Y-%m'
    year_format = '%Y'

    def __init__(self, url=None, form_data=None):
        if form_data is None:
            form_data = {}
        self.url = url
        self.form_data = form_data
        self.html_response = None
        self.html_text = None

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

    def get_month_list_with_length(self, length, start):
        month_list = []
        today = datetime.date.today()
        year = today.strftime(self.year_format)
        month = year + "-" + start if len(start) == 2 else year + "-0" + start
        month_list.append(month)
        for i in range(length):
            date = datetime.datetime.strptime(month, "%Y-%m")
            month = (date - datetime.timedelta(days=1)).strftime(self.mon_format)
            month_list.append(month)
            # length -= 1

        # for i in range(length):
        #     date = datetime.datetime.strptime(month, "%Y-%m")
        #     days = calendar.monthrange(date.year, date.month)[1]
        #     dalta = datetime.timedelta(days=days)
        #     month = (date - dalta).strftime(self.mon_format)
        #     month_list.append(month)
        return month_list

    # post发送请求
    def send_request_post(self):
        session = requests.Session()
        response = session.post(self.url, self.form_data, headers=self.headers)
        html = BeautifulSoup(response.text, "html5lib", from_encoding="utf-8")
        return html

    # get发送请求
    def send_request_get(self, payload={}, timeout=None):
        response = requests.get(self.url, params=payload, headers=self.headers, timeout=timeout)
        response.encoding = response.apparent_encoding  # 网页设定编码格式
        html = BeautifulSoup(response.text, "html5lib")
        return html

    def send_request(self, method, data, proxies, timeout, verify):
        response = requests.request(method=method, url=self.url, data=data,
                                    headers=self.headers, proxies=proxies, timeout=timeout, verify=verify)
        response.encoding = response.apparent_encoding  # 网页设定编码格式
        html = BeautifulSoup(response.text, "html5lib")
        return html

    def make_post_para(self, paras):
        for (k, v) in paras.iteritems():
            self.form_data[k] = v

    def make_header_para(self, paras):
        for (k, v) in paras.iteritems():
            self.headers[k] = v

    def send_get_return_text(self):
        response = requests.get(self.url, headers=self.headers)
        response.encoding = response.apparent_encoding  # 网页设定编码格式
        self.html_response = BeautifulSoup(response.text, "html5lib")
        self.html_text = response.text
        return response.text

    def get_html_response(self):
        return self.html_response

    def get_html_text(self):
        return self.html_text
