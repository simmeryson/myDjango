# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import re

from colorama import Fore
from django.shortcuts import render
from prettytable import PrettyTable
import requests


class TrainsCollection:
    header = '车次 车站 时间 历时 一等 二等 软卧 硬卧 硬座 无座'.split()

    def __init__(self, available_trains, options):
        """查询到的火车班次集合

        :param available_trains: 一个列表, 包含可获得的火车班次, 每个
                                 火车班次是一个字典
        :param options: 查询的选项, 如高铁, 动车, etc...
        """
        self.available_trains = available_trains
        self.options = options

    def _get_duration(self, raw_train):
        duration = raw_train.get('lishi').replace(':', '小时') + '分'
        if duration.startswith('00'):
            return duration[4:]
        if duration.startswith('0'):
            return duration[1:]
        return duration

    @property
    def trains(self):
        for raw_train in self.available_trains:
            raw_train = raw_train['queryLeftNewDTO']
            train_no = raw_train['station_train_code']
            initial = train_no[0].lower()
            if not self.options or initial in self.options:
                train = [
                    train_no,
                    # '\n'.join([Fore.GREEN + raw_train['from_station_name'] + Fore.RESET,
                    #            Fore.RED + raw_train['to_station_name'] + Fore.RESET]),
                    # '\n'.join([Fore.GREEN + raw_train['start_time'] + Fore.RESET,
                    #            Fore.RED + raw_train['arrive_time'] + Fore.RESET]),
                    '\n'.join([raw_train['from_station_name'],
                               raw_train['to_station_name']]),
                    '\n'.join([raw_train['start_time'],
                               raw_train['arrive_time']]),
                    self._get_duration(raw_train),
                    raw_train['zy_num'],
                    raw_train['ze_num'],
                    raw_train['rw_num'],
                    raw_train['yw_num'],
                    raw_train['yz_num'],
                    raw_train['wz_num'],
                ]
                yield train

    def pretty_print(self):
        pt = PrettyTable()
        pt._set_field_names(self.header)
        for train in self.trains:
            pt.add_row(train)
        print(pt)
        return pt


def parse_station_name():
    url = "https://kyfw.12306.cn/otn/resources/js/framework/station_name.js?station_version=1.8994"
    response = requests.get(url, verify=False)
    stations = re.findall(u'([\u4e00-\u9fa5]+)\|([A-Z]+)', response.text)
    # pprint(dict(stations), indent=4)
    return dict(stations)


# Create your views here.
def query(request):
    dict_get = request.GET
    if dict_get:
        station_dict = parse_station_name()
        from_station = dict_get['from_station']
        to_station = dict_get['to_station']

        from_station = station_dict.get(from_station)
        to_station = station_dict.get(to_station)
        date = dict_get['date'].encode('utf-8')
        url = 'https://kyfw.12306.cn/otn/leftTicket/queryZ?leftTicketDTO.train_date={}&leftTicketDTO.from_station={}&leftTicketDTO.to_station={}&purpose_codes=ADULT'.format(
            date, from_station, to_station)
        # 获取参数
        options = ''.join([key for key, value in dict_get.items() if value is True])
        res = requests.get(url, verify=False)
        available_trains = res.json()['data']
        pt = TrainsCollection(available_trains, options).pretty_print()
        return render(request, 'ticket/query.html', {'post_list': str(pt), 'width': 20, 'height': 100})
    return render(request, 'ticket/query.html', {})
