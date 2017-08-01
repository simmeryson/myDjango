# -*- coding:utf-8 -*-

import time
import re
import os
import sys
import codecs
import shutil
import urllib
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pickle
from collections import OrderedDict

sys.path.append('../../..')
sys.path.append('../..')
from webscraping.db_manage import DbManager

login_users = [('17791252591', 'LKJHGF'), ('17791252590', 'LKJHGF'), ('17791252596', 'LKJHGF')]

person_list = [('bladeofwind', ur'勿怪幸'), ('1852299857', ur'屠龙的胭脂井'), ('5506682114', ur'断线的红风筝'),
               ('1907214345', ur'侯安扬HF '), ('xaymaca', ur'交易员评论'), ('1704422861', ur'振波二象'),
               ('lapetitprince', ur'杰瑞Au'), ('508637358', ur'___ER___'), ('1318809415', ur'yanhaijin'),
               ('1614106000', ur'椒图炼丹炉'), ('cloudly', ur'cloudly'), ('2538681243', ur'小不丢灵'), ('wpeak', ur'西峯'),
               ('kuhasu', u'kuhasu'), ('3235523970', ur'霸王日本金融生活史')]

driver = webdriver.Firefox(executable_path='/usr/local/bin/geckodriver')
wait = ui.WebDriverWait(driver, 10)


def fetch_new_weibo():
    start = time.time()
    db = DbManager()
    username, password = login_users[2]  # 输入你的用户名

    # 操作函数
    if LoginWeibo(username, password):  # 登陆微博 ('1852299857', ur'屠龙的胭脂井'),
        db.create_db("weibo")
        # person_url, person_name = person_list[3]
        for person_url, person_name in person_list:
            db.create_table(create_table_sql(person_url, person_name))
            VisitPersonPage(person_url, db, parse_new_weibo)
    end = time.time()
    print end - start
    db.close_db()


def fetch_all_personal():
    start = time.time()
    db = DbManager()
    username, password = login_users[2]  # 输入你的用户名
    user_id = '1852299857'  # 用户id url+id访问个人   https://weibo.cn/bladeofwind?page=188

    # 操作函数
    if LoginWeibo(username, password):  # 登陆微博
        db.create_db("weibo")

        person_url, person_name = person_list[-1]
        db.create_table(create_table_sql(person_url, person_name))
        db.create_table(create_quanwen_table())
        VisitPersonPage(person_url, db, parse_all_weibo)  # 最后参数确定从哪一页开始

    end = time.time()
    print end - start


# ********************************************************************************
#                  第一步: 登陆weibo.cn 获取新浪微博的cookie
#        该方法针对weibo.cn有效(明文形式传输数据) weibo.com见学弟设置POST和Header方法
#                LoginWeibo(username, password) 参数用户名 密码
#                             验证码暂停时间手动输入
# ********************************************************************************

def LoginWeibo(username, password):
    try:
        # **********************************************************************
        # 直接访问driver.get("http://weibo.cn/5824697471")会跳转到登陆页面 用户id
        #
        # 用户名<input name="mobile" size="30" value="" type="text"></input>
        # 密码 "password_4903" 中数字会变动,故采用绝对路径方法,否则不能定位到元素
        #
        # 勾选记住登录状态check默认是保留 故注释掉该代码 不保留Cookie 则'expiry'=None
        # **********************************************************************

        # 输入用户名/密码登录
        print u'准备登陆Weibo.cn网站...'
        driver.get("http://weibo.com/")
        # elem_user = find_wait_by_xpath("//input[@id='loginname']")
        login_btn = find_wait_by_xpath("//a[@node-type='loginBtn']")
        login_btn.click()

        # 载入 Cookies
        # try:
        #     with open("cookies.pkl", 'rb') as f:
        #         dic = pickle.load(f)
        #         cookies = dic[username] if dic else None
        #         if cookies:
        #             for cookie in cookies:
        #                 cookie['domain'] = str(cookie['domain']).replace(".", "", 1) if str(
        #                     cookie['domain']).startswith(".") else cookie['domain']
        #                 driver.add_cookie(cookie)
        # except EOFError:
        #     pass

        login_div = find_wait_by_xpath("//div[@node-type='layoutContent']")
        #
        elem_user = login_div.find_element_by_xpath("//input[@node-type='username' and @tabindex='3']")
        elem_user.clear()
        elem_user.send_keys(username)  # 用户名
        elem_pwd = driver.find_element_by_xpath("//input[@node-type='password' and @tabindex='4']")
        elem_pwd.clear()
        elem_pwd.send_keys(password)  # 密码
        elem_submits = driver.find_elements_by_xpath(
            "//a[@node-type='submitBtn' and @class='W_btn_a btn_34px' and @tabindex='6']")
        for elm in elem_submits:
            if elm.text == u'登录':
                elem_submit = elm
                elem_submit.click()
        # 重点: 暂停时间输入验证码
        # pause(millisenconds)
        time.sleep(30)

        print driver.current_url
        print u'登陆成功...'
        return True

    except Exception, e:
        print "Error: ", e
        return False


# ********************************************************************************
#                  第二步: 访问个人页面http://weibo.cn/5824697471并获取信息
#                                VisitPersonPage()
#        编码常见错误 UnicodeEncodeError: 'ascii' codec can't encode characters
# ********************************************************************************

def VisitPersonPage(user_id, db, parse_item_func, page_index=-1):
    try:
        global infofile
        print u'准备访问个人网站.....'
        # 原创内容 http://weibo.cn/guangxianliuyan?filter=1&page=2
        url = "http://weibo.cn/" + user_id
        driver.get(url)

        # **************************************************************************
        # No.1 直接获取 用户昵称 微博数 关注数 粉丝数
        #      str_name.text是unicode编码类型
        # **************************************************************************

        # 用户id
        print u'个人详细信息'
        print '**********************************************'
        print u'用户id: ' + user_id

        # 昵称
        str_name = find_wait_by_xpath("//div[@class='ut']")
        str_t = str_name.text.split(" ")
        num_name = str_t[0]  # 空格分隔 获取第一个值 "Eastmount 详细资料 设置 新手区"
        print u'昵称: ' + num_name

        # 微博数 除个人主页 它默认直接显示微博数 无超链接
        # Error:  'unicode' object is not callable
        # 一般是把字符串当做函数使用了 str定义成字符串 而str()函数再次使用时报错
        str_wb = driver.find_element_by_xpath("//div[@class='tip2']")
        pattern = r"\d+\.?\d*"  # 正则提取"微博[0]" 但r"(.∗?)"总含[]
        guid = re.findall(pattern, str_wb.text, re.S | re.M)
        print str_wb.text  # 微博[294] 关注[351] 粉丝[294] 分组[1] @他的
        for value in guid:
            num_wb = int(value)
            break
        print u'微博数: ' + str(num_wb)

        # 关注数
        str_gz = driver.find_element_by_xpath("//div[@class='tip2']/a[1]")
        guid = re.findall(pattern, str_gz.text, re.M)
        num_gz = int(guid[0])
        print u'关注数: ' + str(num_gz)

        # 粉丝数
        str_fs = driver.find_element_by_xpath("//div[@class='tip2']/a[2]")
        guid = re.findall(pattern, str_fs.text, re.M)
        num_fs = int(guid[0])
        print u'粉丝数: ' + str(num_fs)

        parse_item_func(db, page_index, url, user_id)

        print '**********************************************'


    except Exception, e:
        print "Error: ", e
        db.close_db()
    finally:
        print u'VisitPersonPage!\n\n'
        print '**********************************************\n'


def parse_all_weibo(db, page_index, url, user_id):
    input_hidden = find_wait_by_xpath("//input[@name='mp' and @type='hidden']")
    if input_hidden:
        all_pages = input_hidden.get_attribute('value') if page_index < 1 else page_index
        for page in range(int(all_pages), 0, -1):
            count = 1
            while count < 6:
                try:
                    parse_weibo_item(url, page, db, user_id)
                    break
                except Exception, e:
                    print "Error: ", e
                    count += 1
                    time.sleep(0.5)
                    print "count: ", count
            else:
                raise Exception("parse weibo item error!")


def get_quanwen_item(db, user_id):
    """全文还需要打开"""
    quanwen_list = driver.find_elements_by_xpath("//span[a='全文']")
    url_list = [quanwen_span.find_element_by_xpath("./a").get_attribute('href') for quanwen_span in quanwen_list]
    for url_ in url_list:
        split_url = url_.split('/')
        if not split_url[-2] == 'comment':
            continue
        driver.get(url_)
        find_wait_by_xpath("//input[@value='评论' and @type='submit']")
        out = driver.find_element_by_xpath("//body").get_attribute('outerHTML')
        maps = {'id': split_url[-1], 'userId': user_id, 'outerHtml': out}
        print u"全文: " + split_url[-1]
        db.insert_db_values(insert_quanwen_values(), insert_quanwen_dic(maps))


def parse_weibo_item(url, page, db, user_id):
    item_url = url + "?page=" + str(page)
    print u'访问 page = ' + item_url
    driver.get(item_url)
    find_wait_by_xpath("//input[@name='mp' and @type='hidden']")
    # for item in driver.find_elements_by_xpath("//div[@class='c']")[::-1]:
    #     _id = item.get_attribute('id')
    #     if _id:
    #         # inner = item.get_attribute("innerHTML")
    #         outer = item.get_attribute("outerHTML")
    #         dic = {'id': _id, 'outerHtml': outer}
    #         db.insert_db_values(insert_sql_values(user_id), insert_dic(dic))
    get_quanwen_item(db, user_id)


def parse_new_weibo(db, page_index, url, user_id):
    new_dic = OrderedDict()
    input_hidden = find_wait_by_xpath("//input[@name='mp' and @type='hidden']")
    if input_hidden:
        last_id = query_last_item_id(db, user_id)
        all_pages = input_hidden.get_attribute('value')
        for page in range(1, int(all_pages)):
            count = 1
            while count < 6:
                try:
                    if parse_new_item(url, page, db, user_id, new_dic, last_id):
                        return
                    else:
                        break
                except Exception, e:
                    print "Error: ", e
                    count += 1
                    time.sleep(0.5)
                    print "count: ", count
            else:
                raise Exception("parse weibo item error!")
            if page == int(all_pages):
                save_whole_page(db, new_dic, user_id)


def parse_new_item(url, page, db, user_id, new_dic, ids):
    item_url = url + "?page=" + str(page)
    print u'访问 page = ' + item_url
    driver.get(item_url)
    find_wait_by_xpath("//input[@name='mp' and @type='hidden']")
    for item in driver.find_elements_by_xpath("//div[@class='c']"):
        _id = item.get_attribute('id')
        if _id:
            # inner = item.get_attribute("innerHTML")
            outer = item.get_attribute("outerHTML")
            new_dic[_id] = outer
            # print u'更新 ' + str(len(new_dic)) + u' 条'
            if ids == _id:
                save_whole_page(db, new_dic, user_id)
                get_quanwen_item(db, user_id)
                return True
    else:
        get_quanwen_item(db, user_id)
        return False


def save_whole_page(db, new_dic, user_id):
    print u'共 ' + str(len(new_dic)) + u'条'
    for new_id in new_dic.keys()[::-1]:
        print u'更新 id: ' + new_id
        dic = {'id': new_id, 'outerHtml': new_dic[new_id]}
        db.insert_db_values(insert_sql_values(user_id), insert_dic(dic))


def find_wait_by_xpath(xpath):
    return WebDriverWait(driver, 100).until(
        EC.presence_of_element_located((By.XPATH, xpath)))

    # *******************************************************************************
    #                                程序入口 预先调用
    # *******************************************************************************

    # if __name__ == '__main__':

    # 定义变量


# driver.add_cookie({'name':'name', 'value':'_T_WM'})
# driver.add_cookie({'name':'value', 'value':'c86fbdcd26505c256a1504b9273df8ba'})

# 注意
# 因为sina微博增加了验证码,但是你用Firefox登陆一次输入验证码,再调用该程序即可,因为Cookies已经保证
# 会直接跳转到明星微博那部分,即: http://weibo.cn/guangxianliuyan


# 在if __name__ == '__main__':引用全局变量不需要定义 global inforead 省略即可
# print 'Read file:'
# user_id = inforead.readline()
# VisitPersonPage(user_id)  # 访问个人页面
# while user_id != "":
#     user_id = user_id.rstrip('\r\n')
#     user_id = inforead.readline()
#     # break

# infofile.close()
# inforead.close()



def create_table_sql(table_name, person_name):
    sql = "CREATE TABLE IF NOT EXISTS `%s`" \
          " (id int(11) NOT NULL AUTO_INCREMENT," \
          "`itemId` VARCHAR(30)  NOT NULL , " \
          "`contentHtml` VARCHAR(15000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL," \
          "PRIMARY KEY(id)," \
          "UNIQUE KEY (`itemId`)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci " \
          "comment='%s'" \
          % (table_name, person_name)
    return sql


def insert_sql_values(table_name):
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "REPLACE INTO  `" + table_name + \
          "` (`itemId` , " \
          "`contentHtml`" \
          ") " \
          "VALUES (%s, %s)"
    return sql


def insert_dic(dic):
    return (dic['id'], dic['outerHtml'])


def query_last_item_id(db, table_name):
    """置顶问题"""
    sql = 'select `itemId`,`contentHtml` from `%s` order by id desc limit 2' % table_name
    ids = db.query(sql)
    if len(ids) == 0:
        return '-1'
    elif len(ids) == 1:
        return ids[0][0]
    elif str(ids[0][1]).find('置顶') != -1:
        return ids[1][0]
    else:
        return ids[0][0]


def create_quanwen_table():
    sql = "CREATE TABLE IF NOT EXISTS `quanwen` " \
          " (id INT(11) NOT NULL AUTO_INCREMENT," \
          "`itemId` VARCHAR(30)  NOT NULL , " \
          "`userId` VARCHAR(30)  NOT NULL , " \
          "`contentHtml` VARCHAR(15000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL," \
          "PRIMARY KEY(id)," \
          "UNIQUE KEY (`itemId`)" \
          ")" \
          "ENGINE=InnoDB " \
          "AUTO_INCREMENT=1 " \
          "DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci "
    return sql


def insert_quanwen_values():
    # 引号坑死人.字段内不能加特殊符号 比如%
    sql = "REPLACE INTO  `quanwen` " \
          "(`itemId` , " \
          "`userId` ," \
          "`contentHtml`" \
          ") " \
          "VALUES (%s, %s, %s)"
    return sql


def insert_quanwen_dic(dic):
    return (dic['id'], dic['userId'], dic['outerHtml'])


fetch_all_personal()

# fetch_new_weibo()
