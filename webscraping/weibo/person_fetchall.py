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

sys.path.append('../../..')
sys.path.append('../..')
from webscraping.db_manage import DbManager

person_list = [('bladeofwind', ur'勿怪幸'), ('1852299857', ur'屠龙的胭脂井'), ('5506682114', ur'断线的红风筝'),
               ('1907214345', ur'侯安扬HF '), ('xaymaca', ur'交易员评论'), ('1704422861', ur'振波二象'),
               ('lapetitprince', ur'杰瑞Au'), ('508637358', ur'___ER___')]
login_users = [('17791252591', 'LKJHGF'), ('17791252590', 'LKJHGF'), ('17791252596', 'LKJHGF')]

driver = webdriver.Firefox(executable_path='/usr/local/bin/geckodriver')
wait = ui.WebDriverWait(driver, 10)


def fetch_weibo_personal():
    start = time.time()
    db = DbManager()
    username, password = login_users[0]  # 输入你的用户名
    user_id = '1852299857'  # 用户id url+id访问个人   https://weibo.cn/bladeofwind?page=188

    # 操作函数
    if LoginWeibo(username, password):  # 登陆微博
        db.create_db("weibo")

        person_url, person_name = person_list[-1]
        db.create_table(create_table_sql(person_url, person_name))
        VisitPersonPage(person_url, db, 857)  # 访问个人页面

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
        # elem_pwd = login_div.find_element_by_xpath("//input[@node-type='password']")
        # elem_pwd = driver.find_element_by_xpath("/html/body/div[2]/form/div/input[2]")
        elem_pwd.clear()
        elem_pwd.send_keys(password)  # 密码
        # elem_rem = driver.find_element_by_name("remember")
        # elem_rem.click()             #记住登录状态
        elem_submits = driver.find_elements_by_xpath(
            "//a[@node-type='submitBtn' and @class='W_btn_a btn_34px' and @tabindex='6']")
        for elm in elem_submits:
            if elm.text == u'登录':
                elem_submit = elm
                elem_submit.click()
        # 重点: 暂停时间输入验证码
        # pause(millisenconds)
        time.sleep(30)

        # elem_sub = driver.find_element_by_name("submit")
        # elem_sub.click()  # 点击登陆
        # time.sleep(2)

        # 获取Coockie 推荐 http://www.cnblogs.com/fnng/p/3269450.html
        print driver.current_url
        # print driver.get_cookies()  # 获得cookie信息 dict存储

        # 保存 Cookies
        # try:
        #     with open("cookies.pkl", 'wb') as f:
        #         dic = {username: driver.get_cookies()}
        #         pickle.dump(dic, f)
        # except EOFError:
        #     pass

        # print u'输出Cookie键值对信息:'
        # for cookie in driver.get_cookies():
        #     # print cookie
        #     for key in cookie:
        #         print key, cookie[key]

        # driver.get_cookies()类型list 仅包含一个元素cookie类型dict
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

def VisitPersonPage(user_id, db, page_index=-1):
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
        # ***************************************************************************
        # No.2 获取微博内容
        # http://weibo.cn/guangxianliuyan?filter=0&page=1
        # 其中filter=0表示全部 =1表示原创
        # ***************************************************************************

        # print '\n'
        # print u'获取微博内容信息'
        # num = 1
        # while num <= 5:
        #     url_wb = "http://weibo.cn/" + user_id + "?filter=0&page=" + str(num)
        #     print url_wb
        #     driver.get(url_wb)
        #     # info = driver.find_element_by_xpath("//div[@id='M_DiKNB0gSk']/")
        #     info = driver.find_elements_by_xpath("//div[@class='c']")
        #     for value in info:
        #         print value.text
        #         info = value.text
        #
        #         # 跳过最后一行数据为class=c
        #         # Error:  'NoneType' object has no attribute 'groups'
        #         if u'设置:皮肤.图片' not in info:
        #             if info.startswith(u'转发'):
        #                 print u'转发微博'
        #                 infofile.write(u'转发微博\r\n')
        #             else:
        #                 print u'原创微博'
        #                 infofile.write(u'原创微博\r\n')
        #
        #             # 获取最后一个点赞数 因为转发是后有个点赞数
        #             str1 = info.split(u" 赞")[-1]
        #             if str1:
        #                 val1 = re.match(r'(.∗?)', str1).groups()[0]
        #                 print u'点赞数: ' + val1
        #                 infofile.write(u'点赞数: ' + str(val1) + '\r\n')
        #
        #             str2 = info.split(u" 转发")[-1]
        #             if str2:
        #                 val2 = re.match(r'(.∗?)', str2).groups()[0]
        #                 print u'转发数: ' + val2
        #                 infofile.write(u'转发数: ' + str(val2) + '\r\n')
        #
        #             str3 = info.split(u" 评论")[-1]
        #             if str3:
        #                 val3 = re.match(r'(.∗?)', str3).groups()[0]
        #                 print u'评论数: ' + val3
        #                 infofile.write(u'评论数: ' + str(val3) + '\r\n')
        #
        #             str4 = info.split(u" 收藏 ")[-1]
        #             flag = str4.find(u"来自")
        #             print u'时间: ' + str4[:flag]
        #             infofile.write(u'时间: ' + str4[:flag] + '\r\n')
        #
        #             print u'微博内容:'
        #             print info[:info.rindex(u" 赞")]  # 后去最后一个赞位置
        #             infofile.write(info[:info.rindex(u" 赞")] + '\r\n')
        #             infofile.write('\r\n')
        #             print '\n'
        #         else:
        #             print u'跳过', info, '\n'
        #             break
        #     else:
        #         print u'next page...\n'
        #         infofile.write('\r\n\r\n')
        #     num += 1
        #     print '\n\n'
        print '**********************************************'


    except Exception, e:
        print "Error: ", e
    finally:
        print u'VisitPersonPage!\n\n'
        print '**********************************************\n'


def parse_weibo_item(url, page, db, user_id):
    item_url = url + "?page=" + str(page)
    print u'访问 page = ' + item_url
    driver.get(item_url)
    find_wait_by_xpath("//input[@name='mp' and @type='hidden']")
    for item in driver.find_elements_by_xpath("//div[@class='c']")[::-1]:
        _id = item.get_attribute('id')
        if _id:
            # inner = item.get_attribute("innerHTML")
            outer = item.get_attribute("outerHTML")
            dic = {'id': _id, 'outerHtml': outer}
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
    sql = "replace into  `" + table_name + \
          "` (`itemId` , " \
          "`contentHtml`" \
          ") " \
          "VALUES (%s, %s)"
    return sql


def insert_dic(dic):
    return (dic['id'], dic['outerHtml'])


fetch_weibo_personal()
