#!/usr/local/bin/python3.6
# coding:utf-8

# 本脚本功能：
# 抓取上个月的人民币中间价平均涨幅

import urllib.parse

from pyquery import PyQuery as PQ

import time

from PyMySQL import pymysql

# 准备服务器连接配置数据
print('连接数据库..')
configs = {
    'host': 'localhost',
    'port': 8889,
    'user': 'root',
    'passwd': 'root',
    'db': 'oms',
    'charset': 'utf8'
}
# 连接数据库
conn = pymysql.connect(**configs)
print('连接数据库成功！')
# 创建数据库游标
cursor = conn.cursor()
print('开始访问目标服务器')
# 准备需要访问的url
url = 'http://www.chinamoney.com.cn/fe/static/html/column/basecurve/rmbparity/latestMonthlyAvgParity.html'
# 将读取到的数据储存起来
print('抓取数据中...请耐心等待！')
doc = urllib.request.urlopen(url=url).read().decode()
# 使用PYquery对读取的数据进行操作
doc = PQ(doc)
print('清洗数据中...')
table = doc('body>table').find('div').find('table')
for x in table.items('tr'):
    # 获取需要的数据
    cur_name = x.find('td').eq(1).text()
    mid_price = x.find('td').eq(2).text()
    state_num = x.find('td').eq(3).find('span').text()
    state = x.find('td').eq(3).find('span').attr('class')
    # 对涨跌进行判断
    if state == 'txt-dwn':
        state = 2
    else:
        state = 1

    month = time.strftime("%m", time.localtime())
    year = time.strftime("%Y", time.localtime())
    if int(month) - 1 == 0:
        month = 12
        year = int(year) - 1
    else:
        month = int(month) - 1
    date = str(year) + '-' + str(month)

    # 清洗数据
    _tuple = (str(date), str(cur_name), str(mid_price), int(state), str(state_num))
    # 准备sql
    sql = "INSERT INTO `oms_exchange_change_avg` (`date`, `cur_name`, `mid_price`, `state`, `state_num`) VALUES " + str(
        _tuple)
    # 执行sql

    cursor.execute(sql)
    # 返回最新自增id
    row_id = cursor.lastrowid

conn.commit()

cursor.close()

conn.close()
print('抓取成功！bye~')
