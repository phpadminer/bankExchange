#!/usr/local/bin/python3.6
# coding:utf-8

# 本脚本功能：
# 抓取每日的人民币中间价涨幅
# 每隔半小时抓取一次
import urllib.parse

from pyquery import PyQuery as PQ

import time

from MysqlConfig import conn , cursor


print('开始访问目标服务器')
# 准备需要访问的url
url = 'http://www.chinamoney.com.cn/fe/static/html/column/basecurve/rmbparity/latestRMBParity.html'
# 将读取到的数据储存起来
print('抓取数据中...请耐心等待！')
doc = urllib.request.urlopen(url=url).read().decode()
# 使用PYquery对读取的数据进行操作
doc = PQ(doc)
print('清洗数据中...')
table = doc('body table').find('table').find('table').eq(1)
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

    date = time.strftime("%Y-%m-%d", time.localtime())
    # 清洗数据
    _tuple = (str(date), str(cur_name), str(mid_price), int(state), str(state_num))
    # 准备sql
    sql = "INSERT INTO `oms_exchange_change` (`date`, `cur_name`, `mid_price`, `state`, `state_num`) VALUES " + str(
        _tuple)
    # 执行sql
    cursor.execute(sql)
    # 返回最新自增id
    row_id = cursor.lastrowid

conn.commit()

cursor.close()

conn.close()
print('抓取成功！bye~')
