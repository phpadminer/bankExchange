#!/usr/local/bin/python3.6
# coding:utf-8

# 本脚本功能：
# 抓取每日的人民币中间价涨幅

import urllib

import json

import datetime

from PyMySQL import pymysql

from MysqlConfig import configs

startDate = datetime.datetime.now().strftime("%Y-%m-%d")
endDate = datetime.datetime.now().strftime("%Y-%m-%d")

# 连接数据库
conn = pymysql.connect(**configs)
print('连接数据库..')
# 使用cursor()方法创建一个游标对象
print('连接数据库成功！')
print('正在创建数据库游标...')
cursor = conn.cursor()

print('开始访问目标服务器')
# 准备需要访问的url
url = 'http://www.chinamoney.com.cn/dqs/rest/cm-u-pt/CcprHis?startDate='+startDate+'&endDate='+endDate
# 将读取到的数据储存起来
print('抓取数据中...请耐心等待！')
data = urllib.request.urlopen(url=url).read().decode()
print('抓取成功！')
print('清洗数据中...')
# 将读取的数据转化为dict
DATA = json.loads(data)
field = DATA['data']['head']
FIELD = '('
for x in field:
    # print
    FIELD = FIELD + str(x)
    # FIELD.append(`+str(x)+`)
FIELD = str(tuple(FIELD))
# print(str(FIELD))
records = DATA['records']
print('正在写入数据库...')
for x in range(len(records)-1,0,-1):
    date = records[x]['date']
    record = records[x]['values']
    record = str(tuple(record))
    # print(date)
    year = date.split('-')[0]
    month = date.split('-')[1]
    day = date.split('-')[2]

    sql = "INSERT INTO `oms_exchange_cnums` (`USD/CNY`, `EUR/CNY`, `100JPY/CNY`, `HKD/CNY`, `GBP/CNY`, `AUD/CNY`, `NZD/CNY`, `SGD/CNY`, `CHF/CNY`, `CAD/CNY`, `CNY/MYR`, `CNY/RUB`, `CNY/ZAR`, `CNY/KRW`, `CNY/AED`, `CNY/SAR`, `CNY/HUF`, `CNY/PLN`, `CNY/DKK`, `CNY/SEK`, `CNY/NOK`, `CNY/TRY`, `CNY/MXN`) VALUES "+record
    # print(sql)
    cursor.execute(sql)
    row_id = cursor.lastrowid
    conn.commit()
    _tuple = (int(row_id), int(year), int(month), int(day), date)
    sql = "INSERT INTO `oms_exchange_cmain` (`record_id`,`year`,`month`,`day`,`date`) VALUES "+str(_tuple)
    print(cursor.execute(sql))

cursor.close()

conn.close()

