#!/usr/local/bin/python3.6
# coding:utf-8

# 本脚本功能：
# 抓取每日的人民币中间价涨幅

import urllib.request

import json

import datetime


from PyMySQL import pymysql

from MysqlConfig import configs

startDate = datetime.datetime.now().strftime("%Y-%m-%d")
endDate = datetime.datetime.now().strftime("%Y-%m-%d")

startYear = startDate.split('-')[0]
startMonth = startDate.split('-')[1]
endYear = endDate.split('-')[0]
endMonth = endDate.split('-')[1]


# 连接数据库
conn = pymysql.connect(**configs)
print('连接数据库..')
# 使用cursor()方法创建一个游标对象
print('连接数据库成功！')
print('正在创建数据库游标...')
cursor = conn.cursor()

print('开始访问目标服务器')
# 准备需要访问的url
url = 'http://www.chinamoney.com.cn/dqs/rest/cm-u-pt/CcprMthAvgHis?startYear=2018&startMonth=1&endYear=2018&endMonth=1'
# 将读取到的数据储存起来
print('抓取数据中...请耐心等待！')
data = urllib.request.urlopen(url=url).read().decode()
print('抓取成功！')
print('清洗数据中...')
# 将读取的数据转化为dict
DATA = json.loads(data)
print(DATA)
field = DATA['data']['head']

records = DATA['records']
print('正在写入数据库...')
for x in range(len(records)-1,-1,-1):
    date = records[x]['date']
    record = records[x]['values']
    # 组织update语句
    updateinfo = ''
    for x in range(0, len(field)):
        updateinfo = updateinfo + '`'+field[x] + '` =' + record[x] + ','
    updateinfo = updateinfo.rstrip(',')

    # 将record信息变为str类型
    record = str(tuple(record))
    # print(type(date))
    year = date.split('-')[0]
    month = date.split('-')[1]
    date = date+'-01 00:00:00'
    # print(date)
    # print(field)
    # 先查询数据库是否有当前的数据 如果有的话就更新 如果没有就插入
    sql = "SELECT `record_id` FROM  `oms_exchange_cmain_avg` WHERE `date` = '"+str(date)+"'"
    # print(sql)
    res = cursor.execute(sql)
    res_id = cursor.fetchone()

    if res > 0:
        sql = "UPDATE `oms_exchange_cnums_avg` SET "+str(updateinfo)+" WHERE `id` = " +str(res_id[0])
        # print(sql)
        res = cursor.execute(sql)
        print(res)
    else:
        sql = "INSERT INTO `oms_exchange_cnums_avg` (`USD/CNY`, `EUR/CNY`, `100JPY/CNY`, `HKD/CNY`, `GBP/CNY`, `AUD/CNY`, `NZD/CNY`, `SGD/CNY`, `CHF/CNY`, `CAD/CNY`, `CNY/MYR`, `CNY/RUB`, `CNY/ZAR`, `CNY/KRW`, `CNY/AED`, `CNY/SAR`, `CNY/HUF`, `CNY/PLN`, `CNY/DKK`, `CNY/SEK`, `CNY/NOK`, `CNY/TRY`, `CNY/MXN`) VALUES "+record
        # print(sql)
        cursor.execute(sql)
        row_id = cursor.lastrowid
        conn.commit()
        _tuple = (int(row_id), int(year), int(month),date)
        sql = "INSERT INTO `oms_exchange_cmain_avg` (`record_id`,`year`,`month`,`date`) VALUES "+str(_tuple)
        cursor.execute(sql)
    conn.commit()

cursor.close()

conn.close()

