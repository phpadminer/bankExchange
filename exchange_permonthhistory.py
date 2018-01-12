#! /usr/local/bin/python3.6
# coding:utf-8

import ssl, urllib.request, json, time

from urllib.error import URLError, HTTPError

from PyMySQL import pymysql

ssl._create_default_https_context = ssl._create_unverified_context

# 设置数据库链接数据
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
# 创建数据库游标
cursor = conn.cursor()

def getInfo(start_year, end_year):
    DATAS = []

    url = 'http://www.chinamoney.com.cn/dqs/rest/cm-u-pt/CcprMthAvgHis?startYear=' + str(start_year) + '&startMonth=12&endYear=' + str(end_year) + '&endMonth=12'
    try:
        response = urllib.request.urlopen(url=url).read().decode()
    except HTTPError as e:
        print('httperror:' + e.reason)
    except URLError as e:
        print('urlerror:' + e.reason)
    else:
        DATAS.extend(json.loads(response)['records'])
    return DATAS


# 获取当前的年份
endtimeyear = int(time.strftime('%Y', time.localtime()))


for x in range(endtimeyear, 2005, -1):
    start_year = x - 1
    end_year = x
    DATAS = getInfo(start_year,end_year)
    # print(DATAS)

print(len(DATAS))
# print(DATAS[1]['date'])
#
length = len(DATAS)

for x in range(length, 1, -1):
    m = x - 1
    # print(m)
    Date = DATAS[m]['date']+' 00:00:00'

    dates = DATAS[m]['date'].split('-')

    date = DATAS[m]['date']

    NUMS = DATAS[m]['values']



    effort_rows = cursor.execute(
        "insert into `oms_exchange_cnums_avg` (`USD/CNY`, `EUR/CNY`, `100JPY/CNY`, `HKD/CNY`, `GBP/CNY`, `AUD/CNY`, `NZD/CNY`, `SGD/CNY`, `CHF/CNY`, `CAD/CNY`, `CNY/MYR`, `CNY/RUB`, `CNY/ZAR`, `CNY/KRW`, `CNY/AED`, `CNY/SAR`, `CNY/HUF`, `CNY/PLN`, `CNY/DKK`, `CNY/SEK`, `CNY/NOK`, `CNY/TRY`, `CNY/MXN`) VALUES " + str(
            tuple(NUMS)))

    new_insert = cursor.lastrowid

    DATES = (new_insert, dates[0], dates[1], Date)

    cursor.execute("insert into `oms_exchange_cmain_avg` (`record_id`,`year`,`month`,`date`) VALUES " + str(DATES))
    conn.commit()




cursor.close()

conn.close()
