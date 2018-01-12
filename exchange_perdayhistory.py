#! /usr/local/bin/python3.6
# coding:utf-8

import urllib.request,json,time

from urllib.error import URLError, HTTPError

from MysqlConfig import configs

from PyMySQL import pymysql

###
# 本脚本是用来拉取指定日期的数据
# 连接数据库
conn = pymysql.connect(**configs)
print('连接数据库成功！')
# 创建数据库游标
cursor = conn.cursor()
print('开始访问目标服务器')

#获取到当前的年份
endtime = int(time.strftime('%Y',time.localtime()))

#初始化一个空的list
DATAS = []

for x in range(endtime,2005,-1):
    print(x)
    start = str(x)+'-01-01'

    end =str(x)+'-12-31'

    url = 'http://www.chinamoney.com.cn/dqs/rest/cm-u-pt/CcprHis?startDate='+start+'&endDate='+end

    try:
        response = urllib.request.urlopen(url=url).read().decode()
    except HTTPError as e:
        print('httperror:'+e.reason)
    except URLError as e :
        print('urlerror:'+e.reason)
    except urllib.socket.timeout as e:#解决error[60]错误
        print(e)
    else:
        # print(response)

        DATAS.extend(json.loads(response)['records'])







# print(DATAS)

print(len(DATAS))
# print(DATAS[1]['date'])
#
length = len(DATAS)

for x in range(length,1,-1):
    m = x-1
    # print(m)
    Date = DATAS[m]['date']

    dates = DATAS[m]['date'].split('-')

    date = DATAS[m]['date']

    NUMS = DATAS[m]['values']


    effort_rows  = cursor.execute("insert into `oms_exchange_cnums` (`USD/CNY`, `EUR/CNY`, `100JPY/CNY`, `HKD/CNY`, `GBP/CNY`, `AUD/CNY`, `NZD/CNY`, `SGD/CNY`, `CHF/CNY`, `CAD/CNY`, `CNY/MYR`, `CNY/RUB`, `CNY/ZAR`, `CNY/KRW`, `CNY/AED`, `CNY/SAR`, `CNY/HUF`, `CNY/PLN`, `CNY/DKK`, `CNY/SEK`, `CNY/NOK`, `CNY/TRY`, `CNY/MXN`) VALUES "+str(tuple(NUMS)))

    new_insert = cursor.lastrowid

    DATES = (new_insert,dates[0],dates[1],dates[2],Date)

    cursor.execute("insert into `oms_exchange_cmain` (`record_id`,`year`,`month`,`day`,`date`) VALUES "+str(DATES))

    # print(new_insert)

    conn.commit()

cursor.close()

conn.close()
print('success')