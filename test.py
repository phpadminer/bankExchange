#! /usr/local/bin/python3.6
# coding: utf-8

import datetime

from MysqlConfig import configs

import json

import urllib

from apscheduler.schedulers.background import BackgroundScheduler

import urllib.parse

from pyquery import PyQuery as PQ

import time

from PyMySQL import pymysql

# 获取当前时间
now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
startDate = datetime.datetime.now().strftime("%Y-%m-%d")
endDate = datetime.datetime.now().strftime("%Y-%m-%d")
startYear = startDate.split('-')[0]
startMonth = startDate.split('-')[1]
endYear = endDate.split('-')[0]
endMonth = endDate.split('-')[1]

FIELD = '(`USD/CNY`, `EUR/CNY`, `100JPY/CNY`, `HKD/CNY`, `GBP/CNY`, `AUD/CNY`, `NZD/CNY`, `SGD/CNY`, `CHF/CNY`, `CAD/CNY`, `CNY/MYR`, `CNY/RUB`, `CNY/ZAR`, `CNY/KRW`, `CNY/AED`, `CNY/SAR`, `CNY/HUF`, `CNY/PLN`, `CNY/DKK`, `CNY/SEK`, `CNY/NOK`, `CNY/TRY`, `CNY/MXN`)'

# 写运行日志
def writelog(filename,_method,loginfo):
    file = open(filename,_method)
    # print(file)
    file.write(str(loginfo))
    file.close()

# 组织日志信息
def loginfo(type,event,info,date):
    return "status : "+ type + "\ndate : " + date + "\nevent : " + event + "\ninfo : " + info + "\n--------------------------\n"

# 打开数据库连接
def connectMysql(configs):
    # 连接数据库
    conn = pymysql.connect(**configs)
    print('连接数据库..')
    # 使用cursor()方法创建一个游标对象
    print('连接数据库成功！')
    return conn

# 关闭数据库连接
def closeMysql(_conn,_cursor):
    _cursor.close()
    _conn.close()

# 每天的汇率涨幅变化
def perdaychange(configs):
    conn = connectMysql(configs)
    print('正在创建数据库游标...')
    cursor = conn.cursor()
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
        try:
            # 执行sql
            cursor.execute(sql)
            info = loginfo('success', '抓取每天人民币中间价涨幅信息', '写入数据成功', date)
            print(info)
            writelog('./runlog.txt', 'a+', info)
            conn.commit()
        except:
            conn.rollback()
            info = loginfo('error', '抓取每天人民币中间价涨幅信息', '写入数据失败', date)
            print(info)
            writelog('./runlog.txt', 'a+', info)

    closeMysql(conn,cursor)

# 每个月的上一个月汇率涨幅变化
def permonthchange(configs):
    conn = connectMysql(configs)
    print('正在创建数据库游标...')
    cursor = conn.cursor()
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
        try:
            cursor.execute(sql)
            info = loginfo('success', '抓取上个月人民币中间价平均涨幅信息', '写入数据成功', date)
            print(info)
            writelog('./runlog.txt', 'a+', info)
            conn.commit()
        except:
            conn.rollback()
            info = loginfo('error', '抓取上个月人民币中间价平均涨幅信息', '写入数据失败', date)
            print(info)
            writelog('./runlog.txt', 'a+', info)
    closeMysql(conn,cursor)

# 显示时间
def showtime():
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

# 每天的人民币中间价信息
def perdayinfo(configs,startDate,endDate):
    conn = connectMysql(configs)
    print('正在创建数据库游标...')
    cursor = conn.cursor()
    print('开始访问目标服务器')
    # 准备需要访问的url
    url = 'http://www.chinamoney.com.cn/dqs/rest/cm-u-pt/CcprHis?startDate=' + startDate + '&endDate=' + endDate
    # 将读取到的数据储存起来
    print('抓取数据中...请耐心等待！')
    data = urllib.request.urlopen(url=url).read().decode()
    print('抓取成功！')
    print('清洗数据中...')
    # 将读取的数据转化为dict
    DATA = json.loads(data)
    print(DATA)
    records = DATA['records']
    print('正在写入数据库...')
    for x in range(len(records)-1,-1,-1):
        date = records[x]['date']
        print(date)
        record = records[x]['values']
        record = str(tuple(record))
        year = date.split('-')[0]
        month = date.split('-')[1]
        day = date.split('-')[2]
        sql = "INSERT INTO `oms_exchange_cnums` (`USD/CNY`, `EUR/CNY`, `100JPY/CNY`, `HKD/CNY`, `GBP/CNY`, `AUD/CNY`, `NZD/CNY`, `SGD/CNY`, `CHF/CNY`, `CAD/CNY`, `CNY/MYR`, `CNY/RUB`, `CNY/ZAR`, `CNY/KRW`, `CNY/AED`, `CNY/SAR`, `CNY/HUF`, `CNY/PLN`, `CNY/DKK`, `CNY/SEK`, `CNY/NOK`, `CNY/TRY`, `CNY/MXN`) VALUES " + record
        try:
            cursor.execute(sql)
            row_id = cursor.lastrowid
            conn.commit()
            info = loginfo('success', '抓取每天人民币中间价信息', '更新数据表信息成功', date)
            print(info)
            writelog('./runlog.txt', 'a+', info)
            _tuple = (int(row_id), int(year), int(month), int(day), date)
            sql = "INSERT INTO `oms_exchange_cmain` (`record_id`,`year`,`month`,`day`,`date`) VALUES " + str(_tuple)
            try:
                cursor.execute(sql)
                info = loginfo('success', '抓取每天人民币中间价信息', '更新数据主表信息成功', date)
                print(info)
                writelog('./runlog.txt', 'a+', info)
                conn.commit()
            except:
                conn.rollback()
                info = loginfo('error', '抓取每天人民币中间价信息', '更新数据主表信息失败', date)
                print(info)
                writelog('./runlog.txt', 'a+', info)
        except:
            conn.rollback()
            info = loginfo('error', '抓取每天人民币中间价信息', '更新数据表信息失败', date)
            print(info)
            writelog('./runlog.txt', 'a+', info)

    closeMysql(conn,cursor)

# 每个月人民币中间价平均值信息
def permonthinfo(configs,startYear,startMonth,endYear,endMonth):
    conn = connectMysql(configs)
    cursor = conn.cursor()
    print('开始访问目标服务器')
    # 准备需要访问的url
    url = 'http://www.chinamoney.com.cn/dqs/rest/cm-u-pt/CcprMthAvgHis?startYear='+str(startYear)+'&startMonth='+str(startMonth)+'&endYear='+str(endYear)+'&endMonth='+str(endMonth)
    # 将读取到的数据储存起来
    print('抓取数据中...请耐心等待！')
    data = urllib.request.urlopen(url=url).read().decode()
    print('抓取成功！')
    print('清洗数据中...')
    # 将读取的数据转化为dict
    DATA = json.loads(data)
    # print(DATA)
    field = DATA['data']['head']
    records = DATA['records']
    print('正在写入数据库...')
    for x in range(len(records) - 1, -1, -1):
        date = records[x]['date']
        record = records[x]['values']
        # 组织update语句
        updateinfo = ''
        for x in range(0, len(field)):
            updateinfo = updateinfo + '`' + field[x] + '` = "' + record[x] + '",'
        updateinfo = updateinfo.rstrip(',')
        # 将record信息变为str类型
        record = str(tuple(record))
        year = date.split('-')[0]
        month = date.split('-')[1]
        date = date + '-01 00:00:00'
        # 先查询数据库是否有当前的数据 如果有的话就更新 如果没有就插入
        sql = "SELECT `record_id` FROM  `oms_exchange_cmain_avg` WHERE `date` = '" + str(date) + "'"
        try:
            res = cursor.execute(sql)
            info = loginfo('success', '抓取每个月人民币中间价平均值信息', '查询是否有今天的数据成功', date)
            print(info)
            writelog('./runlog.txt', 'a+', info)
            res_id = cursor.fetchone()

            if res > 0:
                sql = "UPDATE `oms_exchange_cnums_avg` SET " + str(updateinfo) + " WHERE `id` = " + str(res_id[0])
                try:
                    res = cursor.execute(sql)
                    info = loginfo('success', '抓取每个月人民币中间价平均值信息', '更新数据成功', date)
                    print(info)
                    writelog('./runlog.txt', 'a+', info)
                except:
                    conn.rollback()
                    info = loginfo('error', '抓取每个月人民币中间价平均值信息', '更新数据失败', date)
                    print(info)
                    writelog('./runlog.txt', 'a+', info)
            else:
                sql = "INSERT INTO `oms_exchange_cnums_avg` (`USD/CNY`, `EUR/CNY`, `100JPY/CNY`, `HKD/CNY`, `GBP/CNY`, `AUD/CNY`, `NZD/CNY`, `SGD/CNY`, `CHF/CNY`, `CAD/CNY`, `CNY/MYR`, `CNY/RUB`, `CNY/ZAR`, `CNY/KRW`, `CNY/AED`, `CNY/SAR`, `CNY/HUF`, `CNY/PLN`, `CNY/DKK`, `CNY/SEK`, `CNY/NOK`, `CNY/TRY`, `CNY/MXN`) VALUES " + record
                cursor.execute(sql)
                row_id = cursor.lastrowid
                conn.commit()
                _tuple = (int(row_id), int(year), int(month), date)
                sql = "INSERT INTO `oms_exchange_cmain_avg` (`record_id`,`year`,`month`,`date`) VALUES " + str(_tuple)
                cursor.execute(sql)
            conn.commit()
        except:
            conn.rollback()
            info = loginfo('error', '抓取每个月人民币中间价平均值信息', '查询是否有今天的数据失败', date)
            print(info)
            writelog('./runlog.txt', 'a+', info)

    closeMysql(conn,cursor)

# 上一个月最后一条人民币中间价数据
def perlastmonthinfo(configs,date):
    # 连接数据库
    conn = pymysql.connect(**configs)
    print('连接数据库..')
    # 使用cursor()方法创建一个游标对象
    print('连接数据库成功！')
    print('正在创建数据库游标...')
    cursor = conn.cursor()
    year = date.split('-')[0]
    month = date.split('-')[1]
    if int(month) == 1:
        year = int(year) - 1
        month = 12
    else:
        month = int(month) -1
    # 找出上个月最后一个数据
    sql = "SELECT cn.* FROM `oms_exchange_cmain` as cm ,`oms_exchange_cnums` as cn WHERE cm.`year` = '" +str(year) + "' AND cm.`month` = '" +str(month) + "' AND cm.`record_id` = cn.`id` ORDER BY `date` DESC limit 0,1 "
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 提交到数据库执行
        conn.commit()
        info = loginfo('success','抓取上月最后一条人民币中间价数据','查询上个月信息成功', date)
        print(info)
        writelog('./runlog.txt','a+', info)
        # 准备数据
        res = cursor.fetchone();
        res = str(tuple(list(res)))
        sql = "INSERT INTO `oms_exchange_cnums_last` (`cums_id`,`USD/CNY`, `EUR/CNY`, `100JPY/CNY`, `HKD/CNY`, `GBP/CNY`, `AUD/CNY`, `NZD/CNY`, `SGD/CNY`, `CHF/CNY`, `CAD/CNY`, `CNY/MYR`, `CNY/RUB`, `CNY/ZAR`, `CNY/KRW`, `CNY/AED`, `CNY/SAR`, `CNY/HUF`, `CNY/PLN`, `CNY/DKK`, `CNY/SEK`, `CNY/NOK`, `CNY/TRY`, `CNY/MXN`) VALUES " + res
        try:
            cursor.execute(sql)
            conn.commit()
            info = loginfo('success', '抓取上月最后一条人民币中间价数据', '写入信息成功', date)
            print(info)
            writelog('./runlog.txt', 'a+', info)
        except:
            conn.rollback()
            info = loginfo('error', '抓取上月最后一条人民币中间价数据', '写入信息失败', date)
            print(info)
            writelog('./runlog.txt', 'a+', info)

    except:
        # 发生错误时回滚
        conn.rollback()
        info = loginfo('error', '抓取上月最后一条人民币中间价数据', '查询上个月信息失败', date)
        print(info)
        writelog('./runlog.txt', 'a+', info)

    closeMysql(conn,cursor)


scheduler = BackgroundScheduler()
# 每隔5秒钟执行一次 showtime 方法
#
# scheduler .add_job(showtime, 'interval', seconds=5)
# 从 2018-01-01 09:15:00 开始, 每隔1天执行一次 perdaychange 方法
scheduler .add_job(perdaychange, 'interval', days=1, start_date='2018-01-01 09:15:00',args=[configs])
# 从 2018-01-01 00:00:00 开始, 每隔1个月执行一次 permonthchange 方法
scheduler .add_job(permonthchange, 'cron', month='1-12', day='1',hour='8',minute='0',args=[configs])
# 从 2006-01-01 00:00:00 开始, 每隔1天执行一次 perdayinfo 方法
scheduler .add_job(perdayinfo, 'interval', days=1,start_date='2006-01-01 00:00:00',args=[configs,startDate,endDate])
# 从 2006-01-01 00:00:00 开始, 每隔1天执行一次 permonthinfo 方法
scheduler .add_job(permonthinfo, 'interval', days=1,start_date='2006-01-01 00:00:00',args=[configs,startYear,startMonth,endYear,endDate])
# 从 2006-01-01 00:00:00 开始, 每隔1天执行一次 perlastmonthinfo 方法
scheduler .add_job(perlastmonthinfo, 'interval', days=1 ,start_date='2006-01-01 00:00:00',args=[configs,now])

scheduler.start()

while True:

    time.sleep(5)

