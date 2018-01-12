#! /usr/local/bin/python3.6
# coding:utf-8

import calendar,datetime
# mondays = []
# for x in range(2017,2018):
#     for y in range(1,12):
#         monthRange = calendar.monthrange(x,y)[1]
#
#         monday = datetime.date(year=x,month=y,day=monthRange)
#         monday = str(monday)
#         # print(type(monday))
#
#         mondays.append(monday)

# print(mondays)

def gen_dates(b_date, days):
    day = datetime.timedelta(days=1)

    for i in range(days):
        yield b_date + day*i


def get_date_list(start=None, end=None):
    """
    获取日期列表
    :param start: 开始日期
    :param end: 结束日期
    :return:
    """
    if start is None:
        start = datetime.datetime.strptime("2017-12-01", "%Y-%m-%d")

    if end is None:
        end = datetime.datetime.now()
    data = []
    for d in gen_dates(start, (end-start).days):
        data.append(str(d.strftime('%Y-%m-%d')))
    return data

for x in range(2006,2018):
    # print(str(x)+'-01-01')
    start = datetime.datetime.strptime((str(x)+'-01-01'), "%Y-%m-%d")

    end = datetime.datetime.strptime(str(x)+'-12-31','%Y-%m-%d')
    Days = get_date_list(start,end)