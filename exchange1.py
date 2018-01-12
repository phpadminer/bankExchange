#! /usr/local/bin/python3.6
# coding:utf-8

from pyquery import PyQuery as pq

import yearMonth as ym


import urllib.request,ssl,urllib.parse,json


from urllib.error import URLError, HTTPError

ssl._create_default_https_context = ssl._create_unverified_context

# url = 'http://www.safe.gov.cn/AppStructured/view/project_RMBQuery.action'
# header = {
#     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
#     "Accept-Encoding": "gzip, deflate",
#     "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
#     "Cache-Control": "no-cache",
#     "Cookie":"JSESSIONID=0000lPXHyoDBKwEUWoGiSHurNuX:15rgksk85; _ga=GA1.3.1128971793.1514380036; JSESSIONID2=00001Q89nsjzwhPVh_Regb8DVjM:168ptcb4l" ,
#     "Host": "www.safe.gov.cn",
#     "Pragma": "no-cache",
#     "Upgrade-Insecure-Requests": "1",
#     "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36"
# }
# print(ym.mondays)



# for x in ym.mondays:
# data = {
#     "projectBean.startDate":x,
#     "projectBean.endDate":x,
#     "queryYN":"true"
# }
# data = {
#     "startDate":"2017-01-01",
#     "endDate":"2017-12-28"
# }
# data = urllib.parse.urlencode(data).encode('utf-8')
url = 'http://www.chinamoney.com.cn/dqs/rest/cm-u-pt/CcprHis?startDate=2017-11-01&endDate=2017-12-28'

response = urllib.request.urlopen(url=url).read().decode()
DATAS = json.loads(response)
print(DATAS['data']['head'])
print(DATAS['records'])
# try:
#     doc = urllib.request.urlopen(response).read().decode()
# except HTTPError as e:
#     print('httperror:'+e.reason)
# except URLError as e :
#     print('urlerror:'+e.reason)
# else:
#     content = pq(doc)
# print(content)
# # 获取到指定的表格
# table = content('#InfoTable')
# # 获取到所有的货币名称
# names = table.find('.table_head').text()
# names = names.split(' ')[1::]
# # print(len(names))
# nums = table.find('.first').find('td').text()
# nums = nums.split(' ')[1::]
# if nums:
#     print(nums)
# else:
#     ntemp = []
#     for x in range(0,23):
#         ntemp.append('-')
#     nums = ntemp
#     print(nums)



