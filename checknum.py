# -*- coding: UTF-8 -*-
#核对爬取数目
import pymysql
from lxml import etree
import requests
import re

connection = pymysql.connect(host='localhost',user='root',password='',db='wikiart',charset='utf8')
cur=connection.cursor()
url="https://www.wikiart.org/en/paintings-by-style"
sum = 0

def get_all_style():
    global url,sum
    req = requests.get(url)
    cont = req.text
    html = etree.HTML(cont)
    result = html.xpath("//li[@class='dottedItem']/a")
    front_url = "https://www.wikiart.org"
    stylelist=[]
    for a in result:
        style={}
        name = ' '.join(str(a.text).split(' ')[:-1])
        num = re.search('\d+', a.text).group()
        url = front_url + str(a.xpath('@href')[0]).split('?')[0]
        style['style']=name
        style['num']=num
        style['url']=url
        stylelist.append(style)
        sum += int(num)
    req.close()

    return stylelist

if __name__ == '__main__':
    allstyle = get_all_style()
    i=0
    mysum=0
    print('实际    爬取   差 style')
    while i<len(allstyle):
        styleName = allstyle[i]['style']
        styletruenum = allstyle[i]['num']
        sql="SELECT COUNT(*) FROM all_url WHERE style='%s'"%styleName
        cur.execute(sql)
        num = cur.fetchone()[0]
        mysum+=num
        mult = int(styletruenum)-num
        print(mult,styletruenum,num,styleName)
        i+=1
    print("实际总数","爬取总数")
    print(sum,mysum)
    cur.close()
    connection.close()