# -*- coding: UTF-8 -*-
import requests
from lxml import etree
import re
import pymysql


headers={
    'Referer':'https://www.wikiart.org/en/paintings-by-style',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
    'Connection':'Keep-Alive'
}
url = 'https://www.wikiart.org/en/paintings-by-style'
sum = 0
i = 0
front_url="https://www.wikiart.org"
connection = pymysql.connect(host='localhost',user='root',password='',db='wikiart',charset='utf8')
cur=connection.cursor()

def createnewtable():
    sql="CREATE TABLE `all_style` " \
        "(`id` INT (11) NOT NULL," \
        "`style` VARCHAR (150) DEFAULT NULL," \
        "`num` INT (6) DEFAULT NULL," \
        "`url` VARCHAR (255) DEFAULT NULL," \
        "PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    cur.execute(sql)
    connection.commit()

# def deletetable():#禁术 谨慎使用
#     sql="DROP TABLE all_style"
#     cur.execute(sql)
#     connection.commit()

def insertdata():
    global url,i,sum,front_url
    sess = requests.session()
    req = sess.get(url=url, headers=headers, verify=False)
    cont = req.text
    html = etree.HTML(cont)
    result = html.xpath("//li[@class='dottedItem']/a")
    print(len(result))
    for a in result:
        name = a.text #获得名字
        print (name)
        num = re.search('\d+', a.text).group()
        sum += int(num)
        shorturl = str(a.xpath('@href')[0]).split('?')[0]  # 获得URL并去除后面的URL参数列表
        url = front_url + shorturl #获得完整URL
        name = ' '.join(name.split(' ')[:-1])  # 进一步加工名字除后面的数字
        try:
            i += 1
            sql = "INSERT INTO all_style (id,style,num,url) VALUES (%s,'%s',%s,'%s')" % (i, name, num, url)
            cur.execute(sql)
            connection.commit()
        except Exception as e:
            print(e)
        print(url)
    print(sum)
    req.close()


if __name__ == '__main__':
    #deletetable()
    createnewtable()
    insertdata()
    cur.close()
    connection.close()
