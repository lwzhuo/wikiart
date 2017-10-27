# -*- coding: UTF-8 -*-
#https://www.wikiart.org/en/paintings-by-style
#计算wikiart所有类别的url和画作数量
import re
import requests
from lxml import etree
#url0-url2是通过画作风格查找的
url0 = "https://www.wikiart.org/en/paintings-by-style"#148547幅画作 通过时间排序
url1="https://www.wikiart.org/en/paintings-by-style?sortby=1"#148547幅画作 通过数量排序
url2="https://www.wikiart.org/en/paintings-by-style?sortby=2"#148547幅画作 通过名字排序
#url3-url4是通过画作流派查找的
url3="https://www.wikiart.org/en/paintings-by-genre?sortby=2"#144386幅画作 通过名字排序
url4="https://www.wikiart.org/en/paintings-by-genre"#144386幅画作 通过数量排序

#url5-url6是通过画作材料查找的
url5="https://www.wikiart.org/en/paintings-by-media?sortby=2"#94342幅画作 通过名字排序
url6="https://www.wikiart.org/en/paintings-by-media"#94342幅画作 通过数量排序

req = requests.get(url0)
cont = req.text
html = etree.HTML(cont)
result = html.xpath("//li[@class='dottedItem']/a")
sum=0
front_url="https://www.wikiart.org"
print(result)
print(len(result))
for a in result:
    name = ' '.join(str(a.text).split(' ')[:-1])
    print(name)
    num=re.search('\d+',a.text).group()
    print(num)
    sum+=int(num)
    url=front_url+str(a.xpath('@href')[0]).split('?')[0]
    #url = front_url + str(a.xpath('@href')[0])
    print(url)
print(sum)
req.close()
