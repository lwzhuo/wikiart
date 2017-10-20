import sys
import requests
import pymysql
reload(sys)
sys.setdefaultencoding("utf8")
headers = {"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36",
           'Connection': 'close'}
conn = pymysql.connect(host='localhost',user='root',password='',db='wikiart',charset='utf8')
cur=conn.cursor()
cur.execute("SELECT * FROM mozarabic")
all = cur.fetchall()
i=0
jsonfile = open("pic.json",'w')
jsonfile.write('[')
for paint in all:
    print(paint[1:-1])
    picurl=paint[4]
    req=requests.get(url=picurl,headers=headers,verify=False)
    picfile = open("art\\Medieval Art\\Mozarabic\\" + "%s"%paint[1] + ".jpg", "wb")
    picfile.write(req.content)
    picfile.close()
    req.close()
    jsonfile.write('\n{')
    jsonfile.write('\n    "title":"%s",'%paint[1])
    jsonfile.write('\n    "style":"%s",' %"Mozarabic")
    jsonfile.write('\n    "artistName":"%s",' % paint[2])
    jsonfile.write('\n    "year":"%s",' % paint[3])
    jsonfile.write('\n    "url":"%s"' % paint[4])
    jsonfile.write("\n}")
    i+=1
    if i!=59:
        jsonfile.write(",")
jsonfile.write(']')
jsonfile.close()
cur.close()
conn.close()
