# -*- coding: utf-8 -*-
import requests
import pymysql
import time

headers={
    # 'Referer':'https://www.wikiart.org/en/paintings-by-style',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
}
connection = pymysql.connect(host='localhost',user='root',password='',db='wikiart',charset='utf8')
cur=connection.cursor()

def get_style_sum():
    sql = "SELECT COUNT(*) FROM all_style"
    cur.execute(sql)
    num = cur.fetchone()
    return num[0]

def get_all_style():
    sql = "SELECT * FROM all_style"
    cur.execute(sql)
    allstyle = cur.fetchall()
    return allstyle

# def delete_table():#禁术 谨慎使用
#     sql = "DROP TABLE all_url"
#     cur.execute(sql)
#     connection.commit()

def create_new_table():
    sql = "CREATE TABLE `all_url` (" \
          "`id` int(11) NOT NULL," \
          "`name` varchar(500) DEFAULT NULL," \
          "`artist` VARCHAR(200) NULL," \
          "`date` VARCHAR(100) NULL," \
          "`painturl` varchar(1000) DEFAULT NULL," \
          "`imageurl` varchar(1000) DEFAULT NULL," \
          "`style` varchar(150) DEFAULT NULL," \
          "`flag` int(11) DEFAULT '0'," \
          "PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8"
    cur.execute(sql)
    connection.commit()

def insert_data(id,name,artist,date,paintingurl,imageurl,style):
    sql = "INSERT INTO all_url(id,name,artist,date,painturl,imageurl,style)" \
          "VALUES (%s,'%s','%s','%s','%s','%s','%s')"%(id,name,artist,date,paintingurl,imageurl,style)
    cur.execute(sql)
    connection.commit()

if __name__ == '__main__':
    # delete_table()
    #create_new_table()
    styleSet = get_all_style()
    i = 0
    num=0
    while i<len(styleSet):
        styleName = styleSet[i][1]
        paintingNum = styleSet[i][2]
        styleUrl = styleSet[i][3]
        print(styleName,paintingNum,styleUrl)
        j = 1
        while True:
            sess = requests.session()
            requests.adapters.DEFAULT_RETRIES = 5#增加重试连接次数
            sess.keep_alive = False#keepalive 防止断开
            req=''
            sleeptime=0
            while req=='':
                try:
                    payload = {'json':2,'page':j}
                    req = sess.get(url=styleUrl,headers=headers,params=payload,timeout=10)
                except Exception as e:
                    print("***********************************ERROR***********************************")
                    print(e)
                    print("sleep 5s")
                    time.sleep(5)
                    sleeptime+=1
                    if sleeptime==5:
                        print("error page:%s?json=2&page=%d"%(styleUrl,j))
                        j+=1
                        sleeptime=0
                        print("***********************************ERROR***********************************")
                    continue

            req.encoding = 'utf-8'
            print("*******************\npage %d staus:"%j,req.status_code)
            jsonData = req.json()
            paintingList = jsonData['Paintings']
            if paintingList!=None:
                for painting in paintingList:
                    num+=1
                    insert_data(num,painting['title'],painting['artistName'],painting['year'],painting['paintingUrl'],painting['image'],styleName)
                    print("NO.",num,painting['title'])
                j+=1
            else:
                break
            req.close()
        i+=1
        #print("sleep 5s")
        #time.sleep(5)
    cur.close()
    connection.close()

