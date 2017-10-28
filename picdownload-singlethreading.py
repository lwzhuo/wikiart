# -*- coding:utf-8 -*-
#单线程测试 100张图片5min
import requests
import pymysql
import time
start = time.clock()
headers={
    # 'Referer':'https://www.wikiart.org/en/paintings-by-style',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
}
connection = pymysql.connect(host='localhost',user='root',password='',db='wikiartbackup',charset='utf8')
cur=connection.cursor()
sql="SELECT id,imageurl FROM backupallurl"
cur.execute(sql)
result=cur.fetchall()
i=0
idset=set()
failset=set()
while i<1000:

    sess = requests.session()
    requests.adapters.DEFAULT_RETRIES = 5  # 增加重试连接次数
    sess.keep_alive = False  # keepalive 防止断开
    response=""
    sleeptime=0
    while response=="":
        try:
            url=result[i][1]
            pid=result[i][0]
            response = sess.get(url=url,headers=headers,verify=False,timeout=10)
            print(pid,url,response.status_code)
        except Exception as e:
            print("***********************************ERROR***********************************")
            print(e)
            print("sleep 5s")
            time.sleep(5)
            sleeptime+=1
            if sleeptime==5:
                print("error page:%s",url)
                failset.add(pid)
                i+=1
                sleeptime=0
            print("***********************************ERROR***********************************")
            continue
    picfile=open("art\\%d"%pid+".jpg","wb")
    picfile.write(response.content)
    picfile.close()
    response.close()
    idset.add(pid)
    i+=1
cur.close()
connection.close()
end = time.clock()
print(idset)
print(failset)
print("%f"%(end-start))
