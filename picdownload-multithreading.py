# -*- coding:utf-8 -*-
import requests
import pymysql
import time
import threading
import urllib3
urllib3.disable_warnings()
start = time.clock()
headers={
    # 'Referer':'https://www.wikiart.org/en/paintings-by-style',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
}
connection = pymysql.connect(host='localhost',user='root',password='',db='wikiartbackup',charset='utf8')
cur=connection.cursor()
sess = requests.session()
requests.adapters.DEFAULT_RETRIES = 5  # 增加重试连接次数
sess.keep_alive = False  # keepalive 防止断开
selectsql="SELECT id,imageurl FROM backupallurl"
updatesql="UPDATE backupallurl SET flag=1 WHERE id=%s"
cur.execute(selectsql)
result=cur.fetchall()
i=0
idset=[]
failset=set()
lock = threading.Lock()#拿锁

def download(x):
    while True:
        global i
        response = ""
        sleeptime = 0
        lock.acquire()
        flag=i
        i+=1
        lock.release()
        if flag>=50000:
            break
        while response == "":
            try:
                url = result[flag][1]
                pid = result[flag][0]
                response = sess.get(url=url, headers=headers, verify=False, timeout=10)
                print("thread:%s"%x,pid,response.status_code,url)
            except Exception as e:
                lock.acquire()#上锁
                print("***********************************ERROR***********************************")
                print(e)
                print("sleep 5s")
                time.sleep(5)
                sleeptime += 1
                if sleeptime == 5:
                    print("error page:%s", url)
                    failset.add(pid)
                    flag=i+1
                    i=flag
                    sleeptime = 0
                print("***********************************ERROR***********************************")
                lock.release()
                continue
        picfile = open("art\\%d" % pid + ".jpg", "wb")
        picfile.write(response.content)
        picfile.close()
        response.close()
        lock.acquire()#上锁
        cur.execute(updatesql,pid)
        connection.commit()
        lock.release()
        idset.append(pid)


if __name__ == '__main__':
    for x in range(100):
        t = threading.Thread(target=download,args=(x,))
        t.start()
    t.join()
    cur.close()
    connection.close()
    sess.close()
    end = time.clock()
    print("success:",len(idset))
    print(idset)
    print(failset)
    print("%f"%(end-start))
    print(threading.activeCount())
