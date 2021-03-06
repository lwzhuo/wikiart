# -*- coding:utf-8 -*-
import pymysql
from bs4 import BeautifulSoup
import requests
import time
import urllib3
import threading
import random

urllib3.disable_warnings()
conn = pymysql.connect(host='localhost',user='root',password='',db='wikiartbackup',charset='utf8')
cur=conn.cursor()
headers = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Connection': 'Keep-Alive',
    'Referer':'https://www.wikiart.org/',
    'Upgrade-Insecure-Requests':'1',
    'Host':'www.wikiart.org',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'
    }
i = 0
lock = threading.Lock()
errorPid = []

def getallurl():
    sql = "SELECT id,painturl FROM backupallurl"
    cur.execute(sql)
    urllist = cur.fetchall()
    return urllist

def downloadpage(url,Pid):
    sess = requests.session()
    requests.adapters.DEFAULT_RETRIES = 5  # 增加重试连接次数
    sess.keep_alive = False  # keepalive 防止断开
    response = ""
    errortime = 0
    while response == "":
        try:
            response = sess.get(url=url, headers=headers, verify=False, timeout=10)
        except Exception as e:
            if errortime<30:#试错机会
                lock.acquire()
                print("******WARNING******",e)#打印错误
                errortime += 1
                sess.close()
                lock.release()
            else:
                lock.acquire()
                print("***********************************ERROR***********************************")
                file = open("ERRLOG.txt",'a')
                str="ERROR:download Exception %d  %s\n" % (Pid, url)
                file.write(str)
                file.close()
                lock.release()
                break
        continue
    if response == "":#连接失败 返回None
        return None
    else:
        response.close()
        sess.close()
        return response.content

def getpicinfo(content):
    try:
        attritubeDict = {}
        soup = BeautifulSoup(content, 'lxml')
        divlist = soup.find_all('div', class_='info')
        title = divlist[0].find('div', class_='info-line painting-header').find('h1').text  # 标题
        # artist开始
        artist = divlist[0].find('div', class_='info-line painting-header').find('a', class_='artist-name').text
        # artist结束
        # date开始
        date = divlist[0].find('span', itemprop='dateCreated')
        if date is not None:
            date = date.text
        # style开始
        stylelist = []
        style = divlist[0].find('span', text='Style:')
        if style is not None:
            style = style.parent.find_all('a')
            for s in style:
                stylelist.append(s.text)
        # style结束
        # genre开始
        genrelist = []
        genre = divlist[0].find('span', text='Genre:')
        if genre is not None:
            genre = genre.parent.find_all('a')
            for g in genre:
                genrelist.append(g.text)
        # genre结束
        # media开始
        medialist = []
        media = divlist[0].find('span', text='Media:')
        if media is not None:
            media = media.parent.find_all('a')
            for m in media:
                medialist.append(m.text)
        # media 结束
        # 组装开始
        attritubeDict['title'] = title
        attritubeDict['artist'] = artist
        attritubeDict['date'] = date
        attritubeDict['style'] = stylelist
        attritubeDict['genre'] = genrelist
        attritubeDict['media'] = medialist
        # 组装结束
        return attritubeDict
    except Exception as e:
        print(e)
        return None

def insertinfo(attritubeDict,id):
    status = True
    title = attritubeDict['title']
    date = attritubeDict['date']
    artist = attritubeDict['artist']
    styleList = attritubeDict['style']
    mediaList = attritubeDict['media']
    genreList = attritubeDict['genre']
    #组装sql语句
    argc=[]
    sql1 = "INSERT INTO picinfo(id"
    sql2 = "VALUES(%s"
    argc.append(id)
    if title is not None:
        sql1+=",title"
        sql2+=",%s"
        argc.append(title)
    if date is not None:
        sql1 += ",date"
        sql2 += ",%s"
        argc.append(date)
    if artist is not None:
        sql1 += ",artist"
        sql2 += ",%s"
        argc.append(artist)
    for x in range(len(styleList)):
        sql1+=",style%d"%(x+1)
        sql2+=",%s"
        argc.append(styleList[x])
    for x in range(len(mediaList)):
        sql1+=",media%d"%(x+1)
        sql2+=",%s"
        argc.append(mediaList[x])
    for x in range(len(genreList)):
        sql1+=",genre%d"%(x+1)
        sql2+=",%s"
        argc.append(genreList[x])
    sql1+=")"
    sql2+=")"
    sql=sql1+sql2
    lock.acquire()  # 给整个try语句上锁 防止造成卡死
    try:
        cur.execute(sql, argc)
        conn.commit()
    except Exception as e:  # 插入异常处理
        file = open("ERRLOG.txt", 'a')
        str = "ERROR:SQL Exception Pid:%d\n" % (id)
        file.write(str)
        file.close()
        status = False
    lock.release()
    return status

class crawlThread(threading.Thread):
    def __init__(self,Tid,urllist):
        threading.Thread.__init__(self)
        self.Tid = Tid
        self.urllist = urllist
    def run(self):
        while True:
            lock.acquire()
            global i
            flag = i
            i+=1
            lock.release()
            if flag>=len(urllist):#结束条件
                break
            Pid = self.urllist[flag][0]
            fullurl = "https://www.wikiart.org" + self.urllist[flag][1]#补全URL
            htmlpage = downloadpage(fullurl,Pid)
            if htmlpage is None:#处理下载异常
                print(self.Tid,"ERROR url:",fullurl,"Pid: ",Pid)
            else:
                datadict = getpicinfo(htmlpage)
                if datadict is None:#处理html解析异常
                    lock.acquire()
                    file = open("ERRLOG.txt", 'a')
                    str = "ERROR:parse Exception Pid:%d  URL:%s\n" % (Pid, fullurl)
                    file.write(str)
                    file.close()
                    lock.release()
                else:
                    if insertinfo(datadict,Pid) is True:
                        print("Thread ",self.Tid,"success Pid:",Pid," URL:",fullurl)
                    else:
                        print("Thread ", self.Tid, "insert failure Pid:", Pid, " URL:", fullurl)

if __name__ == '__main__':

    #测试开始
    # print(urllist[0][0])
    # url = "https://www.wikiart.org"+urllist[0][1]
    # url = "https://www.wikiart.org/en/mathias-goeritz/torres-de-sat-lite-collaboration-with-luis-barrag-n-and-jes-s-reyes-ferreira"
    # print(url)
    # page = downloadpage(url)
    # data = getpicinfo(page)
    # print(data['title'])
    # print(type(data['title']))
    # print data['media']
    # print len(data['media'])
    # print data['date'] is None
    # for style in data['style']:
    #     print style
    # for media in data['media']:
    #     print style
    #空值插入测试开始
    # url = "https://www.wikiart.org/en/facundus/la-sixi-me-trompette-les-anges-prisonniers-au-bord-de-l-euphrate-apoc-ix"
    # cont = downloadpage(url)
    # info = getpicinfo(cont)
    # print(info)
    # print (type(info['date']))
    # insertinfo(info,1)
    #空值插入测试结束
    start = time.time()
    urllist = getallurl()
    for i in range(40):#使用线程数
        t = crawlThread(i,urllist)
        t.start()
    t.join()
    cur.close()
    conn.close()
    print("爬虫结束")
    end = time.time()
    print(end-start)