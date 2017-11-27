# -*- coding:utf-8 -*-
import pymysql
from bs4 import BeautifulSoup
import requests
import time
import urllib3
import threading
import random

urllib3.disable_warnings()
conn = pymysql.connect(host='localhost',user='root',password='abc123',db='wikiartbackup',charset='utf8')
cur=conn.cursor()
headers = {
    'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Connection': 'Keep-Alive',
    'Referer':'https://www.wikiart.org/',
    'Upgrade-Insecure-Requests':'1',
    'Host':'www.wikiart.org',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'
    }
apiUrl = ""
i = 0
lock = threading.Lock()
errorPid = []
proxyList = []#代理列表

def getallurl():
    sql = "SELECT id,painturl FROM backupallurl"
    cur.execute(sql)
    urllist = cur.fetchall()
    return urllist

def getproxy():
    response = requests.get(apiUrl)
    ip = response.content
    response.close()
    birthtime = time.time()#获取ip生成时间
    # print(response.status_code)
    if response.status_code == 200:
        proxy = dict()
        proxy["http"] = str(ip, encoding="utf8").strip('\n')
        proxy["https"] = str(ip, encoding="utf8").strip('\n')
        proxyInfo = (birthtime, proxy)
        lock.acquire()
        proxyList.append(proxyInfo)
        file = open("PROXY.txt", 'a')
        p = "Proxy:%s  Time:%s\n" % (proxyInfo[1]["http"],proxyInfo[0])
        file.write(p)
        file.close()
        lock.release()

def needGetProxy():#获得最后加进列表的ip的时间 计算是否大于6s 大于就加入 比避免频繁申请
    nowtime = time.time()
    birthtime = proxyList[len(proxyList)-1][0]
    if nowtime - birthtime>=6:
        return True
    else:
        return False

def updateProxyList():#清除过时代理
    lock.acquire()
    nowtime = time.time()
    if len(proxyList)>0:
        for proxy in proxyList:
            if nowtime - proxy[0]>110:#这批代理生存期120s
                proxyList.remove(proxy)
    lock.release()

def downloadpage(url,Pid):
    sess = requests.session()
    requests.adapters.DEFAULT_RETRIES = 5  # 增加重试连接次数
    sess.keep_alive = False  # keepalive 防止断开
    response = ""
    errortime = 0
    while response == "":
        try:
            updateProxyList()
            lock.acquire()
            proxy = random.choice(proxyList)#随机抽取一个代理
            proxy = proxy[1]
            lock.release()
            response = sess.get(url=url, headers=headers, proxies=proxy, verify=False, timeout=10)
            # print("Proxy:",proxylist["http"])
        except Exception as e:
            print(e)
            if errortime<30:#试错机会
                lock.acquire()
                print("***********************************WARNING***********************************%s"%response)
                # file = open("ERRLOG.txt",'a')
                # str = "WARNING:%d  %s\n" % (Pid, url)
                # file.write(str)
                # file.close()
                sess.close()
                lock.release()
                # time.sleep(5)
                # getproxy()
                errortime+=1
            else:
                lock.acquire()
                print("***********************************ERROR***********************************")
                file = open("ERRLOG.txt",'a')
                str="ERROR:%d  %s\n" % (Pid, url)
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
    #print(attritubeDict)
    return attritubeDict

def insertinfo(attritubeDict,id):
    title = attritubeDict['title']
    date = attritubeDict['date']
    artist = attritubeDict['artist']
    styleList = attritubeDict['style']
    mediaList = attritubeDict['media']
    genreList = attritubeDict['genre']
    #空值判断预处理
    if title is None:
        title = "NULL"
    if date is None:
        date = "NULL"
    if artist is None:
        artist = "NULL"
    #空值判断预处理结束
    lock.acquire()
    info_sql = "INSERT INTO picinfo(id,title,date,artist) " \
               "VALUES(%s,%s,%s,%s)"
    cur.execute(info_sql,(id,title,date,artist))
    conn.commit()
    if len(styleList)!=0:
        for style in styleList:
            style_sql = 'INSERT INTO style(id,style) VALUES ("%s","%s")'%(id,style)
            cur.execute(style_sql)
            conn.commit()
    if len(mediaList)!=0:
        for media in mediaList:
            media_sql = 'INSERT INTO media(id,media) VALUES ("%s","%s")'%(id,media)
            cur.execute(media_sql)
            conn.commit()
    if len(genreList)!=0:
        for genre in genreList:
            genre_sql = 'INSERT INTO genre(id,genre) VALUES ("%s","%s")'%(id,genre)
            cur.execute(genre_sql)
            conn.commit()
    lock.release()

class crawlThread(threading.Thread):
    def __init__(self,Tid,urllist):
        threading.Thread.__init__(self)
        self.Tid = Tid
        self.urllist = urllist
    def run(self):
        while True:
            # if self.Tid%15 == 1:#部分线程执行抓取新的代理的工作
            #     getproxy()
            lock.acquire()
            flag = needGetProxy()
            lock.release()
            if flag:
                getproxy()
            lock.acquire()
            global i
            flag = i
            i+=1
            # print("Thread:",self.Tid,"run Pid:",flag+1)
            lock.release()
            if flag>len(urllist):#结束条件
                break
            Pid = self.urllist[flag][0]
            fullurl = "https://www.wikiart.org" + self.urllist[flag][1]#补全URL
            htmlpage = downloadpage(fullurl,Pid)
            if htmlpage is None:
                # lock.acquire()
                print(self.Tid,"ERROR url:",fullurl,"Pid: ",Pid)
                # errorPid.append(Pid)#加入错误列表
                # lock.release()
            else:
                datadict = getpicinfo(htmlpage)
                insertinfo(datadict,Pid)
                print("Thread ",self.Tid,"success Pid:",Pid," URL:",fullurl," sleep 5s")
                # time.sleep(5)

if __name__ == '__main__':
    urllist = getallurl()
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
    print("初始化代理列表")
    for i in range(15):
        getproxy()
        print(i,proxyList[i])
        time.sleep(6)
    print("代理列表初始化结束")
    for i in range(300):
        t = crawlThread(i,urllist)
        t.start()
    t.join()
    cur.close()
    conn.close()
    print("爬虫结束")