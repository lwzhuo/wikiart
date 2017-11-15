# -*- coding:utf-8 -*-
import pymysql
from bs4 import BeautifulSoup
import requests
import time
import urllib3
import threading

urllib3.disable_warnings()
conn = pymysql.connect(host='localhost',user='root',password='abc123',db='wikiartbackup',charset='utf8')
cur=conn.cursor()
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
        'Connection': 'Keep-Alive'
    }
i = 0
lock = threading.Lock()
errorPid = []

def getallurl():
    sql = "SELECT id,painturl FROM backupallurl"
    cur.execute(sql)
    urllist = cur.fetchall()
    return urllist

def downloadpage(url):
    sess = requests.session()
    requests.adapters.DEFAULT_RETRIES = 5  # 增加重试连接次数
    sess.keep_alive = False  # keepalive 防止断开
    response = ""
    errortime = 0
    while response == "":
        try:
            response = sess.get(url=url, headers=headers, verify=False, timeout=10)
        except Exception as e:
            print(e)
            if errortime<5:
                print("***********************************WARNING***********************************")
                time.sleep(5)
                errortime+=1
            else:
                print("***********************************ERROR***********************************")
                break
        continue
    if response is None:#连接失败 返回None
        response.close()
        return None
    else:
        response.close()
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
    info_sql = "INSERT INTO picinfo(id,title,date,artist) " \
               "VALUES(%s,'%s','%s','%s')"%(id,title,date,artist)
    cur.execute(info_sql)
    conn.commit()
    if len(styleList)!=0:
        for style in styleList:
            style_sql = "INSERT INTO style(id,style) VALUES ('%s','%s')"%(id,style)
            cur.execute(style_sql)
            conn.commit()
    if len(mediaList)!=0:
        for media in mediaList:
            media_sql = "INSERT INTO media(id,media) VALUES ('%s','%s')"%(id,media)
            cur.execute(media_sql)
            conn.commit()
    if len(genreList)!=0:
        for genre in genreList:
            genre_sql = "INSERT INTO genre(id,genre) VALUES ('%s','%s')"%(id,genre)
            cur.execute(genre_sql)
            conn.commit()

class crawlThread(threading.Thread):
    def __init__(self,Tid,urllist):
        threading.Thread.__init__(self)
        self.Tid = Tid
        self.urllist = urllist
    def run(self):
        while True:
            print("Thread:",self.Tid,"run")
            global i
            lock.acquire()
            flag = i
            i+=1
            lock.release()
            if flag>len(urllist):#结束条件
                break
            Pid = self.urllist[flag][0]
            fullurl = "https://www.wikiart.org" + self.urllist[flag][1]#补全URL
            htmlpage = downloadpage(fullurl)
            if htmlpage is None:
                lock.acquire()
                print(self.Tid,"ERROR url:",fullurl,"Pid: ",Pid)
                errorPid.append(Pid)
                lock.release()
            else:
                datadict = getpicinfo(htmlpage)
                lock.acquire()#读入数据库上锁
                insertinfo(datadict,Pid)
                print(self.Tid,"success Pid:",Pid," URL:",fullurl," sleep 5s")
                lock.release()
                time.sleep(5)

if __name__ == '__main__':
    urllist = getallurl()
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
    # id=1
    # title =None
    # date = None
    # artist =None
    # info_sql = "INSERT INTO picinfo(id,title,date,artist) " \
    #            "VALUES(%s,'%s','%s','%s')" % (id, title, date, artist)
    # cur.execute(info_sql)
    # conn.commit()
    #空值插入测试结束
    for i in range(200):
        t = crawlThread(i,urllist)
        t.start()
    t.join()
    cur.close()
    conn.close()