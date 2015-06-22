#!/usr/bin/env python
#-*- coding: utf-8 -*-
import urllib
import urllib2
import re
import optparse
import pdb
import sqlite3
import os
import sys
import BeautifulSoup
import threading
import chardet
import StringIO
import gzip
from datetime import datatime

class WorkerManager:
    def __init__( self, num_of_workers=10, timeout = 2):
        self.workQueue = Queue.Queue()
        self.resultQueue = Queue.Queue()
        self.workers = []
        self.timeout = timeout
        self._recruitThreads( num_of_workers )

    def _recruitThreads( self, num_of_workers ):
        for i in range( num_of_workers ):
            worker = Worker( self.workQueue, self.resultQueue )
            self.workers.append(worker)

    def wait_for_complete( self):
        # ...then, wait for each of them to terminate:
        while len(self.workers):
            worker = self.workers.pop()
            worker.join( )
            if worker.isAlive() and not self.workQueue.empty():
                self.workers.append( worker )
        print "All jobs are are completed."

    def add_job( self, callable, *args, **kwds ):
        self.workQueue.put( (callable, args, kwds) )

    def get_result( self, *args, **kwds ):
        return self.resultQueue.get( *args, **kwds )

# working thread
class Worker(threading.Thread):
    worker_count = 0
    timeout = 1
    def __init__( self, workQueue, resultQueue, **kwds):
        threading.Thread.__init__( self, **kwds )
        self.id = Worker.worker_count
        Worker.worker_count += 1
        self.setDaemon( True )
        self.workQueue = workQueue
        self.resultQueue = resultQueue
        self.start()

    def run( self ):
        ''' the get-some-work, do-some-work main loop of worker threads '''
        while True:
            try:
                callable, args, kwds = self.workQueue.get(timeout=Worker.timeout)
                res = callable(*args, **kwds)
                print "worker[%2d]: %s" % (self.id, str(res) )
                self.resultQueue.put( res )
                #time.sleep(Worker.sleep)
            except Queue.Empty:
                break
            except :
                print 'worker[%2d]' % self.id, sys.exc_info()[:2]
                raise



class Spider:
    dbpath = os.getcwd() + '\\spider.db'
    conn = ''
    urllist = []

    def __init__(self):
        pass


    def main(self):
        if (options.deep == 'deep'):   #u'深度爬虫'
            if (options.key != ''):   #u'如果有指定的关键字'
                text = self.SendRequest(options.url, options.key)
                if(text != ''):
                    self.DBsave(options.url,text)
                for item in self.deepAnalysis(text):
                    temp = self.SendRequest(item, options.key)
                    if(temp != ''):
                        self.DBsave(item, temp)
            else:  #u'没有指定关键字爬虫'
                text = self.SendRequest(options.url, options.key)
                self.DBsave(options.url, text)
                for item in self.deepAnalysis(text):
                    temp = self.SendRequest(item, options.key)
                    self.DBsave(item, temp)
        else:  #u'浅爬虫'
            if (options.key != ''):
                text = self.SendRequest(options.url, options.key)
                if(text != ''):
                    self.DBsave(options.url, text)

    def SendRequest(self, url, key=None):
        headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1;en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6', 'Accept-encoding':'gzip'}
        req = urllib2.Request(url, headers=headers)
        print '[!Info:] url is %s \n' % url
        try:
            opener = urllib2.urlopen(req)
            self.logging("Send Request to " + url)
        except:
            print "url error"
            raise
        data = opener.read()
        isGzip = opener.headers.get('Content-Encoding')
        if isGzip:
            compressedstream = StringIO.StringIO(data)
            gzipper = gzip.GzipFile(fileobj=compressedstream)
            data = gzipper.read()

        mychar = chardet.detect(data)
        encoding = mychar['encoding']
        if encoding == 'utf-8' or encoding == 'UTF-8':
            html = data
        else:
            html = data.decode('gb2312', 'ignore').encode('utf-8')
        if (key != None):
            if (html.find(key) != -1):
                return html
            else:
                pass
        return html

    def deepAnalysis(self, urltext, key =None):
        soup = BeautifulSoup.BeautifulSoup(urltext)
        for i in soup.findAll('a'):
            if i.has_key('href') == False:
                print '[!warnning] maybe messy code with %s \n' % i
                self.logging('[!warnning] maybe messy code with %s \n' % i)
                continue
            if i['href'].find(options.url[options.url.find('.')+1:]) != -1: # u'查找href中的url是否是同域的，
            # 或者二级域名，别把友情链接也给爬出来了...'
                self.logging('spidering ' + i['href'])
                self.urllist.append(i['href'])
            elif i['href'].startswith('/') :  #u'判断是否是网站的下级目录'
                self.logging('spidering ' + options.url + i['href'])
                self.urllist.append(options.url + i['href'])
            else:
                continue
        return self.urllist

    def filesave(self, data, num):
        print os.getcwd()+ '\\db'+ str(num) +'.txt'
        f = open(os.getcwd() + '\\db'+ str(num) +'.txt', 'w')
        f.write(data)
        f.close()


    def logging(self, text):
        formats = {  #设置日志级别
        1:'%(asctime)s %(filename)s %(message)s',
        2:'%(asctime)s %(filename)s %(levelname)s %(message)s',
        3:'%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
        4:'%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(thread)d %(message)s',
        5:'%(asctime)s %(filename)s %(pathname)s [line:%(lineno)d] %(levelname)s %(thread)d %(funcName)s %(message)s'}

        logging.basicConfig(level=logging.DEBUG,  #
                    format=formats[5],
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='myapp.log',
                    filemode='w')

        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)

        logging.info(text)

    def DBsave(self, url, datas):  #u'sqlite3数据库保存'
        conn = sqlite3.connect(self.dbpath)
        c = conn.cursor()
        sql = '''create table if not exists content (
        urls TEXT not null,
        content TEXT not null
        )'''
        c.execute(sql)
        sql='insert into content values(?,?)'
        try:
            c.execute(sql, (url.decode('utf-8', 'ignore'), datas.decode('utf-8', 'ignore')))
        except:
             c.execute(sql, (url.decode('gb2312', 'ignore'), datas.decode('gb2312', 'ignore')))
        conn.commit()
        conn.close()


def usage():
    if sys.argv != 1:
        parser = optparse.OptionParser()
        parser.add_option("-f", dest = "filename" , help="write report to FIle",  default='1.txt')
        parser.add_option("-u", dest = "url" , help="The URL you want to spider")
        parser.add_option("-d", dest = "deep" , help="deep level spider")
        parser.add_option("-t", dest = "trnum" , help="Thread num")
        parser.add_option("-k", dest = "key" , help="key")
        global options
        (options, args) = parser.parse_args()
    else:
        print '''
usage: %s  [options]

options:
  -h    show this help message and exit
  -u    The URL you want to spider
  -d    deep level spider
  -f    write report to FILE
  -t    Thread num
  -k    key
  -ts   testself
''' % sys.argv[0]


def main():

    #使用线程池
    socket.setdefaulttimeout(10)
    print 'start testing'
    wm = WorkerManager(50)
    for url_name in url_list.keys():
        wm.add_job(do_get_con, url_name, url_list[url_name])
    wm.wait_for_complete()
    print 'end testing'
if __name__ == "__main__":
    usage()

