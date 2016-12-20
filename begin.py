#-*-coding:utf-8-*-
import time
import logging
import scrapy
import threading
import multiprocessing
from scrapy import cmdline

def run():
    print 'start!'
    cmdline.execute("scrapy crawl tianya".split())


#定时器：每隔一定的时间执行一次爬虫
def scheduler():
    while True:
        try:
            print "********************************************************************************"
            p = multiprocessing.Process(target=run)
            p.start()
            p.join()
            # print (1/0)
            #此处设置运行间隔时间，一小时
            time.sleep(3600) #seconds
        except Exception as e:
            # logging.exception(e)
            time_now = time.strftime("%Y%m%d %H %M", time.localtime())
            filename = 'G://' + str(time_now) + '.log'
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                                datefmt='%a, %d %b %Y %H:%M:%S',
                                filename='./logs/' + str(time_now) + '.log',
                                filemode='w')
            logging.debug(e)
            print 'failed!'
            # break
            # logging.info(e)
            # logging.warning(e)
            # logging.error(e)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    scheduler()
