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


#设定准确的时间
# time_setting = "2016-12-14 21:35"


#定时器1:在指定的时间（比如每天的九点）执行爬虫
# while True:
#     time_now = time.strftime("%Y-%m-%d %H:%M", time.localtime())
#
#     if time_now == time_setting:
#         scrapy()    #或者输入命令行来跑scrapy
#         time.sleep(10)
#     else:
#         time.sleep(10)
#         print("Sleep!")


#定时器2：每隔一定的时间执行一次爬虫
def scheduler():
    while True:
        try:
            print "汤亦凡shi傻逼********************************************************************************"
            p = multiprocessing.Process(target=run)
            p.start()
            p.join()
            print (1/0)
            time.sleep(15) #seconds
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
