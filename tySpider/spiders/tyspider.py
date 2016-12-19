
# -*- coding:utf-8 -*-

from scrapy.contrib.loader import ItemLoader
from scrapy.spider import Spider
from scrapy.http import Request
from scrapy.selector import Selector
from tySpider.items import TyspiderItem
import scrapy
import lxml.html as lh
import sys
import re
import time
import os
from scrapy.contrib.spiders import CrawlSpider, Rule
from urlparse import urljoin
from xml.dom.minidom import parse
import xml.dom.minidom
ISOTIMEFORMAT='%Y-%m-%d %X'
localtime=time.strftime( ISOTIMEFORMAT, time.localtime() )
class tianyaBBSspider(CrawlSpider):
    reload(sys)
    sys.setdefaultencoding('utf8')

    # 爬虫名称，非常关键，唯一标示
    name = "tianya"

    # 域名限定
    allowed_domains = ["bbs.tianya.cn"]

    # 爬虫的爬取得起始url
    start_urls = [

        # 天涯论坛热帖榜  可以写多个用，分隔

        "http://bbs.tianya.cn/list-828-1.shtml",

    ]
    baseurl = 'http://bbs.tianya.cn'
    f=open('last_time.txt','r')
    last_time_new=unicode(f.readline())#门槛时间
    f.close()
    last_time_past=last_time_new
    isSet=0#标志是否已设置下一次的门槛时间

    def parse(self, response):
        # 选择器
        sel = Selector(response)
        item = TyspiderItem()
        # 文章url列表
        article_url = sel.xpath(
            '//div[@class="mt5"]/table[@class="tab-bbs-list tab-bbs-list-2"]//tr/td[1]/a/@href').extract()
        #文章最终回复时间
        article_time=sel.xpath('//div[@class="mt5"]/table[@class="tab-bbs-list tab-bbs-list-2"]//tr/td[5]/@title').extract()
        # 下一页地址
        next_page_url = sel.xpath('//div[@class="short-pages-2 clearfix"]/div[@class="links"]/a[@rel]/@href').extract()
        real_article_url=[]
        real_article_time=[]
        for i in range(len(article_url)):
            if article_time[i]>self.last_time_past:
                if self.isSet==0:
                    self.isSet=1
                    self.last_time_new=article_time[i]
                    f=open('last_time.txt','w')
                    f.write(self.last_time_new)
                    f.close()
                real_article_url.append(article_url[i])
                real_article_time.append(article_time[i])
        if len(real_article_url)==0:
            return
        for url in real_article_url:
            # 拼接url
            urll = urljoin(self.baseurl, url)
            # 调用parse_item解析文章内容
            request = scrapy.Request(urll, callback=self.parse_item)
            request.meta['article_last_time']=real_article_time[real_article_url.index(url)]
            request.meta['item'] = item
            yield request

        if next_page_url[0]:
            # 调用自身进行迭代
            request = scrapy.Request(urljoin(self.baseurl, next_page_url[0]), callback=self.parse)
            yield request

    def parse_item(self, response):
        content =u''
        sel = Selector(response)
        article_time = sel.xpath(
            '//div[@id="post_head"]/div[@class="atl-menu clearfix js-bbs-act"]/div[@class="atl-info"]/span[2]/text()').extract()[0].replace(u'时间：','')
        #检查是否为当日新帖
        if (article_time.split())[0]==(localtime.split()[0]):
           isNew=True
        else:
           isNew=False

        item = response.meta['item']
        article_last_time=response.meta['article_last_time']
        l = ItemLoader(item=item, response=response)
        article_url = str(response.url)
        article_name = sel.xpath('//title/text()').extract()
        article_author = sel.xpath('//div[@id="post_head"]/div[@class="atl-menu clearfix js-bbs-act"]/div[@class="atl-info"]/span[1]/a/text()').extract()
        article_authorID=sel.xpath('//div[@id="post_head"]/div[@class="atl-menu clearfix js-bbs-act"]/div[@class="atl-info"]/span[1]/a/@href').extract()
        article_clik_num = sel.xpath('//div[@id="post_head"]/div[@class="atl-menu clearfix js-bbs-act"]/div[@class="atl-info"]/span[3]/text()').extract()
        article_reply_num = sel.xpath('//div[@id="post_head"]/div[@class="atl-menu clearfix js-bbs-act"]/div[@class="atl-info"]/span[4]/text()').extract()
        article_description = sel.xpath('//div[@class="atl-main"]/div[@class="atl-item host-item"]/div[@class="atl-content"]/div[2]/div[@class="bbs-content clearfix"]/text()').extract()
        # 文章内容拼起来
        for article in article_description:
            content = content + article
        if content == '' or content == '\r\n' or content == '\r\n\r\n':
            pass
        else:
            content = content.replace(u'\r', '').replace(u'\n', '').replace(u' ', '').replace(u'\t', '')
            content = re.sub(u'[\u3000,\xa0]', u'', content)
        #处理回帖
        parent_reply_author=[]
        parent_reply_authorID=[]
        parent_reply_lenth=[]
        parent_reply_time=[]
        parent_reply_content=[]
        child_reply_author = []
        child_reply_authorID = []
        child_reply_time = []
        child_reply_content = []
        child_reply_lenth=[]
        child_reply_num=[]
        reply=sel.xpath('//div[@class="atl-main"]//div[@class="atl-item"]')
        for i_reply in reply:
            '''
         此处为回复的爬取逻辑（回复下的评论不属于此列，回复是其下评论的父贴）
            '''
            ireply_parent_author=i_reply.xpath('@_host').extract()[0]
            parent_reply_author.append(ireply_parent_author)
            ireply_parent_authorID=i_reply.xpath('@_hostid').extract()[0]
            parent_reply_authorID.append(ireply_parent_authorID)
            # ireply_ID=i_reply.xpath('@replyid').extract()[0]
            # parent_replyID.append(ireply_ID)
            ireply_parent_time=i_reply.xpath("@js_restime").extract()[0]
            parent_reply_time.append(ireply_parent_time)
            ireply_parent_content=i_reply.xpath("div[2]/div[2]/div[1]/text()|"
                                                "div[2]/div[2]/div[1]/div[1]/text()").extract()
            for i in reversed(range(0, len(ireply_parent_content))):
                ireply_parent_content[i] = ireply_parent_content[i].replace(u'\r', '').replace(u'\n', '').replace(u' ', '').replace('\t','')
                ireply_parent_content[i] = re.sub(u'[\u3000,\xa0]', u'', ireply_parent_content[i])
                if ireply_parent_content[i] == '':
                    del ireply_parent_content[i]
            parent_content=u''
            for pcontent in ireply_parent_content:
                parent_content+=pcontent
            parent_reply_content.append(parent_content)
            parent_reply_lenth.append(len(parent_content))
            ireply_child_author=i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li/@_username').extract()
            child_reply_author.append(ireply_child_author)
            ireply_child_athorID=i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li/@_userid').extract()
            child_reply_authorID.append(ireply_child_athorID)
            ireply_child_time=i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li/@_replytime').extract()
            child_reply_time.append(ireply_child_time)

            #处理对回复的评论，爬取每一条评论child_content,再将他们存入ireply_c_c,再将这一个回复的评论list存入child_reply_content.
            #即对于每一条回复,都有一个对应的评论列表ireply_c_c
            ireply_child=i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li')
            ireply_c_c=[]
            ireply_c_l=[]
            for ireply_child_c in ireply_child:
                ireply_child_content=ireply_child_c.xpath('span/text()').extract()
                for i in reversed(range(0, len(ireply_child_content))):
                    ireply_child_content[i] = ireply_child_content[i].replace(u'\r', '').replace(u'\n', '').replace(
                        u' ', '').replace('\t', '')
                    ireply_child_content[i] = re.sub(u'[\u3000,\xa0]', u'', ireply_child_content[i])
                    if ireply_child_content[i] == '':
                        del ireply_child_content[i]
                child_content = u''
                for chcontent in ireply_child_content:
                    child_content += chcontent
                ireply_c_c.append(child_content)
                ireply_c_l.append(len(child_content))
            child_reply_content.append(ireply_c_c)
            child_reply_lenth.append(ireply_c_l)
            child_reply_num.append(len(ireply_child_author))

        article_name = article_name[0].replace(u'_百姓声音_天涯论坛', '')
        description_content = content
        article_lenth=len(description_content)
        article_url = article_url
        article_time = article_time
        article_author = article_author[0]
        article_authorID = article_authorID[0].replace(u'http://www.tianya.cn/', '')
        click_num = article_clik_num[0]
        reply_num = article_reply_num[0]
        click_num = str(click_num).split(u'：')[1]
        reply_num = str(reply_num).split(u'：')[1]
        print click_num, reply_num
        l.add_value('article_name', article_name)
        l.add_value('article_description', description_content)
        l.add_value('article_lenth',article_lenth)
        l.add_value('article_url', article_url)
        l.add_value('article_time', article_time)
        l.add_value('article_last_time', article_last_time)
        l.add_value('reply_num', reply_num)
        l.add_value('click_num', click_num)
        l.add_value('article_author', article_author)
        l.add_value('article_authorID', article_authorID)
        l.add_value("isNew", isNew)
        next_page_reply=sel.xpath('//body/div[not(@id)]/div[@id="doc"]/div[@id="bd"]/div[@id="post_head"]/div[3]/div[@class="atl-pages"]/form/a[@class="js-keyboard-next"]/@href'
                                  ).extract()
        if len(next_page_reply)!=0:
            request = scrapy.Request(urljoin(self.baseurl, next_page_reply[0]), callback=self.parse_more_reply)
            request.meta["parent_reply_author"]=parent_reply_author
            request.meta["parent_reply_authorID"]=parent_reply_authorID
            # request.meta[" parent_replyID"]=parent_replyID
            request.meta["parent_reply_time"]=parent_reply_time
            request.meta["parent_reply_content"]=parent_reply_content
            request.meta["child_reply_author"]=child_reply_author
            request.meta["child_reply_authorID"]=child_reply_authorID
            request.meta["child_reply_time"]=child_reply_time
            request.meta["child_reply_content"]=child_reply_content
            request.meta["child_reply_num"]=child_reply_num
            request.meta["parent_reply_lenth"]=parent_reply_lenth
            request.meta["child_reply_lenth"]=child_reply_lenth
            request.meta["l"]=l
            yield request
        else:
            l.add_value('parent_reply_author', parent_reply_author)
            l.add_value('parent_reply_authorID', parent_reply_authorID)
            # l.add_value('parent_replyID', parent_replyID)
            l.add_value('parent_reply_time', parent_reply_time)
            l.add_value('parent_reply_content', parent_reply_content)
            l.add_value('child_reply_author', child_reply_author)
            l.add_value('child_reply_authorID', child_reply_authorID)
            l.add_value('child_reply_time', child_reply_time)
            l.add_value('child_reply_content', child_reply_content)
            l.add_value('child_reply_num', child_reply_num)
            l.add_value('parent_reply_lenth',parent_reply_lenth)
            l.add_value('child_reply_lenth',child_reply_lenth)
            yield l.load_item()

    def parse_more_reply(self,response):
        sel = Selector(response)
        parent_reply_author = response.meta["parent_reply_author"]
        parent_reply_authorID = response.meta["parent_reply_authorID"]
        parent_reply_lenth = response.meta["parent_reply_lenth"]
        parent_reply_time = response.meta["parent_reply_time"]
        parent_reply_content = response. meta["parent_reply_content"]
        child_reply_author = response.meta["child_reply_author"]
        child_reply_authorID = response. meta["child_reply_authorID"]
        child_reply_time = response. meta["child_reply_time"]
        child_reply_content = response. meta["child_reply_content"]
        child_reply_num = response. meta["child_reply_num"]
        child_reply_lenth=response.meta["child_reply_lenth"]
        l=response.meta["l"]
        reply = sel.xpath('//div[@class="atl-main"]//div[@class="atl-item"]')
        for i_reply in reply:
            '''
         此处为回复的爬取逻辑（回复下的评论不属于此列，回复是其下评论的父贴）
            '''
            ireply_parent_author = i_reply.xpath('@_host').extract()[0]
            parent_reply_author.append(ireply_parent_author)
            ireply_parent_authorID = i_reply.xpath('@_hostid').extract()[0]
            parent_reply_authorID.append(ireply_parent_authorID)
            # ireply_ID = i_reply.xpath('@replyid').extract()[0]
            # parent_replyID.append(ireply_ID)
            ireply_parent_time = i_reply.xpath("@js_restime").extract()[0]
            parent_reply_time.append(ireply_parent_time)
            ireply_parent_content = i_reply.xpath("div[2]/div[2]/div[1]/text()|"
                                                  "div[2]/div[2]/div[1]/div[1]/text()").extract()
            for i in reversed(range(0, len(ireply_parent_content))):
                ireply_parent_content[i] = ireply_parent_content[i].replace(u'\r', '').replace(u'\n', '').replace(u' ',
                                                                                                                  '').replace(
                    '\t', '')
                ireply_parent_content[i] = re.sub(u'[\u3000,\xa0]', u'', ireply_parent_content[i])
                if ireply_parent_content[i] == '':
                    del ireply_parent_content[i]
            parent_content = u''
            for pcontent in ireply_parent_content:
                parent_content += pcontent
            parent_reply_content.append(parent_content)
            parent_reply_lenth.append(len(parent_content))
            ireply_child_author = i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li/@_username').extract()
            child_reply_author.append(ireply_child_author)
            ireply_child_athorID = i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li/@_userid').extract()
            child_reply_authorID.append(ireply_child_athorID)
            ireply_child_time = i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li/@_replytime').extract()
            child_reply_time.append(ireply_child_time)

            # 处理对回复的评论，爬取每一条评论child_content,再将他们存入ireply_c_c,再将这一个回复的评论list存入child_reply_content.
            # 即对于每一条回复,都有一个对应的评论列表ireply_c_c
            ireply_child = i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li')
            ireply_c_c = []
            ireply_c_l=[]
            for ireply_child_c in ireply_child:
                ireply_child_content = ireply_child_c.xpath('span/text()').extract()
                for i in reversed(range(0, len(ireply_child_content))):
                    ireply_child_content[i] = ireply_child_content[i].replace(u'\r', '').replace(u'\n', '').replace(
                        u' ', '').replace('\t', '')
                    ireply_child_content[i] = re.sub(u'[\u3000,\xa0]', u'', ireply_child_content[i])
                    if ireply_child_content[i] == '':
                        del ireply_child_content[i]
                child_content = u''
                for chcontent in ireply_child_content:
                    child_content += chcontent
                ireply_c_c.append(child_content)
                ireply_c_l.append(len(child_content))
            child_reply_content.append(ireply_c_c)
            child_reply_lenth.append(ireply_c_l)
            child_reply_num.append(len(ireply_child_author))

        next_page_reply = sel.xpath('/div[3]/div[@id="doc"]/div[@id="bd"]/div[@class="clearfix"]/div[@class="atl-pages"]/form/a[@class="js-keyboard-next"]/@href').extract()
        if len(next_page_reply)!=0:
            request = scrapy.Request(urljoin(self.baseurl, next_page_reply[0]), callback=self.parse_more_reply)
            request.meta["parent_reply_author"]=parent_reply_author
            request.meta["parent_reply_authorID"]=parent_reply_authorID
            # request.meta[" parent_replyID"]=parent_replyID
            request.meta["parent_reply_time"]=parent_reply_time
            request.meta["parent_reply_content"]=parent_reply_content
            request.meta["parent_reply_lenth"]=parent_reply_lenth
            request.meta["child_reply_author"]=child_reply_author
            request.meta["child_reply_authorID"]=child_reply_authorID
            request.meta["child_reply_time"]=child_reply_time
            request.meta["child_reply_content"]=child_reply_content
            request.meta["child_reply_lenth"]=child_reply_lenth
            request.meta["child_reply_num"]=child_reply_num
            request.meta["l"]=l
            yield request
        else:
            l.add_value('parent_reply_author', parent_reply_author)
            l.add_value('parent_reply_authorID', parent_reply_authorID)
            # l.add_value('parent_replyID', parent_replyID)
            l.add_value('parent_reply_time', parent_reply_time)
            l.add_value('parent_reply_content', parent_reply_content)
            l.add_value('parent_reply_lenth',parent_reply_lenth)
            l.add_value('child_reply_author', child_reply_author)
            l.add_value('child_reply_authorID', child_reply_authorID)
            l.add_value('child_reply_time', child_reply_time)
            l.add_value('child_reply_content', child_reply_content)
            l.add_value('child_reply_num', child_reply_num)
            l.add_value('child_reply_lenth',child_reply_lenth)
            yield l.load_item()


    # def reply_process(reply):
    #     # 处理回帖
    #
    #     parent_reply_author = []
    #     parent_reply_authorID = []
    #     parent_replyID = []
    #     parent_reply_time = []
    #     parent_reply_content = []
    #     child_reply_author = []
    #     child_reply_authorID = []
    #     child_reply_time = []
    #     child_reply_content = []
    #     child_reply_num = []
    #     for i_reply in reply:
    #         '''
    #      此处为回复的爬取逻辑（回复下的评论不属于此列，回复是其下评论的父贴）
    #         '''
    #         ireply_parent_author = i_reply.xpath('@_host').extract()[0]
    #         parent_reply_author.append(ireply_parent_author)
    #         ireply_parent_authorID = i_reply.xpath('@_hostid').extract()[0]
    #         parent_reply_authorID.append(ireply_parent_authorID)
    #         ireply_ID = i_reply.xpath('@replyid').extract()[0]
    #         parent_replyID.append(ireply_ID)
    #         ireply_parent_time = i_reply.xpath("@js_restime").extract()[0]
    #         parent_reply_time.append(ireply_parent_time)
    #         ireply_parent_content = i_reply.xpath("div[2]/div[2]/div[1]/text()|"
    #                                               "div[2]/div[2]/div[1]/div[1]/text()").extract()
    #         for i in reversed(range(0, len(ireply_parent_content))):
    #             ireply_parent_content[i] = ireply_parent_content[i].replace(u'\r', '').replace(u'\n', '').replace(u' ',
    #                                                                                                               '').replace(
    #                 '\t', '')
    #             ireply_parent_content[i] = re.sub(u'[\u3000,\xa0]', u'', ireply_parent_content[i])
    #             if ireply_parent_content[i] == '':
    #                 del ireply_parent_content[i]
    #         parent_content = u''
    #         for pcontent in ireply_parent_content:
    #             parent_content += pcontent
    #         parent_reply_content.append(parent_content)
    #
    #         ireply_child_author = i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li/@_username').extract()
    #         child_reply_author.append(ireply_child_author)
    #         ireply_child_athorID = i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li/@_userid').extract()
    #         child_reply_authorID.append(ireply_child_athorID)
    #         ireply_child_time = i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li/@_replytime').extract()
    #         child_reply_time.append(ireply_child_time)
    #
    #         # 处理对回复的评论，爬取每一条评论child_content,再将他们存入ireply_c_c,再将这一个回复的评论list存入child_reply_content.
    #         # 即对于每一条回复,都有一个对应的评论列表ireply_c_c
    #         ireply_child = i_reply.xpath('div[2]/div[2]/div[3]/div[1]/ul//li')
    #         ireply_c_c = []
    #         for ireply_child_c in ireply_child:
    #             ireply_child_content = ireply_child_c.xpath('span/text()').extract()
    #             for i in reversed(range(0, len(ireply_child_content))):
    #                 ireply_child_content[i] = ireply_child_content[i].replace(u'\r', '').replace(u'\n', '').replace(
    #                     u' ', '').replace('\t', '')
    #                 ireply_child_content[i] = re.sub(u'[\u3000,\xa0]', u'', ireply_child_content[i])
    #                 if ireply_child_content[i] == '':
    #                     del ireply_child_content[i]
    #             child_content = u''
    #             for chcontent in ireply_child_content:
    #                 child_content += chcontent
    #             ireply_c_c.append(child_content)
    #         child_reply_content.append(ireply_c_c)
    #         child_reply_num.append(len(ireply_child_author))
