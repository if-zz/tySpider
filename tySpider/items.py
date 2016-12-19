# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TyspiderItem(scrapy.Item):
    # define the fields for your item here like:
    #帖子表
    isNew=scrapy.Field()#该字段表示帖子是否为新
    article_name = scrapy.Field()
    article_url=scrapy.Field()
    article_description=scrapy.Field()
    article_lenth=scrapy.Field()
    article_time = scrapy.Field()
    article_last_time=scrapy.Field()
    click_num = scrapy.Field()
    reply_num = scrapy.Field()
    article_author = scrapy.Field()
    article_authorID=scrapy.Field()
    # 回帖表
    parent_reply_author =scrapy.Field()
    parent_reply_authorID = scrapy.Field()
    # parent_replyID = scrapy.Field()
    parent_reply_time = scrapy.Field()
    parent_reply_content = scrapy.Field()
    parent_reply_lenth=scrapy.Field()
    child_reply_author = scrapy.Field()
    child_reply_authorID = scrapy.Field()
    child_reply_time = scrapy.Field()
    child_reply_content = scrapy.Field()
    child_reply_lenth=scrapy.Field()
    child_reply_num = scrapy.Field()#该回复的评论数
    pass

