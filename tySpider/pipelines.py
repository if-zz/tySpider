# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs

# 根据帖子是否为新加入旧帖子的更新流程
class TyspiderPipeline(object):
    def __init__(self):
        self.file = codecs.open('items.txt', 'w', encoding='utf-8')

    def process_item(self, item, spider):
        '''
        两个流程
        if item["url"]在数据库中：update
        else:insert
        :param item:
        :param spider:
        :return:
        '''

        if item["article_description"][0]!= '':
            line = item["article_url"][0] + ' ' + item["article_name"][0] + ' ' + item["article_description"][0] + "\n"
        else:
            line = item["article_url"][0] + ' ' + item["article_name"][0] + ' ' + item["article_name"][0] + "\n"
        self.file.write(line)
        return item

    def spider_closed(self, spider):
        self.file.close()