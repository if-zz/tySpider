# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import codecs
import MySQLdb

# 根据帖子是否为新加入旧帖子的更新流程
class TyspiderPipeline(object):
    def __init__(self):
        # self.file = codecs.open('items.txt', 'w', encoding='utf-8')
        # self.file2 = codecs.open('reply.txt', 'w', encoding='utf-8')

        self.conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd="123456", db="scrapy", port=3306, charset = "utf8")
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute("CREATE TABLE main_table(ID BIGINT AUTO_INCREMENT,	URL TEXT,title NVARCHAR(200),authorID VARCHAR(20),author NVARCHAR(100),publishTime DATETIME,context TEXT,replyNum INT,viewNum INT,newstTime DATETIME,wordsNum INT,keyWords TEXT,eventID BIGINT,nameEntity TEXT,placeEntity TEXT,organizationEntity TEXT,PRIMARY KEY(ID));")
            self.cursor.execute("CREATE TABLE reply_table(ID BIGINT AUTO_INCREMENT,parentID BIGINT,postID BIGINT,replierID VARCHAR(20) NOT NULL,replierName NVARCHAR(100) NOT NULL,content TEXT,replyDate DATETIME,commentNum INT DEFAULT '0',wordsNum INT,keyWords TEXT,PRIMARY KEY(ID),FOREIGN KEY(postID) REFERENCES main_table(ID) ON DELETE CASCADE ON UPDATE CASCADE);")
        except:
            pass

    def process_item(self, item, spider):
        '''
        两个流程
        if item["url"]在数据库中：update
        else:insert
        :param item:
        :param spider:
        :return:
        '''

        if item["article_description"][0]== '':
            item["article_description"][0] = item["article_name"][0]

        #先判断数据库中是否已存在该帖
        result = self.cursor.execute("SELECT* FROM main_table WHERE URL = '%s'" % str(item["article_url"][0]))

        if result == 0:
            self.cursor.execute(
                "INSERT INTO main_table VALUES(NULL,'%s','%s','%s','%s','%s','%s',%d,%d,'%s',%d,NULL,NULL,NULL,NULL,NULL)" % (
                    str(item["article_url"][0]), str(item["article_name"][0]), str(item["article_authorID"][0]),
                    str(item["article_author"][0]), str(item["article_time"][0]),str(item["article_description"][0]),
                    int(item["reply_num"][0]), int(item["click_num"][0]), str(item["article_last_time"][0] + ':00'), int(item["article_lenth"][0])))
        else:
            self.cursor.execute(
                "UPDATE main_table SET replyNum = %d, viewNum = %d, newstTime = '%s' WHERE URL = '%s'" % (
                    int(item["reply_num"][0]), int(item["click_num"][0]), str(item["article_last_time"][0] + ':00'), str(item["article_url"][0])
                ))
            print "我正在更新！update!"

        self.conn.commit()


        if len(item["parent_reply_author"]):
            for i in xrange(0, len(item["parent_reply_author"])):
                #先得到该回复帖集的帖子ID
                self.cursor.execute("SELECT ID FROM main_table WHERE URL = '%s'" %  str(item["article_url"][0]))
                main_id = self.cursor.fetchone()[0]

                #数据库中是否已经存在该回复帖
                parent_result = self.cursor.execute("SELECT* FROM reply_table WHERE postID = %d AND replierName = '%s' AND replyDate = '%s'" %
                                                    (int(main_id), str(item["parent_reply_author"][i]), str(item["parent_reply_time"][i])))
                if parent_result==0:
                    self.cursor.execute("INSERT INTO reply_table VALUES(NULL,0,%d,%d,'%s','%s','%s',%d,%d,NULL)" %
                                        (int(main_id), int(item["parent_reply_authorID"][i]), str(item["parent_reply_author"][i]), str(item["parent_reply_content"][i]),
                                         str(item["parent_reply_time"][i]), int(str(item["child_reply_num"][i])), int(item["parent_reply_lenth"][i])))
                else:
                    self.cursor.execute("UPDATE reply_table SET commentNum = %d WHERE postID = %d AND replierName = '%s' AND replyDate = '%s' "%
                                        (int(item["child_reply_num"][i]),int(main_id), str(item["parent_reply_author"][i]), str(item["parent_reply_time"][i])))
                self.conn.commit()

                for j in xrange(0, len(item["child_reply_author"][i])):
                    child_result = self.cursor.execute("SELECT* FROM reply_table WHERE parentID IS NULL AND replierName = '%s' AND replyDate = '%s'"%
                                                       (str(item["child_reply_author"][i][j]), str(item["child_reply_time"][i][j])))
                    if child_result==0:
                        self.cursor.execute("SELECT ID FROM reply_table WHERE parentID = 0 AND replierName = '%s' AND replyDate = '%s'"%
                                                        (str(item["parent_reply_author"][i]), str(item["parent_reply_time"][i])))
                        parent_id = self.cursor.fetchone()[0]
                        print "parent_id************************",parent_id,main_id
                        self.cursor.execute("INSERT INTO reply_table VALUES(NULL,%d,%d,%d,'%s','%s','%s',0,%d,NULL)" %
                                            (int(parent_id), int(main_id), int(item["child_reply_authorID"][i][j]), str(item["child_reply_author"][i][j]),
                                             str(item["child_reply_content"][i][j]), str(item["child_reply_time"][i][j]), int(item["child_reply_lenth"][i][j])))
                        self.conn.commit()
                # self.file2.write(reply_line)
        self.conn.commit()
        return item

    def spider_closed(self, spider):
        # self.file.close()
        # self.file2.close()
        self.cursor.close()
        self.conn.close()