# -*- coding: utf-8 -*-
"""
@File   : pipelines/mongo.py
@Author : dong bao
@Date   : 2020/1/26
@Description : mongodb pipelines
"""

import time
import logging
import pymongo
import pymongo.errors
from scrapy.exceptions import CloseSpider
from twisted.internet.threads import deferToThread
from ConstructionIndustrySpider.items.basic_item import BasicItem


class MongoPipeline(object):
    def __init__(self, mongo_url, mongo_host, mongo_port, mongo_db, mongo_auth):
        self.mongo_url = mongo_url
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_db = mongo_db
        self.mongo_auth = mongo_auth

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_url=crawler.settings.get('MONGO_URL'),
            mongo_host=crawler.settings.get('MONGO_HOST'),
            mongo_port=crawler.settings.get('MONGO_PORT'),
            mongo_db=crawler.settings.get('MONGO_DB'),
            mongo_auth=crawler.settings.get('MONGO_AUTH')
        )

    def open_spider(self, spider):
        try:
            if self.mongo_url:
                self.client = pymongo.MongoClient(self.mongo_url)
            else:
                self.client = pymongo.MongoClient(host=self.mongo_host, port=self.mongo_port,
                                                  authSource=self.mongo_db, **self.mongo_auth)
            self.db = self.client[self.mongo_db]
            spider.log('MongoDB ' + str(self.db.command('hostInfo')))
        except pymongo.errors.PyMongoError as e:
            spider.log('MongoDB ' + str(e), level=logging.ERROR)
            raise CloseSpider(reason=str(e))

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        if isinstance(item, BasicItem):
            data = dict(item)
            collection = data['collection']
            del data['collection']
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            find_result = self.db[collection].find_one_and_update({'url': data['url']}, {'$set': {'last_crawled': now}})
            if find_result is None or find_result['raw_data'] != data['raw_data']:
                data['last_updated'] = now
                data['last_crawled'] = now
                self.db[collection].update_one({'url': data['url']}, {'$set': data}, upsert=True)
        return item
