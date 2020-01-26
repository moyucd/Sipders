# -*- coding: utf-8 -*-
"""
@File   : pipelines/nfs.py
@Author : dong bao
@Date   : 2020/1/26
@Description : nfs pipelines
"""

import os
import errno
import logging
from scrapy.exceptions import CloseSpider
from twisted.internet.threads import deferToThread
from ConstructionIndustrySpider.items.basic_item import FileItem


class NfsPipeline(object):
    def __init__(self, path):
        self.path = path

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings.get('NFS_MOUNT'))

    def open_spider(self, spider):
        # if not os.path.exists(self.path):
        # 检查路径是否存在、是否为挂载点
        if not os.path.exists(self.path) or not os.path.ismount(self.path):
            spider.log('Mount point not exist', level=logging.ERROR)
            raise CloseSpider(reason='Mount point not exist')

    def process_item(self, item, spider):
        return deferToThread(self._process_item, item, spider)

    def _process_item(self, item, spider):
        if isinstance(item, FileItem):
            data = dict(item)
            filepath = self.path + data['path']
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

            with open(filepath, 'wb+') as f:
                f.write(data['data'])
        return item

