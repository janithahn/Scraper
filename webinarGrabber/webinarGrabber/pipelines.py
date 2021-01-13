# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd
from .items import WebinarItem
from scrapy.exceptions import DropItem


class WebinargrabberPipeline:

    def process_item(self, item, spider):
        return item


class DataframePipeline:

    def close_spider(self, spider):
        if spider.name == 'webinarbot':
            df = pd.DataFrame(spider.items, columns=['Link', 'Filtered Data'])
            df = df.set_index('Link').drop_duplicates()

            print(df)
            df.to_csv('filtered_data.csv')

        return None


class DuplicatesPipeline:

    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if spider.name == 'webinarbot':
            adapter = WebinarItem(item)
            if adapter['link'] in self.ids_seen:
                raise DropItem(f"Duplicate item found: {item!r}")
            else:
                self.ids_seen.add(adapter['link'])
                return item
