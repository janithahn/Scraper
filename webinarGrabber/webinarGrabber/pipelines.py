# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import pandas as pd
from scrapy.exceptions import DropItem
import pymongo
from itemadapter import ItemAdapter
import json
import spacy
from .items import EventItem
import re


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
            adapter = ItemAdapter(item)
            if adapter['link'] in self.ids_seen:
                raise DropItem(f"Duplicate item found: {item!r}")
            else:
                self.ids_seen.add(adapter['link'])
                return item


class MongoPipeline:

    collection_name = 'webinar_collection'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.ids_seen = set()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('mongodb://127.0.0.1:27017'),
            mongo_db=crawler.settings.get('sciCrawler', 'sciCrawler')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    '''Saving to mongodb while filtering duplicates'''
    def process_item(self, item, spider):
        if spider.name == 'webinarbot':
            adapter = ItemAdapter(item)
            if adapter['link'] in self.ids_seen:
                raise DropItem(f"Duplicate item found: {item!r}")
            else:
                if self.db[self.collection_name].find({"link": adapter["link"]}).count() > 0:
                    # raise DropItem(f"Duplicate item found in the database: {item!r}")
                    self.db[self.collection_name].update({"link": adapter["link"]}, adapter.asdict())
                else:
                    self.db[self.collection_name].insert_one(adapter.asdict())
                self.ids_seen.add(adapter['link'])
                return item
        return None


class EventDuplicatesPipeline:

    def __init__(self):
        self.ids_seen = set()

    def open_spider(self, spider):
        self.file = open('events.json', 'w')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        if spider.name == 'eventbot':
            adapter = EventItem(item)
            nlp = spacy.load("en_core_web_sm")

            if adapter['title'] in self.ids_seen:
                raise DropItem(f"Duplicate item found: {item!r}")
            else:
                if 'event' in adapter['link'] or 'news' in adapter['link']:
                    self.ids_seen.add(adapter['title'])

                    doc = nlp(adapter['texts'])
                    sentences = list(doc.sents)
                    # str(sent).split()
                    sentences = [" ".join(re.split(r"\s{2,}", str(sent))) for sent in sentences]
                    adapter['sentences'] = str(sentences)

                    write_dic = {
                        'title': adapter['title'],
                        'link': adapter['link'],
                        'sentences': adapter['sentences']
                    }

                    line = json.dumps(dict(write_dic)) + "\n"
                    self.file.write(line)

                    with open('events.txt', 'a', encoding='utf-8') as file:
                        file.write(str(adapter) + '\n\n\n-------------------------------------------\n\n\n')
                    return item
                return None
