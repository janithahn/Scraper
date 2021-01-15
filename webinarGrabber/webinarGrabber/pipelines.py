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
            df.to_csv('webinars.csv')

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


class EventDuplicatesPipeline:

    event_collection = 'event_collection'
    webinar_collection = 'webinar_collection'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.ids_seen = set()

    def open_spider(self, spider):
        if spider.name == 'webinarbot':
            self.file = open('webinars.json', 'w')
        elif spider.name == 'eventbot':
            self.file = open('events.json', 'w')
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.file.close()
        self.client.close()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('mongodb://127.0.0.1:27017'),
            mongo_db=crawler.settings.get('sciCrawler', 'sciCrawler')
        )

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
                    adapter['sentences'] = sentences

                    write_dic = {
                        'title': adapter['title'],
                        'link': adapter['link'],
                        'sentences': adapter['sentences']
                    }

                    line = json.dumps(dict(write_dic)) + "\n"
                    self.file.write(line)

                    if self.db[self.event_collection].find({"title": adapter["title"]}).count() > 0:
                        # raise DropItem(f"Duplicate item found in the database: {item!r}")
                        self.db[self.event_collection].update({"title": adapter["title"]}, write_dic)
                    else:
                        self.db[self.event_collection].insert_one(write_dic)
                    return item
                return None

        elif spider.name == 'webinarbot':
            adapter = ItemAdapter(item)
            if adapter['link'] in self.ids_seen:
                raise DropItem(f"Duplicate item found: {item!r}")
            else:
                line = json.dumps(adapter.asdict()) + "\n"
                self.file.write(line)

                if self.db[self.webinar_collection].find({"link": adapter["link"]}).count() > 0:
                    # raise DropItem(f"Duplicate item found in the database: {item!r}")
                    self.db[self.webinar_collection].update({"link": adapter["link"]}, adapter.asdict())
                else:
                    self.db[self.webinar_collection].insert_one(adapter.asdict())
                self.ids_seen.add(adapter['link'])
                return item
            return None