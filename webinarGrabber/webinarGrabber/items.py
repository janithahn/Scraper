# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class WebinarItem(scrapy.Item):
    link = scrapy.Field()
    texts = scrapy.Field()


class EventItem(scrapy.Item):
    title = scrapy.Field()
    link = scrapy.Field()
    texts = scrapy.Field()
    sentences = scrapy.Field()
