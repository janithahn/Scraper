import scrapy


class WebinarItem(scrapy.Item):
    link = scrapy.Field()
    parent = scrapy.Field()