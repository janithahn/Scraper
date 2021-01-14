import scrapy
from ..html import remove_tags, remove_tags_with_content, replace_entities, replace_escape_chars
from ..items import EventItem


class EventbotSpider(scrapy.Spider):
    name = 'eventbot'

    allowed_domains = [
        'sci.pdn.ac.lk',
        # 'www.fos.pdn.ac.lk',
        # 'sired.soc.pdn.ac.lk',
        # 'botsoc.soc.pdn.ac.lk',
        'csup.soc.pdn.ac.lk'
    ]
    start_urls = [
        'https://sci.pdn.ac.lk/',
        'https://csup.soc.pdn.ac.lk/',
        # 'https://sired.soc.pdn.ac.lk/',
        # 'https://botsoc.soc.pdn.ac.lk/',
        # 'https://www.fos.pdn.ac.lk/fosid/'
    ]

    custom_settings = {
        'DEPTH_LIMIT': 2,
    }

    item = EventItem()

    def parse(self, response):
        tag_selector = response.xpath('//a')
        for tag in tag_selector:
            link = tag.xpath('@href').extract_first()
            url = response.url

            if 'event' in str(link).lower() or 'news' in str(link).lower():
                response_text = response.text
                title = tag.xpath('//title/text()').extract()

                texts = remove_tags_with_content(response_text, which_ones=('script', 'header', 'style', 'styles', 'footer'))
                texts = replace_entities(texts)
                texts = remove_tags(texts)
                texts = replace_escape_chars(texts, replace_by=" ")

                self.item['title'] = title[0]
                self.item['link'] = url
                self.item['texts'] = texts
                yield self.item

            if link is not None and (str(link).find('download') == -1 or str(link).find('archive') == -1):
                yield response.follow(link, callback=self.parse)
