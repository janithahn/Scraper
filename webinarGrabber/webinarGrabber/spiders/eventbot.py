import scrapy
import spacy
from ..html import remove_tags, remove_tags_with_content, replace_entities, replace_escape_chars
from ..items import EventItem


class EventbotSpider(scrapy.Spider):
    name = 'eventbot'

    allowed_domains = [
        # 'sci.pdn.ac.lk',
        # 'www.fos.pdn.ac.lk',
        # 'sired.soc.pdn.ac.lk',
        # 'botsoc.soc.pdn.ac.lk',
        'csup.soc.pdn.ac.lk'
    ]
    start_urls = [
        # 'https://sci.pdn.ac.lk/',
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

            if str(link).find('event') != -1:
                title = tag.xpath('//title/text()').extract()
                response_text = response.text
                texts = remove_tags_with_content(response_text, which_ones=('script', 'header', 'style', 'styles', 'footer'))
                texts = replace_escape_chars(texts, replace_by=" ")
                texts = remove_tags(texts)
                nlp = spacy.load("en_core_web_sm")

                self.item['title'] = title[0]
                self.item['texts'] = texts
                yield self.item

                '''with open('eventbot.txt', 'a', encoding='utf-8') as file:
                    file.write(str(tag.xpath('//title/text()').extract()) + '\n' +
                               str(texts) + '\n\n\n-------------------------------------------\n\n\n')'''

                '''for doc in nlp.pipe(texts, disable=["tagger", "parser"]):
                    with open('eventbot.txt', 'a', encoding='utf-8') as file:
                        # file.write(str(tag.xpath('//title/text()').extract()) + '\n' + str([(ent.text, ent.label_) for ent in doc.ents]) + '\n\n\n-------------------------------------------\n\n\n')
                        file.write(str(tag.xpath('//title/text()').extract()) + '\n' +
                            str(texts) + '\n\n\n-------------------------------------------\n\n\n')'''

            if link is not None and (str(link).find('download') == -1 or str(link).find('archive') == -1):
                yield response.follow(link, callback=self.parse)
