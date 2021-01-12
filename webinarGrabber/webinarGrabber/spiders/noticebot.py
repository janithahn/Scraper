import scrapy
from scrapy_splash import SplashRequest
import base64


class NoticebotSpider(scrapy.Spider):
    name = 'noticebot'
    allowed_domains = ['sites.google.com']
    start_urls = ['https://sites.google.com/sci.pdn.ac.lk/notices/']

    '''custom_settings = {
        'DEPTH_LIMIT': 2,
    }'''

    def start_requests(self):
        for url in self.start_urls:
            yield SplashRequest(url, self.parse, args={'wait': 0.5})

    '''def parse(self, response):

    # response.body is a result of render.html call; it
    # contains HTML processed by a browser.
    # ...'''

    def parse(self, response):
        print('page_body', response.body)

        # magic responses are turned ON by default,
        # so the result under 'html' key is available as response.body
        html = response.body

        with open('notice_response.txt', 'a', encoding='utf-8') as file:
            file.write(str(html))

        # you can also query the html result as usual
        title = response.css('div').extract_first()
        yield title

        # full decoded JSON data is available as response.data:
        png_bytes = base64.b64decode(response.data['png'])

        '''tag_selector = response.xpath('//a')
        for tag in tag_selector:
            link = tag.xpath('@href').extract_first()
            with open('cards.txt', 'a', encoding='utf-8') as file:
                file.write(link.strip() + '\n')
            if link is not None:
                yield response.follow(link, callback=self.parse)'''
