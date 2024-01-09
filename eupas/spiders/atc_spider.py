# NOT DEFAULT
# Define your spiders here
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spiders.html

import re

from scrapy import spiders, http, signals
from tqdm import tqdm


class ATC_Spider(spiders.Spider):
    '''
    This ATC Spider extracts atc data from the WHO.
    '''

    # Overriden Spider Settings
    # name is used to start a spider with the scrapy cmd crawl or runspider
    name = 'atc'
    # custom_settings contains own settings, but can also override the values in settings.py
    custom_settings = {
        'PROGRESS_LOGGING': False,
        'ITEM_PIPELINES': set(),
        'SPIDERMON_ENABLED': False,
        'ITEMHISTORYCOMPARER_ENABLED': False,
        'DEPTH_LIMIT': 4  # NOTE: Can be used to only extract atc codes up to a certain length
    }
    # These are the allowed domains. This spider should only follow urls in these domains
    allowed_domains = ['www.whocc.no']

    # URLS and headers
    base_url = 'https://www.whocc.no/atc_ddd_index/'
    query_url = 'https://www.whocc.no/atc_ddd_index/?code={}&showdescription=no'
    atc_regex = re.compile(r'code=(\S+)&')

    def __init__(self, progress_logging=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_settings.update({
            'PROGRESS_LOGGING': progress_logging
        })
        if self.custom_settings.get('PROGRESS_LOGGING'):
            self.pbar = tqdm(
                total=float('inf'),
                leave=False,
                desc='Scraping Progress',
                unit='ATC codes',
                colour='green',
            )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.idle, signals.spider_idle)
        return spider

    def start_requests(self):
        return [http.Request(self.base_url, cb_kwargs={'base_url_page': True})]

    start_print = False

    def parse(self, response: http.Response, base_url_page=False):
        if self.custom_settings.get('PROGRESS_LOGGING') and isinstance(self.pbar, tqdm):
            self.pbar.update()

        main = response.xpath('//*[@id="content"]')
        if base_url_page:
            main = main.xpath('./div/div')
        atc_links = main.xpath('./p/b//a')
        follow_links = True

        if not atc_links:
            atc_links = main.xpath('.//table//a')
            follow_links = False

        atc_codes = [
            self.atc_regex.search(x).group(1) for x in atc_links.xpath('./@href').getall()
        ]

        atc_values = [
            ''.join(link.xpath('.//text()').getall()) for link in atc_links
        ]

        assert len(atc_codes) == len(atc_values)

        for atc, value in zip(atc_codes, atc_values):
            yield {
                'atc_code': atc,
                'atc_value': value
            }

        if follow_links:
            next_requests = [
                http.Request(f'{self.base_url}{links[2:]}', dont_filter=True) for links in atc_links.xpath('./@href').getall()
            ]
            for request in next_requests:
                yield request

    def idle(self):
        if self.custom_settings.get('PROGRESS_LOGGING') and isinstance(self.pbar, tqdm):
            self.pbar.close()

    def closed(self, reason: str):
        if reason == 'finished':
            self.logger.info('Scraping finished successfully.')
        elif reason == 'shutdown':
            self.logger.info('Scraping was stopped by user.')
        else:
            self.logger.info(f'Scraping finished with reason: {reason}')
