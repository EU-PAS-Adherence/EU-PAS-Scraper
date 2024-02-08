# NOT DEFAULT
# Define your spiders here
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spiders.html

import re

from scrapy import spiders, http, signals
from tqdm import tqdm


class KEGG_Drug_Spider(spiders.Spider):
    '''
    This KEGG Spider extracts drug data from the KEGG Rest Api.
    '''

    # Overriden Spider Settings
    # name is used to start a spider with the scrapy cmd crawl or runspider
    name = 'kegg'
    # custom_settings contains own settings, but can also override the values in settings.py
    custom_settings = {
        'PROGRESS_LOGGING': False,
        'ITEM_PIPELINES': set(),
        'SPIDERMON_ENABLED': False,
        'ITEMHISTORYCOMPARER_ENABLED': False,
        'DEPTH_LIMIT': 1
    }
    # These are the allowed domains. This spider should only follow urls in these domains
    allowed_domains = ['rest.kegg.jp']

    # URLS and headers
    base_url = 'https://rest.kegg.jp'
    query_url = f'{base_url}/get'

    # NOTE: start_urls will be generated and overriden by data from the patch script.
    #       If you don't run the patch script, you can specify the list like below.
    #       Alternativly you can override the start_requests method, which uses this variable by default.
    start_urls = ['https://rest.kegg.jp/get/D06409']

    def __init__(self, drug_ids=[], progress_logging=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if drug_ids:
            self.start_urls = [
                f'{self.query_url}/{drug_id}' for drug_id in drug_ids]
        self.custom_settings.update({
            'PROGRESS_LOGGING': progress_logging
        })
        if self.custom_settings.get('PROGRESS_LOGGING'):
            self.pbar = tqdm(
                total=len(drug_ids),
                leave=False,
                desc='Scraping Progress',
                unit='drugs',
                colour='green',
            )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.idle, signals.spider_idle)
        return spider

    def parse(self, response: http.TextResponse):
        '''
        Parses all responses and extracts atc_code and atc_value as well as KEGG ID and url from the txt-Response.
        '''
        if self.custom_settings.get('PROGRESS_LOGGING') and isinstance(self.pbar, tqdm):
            self.pbar.update()

        # NOTE: Hard-coded magic value: This is the string length of the first "table" column
        data = [[row[:12].strip(), row[12:]]
                for row in response.text.split('\n')]

        entry_id = data[0][1].split(' ')[0]

        first_col = list(map(lambda x: x[0], data))
        headers = list(filter(lambda x: x, first_col))

        # NOTE: A substance can have multiple ATCs!
        atc = None
        try:
            start = 'BRITE'
            start_index = first_col.index(start)
            stop = headers[headers.index(start) + 1]
            stop_index = first_col.index(stop)

            brite = '\n'.join([row[1] for row in data[start_index:stop_index]])
            atc_match = re.match(
                r'Anatomical Therapeutic Chemical.*(?:\n\s.*)+', brite)
            if atc_match:
                atc = re.findall(
                    f'(\\S+) (.+)\\n.*{re.escape(entry_id)}', atc_match.group(0))
        except ValueError:
            pass

        return {
            'kegg_drug_entry_id': entry_id,
            'atc_code': '; '.join(x[0] for x in atc) if atc else atc,
            'atc_value': '; '.join(x[1] for x in atc) if atc else atc,
            'kegg_details_url': response.url
        }

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
