from encepp.spiders.encepp_spider import EU_PAS_Extractor

from scrapy import Request, Spider
from scrapy.dupefilters import RFPDupeFilter

class EupasDupeFilter(RFPDupeFilter):

    def log(self, request: Request, spider: Spider) -> None:
        if isinstance(spider, EU_PAS_Extractor):
            if id := request.meta.get('eupas_id'):
                spider.crawler.stats.inc_value(
                    f'dupefilter/filtered/search_entries/eupas_{id}')

        return super().log(request, spider)
