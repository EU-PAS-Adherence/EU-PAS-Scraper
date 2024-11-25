from eupas.spiders.eupas_spider import EU_PAS_Spider

from scrapy import Request, Spider
from scrapy.dupefilters import RFPDupeFilter


class EupasDupeFilter(RFPDupeFilter):
    # TODO: Adapt to EMA_RWD_Spider

    def log(self, request: Request, spider: Spider) -> None:
        if isinstance(spider, EU_PAS_Spider):
            if eupas_id := request.meta.get('eupas_id'):
                spider.crawler.stats.inc_value(
                    f'dupefilter/filtered/search_entries/eupas_{eupas_id}')

        return super().log(request, spider)
