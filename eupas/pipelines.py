# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import spiders, item, exceptions
from itemadapter.adapter import ItemAdapter

# NOTE: pipelines only work with one type of spider (EU_PAS_Spider/EMA_RWD_Spider)
# and item (Study) and it is assumed that there is only one type of each!


class DuplicatesPipeline:
    '''
    A Pipeline which detects duplicates using the EU PAS Register ID.
    '''

    def open_spider(self, _: spiders.Spider):
        self.ids_seen = set()

    def process_item(self, item: item.Item, _: spiders.Spider):
        adapter = ItemAdapter(item)
        if (eupas_id := adapter.get('eu_pas_register_number')) in self.ids_seen:
            raise exceptions.DropItem(f'Duplicate item found: {item!r}')

        self.ids_seen.add(eupas_id)
        return item
