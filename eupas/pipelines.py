# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import spiders, item, exceptions
from itemadapter.adapter import ItemAdapter

# NOTE: pipelines only work with one type of spider (EU_PAS_Spider)
# and item (Study) and it is assumed that there is only one type of each!


class DuplicatesPipeline:

    def open_spider(self, _: spiders.Spider):
        self.ids_seen = set()

    def process_item(self, item: item.Item, _: spiders.Spider):
        adapter = ItemAdapter(item)
        if (id := adapter.get('eu_pas_register_number')) in self.ids_seen:
            raise exceptions.DropItem(f'Duplicate item found: {item!r}')

        self.ids_seen.add(id)
        return item
