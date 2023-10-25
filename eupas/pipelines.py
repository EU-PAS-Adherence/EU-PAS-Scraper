# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import re

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


# TODO: Remove this pipeline and use patching instead
class MetaFieldPipeline:
    # NOTE: The meta field chars shouldn't be the first letter of any actual field [a-z, A-Z]
    # Furthermore _ can be used but it will be disappear in excel because snake_case
    # will be converted to Capital Case
    meta_field_chars = '$/@'
    # Meta Field names have to begin with one of the meta_field_chars
    matched_meta_field_name_prefix = '$MATCHED_'
    study_cancelled_meta_field_name = '$CANCELLED'
    study_cancelled_patterns = [
        r'\bcancel',
        r'\bdiscontinu',
        r'\bterminat',
        r'\bhalt',
        r'\bnot\s+complet',
        r'\bsuspend',
        r'\bwithdr(a|e)w'
    ]

    def is_study_cancelled(self, description):
        return any(
            re.search(pattern, description.lower())
            for pattern in self.study_cancelled_patterns
        )

    def __init__(self, enabled, fields_to_match):
        self.enabled = enabled
        self.fields_to_match = fields_to_match

    @classmethod
    def from_crawler(cls, crawler):
        enabled = crawler.settings.getbool('METAFIELD_ENABLED')
        fields_to_match = crawler.settings.getdictorlist(
            'METAFIELD_MATCH_FIELDS_NAMES', default=[])
        return cls(enabled, fields_to_match)

    def open_spider(self, _: spiders.Spider):
        if not self.enabled:
            return

    def process_item(self, item: item.Item, _: spiders.Spider):
        if not self.enabled:
            return item

        adapter = ItemAdapter(item)
        adapter.item.fields.setdefault(
            self.study_cancelled_meta_field_name, {})
        if description := adapter.get('description'):
            adapter.setdefault(self.study_cancelled_meta_field_name,
                               'Yes' if self.is_study_cancelled(description) else 'No')
        else:
            adapter.setdefault(self.study_cancelled_meta_field_name, 'No description')
        return item
