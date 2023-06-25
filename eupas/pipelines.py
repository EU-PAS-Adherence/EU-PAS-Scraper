# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from difflib import SequenceMatcher
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


# TODO: Remove
class MetaFieldPipeline:
    # NOTE: The meta field chars shouldn't be the first letter of any actual field [a-z, A-Z]
    # Furthermore _ can be used but it will be disappear in excel because snake_case
    # will be converted to Capital Case
    meta_field_chars = '$/@'
    # Meta Field name has to begin with one of the meta_field_chars
    meta_field_name_prefix = '$MATCHED_'
    junk_chars = ' .,;\n\t'
    junk_words = frozenset([
        'pharmaceuticals', 'pharma',
        'inc', 'gmbh', 'ltd', 'limited', 'co', 'kg', 'spa', 'llc',
        'therapeutics',
    ])

    def __init__(self, enabled, fields_to_group):
        self.enabled = enabled
        self.fields_to_group = fields_to_group

    @classmethod
    def from_crawler(cls, crawler):
        enabled = crawler.settings.getbool('METAFIELD_ENABLED')
        fields_to_group = crawler.settings.getdictorlist(
            'METAFIELD_GROUP_SIMILAR_FIELDS_NAMES', default=[])
        return cls(enabled, fields_to_group)

    def open_spider(self, _: spiders.Spider):
        if not self.enabled:
            return

        self.groups = {}
        self.matcher = SequenceMatcher(isjunk=lambda x: x in self.junk_chars)

    def sub_junk_words(self, m):
        return '' if m.group() in self.junk_words else m.group()

    def filter_junk_words(self, s):
        return re.sub(r'\w+', self.sub_junk_words, s.lower())

    # TODO: Change or remove; Use adapter
    def process_item(self, item: item.Item, _: spiders.Spider):
        if not self.enabled:
            return item

        for field_name in self.fields_to_group:
            meta_field_name = f'{self.meta_field_name_prefix}{field_name.upper()}'
            item.fields.setdefault(meta_field_name, {})
            field_value = item.get(field_name)
            if not field_value:
                continue

            matched = False
            other_values = self.groups.setdefault(field_name, [])
            for other_field_value in other_values:
                self.matcher.set_seqs(self.filter_junk_words(
                    field_value), self.filter_junk_words(other_field_value))
                if self.matcher.ratio() > 0.65:
                    matched = True
                    item.setdefault(meta_field_name, other_field_value)
                    break

            if not matched:
                self.groups.get(field_name).append(field_value)
                item.setdefault(meta_field_name, field_value)

        return item
