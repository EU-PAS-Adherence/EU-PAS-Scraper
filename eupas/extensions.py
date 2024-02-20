from scrapy import signals
from scrapy.exceptions import NotConfigured, NotSupported
from scrapy.exporters import BaseItemExporter
from scrapy.utils.serialize import ScrapyJSONEncoder

import json
from pathlib import Path


class SingleJsonItemStringExporter(BaseItemExporter):
    def __init__(self, **kwargs):
        super().__init__(dont_fail=True, **kwargs)
        self._kwargs.setdefault("indent", 0)
        self.encoder = ScrapyJSONEncoder(**self._kwargs)

    def export_item(self, item):
        itemdict = dict(self._get_serialized_fields(item))
        return self.encoder.encode(itemdict)


class ItemHistoryComparer:
    changed_date_key = '$CHANGED_HAS_NEW_DATE'
    changed_eupas_key = '$CHANGED_EUPAS'
    changed_url_key = '$CHANGED_URL'
    deleted_fields_key = '$DELETED_FIELDS'
    duplicate_fields_key = '$DUPLICATE_SEARCH_ENTRY'
    only_excepted_fields_key = '$ONLY_EXCEPTED_FIELDS_CHANGED'

    # NOTE: If the meta field pipeline is used: all meta field names get ignored
    def __init__(self, file_path_dict, output_path, excepted_fields_dict, duplicate_excepted_fields_dict, crawler):
        self.exporter = SingleJsonItemStringExporter()
        self.updates = []
        self.studies = {}
        self.excepted_fields = set()
        self.duplicate_excepted_fields = set()
        self.file_path_dict = file_path_dict
        self.output_path = output_path
        self.excepted_fields_dict = excepted_fields_dict
        self.duplicate_excepted_fields_dict = duplicate_excepted_fields_dict
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        # first check if the extension should be enabled and raise
        # NotConfigured otherwise
        if not crawler.settings.getbool('ITEMHISTORYCOMPARER_ENABLED'):
            raise NotConfigured

        file_path_dict = crawler.settings.get(
            'ITEMHISTORYCOMPARER_JSON_INPUT_PATH')
        output_path = crawler.settings.get(
            'ITEMHISTORYCOMPARER_JSON_OUTPUT_PATH')
        excepted_fields_dict = crawler.settings.get(
            'ITEMHISTORYCOMPARER_EXCEPTED_FIELDS')
        duplicate_excepted_fields_dict = crawler.settings.get(
            'ITEMHISTORYCOMPARER_DUPLICATE_EXCEPTED_FIELDS')

        # instantiate the extension object
        ext = cls(file_path_dict, output_path, excepted_fields_dict,
                  duplicate_excepted_fields_dict, crawler)

        # connect the extension object to signals
        crawler.signals.connect(
            ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)

        return ext

    def spider_opened(self, spider):
        item_class = getattr(spider, 'item_class', None)
        if file_path := self.file_path_dict.get(item_class):
            with open(file_path, 'rt') as f:
                self.studies = json.load(f)

        if excepted := self.excepted_fields_dict.get(item_class):
            self.excepted_fields = excepted

        if duplicate_excepted := self.duplicate_excepted_fields_dict.get(item_class):
            self.duplicate_excepted_fields = duplicate_excepted

    def spider_idle(self, spider):
        if self.updates:
            path = Path(self.output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('w', encoding='UTF-8') as f:
                json.dump(sorted(
                    self.updates, key=lambda x: x[self.changed_date_key]), f, indent='\t', sort_keys=True)

    def tuplify(self, item):
        return map(lambda x: (x[0], tuple(x[1]) if isinstance(x[1], list) else x[1]), item.items())

    def item_scraped(self, item, spider):
        new_entry = json.loads(self.exporter.export_item(item))

        old_entries = list(filter(
            lambda x: x['eu_pas_register_number'] == new_entry['eu_pas_register_number'], self.studies))

        if not old_entries:
            self.crawler.stats.inc_value(
                'item_history_comparer/item_with_new_register_number_count')
            self.crawler.stats.inc_value(
                f'item_history_comparer/item_with_new_register_number_count/eupas_{new_entry["eu_pas_register_number"]}')
            return
        elif len(old_entries) > 1:
            raise NotSupported

        old_entry = old_entries[0]

        duplicate = self.crawler.stats.get_value(
            f'dupefilter/filtered/search_entries/eupas_{new_entry["eu_pas_register_number"]}', 0) > 0

        difference = frozenset(self.tuplify(new_entry)) - \
            frozenset(self.tuplify(old_entry))
        updated_fields = {d[0] for d in difference}
        deleted_fields = list(frozenset(old_entry.keys()) -
                              frozenset(new_entry.keys()))

        changes_dict = dict(difference)
        only_excepted_fields = False

        if updated_fields or deleted_fields:
            if 'update_date' in changes_dict:
                self.crawler.stats.inc_value(
                    'item_history_comparer/updated_item_with_changed_date_count')
                changes_dict.setdefault(self.changed_date_key, True)
            elif 'update_date' in deleted_fields:
                self.crawler.stats.inc_value(
                    'item_history_comparer/updated_item_with_deleted_date_count')
                changes_dict.setdefault(self.changed_date_key, None)
            else:
                self.crawler.stats.inc_value(
                    'item_history_comparer/updated_item_without_changed_date_count')
                if updated_fields.union(set(deleted_fields)).issubset(self.excepted_fields):
                    only_excepted_fields = True
                    self.crawler.stats.inc_value(
                        'item_history_comparer/updated_item_without_changed_date_count/only_excepted_fields')
                if duplicate and updated_fields.issubset(self.duplicate_excepted_fields):
                    self.crawler.stats.inc_value(
                        'item_history_comparer/updated_item_without_changed_date_count/duplicate_related')
                changes_dict.setdefault(self.changed_date_key, False)

            changes_dict.setdefault(
                self.only_excepted_fields_key, only_excepted_fields)
            changes_dict.setdefault(self.duplicate_fields_key, duplicate)
            changes_dict.setdefault(
                self.changed_eupas_key, new_entry["eu_pas_register_number"])
            changes_dict.setdefault(self.changed_url_key, new_entry["url"])
            changes_dict.setdefault(
                self.deleted_fields_key, deleted_fields or None)
            self.updates.append(changes_dict)
