from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.exporters import BaseItemExporter
from scrapy.utils.serialize import ScrapyJSONEncoder

import json

from encepp.pipelines import MetaFieldPipeline


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

    # TODO: if meta field pipeline is used: Ignore meta field names
    # which may start with the letter $ or _

    def __init__(self, file_path, output_path, crawler, has_meta_fields=False):
        self.exporter = SingleJsonItemStringExporter()
        with open(file_path, 'r') as f:
            self.studies = json.load(f)
        self.updates = []
        self.output_path = output_path
        self.crawler = crawler
        self.has_meta_fields = has_meta_fields

    @classmethod
    def from_crawler(cls, crawler):
        # first check if the extension should be enabled and raise
        # NotConfigured otherwise
        if not crawler.settings.getbool('ITEMHISTORYCOMPARER_ENABLED'):
            raise NotConfigured

        file_path = crawler.settings.get('ITEMHISTORYCOMPARER_JSON_INPUT_PATH')
        output_path = crawler.settings.get(
            'ITEMHISTORYCOMPARER_JSON_OUTPUT_PATH')
        meta_fields = crawler.settings.getbool('METAFIELD_ENABLED')

        # instantiate the extension object
        ext = cls(file_path, output_path, crawler, has_meta_fields=meta_fields)

        # connect the extension object to signals
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)

        return ext

    def spider_idle(self, spider):
        if self.updates:
            with open(self.output_path, 'w') as f:
                json.dump(sorted(
                    self.updates, key=lambda x: not x[self.changed_date_key]), f, indent='\t', sort_keys=True)

    def tuplify(self, item):
        return map(lambda x: (x[0], tuple(x[1]) if isinstance(x[1], list) else x[1]), item.items())

    def item_scraped(self, item, spider):
        new_study = json.loads(self.exporter.export_item(item))
        if self.has_meta_fields:
            new_study = dict(filter(
                lambda item: item[0][0] not in MetaFieldPipeline.meta_field_chars, new_study.items()))
        old_studies = list(filter(
            lambda x: x['eu_pas_register_number'] == new_study['eu_pas_register_number'], self.studies))
        if not old_studies:
            self.crawler.stats.inc_value(
                'item_history_comparer/item_with_new_register_number_count')
            self.crawler.stats.inc_value(
                f'item_history_comparer/item_with_new_register_number_count/eupas_{new_study["eu_pas_register_number"]}')
            return

        old_study = old_studies[0]

        difference = frozenset(self.tuplify(new_study)) - \
            frozenset(self.tuplify(old_study))
        deleted = list(frozenset(old_study.keys()) -
                       frozenset(new_study.keys()))
        updates_dict = dict(difference)

        # TODO: Doesnt keep track of deleted attributes
        if difference or deleted:
            if 'update_date' in updates_dict:
                self.crawler.stats.inc_value(
                    'item_history_comparer/updated_item_with_changed_date_count')
                updates_dict.setdefault(self.changed_date_key, True)
            elif 'update_date' in deleted:
                self.crawler.stats.inc_value(
                    'item_history_comparer/updated_item_with_deleted_date_count')
                updates_dict.setdefault(self.changed_date_key, None)
            else:
                self.crawler.stats.inc_value(
                    'item_history_comparer/updated_item_without_changed_date_count')
                updates_dict.setdefault(self.changed_date_key, False)

            updates_dict.setdefault(
                self.changed_eupas_key, new_study["eu_pas_register_number"])
            updates_dict.setdefault(self.changed_url_key, new_study["url"])
            updates_dict.setdefault(self.deleted_fields_key, deleted or None)
            self.updates.append(updates_dict)
