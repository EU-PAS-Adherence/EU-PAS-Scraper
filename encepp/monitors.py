# Define here the monitors for the spider
#
# See documentation in:
# https://spidermon.readthedocs.io/en/latest/monitors.html

from spidermon import Monitor, MonitorSuite, monitors

from spidermon.contrib.scrapy.monitors import ItemCountMonitor, ItemValidationMonitor
# from spidermon.contrib.scrapy.monitors import FieldCoverageMonitor
from spidermon.contrib.scrapy.monitors import WarningCountMonitor, ErrorCountMonitor, CriticalCountMonitor
from spidermon.contrib.scrapy.monitors import UnwantedHTTPCodesMonitor, RetryCountMonitor, DownloaderExceptionMonitor
from spidermon.contrib.scrapy.monitors import FinishReasonMonitor

from spidermon.contrib.actions.reports.files import CreateFileReport


@monitors.name('Expected Item count')
class ExpectedCountMonitor(Monitor):

    @monitors.name('Expected vs. Extracted number of items')
    def test_extracted_number_of_items_equals_expected(self):
        item_extracted = getattr(
            self.data.stats, 'item_scraped_count', 0)
        duplicates = getattr(
            self.data.stats, 'dupefilter/filtered', 0)
        item_expected = getattr(
            self.data.stats, 'item_expected_count', 1) - duplicates

        msg = f'Extracted {item_extracted} item(s), but expected {item_expected} item(s)'
        self.assertTrue(item_extracted == item_expected, msg=msg)


@monitors.name('Expected Item Updates')
class UpdatedItemsMonitor(Monitor):

    @monitors.name("Expected item updates without date changes or deletes don't exceed treshold")
    def test_extracted_number_of_items_equals_expected(self):
        item_updates = getattr(
            self.data.stats, 'item_history_comparer/updated_item_without_changed_date_count', 0)
        item_date_deletes = getattr(
            self.data.stats, 'item_history_comparer/updated_item_with_deleted_date_count', 0)
        item_updates_expected = self.data.crawler.settings.get(
            'SPIDERMON_MAX_ITEM_UPDATES_WITHOUT_DATE_CHANGES_OR_DATE_DELETES')

        msg = f'Found {item_updates} item(s) without date changes and {item_date_deletes} item(s) with deleted dates, but expected {item_updates_expected} item(s)'
        self.assertTrue(item_updates + item_date_deletes <=
                        item_updates_expected, msg=msg)


@monitors.name('Expected Response count')
class ExpectedResponsesMonitor(Monitor):

    @monitors.name('Expected vs. Actual number of Responses')
    def test_actual_number_of_responses_equals_expected(self):
        actual_requests = getattr(
            self.data.stats, 'response_received_count', 0)
        duplicates = getattr(
            self.data.stats, 'dupefilter/filtered', 0)
        expected_study_detail_requests = getattr(
            self.data.stats, 'item_expected_count', 1) - duplicates
        expected_requests = 1 + expected_study_detail_requests * \
            (2 if self.data.spider.custom_settings.get('SAVE_PDF') else 1)

        msg = f'{actual_requests} Response(s) received, but expected {expected_requests} Response(s)'
        self.assertTrue(actual_requests == expected_requests, msg=msg)


class SpiderCloseMonitorSuite(MonitorSuite):

    name = 'Encepp Checker'

    log_monitors = [
        WarningCountMonitor,
        ErrorCountMonitor,
        CriticalCountMonitor,
    ]

    item_monitors = [
        ExpectedCountMonitor,
        ItemCountMonitor,
        ItemValidationMonitor,  # Can also handle required fields: 100% coverage
        # FieldCoverageMonitor, Only needed for fields with less than 100% coverage
        UpdatedItemsMonitor,
    ]

    http_monitors = [
        ExpectedResponsesMonitor,
        UnwantedHTTPCodesMonitor,
        RetryCountMonitor,
        DownloaderExceptionMonitor,
    ]

    other_monitors = [
        FinishReasonMonitor,
    ]

    monitors = log_monitors + item_monitors + http_monitors + other_monitors

    monitors_finished_actions = [
        CreateFileReport,
    ]
