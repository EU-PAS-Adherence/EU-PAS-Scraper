# Scrapy settings for eupas project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from datetime import datetime as dt
from datetime import timezone
import random

##################################
#      NON SCRAPY VARIABLES      #
##################################
# This value is used as a dynamic version number in pyproject.toml
PACKAGE_VERSION = '0.0.1'

# Custom output folder
# The Output Directory stores all feed exports,
# the spidermon report and updates.json
OUTPUT_DIRECTORY = (
    f"./output/{dt.now(timezone.utc).strftime('%Y_%m_%d_T%H_%M_%S')}"
)
##################################

# All of the variables listed (except extension settings) below are documented here:
# https://docs.scrapy.org/en/latest/topics/settings.html#built-in-settings-reference

BOT_NAME = None

SPIDER_MODULES = ['eupas.spiders']
NEWSPIDER_MODULE = 'eupas.spiders'

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
TWISTED_REACTOR = 'twisted.internet.asyncioreactor.AsyncioSelectorReactor'

##################################
#           USER AGENT           #
##################################
# NOTE: Everytime settings.py is loaded, USER_AGENT changes.
# This isn't a bad thing, but keep in mind that you should use fixed values or
# functions (without sideeffects) returning fixed values if you can.


def random_ua():
    # Code provided by https://www.useragents.me/
    # returns a random useragent from the latest user agents strings list, weigted
    # according to observed prevalence
    ua_pct = {'ua': {'0': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', '1': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.42', '2': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0', '3': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.52', '4': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15', '5': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', '6': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36', '7': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:107.0) Gecko/20100101 Firefox/107.0', '8': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:108.0) Gecko/20100101 Firefox/108.0', '9': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36 Edg/108.0.0.0', '10': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15', '11': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.35', '12': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36', '13': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', '14': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0', '15': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', '16': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', '17': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36 Edg/102.0.1245.33', '18': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', '19': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36', '20': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36', '21': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 OPR/92.0.0.0', '22': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36', '23': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0', '24': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36 Edg/106.0.1370.52', '25': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 YaBrowser/22.11.0.2419 Yowser/2.5 Safari/537.36', '26': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 OPR/93.0.0.0',
                     '27': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko', '28': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36 Edg/102.0.1245.39', '29': 'Mozilla/5.0 (X11; Linux x86_64; rv:107.0) Gecko/20100101 Firefox/107.0', '30': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36', '31': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.26', '32': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 YaBrowser/22.11.0.2424 Yowser/2.5 Safari/537.36', '33': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 OPR/91.0.4516.106', '34': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36 Edg/104.0.1293.63', '35': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36', '36': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.63 Safari/537.36', '37': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 OPR/91.0.4516.106 (Edition GX-CN)', '38': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36', '39': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0', '40': 'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko', '41': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.81 Safari/537.36 Edg/104.0.1293.54', '42': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0', '43': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/108.0', '44': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36', '45': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18362'}, 'pct': {'0': 34.4623200677, '1': 14.2252328535, '2': 7.7900084674, '3': 5.9271803556, '4': 4.5159469376, '5': 4.5159469376, '6': 3.6127575501, '7': 1.806378775, '8': 1.806378775, '9': 1.806378775, '10': 1.806378775, '11': 1.6934801016, '12': 1.4676827547, '13': 1.3547840813, '14': 1.3547840813, '15': 0.9031893875, '16': 0.9031893875, '17': 0.7902907141, '18': 0.7902907141, '19': 0.6773920406, '20': 0.6773920406, '21': 0.6773920406, '22': 0.4515946938, '23': 0.4515946938, '24': 0.3386960203, '25': 0.3386960203, '26': 0.3386960203, '27': 0.3386960203, '28': 0.3386960203, '29': 0.2257973469, '30': 0.2257973469, '31': 0.2257973469, '32': 0.2257973469, '33': 0.2257973469, '34': 0.2257973469, '35': 0.2257973469, '36': 0.2257973469, '37': 0.2257973469, '38': 0.2257973469, '39': 0.2257973469, '40': 0.2257973469, '41': 0.2257973469, '42': 0.2257973469, '43': 0.2257973469, '44': 0.2257973469, '45': 0.2257973469}}
    return random.choices(list(ua_pct['ua'].values()), list(ua_pct['pct'].values()))[0]


# Crawl responsibly by identifying yourself on the user-agent
USER_AGENT = random_ua()
##################################

##################################
#              PROXY             #
##################################
# TODO: This is working right now, but ip rotations might be needed later.
##################################

##################################
#       REQUESTS SETTINGS        #
##################################
# Also check AUTOTHROTTLE further below

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 16

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 0
# If enabled, Scrapy will wait a random amount of time
# between 0.5 * DOWNLOAD_DELAY and 1.5 * DOWNLOAD_DELAY
# while fetching requests from the same website (enabled by default)
RANDOMIZE_DOWNLOAD_DELAY = True
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 12
# CONCURRENT_REQUESTS_PER_IP = 16
##################################

##################################
#   SCRAPY COMPONENTS SETTINGS   #
##################################
# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    'scrapy.spidermiddlewares.urllength.UrlLengthMiddleware': None,
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.downloadtimeout.DownloadTimeoutMiddleware': None,
    'scrapy.downloadermiddlewares.httpauth.HttpAuthMiddleware': None,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.downloadermiddlewares.ajaxcrawl.AjaxCrawlMiddleware': None,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': None,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    'scrapy.extensions.logstats.LogStats': None,
    'scrapy.extensions.telnet.TelnetConsole': None,
    'scrapy.extensions.memusage.MemoryUsage': None,
    'scrapy.extensions.memdebug.MemoryDebugger': None,
    'scrapy.extensions.statsmailer.Statsmailer': None,
    'eupas.extensions.ItemHistoryComparer': 300,
    'spidermon.contrib.scrapy.extensions.Spidermon': 500,
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'eupas.pipelines.DuplicatesPipeline': 0,
    'spidermon.contrib.scrapy.pipelines.ItemValidationPipeline': 800,
}
##################################

##################################
#       SPIDER MIDDLEWARE        #
##################################
DEPTH_LIMIT = 2

REFERRER_ENABLED = True
##################################

##################################
#     DOWNLOADER MIDDLEWARE      #
##################################
# Disable cookies (enabled by default)
COOKIES_ENABLED = True
COOKIES_DEBUG = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en',
    'Accept-Encoding': 'gzip, deflate, br',
}

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
HTTPCACHE_ENABLED = False
HTTPCACHE_EXPIRATION_SECS = 12 * 60 * 60
HTTPCACHE_DIR = 'HttpCache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

REDIRECT_ENABLED = True
REDIRECT_MAX_TIMES = 5

METAREFRESH_ENABLED = False

# These settings tell the engine how often it should retry failed request
# Scrapy will only retry the failed requests which return the response codes defined in RETRY_HTTP_CODES
RETRY_ENABLED = True
RETRY_TIMES = 2
RETRY_HTTP_CODES = [500, 502, 503, 504, 522, 524, 400, 408, 429]

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

DOWNLOADER_STATS = True
##################################

##################################
#           EXTENSIONS           #
##################################

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 1.0
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 10.0
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# CloseSpider Extension
CLOSESPIDER_TIMEOUT = 2 * 60 * 60
CLOSESPIDER_TIMEOUT_NO_ITEM = 30 * 60
CLOSESPIDER_ERRORCOUNT = 5

# Enable and configure Spidermon Extension (https://spidermon.readthedocs.io)
SPIDERMON_ENABLED = True
SPIDERMON_VALIDATION_SCHEMAS = ['eupas/validators/study_schema.json']

# Settings for log_monitors
SPIDERMON_MAX_WARNINGS = 10
SPIDERMON_MAX_ERRORS = 0
SPIDERMON_MAX_CRITICALS = 0

# Settings for item_monitors
SPIDERMON_MIN_ITEMS = 1
SPIDERMON_FIELD_COVERAGE_RULES = {}
SPIDERMON_MAX_ITEM_VALIDATION_ERRORS = 0
# Settings for FieldCoverageMonitor
# SPIDERMON_ADD_FIELD_COVERAGE = True
# SPIDERMON_FIELD_COVERAGE_SKIP_NONE = True
# for field in ['url', 'eu_pas_register_number', 'state', 'title', 'update_date', 'registration_date']:
#   SPIDERMON_FIELD_COVERAGE_RULES.setdefault(f'Study/{field}', 1.0)
# TODO: Find a better way to fix the problem
SPIDERMON_MAX_ITEM_UPDATES_WITHOUT_DATE_CHANGES_OR_DATE_DELETES = 0

# Settings for http_monitors
SPIDERMON_UNWANTED_HTTP_CODES = {
    400: 0,
    401: 0,
    403: 0,
    404: 0,
    406: 10,
    407: 0,
    410: 0,
    429: 50,
    500: 3,
    501: 0,
    502: 10,
    503: 10,
    504: 10,
    523: 0
}
SPIDERMON_MAX_RETRIES = 0
SPIDERMON_MAX_DOWNLOADER_EXCEPTIONS = 0

# Settings for other_monitors
SPIDERMON_EXPECTED_FINISH_REASONS = ['finished']

SPIDERMON_SPIDER_CLOSE_MONITORS = (
    # Monitors log warnings, errors, criticals
    # Monitors different item related tests
    # Monitors request related tests
    # Monitors other tests
    'eupas.monitors.SpiderCloseMonitorSuite'
)

SPIDERMON_REPORT_TEMPLATE = 'reports/email/monitors/result.jinja'
SPIDERMON_REPORT_CONTEXT = {
    'report_title': 'Eupas Spider File Report'
}
SPIDERMON_REPORT_FILENAME = f'{OUTPUT_DIRECTORY}/report.html'

# Custom ITEMCOMPARER Extension
ITEMHISTORYCOMPARER_ENABLED = True
ITEMHISTORYCOMPARER_JSON_INPUT_PATH = 'compare.json'
ITEMHISTORYCOMPARER_JSON_OUTPUT_PATH = f'{OUTPUT_DIRECTORY}/updates.json'
##################################

##################################
#      Other Scrapy Overrides    #
##################################

# These settings will change the logging behavior
LOG_ENABLED = True
# LOG_FILE = None
# LOG_FILE_APPEND = False
# LOG_FORMAT = '[%(asctime)s] %(levelname)s: %(message)s'
LOG_FORMAT = '%(levelname)s: %(message)s'
LOG_LEVEL = 'INFO'

# Here we can define custom scrapy contracts for unit testing (not so great)
# Can be checked with the following command: scrapy check eupas
SPIDER_CONTRACTS = {
    'eupas.contracts.PostEncodedContract': 1,
}

# Module with custom commands
# See: https://docs.scrapy.org/en/latest/topics/commands.html#custom-project-commands
COMMANDS_MODULE = 'eupas.commands'
##################################

##################################
#         FEED SETTINGS          #
##################################


def get_item_name(base='items', moreDetails=True):
    # Using a function, because else the settings will be polluted by a custom setting
    return f'{base}_%(filter)s_p%(batch_id)d_%(time)s' if moreDetails else f'{base}_%(filter)s'


# These settings configure the exporter
# See https://docs.scrapy.org/en/latest/topics/feed-exports.html#feeds for details
FEEDS = {
    f'{OUTPUT_DIRECTORY}/{get_item_name()}.xlsx': {
        'format': 'xlsx',
        'overwrite': True,
        'item_export_kwargs': {
            'include_header_row': True,
            'include_counter_column': True,
            'join_multivalued': '; ',
            'default_value': '',
            'sheet_name': 'PAS',
            'date_format': '%Y-%m-%d',
            'datetime_format': '%Y-%m-%d %H:%M:%S',
        },
    },
    f'{OUTPUT_DIRECTORY}/{get_item_name()}.json': {
        'format': 'json',
        'overwrite': True,
    },
    f'{OUTPUT_DIRECTORY}/{get_item_name()}.xml': {
        'format': 'xml',
        'overwrite': True,
        'item_export_kwargs': {
            'root_element': 'studies',
            'item_element': 'study',
        }
    },
    f'{OUTPUT_DIRECTORY}/{get_item_name()}.csv': {
        'format': 'csv',
        'overwrite': True,
        'item_export_kwargs': {
            'include_headers_line': True,
            'join_multivalued': '; ',
        }
    },
    f'{OUTPUT_DIRECTORY}/data.db': {
        'format': 'sqlite3',
        'overwrite': True,
        'item_export_kwargs': {
            'join_multivalued': '; ',
            'default_value': None,
            'db_name': 'study',
            'date_format': '%Y-%m-%d',
            'datetime_format': '%Y-%m-%d %H:%M:%S',
        },
    }
}

# This custom exporter is needed for xlsx export with the -o (output) command
FEED_EXPORTERS = {
    'xlsx': 'eupas.exporters.XlsxItemExporter',
    'sqlite3': 'eupas.exporters.SQLiteItemExporter'
}

# This setting tells the exporters if they should export empty feeds without any items
FEED_STORE_EMPTY = True
# Splits the files in to batches with up to FEED_EXPORT_BATCH_ITEM_COUNT items in each file
# FEED_EXPORT_BATCH_ITEM_COUNT = 100
# This setting defines the fields to be exported
# Possible values are:
#    None (all fields 2, default)
#
#    A list of fields:
#        ['field1', 'field2']
#
#    A dict where keys are fields and values are output names:
#        {'field1': 'Field 1', 'field2': 'Field 2'}
#
# Some examples:
# FEED_EXPORT_FIELDS = None
# FEED_EXPORT_FIELDS = ['eu_pas_register_number', 'state', 'title']
# FEED_EXPORT_FIELDS = {'eu_pas_register_number': 'ID', 'url': 'URL'}
FEED_URI_PARAMS = 'eupas.exporters.uri_params'
##################################

##################################
#           DUPEFILTER           #
##################################

DUPEFILTER_CLASS = 'eupas.dupefilters.EupasDupeFilter'

# Display all duplicates by turning DUPEFILTER_DEBUG on
# LOG_LEVEL has to be Debug!
# DUPEFILTER_DEBUG = True

# Following duplicates were found last time this was tested:
#   https://www.encepp.eu/encepp/viewResource.htm;?id=47194
#   https://www.encepp.eu/encepp/viewResource.htm;?id=50667
