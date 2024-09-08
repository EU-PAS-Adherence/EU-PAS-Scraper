# NOT DEFAULT
# commands/         Contains all custom commands extending the default scrapy commands
# spiders/          Contains all spiders
# validators/       Contains all jsonschemas for spidermons item validation.
# contracts.py      NOTE: Unused. Scrapys way of unit-testing.
# dupefilters.py    Custom Dupefilter for the eupas spider. Generates extra stats used in the monitors.
# exporters.py      Custom XLSX and SQLITE exporters
# extensions.py     Custom Extensions like the item History Comparer for the eupas item
# items.py          Contains all complex item types (Currently only for the eupas spider)
# monitors.py       Contains all spidermon (extension) monitors (Currently only for the eupas spider)
# pipelines.py      Custom duplicare items pipeline for the eupas spider
# settings.py       Scrapy, Spidermon and custom extension settings
