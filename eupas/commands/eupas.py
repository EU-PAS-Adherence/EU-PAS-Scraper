import os

from scrapy.commands.crawl import Command as CrawlCommand
from scrapy.exceptions import UsageError

from eupas.spiders.eupas_spider import EU_PAS_Spider, RMP


class Command(CrawlCommand):

    def add_options(self, parser):
        CrawlCommand.add_options(self, parser)
        group = parser.add_argument_group(title="Custom Eupas Options")
        group.add_argument(
            "-F",
            "--filter",
            metavar="FILTER_LEVEL",
            default=None,
            help="filter level (disabled by default)",
        )
        group.add_argument(
            "--debug", action="store_true", help="enable debugging"
        )
        group.add_argument(
            "-PDF", "--download-pdf", action="store_true", help="downloads a pdf file for each study detail page"
        )
        group.add_argument(
            "-PR", "--download-protocols-results", action="store_true", help="downloads the latest protocols and results of every study"
        )

    def process_options(self, args, opts):
        CrawlCommand.process_options(self, args, opts)
        if opts.debug:
            self.settings.set("LOG_ENABLED", True,
                              priority=self.settings.maxpriority() + 10)
            self.settings.set("LOG_LEVEL", "DEBUG",
                              priority=self.settings.maxpriority() + 10)
        opts.spargs.setdefault('progress_logging', not opts.debug)
        opts.spargs.setdefault('filter_studies', bool(opts.filter))
        opts.spargs.setdefault('save_pdf', opts.download_pdf)
        opts.spargs.setdefault('save_protocols_and_results',
                               opts.download_protocols_results)
        if opts.filter:
            id_candidate = opts.filter.lower().replace('eupas', '')
            if id_candidate.isdigit():
                opts.spargs.setdefault('filter_eupas_id', int(id_candidate))
            else:
                opts.spargs.setdefault(
                    'filter_rmp_category', self.get_rmp(opts.filter))

    def get_rmp(self, value):
        rmp = value.lower()
        if rmp in ['rmp1', 'risk_management_plan_1']:
            return RMP.EU_RPM_category_1
        elif rmp in ['rmp2', 'risk_management_plan_2']:
            return RMP.EU_RPM_category_2
        elif rmp in ['rmp3', 'risk_management_plan_3']:
            return RMP.EU_RPM_category_3
        elif rmp in ["noneu", "non_eu", "noneurmp", "non_eu_rmp", "otherrmp"]:
            return RMP.non_EU_RPM
        elif rmp in ["na", "n_a", "n/a", "notapplicable", "not_applicable"]:
            return RMP.not_applicable

        raise UsageError(f"Received unsuported filter value: {value}")

    def syntax(self):
        return "[options]"

    def short_desc(self):
        return "Run the Eupas spider"

    def run(self, args, opts):
        if len(args) > 0:
            raise UsageError(
                "running 'scrapy eupas' with additional arguments is not supported"
            )

        new_args = [EU_PAS_Spider.name]
        super().run(new_args, opts)

        monitor_success = os.getenv('MONITOR_SUCCESS') == 'true'
        print(f'All Monitors Successful: {monitor_success}')
        if not monitor_success:
            self.exitcode = 1
