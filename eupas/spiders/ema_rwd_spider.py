# NOT DEFAULT
# Define your spiders here
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spiders.html

from scrapy import spiders, http, signals
from scrapy.utils.sitemap import Sitemap
from tqdm import tqdm

from enum import Enum
from pathlib import Path
import re
from typing import List, Generator, Union

from eupas.items import EMA_RWD_Study


class RMP(Enum):
    '''
    The Risk Management Plan of a PAS
    '''
    EU_RPM_category_1 = 54331
    EU_RPM_category_2 = 54332
    EU_RPM_category_3 = 54333
    non_EU_RPM = 54334
    not_applicable = 54335

# NOTE: This Spider is unnecessary because of the native export capability of the new website.
#       It is also much easier to write a scraper for this new website.


class EMA_RWD_Spider(spiders.Spider):
    '''
    This Scrapy Spider extracts study data from the EU PAS Register.
    '''

    # Overriden Spider Settings
    # name is used to start a spider with the scrapy cmd crawl or runspider
    # The eupas cmd runs this spider directly and simplifies arguments
    name = 'ema_rwd'
    # custom_settings contains own settings, but can also override the values in settings.py
    custom_settings = {
        'PROGRESS_LOGGING': False,
        'FILTER_STUDIES': False,
        'SAVE_PDF': False,
        'SAVE_PROTOCOLS_AND_RESULTS': False
    }
    # These are the allowed domains. This spider should only follow urls in these domains
    allowed_domains = ['catalogues.ema.europa.eu']

    # URLS and headers
    base_url = 'https://catalogues.ema.europa.eu'
    query_url = f'{base_url}/search?sort_bef_combine=title_ASC&f[0]=content_type%3Adarwin_study'
    sitemap_url = f'{base_url}/sitemap.xml'

    # Filter Settings
    # This string is used to query the studies
    # NOTE: There are other queries which aren't included in this string
    template_string = '&f[1]=risk_management_plan_category%3A{risk_management_plan_id}'
    page_regex = re.compile(r'page=(\d+)')
    sitemap_regex = re.compile(rf'{re.escape(base_url)}\/study\/\d+')

    n_studies = 0
    item_class = EMA_RWD_Study

    def clean(self, s: str):
        return s.strip()

    def __init__(self, progress_logging=False, filter_studies=False, filter_rmp_category=None, save_pdf=False, save_protocols_and_results=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_settings.update({
            'PROGRESS_LOGGING': progress_logging,
            'FILTER_STUDIES': filter_studies,
            'SAVE_PDF': save_pdf,
            'SAVE_PROTOCOLS_AND_RESULTS': save_protocols_and_results
        })
        self.rmp_query_val = filter_rmp_category.value if filter_rmp_category else ''

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.idle, signals.spider_idle)
        return spider

    def start_requests(self) -> List[http.Request]:
        '''
        Starts the scraping process by requesting a query for all (or a filtered subset of all) PAS.
        '''
        self.logger.info('Starting Extraction')
        if self.custom_settings.get('FILTER_STUDIES'):
            extra_query = self.template_string.format(
                risk_management_plan_id=self.rmp_query_val,
            )
            self.query_url = f'{self.query_url}{extra_query}'

            return [http.Request(
                url=self.query_url,
                callback=self.parse_search,
                cb_kwargs={
                    'first_page': True
                }
            )]
        else:
            return [http.Request(
                url=self.sitemap_url,
                callback=self.parse_sitemap,
                cb_kwargs={
                    'home_page': True
                }
            )]

    def parse_search(self, response: http.TextResponse, first_page=False) -> Generator[http.Request, None, None]:

        if first_page:
            self.n_studies = int(response.css(
                '.source-summary-count').xpath('./text()').get('(0)')[1:-1])
            self.crawler.stats.set_value('item_expected_count', self.n_studies)

            if self.n_studies == 0:
                self.logger.warning('No study found.')
            else:
                self.logger.info(
                    f'{self.n_studies} studies will be extracted.')

            if self.custom_settings.get('PROGRESS_LOGGING'):
                self.pbar = tqdm(
                    total=self.n_studies,
                    leave=False,
                    desc='Scraping Progress',
                    unit='studies',
                    colour='green',
                )

            if self.n_studies == 0:
                return

            if last_page_url := response.css('.darwin-list-pages nav li:last-child').xpath('./a/@href').get():
                last_page_number = int(
                    self.page_regex.search(last_page_url).group(1)) + 1
                yield from (http.Request(f'{response.url}&page={i}', callback=self.parse_search) for i in range(1, last_page_number))

        entry_urls = response.css(
            '.bcl-listing article').xpath('.//a/@href').getall()
        yield from (http.Request(f'{self.base_url}{url}', callback=self.parse) for url in entry_urls)

    def parse_sitemap(self, response: http.XmlResponse, home_page=False) -> Generator[http.Request, None, None]:
        urls = [
            entry['loc']
            for entry in Sitemap(response.body)
        ]
        if home_page:
            if self.custom_settings.get('PROGRESS_LOGGING'):
                self.pbar = tqdm(
                    total=self.n_studies,
                    leave=False,
                    desc='Scraping Progress',
                    unit='studies',
                    colour='green',
                )
            yield from (http.Request(url, callback=self.parse_sitemap) for url in urls)
        else:
            filtered_urls = [
                url for url in urls if self.sitemap_regex.search(url)
            ]
            self.n_studies += len(filtered_urls)
            self.crawler.stats.set_value('item_expected_count', self.n_studies)

            if self.n_studies == 0:
                self.logger.warning('No study found on one sitemap page.')
            else:
                self.logger.info(
                    f'{self.n_studies} studies will be extracted.')

            if self.custom_settings.get('PROGRESS_LOGGING'):
                self.pbar.total = self.n_studies
                self.pbar.refresh()
            yield from (http.Request(url, callback=self.parse) for url in filtered_urls)

    def parse(self, response: http.TextResponse) -> Generator[http.Request, None, None]:

        study = EMA_RWD_Study()
        # NOTE: Can contain <br>; Maybe better to extract from study identification
        study['title'] = ''.join(response.xpath('.//h1//text()').getall())

        content = response.css('.content-banner-content-wrapper')
        dates = content.css('.dates')
        study['registration_date'] = self.clean(
            dates.xpath('./div[1]//span/text()').get())
        study['update_date'] = self.clean(
            dates.xpath('./div[2]//span/text()').get())

        study['url'] = '/'.join(response.url.split('/')[:-1])
        study['pdf_url'] = f'{self.base_url}{response.css(".bcl-card-link-set").xpath("./a/@href").get()}'
        if self.custom_settings.get('SAVE_PDF'):
            yield http.Request(url=study['pdf_url'], callback=self.save_pdf, cb_kwargs=dict(study=study), meta=dict(download_timeout=180))

        self.parse_admin_details(response=response, study=study)
        yield http.Request(url=f'{study["url"]}/methodological-aspects', callback=self.parse_method_details, cb_kwargs=dict(study=study))

    def save_pdf(self, response: http.Response, study: EMA_RWD_Study, suffix='') -> None:
        file_path = Path(f"{self.settings.get('OUTPUT_DIRECTORY')}/PDFs/")
        file_path.mkdir(parents=True, exist_ok=True)
        pdf_file = file_path / f"{study['eu_pas_register_number']}{suffix}.pdf"
        pdf_file.write_bytes(response.body)

    def parse_admin_details(self, response: http.TextResponse, study: EMA_RWD_Study) -> None:
        '''
        Parses the details of the first tab: "Administrative Details"
        '''

        fieldsets = response.css('fieldset')

        # Study identification
        if study_identification := fieldsets.css('#darwin-study-identification .fieldset-wrapper'):
            study['puri'] = study_identification.xpath(
                './/dd')[0].xpath('.//text()').get()
            study['eu_pas_register_number'] = study_identification.xpath(
                './/dd')[1].xpath('.//text()').get()
            # NOTE: Can contain <br> if extracted from <h1>
            study['title'] = study_identification.xpath(
                './/dd')[3].xpath('.//text()').get()
            study['countries'] = sorted(study_identification.xpath(
                './/dd')[5].xpath('.//text()').getall())
            table = zip(study_identification.xpath('.//dt//text()').getall()[6:],
                        study_identification.xpath('.//dd//text()').getall()[6:])
            table = zip(study_identification.xpath('.//dt//text()').getall()[6:],
                        [entry.xpath('.//text()').get() for entry in study_identification.xpath('.//dd')[6:]])
            for [name, value] in table:
                if 'description' in name.lower():
                    study['description'] = value
                if 'status' in name.lower():
                    study['state'] = value

        # Research institution and networks
        # NOTE: Centres got major changes on the new website
        if institutions_and_networks := fieldsets.css('#darwin-research-institution-and-networks .fieldset-wrapper'):

            if lead_org := institutions_and_networks.css('*[class$="lead-organisation"]'):
                study['lead_institution_encepp'] = \
                    lead_org.xpath('.//a//text()').get()

            if lead_org_other := institutions_and_networks.css('*[class$="lead-organisation-o"]'):
                study['lead_institution_not_encepp'] = \
                    lead_org_other.xpath('./text()').get()

            if additional_orgs := institutions_and_networks.css('*[class$="addit-organis"]'):
                study['additional_institutions_encepp'] = \
                    sorted(additional_orgs.xpath('.//a//text()').getall())

            if additional_orgs_other := institutions_and_networks.css('*[class$="addit-organis-other"]'):
                # NOTE: Some values are seperated by a <br> tag
                study['additional_institutions_not_encepp'] = \
                    ''.join(additional_orgs_other.xpath('./text()').getall())

            if networks := institutions_and_networks.css('*[class$="network"]'):
                study['networks_encepp'] = \
                    sorted(networks.xpath('.//a//text()').getall())

            if additional_networks := institutions_and_networks.css('*[class$="network-other"]'):
                study['networks_not_encepp'] = \
                    additional_networks.xpath('./text()').get()

        # Study timelines
        if study_timelines := fieldsets.css('#darwin-study-timelines .fieldset-wrapper'):
            date_table_rows = [
                [
                    [
                        s.strip()
                        for s in entry.xpath('.//text()').getall()
                    ]
                    for entry in row.xpath('./*')
                ]
                for row in study_timelines.xpath('./div/div')
            ]

            for [[name], *date_entries] in date_table_rows:
                planned = actual = None
                for [date_label, date] in date_entries:
                    if 'planned' in date_label.lower():
                        planned = date
                    elif 'actual' in date_label.lower():
                        actual = date

                field_name = None
                if 'funding contract' in name.lower():
                    field_name = 'funding_contract_date'
                elif 'data collection' in name.lower():
                    field_name = 'data_collection_date'
                elif 'data analysis' in name.lower():
                    field_name = 'data_analysis_date'
                elif 'interim report' in name.lower():
                    field_name = 'iterim_report_date'
                elif 'final study report' in name.lower():
                    field_name = 'final_report_date'

                if planned:
                    study[f'{field_name}_planed'] = planned

                if actual:
                    study[f'{field_name}_actual'] = actual

        # Sources of funding
        # NOTE: Funding got major changes on the new website; No percentages
        if funding := fieldsets.css('#darwin-sources-of-funding .fieldset-wrapper'):

            if sources := funding.xpath('./div[1]'):
                study['funding_sources'] = sorted(
                    sources.xpath('.//text()').getall())

            if details := funding.xpath('./div[2]'):
                study['funding_details'] = details.xpath('./div/text()').get()

        # Study protocol
        if study_protocol := fieldsets.css('#darwin-study-protocol .fieldset-wrapper'):
            # NOTE: Merged latest and normal protocol? No option to hide protocol?
            study['protocol_document_url'] = \
                study_protocol.xpath('.//a/@href').get()

        # Regulatory
        if regulatory := fieldsets.css('#darwin-regulatory .fieldset-wrapper'):
            values = regulatory.xpath('.//dd//text()').getall()
            study['requested_by_regulator'] = values[0]
            table = zip(regulatory.xpath('.//dt//text()')[1:].getall(),
                        values[1:])
            for [name, value] in table:
                if 'risk management plan' in name.lower():
                    study['risk_management_plan'] = value
                elif name.lower() == 'regulatory procedure number':
                    study['regulatory_procedure_number'] = value

        # NOTE: acronym merged with title
        # NOTE: country_type was removed
        # NOTE: collaboration_with_research_network was removed

    def parse_method_details(self, response: http.TextResponse, study: EMA_RWD_Study) -> Generator[http.Request, None, None]:
        '''
        Parses the details of the second tab: "Methodological Aspects"
        '''

        fieldsets = response.css('fieldset')

        # Study type
        # NOTE: This was changed from the old type!
        if study_type := fieldsets.css('#darwin-study-type .fieldset-wrapper'):
            table = zip(study_type.xpath('.//dt//text()').getall(),
                        [entry.xpath('.//text()').getall() for entry in study_type.xpath('.//dd')])
            for [name, values] in table:
                first_value = values[0]
                if name.lower() == 'study topic':
                    study['study_topic'] = sorted(values)
                elif name.lower() == 'study topic, other':
                    study['study_topic_other'] = first_value
                elif name.lower() == 'study type':
                    study['study_type'] = first_value
                elif 'further details on the study type' in name.lower():
                    study['study_type_other'] = first_value

        # Non-interventional study
        if non_interventional_study := fieldsets.css('#darwin-non-interventional-study .fieldset-wrapper'):
            table = zip(non_interventional_study.xpath('.//dt//text()').getall(),
                        [entry.xpath('.//text()').getall() for entry in non_interventional_study.xpath('.//dd')])
            for [name, values] in table:
                first_value = values[0]
                if name.lower() == 'scope of the study':
                    # NOTE: This was changed from the old scopes and there is no 'primary_scope' field!
                    # NOTE: There is a new field: Clinical trial regulatory scope
                    study['non_interventional_scopes'] = sorted(values)
                elif 'further details on the scope of the study' in name.lower():
                    study['non_interventional_scopes_other'] = first_value
                elif name.lower() == 'non-interventional study design':
                    # NOTE: This was changed from study_design?
                    study['non_interventional_study_design'] = sorted(values)
                elif name.lower() == 'non-interventional study design, other':
                    study['non_interventional_study_design_other'] = first_value

        # Study drug and medical condition
        if drugs_and_conditions := fieldsets.css('#darwin-study-drug-and-medical-condition .fieldset-wrapper'):
            table = zip(drugs_and_conditions.xpath('.//dt//text()').getall(),
                        [entry.xpath('.//text()').getall() for entry in drugs_and_conditions.xpath('.//dd')])
            for [name, values] in table:
                first_value = values[0]
                if name.lower() == 'name of medicine':
                    study['substance_brand_name'] = sorted(list(values[1::2]))
                elif name.lower() == 'name of medicine, other':
                    study['substance_brand_name_other'] = first_value
                elif 'international non-proprietary name' in name.lower():
                    study['substance_inn'] = sorted(list(values[1::2]))
                elif 'anatomical therapeutic chemical' in name.lower():
                    study['substance_atc'] = sorted(list(values[1::2]))
                elif name.lower() == 'medical condition to be studied':
                    study['medical_conditions'] = sorted(list(values[1::2]))
                elif 'additional medical condition' in name.lower():
                    study['additional_medical_conditions'] = first_value

        # Population studied
        if population_studied := fieldsets.css('#darwin-population-studied .fieldset-wrapper'):
            table = zip(population_studied.xpath('.//dt//text()').getall(),
                        [entry.xpath('.//text()').getall() for entry in population_studied.xpath('.//dd')])
            for [name, values] in table:
                first_value = values[0]
                if 'age groups' in name.lower():
                    # NOTE: This was changed!
                    study['age_population'] = sorted(values)
                elif 'number of subjects' in name.lower():
                    study['number_of_subjects'] = int(first_value)
                elif name.lower() == 'special population of interest':
                    # NOTE: This was changed from other_population!
                    study['special_population'] = sorted(values)
                elif name.lower() == 'special population of interest, other':
                    study['special_population_other'] = first_value

        # Study design details
        # NOTE: Outcomes instead of primary_outcomes or secondary_outcomes
        if study_design_details := fieldsets.css('#darwin-study-design-details .fieldset-wrapper'):
            table = zip(study_design_details.xpath('.//dt//text()').getall(),
                        study_design_details.xpath('.//dd//text()').getall())
            for [name, value] in table:
                if name.lower() == 'outcomes':
                    study['outcomes'] = value

        # Documents
        if documents := fieldsets.css('#darwin-documents .fieldset-wrapper'):
            if result_tables := documents.css('*[class$="result-tables"]'):
                study['result_tables_url'] = \
                    result_tables.xpath('./div[2]//a/@href').get()

            if study_results := documents.css('*[class$="report-file"]'):
                study['result_document_url'] = \
                    study_results.xpath('./div[2]//a/@href').get()

            if other_documents := documents.css('*[class$="oth-info-file"]'):
                study['other_documents_url'] = \
                    other_documents.xpath('./div[2]//a/@href').getall()

            if publications := documents.css('*[class$="publications"]'):
                study['references'] = \
                    publications.xpath('./div[2]//a/@href').getall()

        # NOTE: follow_up was removed
        # NOTE: sex_population was removed
        # NOTE: uses_established_data_source was removed
        yield http.Request(url=f'{study["url"]}/data-management', callback=self.parse_data_details, cb_kwargs=dict(study=study))

    def parse_data_details(self, response: http.TextResponse, study: EMA_RWD_Study) -> Generator[Union[EMA_RWD_Study, http.Request], None, None]:
        '''
        Parses the details of the third tab: "Data managment"
        '''

        if self.custom_settings.get('PROGRESS_LOGGING') and isinstance(self.pbar, tqdm):
            self.pbar.update()

        fieldsets = response.css('fieldset')

        # Data sources
        if data_sources := fieldsets.css('#darwin-data-sources .fieldset-wrapper'):
            table = zip(data_sources.xpath('.//dt//text()').getall(),
                        [entry.xpath('.//text()').getall() for entry in data_sources.xpath('.//dd')])
            for [name, values] in table:
                first_value = values[0]
                # NOTE: There is a space in the website right now
                if name.lower().strip() == 'data source(s)':
                    study['data_sources_registered_with_encepp'] = sorted(
                        values)
                elif name.lower() == 'data sources, if not available in the list above':
                    study['data_sources_not_registered_with_encepp'] = first_value
                elif name.lower() == 'data sources (types)':
                    study['data_source_types'] = sorted(values)
                elif name.lower() == 'data sources (types), other':
                    study['data_source_types_other'] = first_value

        # Data quality specifications
        if data_sources := fieldsets.css('#darwin-data-quality-specifications .fieldset-wrapper'):
            study['check_conformance'], study['check_completeness'], study['check_stability'], study['check_logical_consistency'] = \
                data_sources.xpath('.//dd//text()').getall()

        # Data characterisation
        if data_characterisation := fieldsets.css('#darwin-data-characterisation .fieldset-wrapper'):
            study['conducted_data_characterisation'] = \
                data_characterisation.xpath('.//dd//text()').get()

        if self.custom_settings.get('SAVE_PROTOCOLS_AND_RESULTS'):
            if protocol_url := study.get('protocol_document_url'):
                yield http.Request(url=protocol_url, callback=self.save_pdf, cb_kwargs=dict(study=study, suffix='_latest_protocols'), meta=dict(download_timeout=60))
            if result_tables := study.get('result_tables_url'):
                yield http.Request(url=result_tables, callback=self.save_pdf, cb_kwargs=dict(study=study, suffix='_latest_result_tables'), meta=dict(download_timeout=60))
            if result_url := study.get('result_document_url'):
                yield http.Request(url=result_url, callback=self.save_pdf, cb_kwargs=dict(study=study, suffix='_latest_results'), meta=dict(download_timeout=60))

        yield study

    def idle(self):
        if self.custom_settings.get('PROGRESS_LOGGING') and isinstance(self.pbar, tqdm):
            self.pbar.close()

    def closed(self, reason: str):
        if reason == 'finished':
            self.logger.info('Scraping finished successfully.')
        elif reason == 'shutdown':
            self.logger.info('Scraping was stopped by user.')
        else:
            self.logger.info(f'Scraping finished with reason: {reason}')

        self.logger.info(
            f'Extraction finished in {self.crawler.stats.get_value("elapsed_time_seconds")} seconds.')
