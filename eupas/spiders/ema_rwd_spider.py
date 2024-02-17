# NOT DEFAULT
# Define your spiders here
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spiders.html

from scrapy import spiders, http, signals
from tqdm import tqdm

from enum import Enum
from pathlib import Path
import re
from typing import List, Generator, Union
import urllib.parse

from eupas.items import EMA_RWD_Study


class RMP(Enum):
    '''
    The Risk Management Plan of a PAS
    '''
    EU_RPM_category_1 = 543351
    EU_RPM_category_2 = 543352
    EU_RPM_category_3 = 543353
    non_EU_RPM = 543354
    not_applicable = 54335


# NOTE: This Spider is unnecessary because of the native export capability of the new website.
#       It is much easier to write a scraper for this new website.
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
    query_url = f'{base_url}/search?sort_bef_combine=title_ASC&f[0]={urllib.parse.quote_plus("content_type:darwin_study")}'
    # pdf_base_url = f'{base_url}//encepp/enceppPrint.pdf?screen=search'

    # Filter Settings
    # This string is used to query the studies
    # NOTE: There are other queries which aren't included in this string
    template_string = '&f[1]=risk_management_plan_category:{risk_management_plan_id}'
    page_regex = re.compile(r'page=(\d+)')

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
        extra_query = ''
        if self.custom_settings.get('FILTER_STUDIES'):
            extra_query = self.template_string.format(
                risk_management_plan_id=self.rmp_query_val,
            )
        self.query_url = f'{self.query_url}{extra_query}'

        self.logger.info('Starting Extraction')
        return [http.Request(
            url=self.query_url,
            callback=self.parse,
            cb_kwargs={
                'first_page': True
            }
        )]

    def parse(self, response: http.TextResponse, first_page=False) -> Generator[http.Request, None, None]:

        if first_page:
            n_studies = int(response.css(
                '.source-summary-count').xpath('./text()').get('(0)')[1:-1])
            self.crawler.stats.set_value('item_expected_count', n_studies)

            if n_studies == 0:
                self.logger.warning('No study found.')
            else:
                self.logger.info(f'{n_studies} studies will be extracted.')

            if self.custom_settings.get('PROGRESS_LOGGING'):
                self.pbar = tqdm(
                    total=n_studies,
                    leave=False,
                    desc='Scraping Progress',
                    unit='studies',
                    colour='green',
                )

            if n_studies == 0:
                return

            if last_page_url := response.css('.darwin-list-pages nav li:last-child').xpath('./a/@href').get():
                last_page_number = int(
                    self.page_regex.search(last_page_url).group(1)) + 1
                for request in (http.Request(f'{response.url}&page={i}', callback=self.parse) for i in range(1, last_page_number)):
                    yield request

        entry_urls = response.css(
            '.bcl-listing article').xpath('.//a/@href').getall()
        for request in (http.Request(f'{self.base_url}{url}', callback=self.parse_details) for url in entry_urls):
            yield request

    def parse_details(self, response: http.TextResponse) -> Generator[Union[EMA_RWD_Study, http.Request], None, None]:

        if self.custom_settings.get('PROGRESS_LOGGING') and isinstance(self.pbar, tqdm):
            self.pbar.update()

        study = EMA_RWD_Study()
        study['title'] = response.xpath('.//h1//text()').get()

        content = response.css('.content-banner-content-wrapper')
        dates = content.css('.dates')
        study['registration_date'] = self.clean(
            dates.xpath('./div[1]//span/text()').get())
        study['update_date'] = self.clean(
            dates.xpath('./div[2]//span/text()').get())

        study['eu_pas_register_number'] = content.xpath(
            './div[2]/div[2]/text()').get().strip()

        study['state'] = content.xpath(
            './div[3]/div[2]/span/span/text()').get().strip()

        study['url'] = '/'.join(response.url.split('/')[:-1])

        if self.custom_settings.get('SAVE_PDF'):
            pdf_url = f'{self.base_url}{response.css(".bcl-card-link-set").xpath("./a/@href").get()}'
            yield http.Request(url=pdf_url, callback=self.save_pdf, cb_kwargs=dict(study=study))

        self.parse_admin_details(response=response, study=study)
        yield http.Request(url=f'{study["url"]}/methodological-aspects', callback=self.parse_method_details, cb_kwargs=dict(study=study))
        yield http.Request(url=f'{study["url"]}/data-management', callback=self.parse_data_details, cb_kwargs=dict(study=study))

        # if self.custom_settings.get('SAVE_PROTOCOLS_AND_RESULTS'):
        #     if protocol_url:
        #         yield http.Request(url=f'{self.base_url}{protocol_url}', callback=self.save_pdf, cb_kwargs=dict(study=study, suffix='_latest_protocols'))
        #     if result_url:
        #         yield http.Request(url=f'{self.base_url}{result_url}', callback=self.save_pdf, cb_kwargs=dict(study=study, suffix='_latest_results'))

        yield study

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
            study['countries'] = sorted(study_identification.xpath(
                './/dd')[5].xpath('.//text()').getall())
            if 'description' in study_identification.xpath('.//dt')[6].xpath('.//text()').get().lower():
                study['description'] = study_identification.xpath(
                    './/dd')[6].xpath('.//text()').get()

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

        # Study protocol
        if study_protocol := fieldsets.css('#darwin-study-protocol .fieldset-wrapper'):
            # NOTE: Merged latest and normal protocol? No option to hide protocol?
            study['protocol_document_url'] = study_protocol.xpath(
                './/a/@href').get()

        # Regulatory
        if regulatory := fieldsets.css('#darwin-regulatory .fieldset-wrapper'):
            study['requested_by_regulator'], study['risk_management_plan'], *rpn = \
                regulatory.xpath('.//dd//text()').getall()

            if rpn:
                study['regulatory_procedure_number'] = rpn[0]

        # NOTE: acronym merged with title
        # NOTE: country_type was removed
        # NOTE: collaboration_with_research_network was removed

        # Second Block: Research institution and networks
        # block = self._get_block_from_details(details, index=2)
        # if len(block) == 2:
        #     study['centre_name'] = block[0].xpath(
        #         './span[2]//text()').get().strip()
        #     study['centre_location'] = block[1].xpath(
        #         './span[2]//text()').get()
        # elif len(block) == 4:
        #     study['centre_name_of_investigator'] = block[0].xpath(
        #         './span[2]//text()').get().strip()
        #     if organisation := block[2].xpath('./span[2]//text()').get():
        #         study['centre_organisation'] = organisation
        # else:
        #     self.logger.warning(
        #         'Found unexpected "Coordinating study entity" table format in the following study:\n %s', study['url'])

        # # Is this study being carried out with the collaboration of a research network?
        # block = self._get_block_from_details(details, index=4)
        # study[''] = block[0].xpath(
        #     './/text()[normalize-space()]').get()

        # # Eight block: Sources of funding
        # block = self._get_block_from_details(details, index=8)
        # other_names, other_percentage = extract_from_table(table=block, sorted_fields=[
        #     'funding_companies_names',
        #     'funding_companies_percentage',
        #     'funding_charities_names',
        #     'funding_charities_percentage',
        #     'funding_government_body_names',
        #     'funding_government_body_percentage',
        #     'funding_research_councils_names',
        #     'funding_research_councils_percentage',
        #     'funding_eu_scheme_names',
        #     'funding_eu_scheme_percentage'
        # ], caster=[str, int])
        # if other_names:
        #     study['funding_other_names'] = other_names
        # if other_percentage:
        #     study['funding_other_percentage'] = other_percentage

    def parse_method_details(self, response: http.TextResponse, study: EMA_RWD_Study) -> None:
        '''
        Parses the details of the second tab: "Methodological Aspects"
        '''
        fieldsets = response.css('fieldset')

        # Study type
        # NOTE: This was changed from the old type!
        if study_type := fieldsets.css('#darwin-study-type .fieldset-wrapper'):
            table = zip(study_type.xpath('.//dt//text()').getall(),
                        study_type.xpath('.//dd//text()').getall())
            for [name, value] in table:
                if name.lower() == 'study topic':
                    study['study_topic'] = value
                elif name.lower() == 'study topic, other':
                    study['study_topic_other'] = value
                elif name.lower() == 'study type':
                    study['study_type'] = value
                elif 'further details on the study type' in name.lower():
                    study['study_type_other'] = value

        # Non-interventional study
        if non_interventional_study := fieldsets.css('#darwin-non-interventional-study .fieldset-wrapper'):
            table = zip(non_interventional_study.xpath('.//dt//text()').getall(),
                        [entry.xpath('.//text()').getall() for entry in non_interventional_study.xpath('.//dd')])
            for [name, values] in table:
                first_value = values[0]
                if 'scope' in name.lower():
                    # NOTE: This was changed from the old scopes and there is no 'primary_scope' field!
                    # NOTE: There is a new field: Clinical trial regulatory scope
                    study['scopes'] = sorted(values)
                elif name.lower() == 'non-interventional study design':
                    # NOTE: This was changed from study_design?
                    study['non_interventional_study_design'] = first_value
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
                elif 'International non-proprietary name' in name.lower():
                    study['substance_inn'] = sorted(list(values[1::2]))
                elif 'anatomical therapeutic chemical' in name.lower():
                    study['substance_atc'] = sorted(list(values[1::2]))
                elif name.lower() == study['medical_conditions']:
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
                    study['age_population'] = values
                elif 'number of subjects' in name.lower():
                    study['number_of_subjects'] = first_value
                elif name.lower() == 'special population of interest':
                    # NOTE: This was changed from other_population!
                    study['special_population'] = first_value
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

        # NOTE: follow_up was removed
        # NOTE: sex_population was removed
        # NOTE: uses_established_data_source was removed

#     # Third Block: Study Results
#     block = self._get_block_from_details(details, index=3)
#     num_cells = len(block[0].xpath('./span'))
#     if num_cells < 4:
#         # num cells will be one if there aren't any urls
#         # num_cells should be 2, when only one url is expected, but sometimes there is an invisible third cell with an empty url
#         if result_url := block[0].xpath('./span[2]/a/@href').get():
#             study['result_document_url'] = result_url
#             latest_result_protocol = result_url
#     elif num_cells == 4:
#         if result_url := block[0].xpath('./span[3]/a/@href').get():
#             study['result_document_url'] = result_url
#         latest_result_protocol = block[0].xpath('./span[4]/a/@href').get()
#         study['latest_result_document_url'] = latest_result_protocol
#     else:
#         self.logger.warning(
#             'Found unexpected number of result document url cells in the following study:\n %s', study['url'])

#     if references := list(filter(lambda x: bool(x), block[2:].xpath('translate(.//a/@href, " ", "")').getall())):
#         study['references'] = references

#     # Fourth Block: Other relevant documents
#     block = self._get_multiblock_from_details(details, index=4, offset=2)
#     if other_documents := next(block, None):
#         if other_documents_url := other_documents.xpath('.//a/@href').getall():
#             study['other_documents_url'] = other_documents_url
#     else:
#         self.logger.warning(
#             'Could not find other document block in the following study:\n %s', study['url'])

    def parse_data_details(self, response: http.TextResponse, study: EMA_RWD_Study) -> None:
        '''
        Parses the details of the third tab: "Data managment"
        '''

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
