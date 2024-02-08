# NOT DEFAULT
# Define your spiders here
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spiders.html

from scrapy import spiders, http, selector, signals
from tqdm import tqdm

from enum import Enum
from pathlib import Path
import re
from typing import List, Generator, Union, Tuple

from eupas.items import Study


class RMP(Enum):
    '''
    The Risk Management Plan of a PAS
    '''
    not_applicable = 1
    EU_RPM_category_1 = 2
    EU_RPM_category_2 = 3
    EU_RPM_category_3 = 4
    non_EU_RPM = 5


class EU_PAS_Spider(spiders.Spider):
    '''
    This Scrapy Spider extracts study data from the EU PAS Register.
    '''

    # Overriden Spider Settings
    # name is used to start a spider with the scrapy cmd crawl or runspider
    # The eupas cmd runs this spider directly and simplifies arguments
    name = 'eupas'
    # custom_settings contains own settings, but can also override the values in settings.py
    custom_settings = {
        'PROGRESS_LOGGING': False,
        'FILTER_STUDIES': False,
        'SAVE_PDF': False,
        'SAVE_PROTOCOLS_AND_RESULTS': False
    }
    # These are the allowed domains. This spider should only follow urls in these domains
    allowed_domains = ['encepp.eu']

    # URLS and headers
    base_url = 'https://www.encepp.eu'
    query_url = f'{base_url}/encepp/studySearch.htm'
    pdf_base_url = f'{base_url}//encepp/enceppPrint.pdf?screen=search'

    # NOTE: Only the Content-Type is important for the POST request
    query_headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': query_url,
        'Origin': base_url,
    }

    # Filter Settings
    # This string is used to query the studies
    # NOTE: There are other queries which aren't included in this string
    template_string = 'studyCriteria.resourceLabel={eu_pas_register_number}&studyCriteria.studyRMP={risk_management_plan}'

    # RegEx is used to remove the sessionid in the urls
    # NOTE: Use proxy rotation for better evasion
    session_regex = re.compile(r'jsessionid=.+\?')

    def __init__(self, progress_logging=False, filter_studies=False, filter_rmp_category=None, filter_eupas_id=None, save_pdf=False, save_protocols_and_results=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_settings.update({
            'PROGRESS_LOGGING': progress_logging,
            'FILTER_STUDIES': filter_studies,
            'SAVE_PDF': save_pdf,
            'SAVE_PROTOCOLS_AND_RESULTS': save_protocols_and_results
        })
        self.rmp_query_val = filter_rmp_category.value if filter_rmp_category else ''
        self.eupas_id_query_val = filter_eupas_id or ''

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.idle, signals.spider_idle)
        return spider

    def start_requests(self) -> List[http.Request]:
        '''
        Starts the scraping process by requesting a query for all (or a filtered subset of all) PAS.
        '''
        query = None
        if self.custom_settings.get('FILTER_STUDIES'):
            query = self.template_string.format(
                risk_management_plan=self.rmp_query_val,
                eu_pas_register_number=self.eupas_id_query_val
            )

        self.logger.info('Starting Extraction')
        return [http.Request(
            url=self.query_url,
            method='POST',
            headers=self.query_headers,
            body=query,
            callback=self.parse,
            meta={
                'download_timeout': 10 * 60,
                'dont_merge_cookies': True
            }
        )]

    def parse(self, response: http.TextResponse) -> Generator[http.Request, None, None]:
        '''Parses the table of the queried studies and follows the urls to the respective study details by parsing each table row seperatly.

        @url https://www.encepp.eu/encepp/studySearch.htm
        @postEncoded studyCriteria.resourceLabel=4&studyCriteria.studyRMP=2
        @returns requests
        @returns items 0 0
        '''

        main_content = response.css('div.insidecentre')[0]
        n_studies = int(main_content.xpath(
            './/h5/text()').get('0 Studies').split()[0])
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

        for row in main_content.xpath('.//table/tr')[1:]:
            yield self.parse_study(data_row=row)

    def parse_study(self, data_row: selector.Selector) -> http.Request:
        '''
        Extracts relevant study data from a table row and follows an extracted url to get more details about the respective study.
        '''
        url = self.base_url + data_row.xpath('./td[3]//a/@href').get()
        url = self.session_regex.sub('?', url)

        study = Study()
        study['state'] = data_row.xpath('./td[1]//text()').get()
        study['eu_pas_register_number'] = data_row.xpath(
            './td[2]//text()').get()
        study['title'] = data_row.xpath('./td[3]//text()').get()
        study['update_date'] = data_row.xpath('./td[4]//text()').get().strip()
        study['url'] = url

        eupas_id = int(study['eu_pas_register_number'][5:])
        return http.Request(
            url=url,
            callback=self.parse_details,
            meta={
                'dont_merge_cookies': not self.custom_settings.get('SAVE_PDF'),
                'eupas_id': eupas_id,
                'cookiejar': eupas_id
            },
            cb_kwargs=dict(study=study)
        )

    def parse_details(self, response: http.TextResponse, study: Study) -> Generator[Union[Study, http.Request], None, None]:
        ''' Parses the study details from all four tabs. Each tab will be processed by it's own method.

        @url https://www.encepp.eu/encepp/viewResource.htm?id=50574
        @cb_kwargs {"study": {"eu_pas_register_number": "EUPAS32302"}}
        @returns requests 0 0
        @returns items 1 1
        '''
        if self.custom_settings.get('PROGRESS_LOGGING') and isinstance(self.pbar, tqdm):
            self.pbar.update()

        study['registration_date'] = response.css(
            'div.insidecentre')[0].xpath('./div[2]/span[3]/text()[normalize-space()]').get().strip()

        if self.custom_settings.get('SAVE_PDF'):
            pdf_url = f"{self.pdf_base_url}&&lastU={study['update_date']}&createdOn={study['registration_date']}"
            yield http.Request(url=pdf_url, callback=self.save_pdf, dont_filter=True, cb_kwargs=dict(study=study), meta={'cookiejar': response.meta['cookiejar']})

        self.parse_admin_details(details=response.xpath(
            './/*[@id="1"]')[0], study=study)
        self.parse_target_details(details=response.xpath(
            './/*[@id="2"]')[0], study=study)
        self.parse_method_details(details=response.xpath(
            './/*[@id="3"]')[0], study=study)
        protocol_url, result_url = self.parse_document_details(
            details=response.xpath('.//*[@id="4"]')[0], study=study)

        if self.custom_settings.get('SAVE_PROTOCOLS_AND_RESULTS'):
            if protocol_url:
                yield http.Request(url=f'{self.base_url}{protocol_url}', callback=self.save_pdf, cb_kwargs=dict(study=study, suffix='_latest_protocols'))
            if result_url:
                yield http.Request(url=f'{self.base_url}{result_url}', callback=self.save_pdf, cb_kwargs=dict(study=study, suffix='_latest_results'))

        yield study

    def save_pdf(self, response: http.Response, study: Study, suffix='') -> None:
        file_path = Path(f"{self.settings.get('OUTPUT_DIRECTORY')}/PDFs/")
        file_path.mkdir(parents=True, exist_ok=True)
        pdf_file = file_path / f"{study['eu_pas_register_number']}{suffix}.pdf"
        pdf_file.write_bytes(response.body)

    def _get_block_from_details(
        self,
        details: selector.Selector,
        index: int,
        block_element: str = 'div',
        seperator_element: str = 'h5'
    ) -> selector.SelectorList:
        '''
        Returns a SelectorList only containing block_elements, by finding blocks of block_elements following a single seperator_element.
        '''
        return details.xpath(f'./{block_element}[count(preceding-sibling::{seperator_element}/following-sibling::{block_element}[1]) = {index}]')

    def _get_multiblock_from_details(
        self,
        details: selector.Selector,
        index: int,
        block_element: str = 'div',
        seperator_element: str = 'h5',
        filter_element: str = 'br',
        offset: int = 0,
        every_nth: int = 1
    ) -> Generator[selector.SelectorList, None, None]:
        '''
        Returns a SelectorList only containing block_elements, by finding blocks of block_elements and filter_element following seperator_element.
        Then it generates smaller blocks by cutting it in chunks using filter_element and python slices.
        '''
        block = details.xpath(
            f'./*[self::{block_element} or self::{filter_element}][count(preceding-sibling::{seperator_element}/following-sibling::*[not(self::{seperator_element})][1]) = {index}]')
        boundary = [i for i, element in enumerate(
            block) if element.xpath(f'./self::{filter_element}')] + [len(block)]

        start = offset if offset == 0 else boundary[offset - 1] + 1
        for end in boundary[offset::every_nth]:
            yield block[start: end]
            start = end + 1

    def parse_admin_details(self, details: selector.Selector, study: Study) -> None:
        '''
        Parses the details of the first tab: "Administrative Details"
        '''
        # First Block: Study identification
        if acronym := details.xpath('./div[3]/span[2]//text()').get():
            study['acronym'] = acronym
        study['study_type'] = details.xpath('./div[4]/span[2]//text()').get()
        if description := details.xpath('./div[5]/span[2]//text()').get():
            study['description'] = description
        study['requested_by_regulator'] = details.xpath(
            './div[6]/span[2]//text()').get()
        if rpm := details.xpath('./div[7]/span[2]//text()').get().strip():
            study['risk_management_plan'] = rpm
        if rpn := details.xpath('./div[8]/span[2]//text()').get():
            study['regulatory_procedure_number'] = rpn

        # Second Block: Research centres and Investigator details
        block = self._get_block_from_details(details, index=2)
        if len(block) == 2:
            study['centre_name'] = block[0].xpath(
                './span[2]//text()').get().strip()
            study['centre_location'] = block[1].xpath(
                './span[2]//text()').get()
        elif len(block) == 4:
            study['centre_name_of_investigator'] = block[0].xpath(
                './span[2]//text()').get().strip()
            if organisation := block[2].xpath('./span[2]//text()').get():
                study['centre_organisation'] = organisation
        else:
            self.logger.warning(
                'Found unexpected "Coordinating study entity" table format in the following study:\n %s', study['url'])

        # Fourth Block:
        # Is this study being carried out with the collaboration of a research network?
        block = self._get_block_from_details(details, index=4)
        study['collaboration_with_research_network'] = block[0].xpath(
            './/text()[normalize-space()]').get()

        # Sixth Block: Countries in which this study is being conducted
        block = self._get_block_from_details(details, index=6)
        study['country_type'] = block[0].xpath(
            './/text()[normalize-space()]').get()
        study['countries'] = [country.strip() for country in block[1:].xpath(
            './/text()[normalize-space()]').getall() if country.strip()]

        def extract_from_table(table: selector.SelectorList, sorted_fields: List[str], caster=[str, str]):
            other = [], []
            for i, row in enumerate(table[1:]):
                if 2 * i + 1 < len(sorted_fields):
                    if first_value := row.xpath('./span[2]//text()').get():
                        study[sorted_fields[2 * i]] = caster[0](first_value)
                    if second_value := row.xpath('./span[3]//text()').get():
                        study[sorted_fields[2 * i + 1]
                              ] = caster[1](second_value)
                else:
                    if first_value := row.xpath('./span[2]//text()').get():
                        other[0].append(caster[0](first_value))
                    if second_value := row.xpath('./span[3]//text()').get():
                        other[1].append(caster[1](second_value))
            return other

        # Seventh Block:
        # Study timelines: initial administrative steps, progress reports and final report
        block = self._get_block_from_details(details, index=7)
        extract_from_table(table=block, sorted_fields=[
            'funding_contract_date_planed',
            'funding_contract_date_actual',
            'data_collection_date_planed',
            'data_collection_date_actual',
            'data_analysis_date_planed',
            'data_analysis_date_actual',
            'iterim_report_date_planed',
            'iterim_report_date_actual',
            'final_report_date_planed',
            'final_report_date_actual'
        ])

        # Eight block: Sources of funding
        block = self._get_block_from_details(details, index=8)
        other_names, other_percentage = extract_from_table(table=block, sorted_fields=[
            'funding_companies_names',
            'funding_companies_percentage',
            'funding_charities_names',
            'funding_charities_percentage',
            'funding_government_body_names',
            'funding_government_body_percentage',
            'funding_research_councils_names',
            'funding_research_councils_percentage',
            'funding_eu_scheme_names',
            'funding_eu_scheme_percentage'
        ], caster=[str, int])
        if other_names:
            study['funding_other_names'] = other_names
        if other_percentage:
            study['funding_other_percentage'] = other_percentage

    def parse_target_details(self, details: selector.Selector, study: Study) -> None:
        '''
        Parses the details of the second tab: "Targets of the Study"
        '''
        # First Block: Study drug(s) information
        block = self._get_block_from_details(details, index=1)
        substance_atc = set()
        substance_inn = set()
        for row in block:
            spans = row.xpath('./span/text()')
            for i in range(0, len(spans), 2):
                if 'Substance INN' in spans[i].get():
                    substance_inn.add(spans[i + 1].get())
                elif 'Substance class' in spans[i].get():
                    substance_atc.add(spans[i + 1].get())

        # NOTE: Have to convert set to list for json and jsonschema to work
        if substance_atc:
            study['substance_atc'] = sorted(list(substance_atc))
        if substance_inn:
            study['substance_inn'] = sorted(list(substance_inn))

        # Second Block: Medical conditions to be studied
        block = self._get_block_from_details(details, index=2)
        study['medical_conditions'] = [block[0].xpath('./span[2]/text()').get()] + [
            row.xpath('./text()').get('').strip() for row in block[1:] if row.xpath('./text()[normalize-space()]').get()]

        # Sometimes there are Additional Medical Condition(s) as seen in:
        # https://www.encepp.eu/encepp/viewResource.htm;?id=48872
        # https://www.encepp.eu/encepp/viewResource.htm;?id=49962
        # https://www.encepp.eu/encepp/viewResource.htm;?id=47071
        if block[-1].xpath('./span[1]').re_first('Additional Medical Condition'):
            # TODO: Check for '\r\n' in other fields too
            study['additional_medical_conditions'] = '\n'.join(
                block[-1].xpath('./span[2]//text()[normalize-space()]').get().splitlines())

        # Third Block: Population under study
        # NOTE: WHENEVER MAP IS USED => Map object has to be turned into list for json and jsonschema to work
        multiblock = self._get_multiblock_from_details(
            details, index=3, filter_element='br', offset=1, every_nth=2)
        if age_block := next(multiblock, None):
            study['age_population'] = list(map(lambda x: x.strip(), age_block.xpath(
                './text()[normalize-space()]').getall()))

            if sex_block := next(multiblock, None):
                study['sex_population'] = list(map(lambda x: x.strip(), sex_block.xpath(
                    './text()[normalize-space()]').getall()))

                if other_block := next(multiblock, None):
                    study['other_population'] = list(map(lambda x: x.strip(), other_block.xpath(
                        './text()[normalize-space()]').getall()))

                    if next(multiblock, None):
                        self.logger.warning(
                            'Found additional population block in the following study:\n %s', study['url'])

        # Fourth Block: Number of subjects
        block = self._get_block_from_details(details, index=4)
        study['number_of_subjects'] = int(
            block[0].xpath('./span[2]//text()').get())

        # Fifth Block: Source of data
        block = self._get_block_from_details(details, index=5)
        study['uses_established_data_source'] = block[0].xpath(
            './span[2]//text()').get()

        multiblock = self._get_multiblock_from_details(
            details, index=5, filter_element='br', offset=1)
        while category := next(multiblock, None):
            if entries := next(multiblock, None):
                if category.re_first('Data sources registered with ENCePP'):
                    study['data_sources_registered_with_encepp'] = list(map(
                        lambda x: x.strip(), entries.xpath('./a/text()[normalize-space()]').getall()))
                elif category.re_first('Data sources not registered with ENCePP'):
                    study['data_sources_not_registered_with_encepp'] = list(map(
                        lambda x: x.strip(), entries.xpath('./text()[normalize-space()]').getall()))
                elif category.re_first('Sources of data'):
                    study['data_source_types'] = entries.xpath(
                        './span//text()[normalize-space()]').getall()
            else:
                self.logger.warning(
                    'Found unexpected empty data source category in the following study:\n %s', study['url'])

    def parse_method_details(self, details: selector.Selector, study: Study) -> None:
        '''
        Parses the details of the third tab: "Methodological Aspects"
        '''
        # First Block: Scope of the study
        block = self._get_block_from_details(details, index=1)
        study['scopes'] = list(map(
            lambda x: x.strip(), block[1:-1].xpath('./text()[normalize-space()]').getall()))
        study['primary_scope'] = block[-1].xpath('./span/text()').get()

        # Second Block: Main objective(s)
        block = self._get_block_from_details(details, index=2)
        block = block.xpath('./self::*[.//b]')
        study['primary_outcomes'] = list(filter(lambda x: x, [
            block[0].xpath('./span[2]/text()').get(),
            block[0].xpath('./following-sibling::*[1][self::br]/following-sibling::div[1]//text()[normalize-space()]').get()]))
        study['secondary_outcomes'] = list(filter(lambda x: x, [
            block[1].xpath('./span[2]/text()').get(),
            block[1].xpath('./following-sibling::*[1][self::br]/following-sibling::div[1]//text()[normalize-space()]').get()]))

        # Third Block: Study design
        block = self._get_block_from_details(details, index=3)
        study['study_design'] = list(map(lambda x: x.strip(), block[1:].xpath(
            './text()[normalize-space()]').getall()))

        # Fourth Block: Follow-up of patients
        block = self._get_block_from_details(details, index=4)
        study['follow_up'] = block[0].xpath('./span[2]/text()').get()

    def parse_document_details(self, details: selector.Selector, study: Study) -> Union[None, Tuple[str]]:
        '''
        Parses the details of the fourth tab: "Documents"
        '''

        latest_protocol_url = latest_result_protocol = None

        # Second Block: Full protocol
        block = self._get_block_from_details(details, index=2)
        num_cells = len(block[0].xpath('./span'))
        if num_cells < 4:
            # num cells will be one if there aren't any urls
            # num_cells should be 2, when only one url is expected, but sometimes there is an invisible third cell with an empty url
            if protocol_url := block[0].xpath('./span[2]/a/@href').get():
                study['protocol_document_url'] = protocol_url
                latest_protocol_url = protocol_url
            elif block[0].xpath('./span[1]').re_first("Available when the study ends"):
                study['protocol_document_url'] = "Not public until study ends"
        elif num_cells == 4:
            if protocol_url := block[0].xpath('./span[3]/a/@href').get():
                study['protocol_document_url'] = protocol_url
            elif block[0].xpath('./span[3]').re_first("Available when the study ends"):
                study['protocol_document_url'] = "Not public until study ends"
            latest_protocol_url = block[0].xpath('./span[4]/a/@href').get()
            study['latest_protocol_document_url'] = latest_protocol_url
        else:
            self.logger.warning(
                'Found unexpected number of protocol document url cells in the following study:\n %s', study['url'])

        # Third Block: Study Results
        block = self._get_block_from_details(details, index=3)
        num_cells = len(block[0].xpath('./span'))
        if num_cells < 4:
            # num cells will be one if there aren't any urls
            # num_cells should be 2, when only one url is expected, but sometimes there is an invisible third cell with an empty url
            if result_url := block[0].xpath('./span[2]/a/@href').get():
                study['result_document_url'] = result_url
                latest_result_protocol = result_url
        elif num_cells == 4:
            if result_url := block[0].xpath('./span[3]/a/@href').get():
                study['result_document_url'] = result_url
            latest_result_protocol = block[0].xpath('./span[4]/a/@href').get()
            study['latest_result_document_url'] = latest_result_protocol
        else:
            self.logger.warning(
                'Found unexpected number of result document url cells in the following study:\n %s', study['url'])

        if references := list(filter(lambda x: bool(x), block[2:].xpath('translate(.//a/@href, " ", "")').getall())):
            study['references'] = references

        # Fourth Block: Other relevant documents
        block = self._get_multiblock_from_details(details, index=4, offset=2)
        if other_documents := next(block, None):
            if other_documents_url := other_documents.xpath('.//a/@href').getall():
                study['other_documents_url'] = other_documents_url
        else:
            self.logger.warning(
                'Could not find other document block in the following study:\n %s', study['url'])

        return (latest_protocol_url, latest_result_protocol)

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
