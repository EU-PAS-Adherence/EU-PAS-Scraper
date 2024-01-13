import difflib
import logging
from pathlib import Path
import re
import requests

from scrapy.commands.crawl import Command as CrawlCommand
from scrapy.exceptions import UsageError

from eupas.spiders.atc_spider import ATC_Spider
from eupas.spiders.kegg_spider import KEGG_Drug_Spider
from eupas.commands import PandasCommand


class Command(PandasCommand):

    classifications = ['BAN', 'DCF', 'INN', 'JAN',
                       'JP18', 'NF', 'Non-JPS', 'prop.INN', 'TM', 'TN', 'USAN', 'USP']

    def add_options(self, parser):
        PandasCommand.add_options(self, parser)
        patch = parser.add_argument_group(title="Custom Patching Options")
        patch.add_argument(
            "-m",
            "--match-input",
            metavar="FILE",
            default=None,
            help="path to the matching file"
        )

    def process_options(self, args, opts):
        PandasCommand.process_options(self, args, opts)
        if opts.match_input:
            self.match_path = Path(opts.match_input)

            if not self.match_path.is_file() and self.match_enabled:
                raise UsageError(
                    "Invalid -m value, use a valid path to a file", print_help=False)

            if self.match_path.suffix != '.xlsx' and self.match_enabled:
                raise UsageError(
                    "Invalid -m value, xlsx file expected", print_help=False)

    def syntax(self):
        return "[options]"

    def short_desc(self):
        return "Match substance INN and ATC to correct ATC with the help of KEGG and the WHO ATC Index"

    def run(self, args, opts):
        super().run(args, opts)
        import numpy as np

        self.logger = logging.getLogger()
        self.logger.info('Starting substances script')
        self.logger.info(f'Pandas {self.pd.__version__}')
        self.logger.info('Reading input data...')
        data = self.read_input()

        substance_inn = data.loc[data['substance_inn'].notna(), [
            'substance_inn']]
        substance_inn = substance_inn.drop_duplicates().assign(
            cleaned_inn=substance_inn['substance_inn'].str.split('; ')).explode('cleaned_inn')
        substance_inn['cleaned_inn'] = substance_inn['cleaned_inn'].str.strip(
        ).str.replace(r'\s+\(\S+\)$', '', regex=True)
        substance_inn.drop_duplicates().reset_index(drop=True)

        substance_atc = data.loc[data['substance_atc'].notna(), [
            'substance_atc']]
        substance_atc = substance_atc.drop_duplicates().assign(
            cleaned_atc_code=substance_atc['substance_atc'].str.split('; ')).explode('cleaned_atc_code')
        substance_atc = substance_atc.assign(cleaned_atc_value=substance_atc['cleaned_atc_code'].str.strip(
        ).str.replace(r'\S+\b\s*\((.*)\)$', r'\1', regex=True))
        substance_atc['cleaned_atc_code'] = substance_atc['cleaned_atc_code'].str.strip(
        ).str.replace(r'\s+\(.+\)$', '', regex=True)
        substance_atc.drop_duplicates().reset_index(drop=True)

        self.logger.info('Aquiring KEGG data...')
        kegg = self.pd.DataFrame()
        kegg_db_path = self.output_folder / 'kegg.txt'
        if kegg_db_path.is_file():
            kegg = self.pd.DataFrame([row.split('\t') for row in kegg_db_path.read_text(
            ).split('\n')[:-1]], columns=['kegg_drug_entry_id', 'all_kegg_drug_names'])
        else:
            response = requests.get('https://rest.kegg.jp/list/drug')
            if response.ok:
                kegg_db_path.write_text(response.text)
                kegg = self.pd.DataFrame([row.split('\t') for row in response.text.split(
                    '\n')[:-1]], columns=['kegg_drug_entry_id', 'all_kegg_drug_names'])
            else:
                raise RuntimeError('KEGG rest api not responding ok.')

        kegg = kegg.assign(cleaned_kegg_drug_name=kegg['all_kegg_drug_names'].str.split(
            '; ')).explode('cleaned_kegg_drug_name')
        kegg = kegg.assign(cleaned_kegg_drug_classification=kegg['cleaned_kegg_drug_name'].str.extract(
            f'\\s+\\((\\S*(?:{"|".join([re.escape(x) for x in self.classifications])})\\S*)\\)$'))
        kegg['cleaned_kegg_drug_name'] = kegg['cleaned_kegg_drug_name'].str.strip(
        ).str.replace(r'(\s+\(\S+\))+$', '', regex=True)
        kegg = kegg.drop_duplicates().reset_index(drop=True)
        self.write_output(kegg, '_kegg')

        self.logger.info('Matching INN to KEGG...')

        def get_matching_rows(search_terms, df, column, assigned_column, prefix=''):
            matching_rows = self.pd.DataFrame()
            for search_term in search_terms:
                info = f'{prefix}No match'
                match = self.pd.DataFrame(
                    [[self.pd.NA] * len(df.columns)], columns=df.columns.values)

                exact_matches = df[df[column].str.fullmatch(
                    re.escape(search_term), case=False, na=False)]

                if exact_matches.empty:
                    matches = df[df[column].str.match(
                        re.escape(search_term), case=False, na=False)]

                    if matches.empty:
                        best_match = difflib.get_close_matches(
                            search_term.lower(), df[column], n=1, cutoff=0.6)
                        if best_match:
                            info = f'{prefix}Best close match with cutoff 0.6'
                            match = df[df[column].str.fullmatch(
                                re.escape(best_match[0]))].iloc[[0]]
                    else:
                        info = f'{prefix}Best partial match'
                        best_match = difflib.get_close_matches(
                            search_term, matches[column], n=1, cutoff=0)
                        match = matches[matches[column].str.fullmatch(
                            re.escape(best_match[0]))].iloc[[0]]
                else:
                    info = f'{prefix}Full match'
                    if exact_matches.shape[0] > 1:
                        if 'cleaned_kegg_drug_classification' in exact_matches:
                            exact_inn_matches = exact_matches[exact_matches['cleaned_kegg_drug_classification'].str.contains(
                                'INN')]
                            if exact_inn_matches.empty:
                                match = exact_matches.iloc[[0]]
                            else:
                                match = exact_inn_matches.iloc[[0]]
                        else:
                            print('NO CLASSIFICATION')
                            print(exact_matches)
                            match = exact_matches.iloc[[0]]
                    else:
                        match = exact_matches.iloc[[0]]

                matching_rows = self.pd.concat([matching_rows, match.assign(
                    **{assigned_column: search_term, 'info': info})], ignore_index=True)

            return matching_rows.reset_index(drop=True)

        matching_rows = get_matching_rows(substance_inn['cleaned_inn'].dropna(
        ).unique(), kegg, 'cleaned_kegg_drug_name', 'cleaned_inn')

        self.logger.info('Aquiring matched INN details from KEGG...')
        kegg_details_path = self.output_folder / 'kegg_details.csv'
        kegg_drug_ids_to_match = set(
            matching_rows['kegg_drug_entry_id'].dropna().unique().tolist())
        if kegg_details_path.is_file():
            kegg_details = self.pd.read_csv(
                kegg_details_path.as_posix()).drop_duplicates()

            kegg_drug_ids_to_match -= set(
                kegg_details['kegg_drug_entry_id'].dropna().unique().tolist())

        if kegg_drug_ids_to_match:
            opts.spargs = {
                'drug_ids': list(kegg_drug_ids_to_match),
                'progress_logging': True
            }
            feeds = {kegg_details_path.as_posix(): {
                'format': 'csv',
                'overwrite': False,
            }}
            self.settings.set("FEEDS", feeds, priority="cmdline")
            CrawlCommand.run(self, [KEGG_Drug_Spider.name], opts)
            kegg_details = self.pd.read_csv(
                kegg_details_path.as_posix()).drop_duplicates()

        self.logger.info('Merging INN data...')
        matching_rows = matching_rows.merge(
            right=kegg_details, on='kegg_drug_entry_id', how='left')
        substance_inn = substance_inn.merge(
            right=matching_rows, on='cleaned_inn', how='left')

        self.logger.info('Aquiring ATC data...')
        atc_details_path = self.output_folder / 'atc_details.csv'
        if not atc_details_path.is_file():
            opts.spargs = {
                'progress_logging': True
            }
            feeds = {atc_details_path.as_posix(): {
                'format': 'csv',
                'overwrite': True,
            }}
            self.settings.set("FEEDS", feeds, priority="cmdline")
            CrawlCommand.run(self, [ATC_Spider.name], opts)

        who_atc = self.pd.read_csv(
            atc_details_path.as_posix()).drop_duplicates()

        self.logger.info('Merging ATC data...')
        substance_atc = substance_atc.merge(
            right=who_atc, left_on='cleaned_atc_code', right_on='atc_code', how='left')

        self.logger.info('Fill missing ATC data...')
        substance_atc = substance_atc.assign(info=np.where(
            substance_atc['atc_code'].notna(), 'Full match', self.pd.NA))

        matching_rows = get_matching_rows(substance_atc.loc[substance_atc['atc_code'].isna(
        ), 'cleaned_atc_value'].unique(), who_atc, 'atc_value', 'cleaned_atc_value', 'ATC Value: ')

        self.logger.info('Merging ATC data...')
        merge_suffix = '_merge'
        substance_atc = substance_atc.merge(
            right=matching_rows, on='cleaned_atc_value', how='left', suffixes=(None, merge_suffix))
        override_columns = ['atc_code', 'atc_value', 'info']
        substance_atc.loc[substance_atc['atc_code'].isna(), override_columns] = substance_atc.loc[substance_atc['atc_code'].isna(
        ), [f'{column}{merge_suffix}' for column in override_columns]].rename(lambda x: x.removesuffix(merge_suffix), axis='columns')
        substance_atc.drop(
            columns=[f'{column}{merge_suffix}' for column in override_columns], inplace=True)

        self.logger.info('Writing substance data...')
        with self.pd.ExcelWriter(self.output_folder / 'substances.xlsx', engine='openpyxl') as writer:
            substance_inn.to_excel(
                writer, sheet_name='substance_inn', index=False)
            substance_atc.to_excel(
                writer, sheet_name='substance_atc', index=False)
