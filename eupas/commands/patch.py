import difflib
import json
import logging
from pathlib import Path
import re
import requests

from scrapy.commands.crawl import Command as CrawlCommand
from scrapy.exceptions import UsageError

from eupas.spiders.atc_spider import ATC_Spider
from eupas.spiders.kegg_spider import KEGG_Drug_Spider
from eupas.commands import PandasCommand
from eupas.items import Study


class Command(PandasCommand):

    commands = frozenset(['match', 'cancel', 'substances'])

    matched_meta_field_name_prefix = '$MATCHED'
    matching_fields = ['centre_name', 'centre_name_of_investigator']
    match_checking_file_name_prefix = 'missing'
    match_checking_fields = matching_fields

    study_cancelled_meta_field_name = '$CANCELLED'
    study_cancelled_patterns = [
        # Best predictor of cancellation
        r'\bcancel',

        # Many false positives; could be reduced by checking the occurence of the words:
        # study, studies, trial, PAS, PASS, PAES, MAH, market authorisation etc. in the same sentence
        r'\bdiscontinu',

        # False positives in pregnancy related studies can be reduced by using the new regex
        # instead pf the old: r'\bterminat'
        r'\b(?<!pregnancy )(?<!elective )(?<!medical )(?<!induced )terminat'

        # No matches found for halted etc.
        r'\bhalt',

        # Only three false positive matches => not completly
        # r'\bnot\s+complet',

        # One false positive, one unclear and one true positive
        r'\bsuspend',

        # Many false positives
        # Also matches withdrawal of meds and withdrawal reactions, withdrawal of consent, etc.
        # r'\bwithdr(?:a|e)w',
        # Cant exclude withdrawal because i.e. withdrawal of the MAH, withdrawal of the study
        r'\bwithdr(?:a|e)w(?!\S*\s+(?:reaction|symptom|rate|(?:of|from)\s+(?:\b\S+\b\s+)?(?:consent|treatment|drug|medication)))',

        # No matches found for revoked etc.
        r'\brevok',

        # The term abort has practically no relevance for study cancellation and is also used often in
        # pregnancy related studies. It can be improved in a similiar way to the termination term
        # r'\babort'
        # r'\b(?<!spontaneous )(?<!medical )(?<!induced )(?<!elective termination/)abort'

        # Only matches treatment interruption and the term "interrupted time series"
        # r'\binterrupt'

        # No matches found for abandoned etc.
        r'\abandon'
    ]

    updated_state_meta_field_name = '$UPDATED_state'

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
        patch.add_argument(
            "-c",
            "--check-matched",
            action="store_true",
            help="checks if all fields matched and sets the correct exitcode based on the result"
        )
        patch.add_argument(
            "-d",
            "--detailed-cancel-fields",
            action="store_true",
            help="adds additional cancel fields"
        )

    def process_options(self, args, opts):
        PandasCommand.process_options(self, args, opts)
        self.match_enabled = 'match' in args
        self.check_matched = self.match_enabled and opts.check_matched

        self.cancel_enabled = 'cancel' in args
        self.detailed_cancel_fields = self.cancel_enabled and opts.detailed_cancel_fields

        self.substances_enabled = 'substances' in args

        self.match_path = Path(opts.match_input or "")

        if not self.match_path.is_file() and self.match_enabled:
            raise UsageError(
                "Invalid -m value, use a valid path to a file", print_help=False)

        if self.match_path.suffix != '.xlsx' and self.match_enabled:
            raise UsageError(
                "Invalid -m value, xlsx file expected", print_help=False)

    def syntax(self):
        return "matching_field_names [options]"

    def short_desc(self):
        return "Patch the input file by matching the provided field_names"

    def run(self, args, opts):
        super().run(args, opts)
        import numpy as np

        if len(args) == 0:
            raise UsageError(
                "running 'scrapy patch' without additional arguments is not supported"
            )

        if not set(args).issubset(self.commands):
            raise UsageError(
                f"running 'scrapy patch' without one of these commands is not supported: {', '.join(self.commands)}"
            )

        self.logger = logging.getLogger()
        self.logger.info('Starting patch script')
        self.logger.info(f'Pandas {self.pd.__version__}')
        self.logger.info('Reading input data...')
        data = self.read_input()

        matched_combined_field_name = f'{self.matched_meta_field_name_prefix}_combined_name'
        if self.match_enabled:
            self.logger.info('Start matching')

            if not set(self.matching_fields).issubset(set(Study.fields)):
                raise UsageError(
                    "At least one patch value isn't a valid field name", print_help=False)

            self.logger.info('\tReading matching data...')
            matching_data = self.pd.read_excel(
                self.match_path, sheet_name=self.matching_fields, header=None, keep_default_na=False, na_values=self.na_values, na_filter=True)

            for field_name in self.matching_fields:
                # NOTE: If validation fails: check for duplicate original values in the matching file or values matching the na_values
                data = self.pd.merge(
                    data,
                    matching_data[field_name].iloc[:, 1:].rename(columns={
                        1: f'{self.matched_meta_field_name_prefix}_{field_name}',
                        2: field_name
                    }),
                    how='left',
                    on=field_name,
                    validate='m:1'
                )

            data[matched_combined_field_name] = data[[f'{self.matched_meta_field_name_prefix}_{field_name}' for field_name in self.matching_fields]].apply(
                lambda x: ''.join([str(y) for y in x.values if isinstance(y, str)]), axis='columns')
            data.loc[data[matched_combined_field_name] ==
                     '', matched_combined_field_name] = self.pd.NA
            self.logger.info('Matching finished')

            if self.check_matched:
                self.logger.info('Start match checking')
                not_matched = data.loc[data[matched_combined_field_name].isna()]
                if not not_matched.empty:
                    check_match_data = {
                        field: sorted(list(set(not_matched.loc[data[field].notna(), field].values))) for field in self.match_checking_fields
                    }

                    with open(self.output_folder / f'{self.match_checking_file_name_prefix}_all.json', 'w', encoding='utf-8') as f:
                        json.dump(check_match_data, f,
                                  indent='\t', ensure_ascii=False)

                    for field_name, missing in check_match_data.items():
                        with open(self.output_folder / f'{self.match_checking_file_name_prefix}_{field_name}.txt', 'w', encoding='utf-8') as f:
                            f.write('\n'.join(missing))

                    self.exitcode = 1

                self.logger.info('Match checking finished')

        if self.cancel_enabled:
            self.logger.info('Start cancel detection')
            data[self.study_cancelled_meta_field_name] = data['description'].str.contains(
                '|'.join(self.study_cancelled_patterns), case=False)

            data[self.updated_state_meta_field_name] = data['state']
            query = data[self.study_cancelled_meta_field_name].fillna(False)
            data.loc[query, self.updated_state_meta_field_name] = 'Cancelled'
            self.logger.info('Cancel detection finished')

            if self.detailed_cancel_fields:
                self.logger.info('Start adding detailed cancel fields')

                # TODO: Only extracts first match
                data[f'{self.study_cancelled_meta_field_name}_extracted_word'] = data['description'].str.extract('|'.join(
                    [fr'({x}\S*\b)' for x in self.study_cancelled_patterns]), flags=re.IGNORECASE).apply(
                        lambda x: '; '.join([str(y) for y in x.values if isinstance(y, str)]), axis='columns')

                # TODO: Only extracts first matched sentence
                data[f'{self.study_cancelled_meta_field_name}_extracted_sentence'] = data['description'].str.extract('|'.join(
                    [fr'(\b[^.!?]*{x}\S*\b[^.!?]*(?P<end{i}>[.!?]+)?(?(end{i})|$))' for i, x in enumerate(self.study_cancelled_patterns)]),
                    flags=re.IGNORECASE).apply(
                        lambda x: '; '.join([str(y) for y in x.values if not re.match(r'nan|[.!?]+', str(y))]), axis='columns')

                self.logger.info('Added detailed cancel fields')

        if self.substances_enabled:
            self.logger.info('Start substance matching')
            substance_inn = data.loc[data['substance_inn'].notna(), [
                'substance_inn']]
            substance_inn = substance_inn.drop_duplicates().assign(cleaned_inn=substance_inn['substance_inn'].str.split(
                '; ')).explode('cleaned_inn')
            substance_inn['cleaned_inn'] = substance_inn['cleaned_inn'].str.strip(
            ).str.replace(r'\s+\(\S+\)$', '', regex=True)
            substance_inn.drop_duplicates().reset_index(drop=True)

            substance_atc = data.loc[data['substance_atc'].notna(), [
                'substance_atc']]
            substance_atc = substance_atc.drop_duplicates().assign(cleaned_atc_code=substance_atc['substance_atc'].str.split(
                '; ')).explode('cleaned_atc_code')
            substance_atc = substance_atc.assign(cleaned_atc_value=substance_atc['cleaned_atc_code'].str.strip(
            ).str.replace(r'\S+\b\s*\((.*)\)$', r'\1', regex=True))
            substance_atc['cleaned_atc_code'] = substance_atc['cleaned_atc_code'].str.strip(
            ).str.replace(r'\s+\(.+\)$', '', regex=True)
            substance_atc.drop_duplicates().reset_index(drop=True)

            self.logger.info('\tAquiring KEGG data...')
            kegg = self.pd.DataFrame()
            kegg_db_path = self.output_folder / 'kegg.txt'
            if kegg_db_path.is_file():
                kegg = self.pd.DataFrame(
                    [row.split('\t') for row in kegg_db_path.read_text().split('\n')[:-1]], columns=['kegg_drug_entry_id', 'all_kegg_drug_names'])
            else:
                response = requests.get('https://rest.kegg.jp/list/drug')
                if response.ok:
                    kegg_db_path.write_text(response.text)
                    kegg = self.pd.DataFrame([row.split('\t') for row in response.text.split('\n')[
                                             :-1]], columns=['kegg_drug_entry_id', 'all_kegg_drug_names'])
                else:
                    raise RuntimeError('KEGG rest api not responding ok.')

            kegg = kegg.assign(cleaned_kegg_drug_name=kegg['all_kegg_drug_names'].str.split(
                '; ')).explode('cleaned_kegg_drug_name')
            kegg['cleaned_kegg_drug_name'] = kegg['cleaned_kegg_drug_name'].str.strip(
            ).str.replace(r'\s+\(\S+\)$', '', regex=True)
            kegg = kegg.drop_duplicates().reset_index(drop=True)
            # self.write_output(kegg, '_kegg')

            self.logger.info('\tMatching INN to KEGG...')

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
                        match = exact_matches.iloc[[0]]

                    matching_rows = self.pd.concat([matching_rows, match.assign(
                        **{assigned_column: search_term, 'info': info})], ignore_index=True)

                return matching_rows.reset_index(drop=True)

            matching_rows = get_matching_rows(substance_inn['cleaned_inn'].dropna(
            ).unique(), kegg, 'cleaned_kegg_drug_name', 'cleaned_inn')

            self.logger.info('\tAquiring matched INN details from KEGG...')
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

            self.logger.info('\tMerging INN data...')
            matching_rows = matching_rows.merge(
                right=kegg_details, on='kegg_drug_entry_id', how='left')
            substance_inn = substance_inn.merge(
                right=matching_rows, on='cleaned_inn', how='left')

            self.logger.info('\tAquiring ATC data...')
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

            self.logger.info('\tMerging ATC data...')
            substance_atc = substance_atc.merge(
                right=who_atc, left_on='cleaned_atc_code', right_on='atc_code', how='left')

            self.logger.info('\tFill missing ATC data...')
            substance_atc = substance_atc.assign(info=np.where(
                substance_atc['atc_code'].notna(), 'Full match', self.pd.NA))

            matching_rows = get_matching_rows(substance_atc.loc[substance_atc['atc_code'].isna(
            ), 'cleaned_atc_value'].unique(), who_atc, 'atc_value', 'cleaned_atc_value', 'ATC Value: ')

            self.logger.info('\tMerging ATC data...')
            merge_suffix = '_merge'
            substance_atc = substance_atc.merge(
                right=matching_rows, on='cleaned_atc_value', how='left', suffixes=(None, merge_suffix))
            override_columns = ['atc_code', 'atc_value', 'info']
            substance_atc.loc[substance_atc['atc_code'].isna(), override_columns] = substance_atc.loc[substance_atc['atc_code'].isna(
            ), [f'{column}{merge_suffix}' for column in override_columns]].rename(lambda x: x.removesuffix(merge_suffix), axis='columns')
            substance_atc.drop(
                columns=[f'{column}{merge_suffix}' for column in override_columns], inplace=True)

            self.logger.info('\tWriting substance data...')
            self.write_output(substance_inn, '_substance_inn')
            self.write_output(substance_atc, '_substance_atc')
            self.logger.info('Substance matching finished')

        self.logger.info('Writing output data...')
        self.write_output(data, '_patched')
