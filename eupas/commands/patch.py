import json
import logging
from pathlib import Path
import re

from scrapy.exceptions import UsageError

from eupas.commands import PandasCommand
from eupas.items import Study


class Command(PandasCommand):

    commands = frozenset(['centre_match', 'substance_match', 'cancel'])

    matched_meta_field_name_prefix = '$MATCHED'

    centre_match_fields = ['centre_name', 'centre_name_of_investigator']

    centre_match_checking_fields = centre_match_fields
    centre_match_missing_file_name_prefix = 'missing'

    substance_match_fields = ['sustance_atc', 'substance_inn']

    study_cancelled_meta_field_name = '$CANCELLED'

    # TODO: Train logistic regression model with ML-Approach
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
        '''
        Adds custom options to the base pandas command.
        '''
        PandasCommand.add_options(self, parser)
        patch = parser.add_argument_group(title="Custom Patching Options")
        patch.add_argument(
            "-ci",
            "--centre-match-input",
            metavar="FILE",
            default=None,
            help="path to the centre matching file"
        )
        patch.add_argument(
            "-cc",
            "--check-centre-match",
            action="store_true",
            help="checks if all centre fields matched and sets the correct exitcode based on the result"
        )
        patch.add_argument(
            "-si",
            "--substance-match-input",
            metavar="FILE",
            default=None,
            help="path to the substance matching file"
        )
        patch.add_argument(
            "-dcf",
            "--detailed-cancel-fields",
            action="store_true",
            help="adds additional cancel fields"
        )

    def process_options(self, args, opts):
        PandasCommand.process_options(self, args, opts)
        self.centre_match_enabled = 'centre_match' in args
        self.check_centre_match = self.centre_match_enabled and opts.check_centre_match

        self.substance_match_enabled = 'substance_match' in args

        self.cancel_enabled = 'cancel' in args
        self.detailed_cancel_fields = self.cancel_enabled and opts.detailed_cancel_fields

        self.centre_match_path = Path(opts.centre_match_input or "")

        if not self.centre_match_path.is_file() and self.centre_match_enabled:
            raise UsageError(
                "Invalid -ci value, use a valid path to a file", print_help=False)

        if self.centre_match_path.suffix != '.xlsx' and self.centre_match_enabled:
            raise UsageError(
                "Invalid -ci value, xlsx file expected", print_help=False)

        self.substance_match_path = Path(opts.substance_match_input or "")

        if not self.substance_match_path.is_file() and self.substance_match_enabled:
            raise UsageError(
                "Invalid -si value, use a valid path to a file", print_help=False)

        if self.substance_match_path.suffix != '.xlsx' and self.substance_match_enabled:
            raise UsageError(
                "Invalid -si value, xlsx file expected", print_help=False)

    def syntax(self):
        return "patch_name [options]"

    def short_desc(self):
        return "Patch the input file by matching the provided field_names"

    def run(self, args, opts):
        '''
        Performs different tasks based on arguments:
            centre_match        This will unify the centre_name columns with the help of a matching spreadsheet\n
            substance_match     This will unify the substances columns with the help of a matching spreadsheet\n
            cancel              This will find cancelled studies with a list of regex patterns
        '''
        super().run(args, opts)

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

        if self.centre_match_enabled:
            self.logger.info('Start centre matching')

            if not set(self.centre_match_fields).issubset(set(Study.fields)):
                raise UsageError(
                    "At least one centre match value isn't a valid field name", print_help=False)

            self.logger.info('\tReading centre matching data...')
            matching_data = self.pd.read_excel(
                self.centre_match_path,
                sheet_name=self.centre_match_fields,
                keep_default_na=False,
                na_values=self.na_values,
                na_filter=True
            )

            for field_name in self.centre_match_fields:
                # NOTE: If validation fails: check for duplicate original values in the matching file or values matching the na_values
                data = self.pd.merge(
                    data,
                    matching_data[field_name].loc[:, ['manual', 'original']].rename(
                        columns={
                            'manual': f'{self.matched_meta_field_name_prefix}_{field_name}',
                            'original': field_name
                        }
                    ),
                    how='left',
                    on=field_name,
                    validate='m:1'
                )

            centre_match_combined_field_name = f'{self.matched_meta_field_name_prefix}_combined_centre_name'

            data[centre_match_combined_field_name] = \
                data[[f'{self.matched_meta_field_name_prefix}_{field_name}' for field_name in self.centre_match_fields]] \
                .apply(lambda x: ''.join([str(y) for y in x.values if isinstance(y, str)]), axis='columns')
            data.loc[data[centre_match_combined_field_name] ==
                     '', centre_match_combined_field_name] = self.pd.NA
            self.logger.info('Centre matching finished')

            if self.check_centre_match:
                self.logger.info('Start centre match checking')
                not_matched = data.loc[
                    data[centre_match_combined_field_name].isna()
                ]
                if not not_matched.empty:
                    check_match_data = {
                        field: not_matched.loc[
                            data[field].notna(), field
                        ].drop_duplicates().sort_values().tolist()
                        for field in self.centre_match_checking_fields
                    }

                    with open(self.output_folder / f'{self.centre_match_missing_file_name_prefix}_all.json', 'w', encoding='utf-8') as f:
                        json.dump(
                            check_match_data,
                            f,
                            indent='\t',
                            ensure_ascii=False
                        )

                    for field_name, missing in check_match_data.items():
                        with open(self.output_folder / f'{self.centre_match_missing_file_name_prefix}_{field_name}.txt', 'w', encoding='utf-8') as f:
                            f.write('\n'.join(missing))

                    # NOTE: The pipeline should fail if new names have to be matched
                    self.exitcode = 1

                self.logger.info('Centre match checking finished')

        if self.substance_match_enabled:
            self.logger.info('Start substance matching')

            if not set(self.substance_match_fields).issubset(set(Study.fields)):
                raise UsageError(
                    "At least one substance match value isn't a valid field name", print_help=False)

            self.logger.info('\tReading substance matching data...')
            matching_data = self.pd.read_excel(
                self.substance_match_path, sheet_name=self.substance_match_fields, keep_default_na=False, na_values=self.na_values, na_filter=True)

            # print(matching_data)
            raise NotImplementedError()

        if self.cancel_enabled:
            self.logger.info('Start cancel detection')
            data[self.study_cancelled_meta_field_name] = data['description'].str.contains(
                '|'.join(self.study_cancelled_patterns), case=False)

            data[self.updated_state_meta_field_name] = data['state']
            query = data[self.study_cancelled_meta_field_name].fillna(False)
            data.loc[query, self.updated_state_meta_field_name] = 'Cancelled'
            self.logger.info('Cancel detection finished')

            # TODO: Extract all matches; useful for creating better regex
            if self.detailed_cancel_fields:
                self.logger.info('Start adding detailed cancel fields')

                # NOTE: Only extracts first match
                data[f'{self.study_cancelled_meta_field_name}_extracted_word'] = data['description'].str.extract('|'.join(
                    [fr'({x}\S*\b)' for x in self.study_cancelled_patterns]), flags=re.IGNORECASE).apply(
                        lambda x: '; '.join([str(y) for y in x.values if isinstance(y, str)]), axis='columns')

                # NOTE: Only extracts first matched sentence
                data[f'{self.study_cancelled_meta_field_name}_extracted_sentence'] = data['description'].str.extract('|'.join(
                    [fr'(\b[^.!?]*{x}\S*\b[^.!?]*(?P<end{i}>[.!?]+)?(?(end{i})|$))' for i, x in enumerate(self.study_cancelled_patterns)]),
                    flags=re.IGNORECASE).apply(
                        lambda x: '; '.join([str(y) for y in x.values if not re.match(r'nan|[.!?]+', str(y))]), axis='columns')

                self.logger.info('Added detailed cancel fields')

        self.logger.info('Writing output data...')
        self.write_output(data, '_patched')
