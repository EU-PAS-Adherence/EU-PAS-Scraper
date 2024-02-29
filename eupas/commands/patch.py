import json
import logging
from pathlib import Path
import re

from scrapy.exceptions import UsageError

from eupas.commands import PandasCommand
from eupas.items import EU_PAS_Study


class Command(PandasCommand):

    commands = frozenset(['centre_match', 'cancel'])

    matched_meta_field_name_prefix = '$MATCHED'

    centre_match_fields = [
        'centre_name',
        'centre_name_of_investigator',
        # 'eu_pas_register_number'
    ]
    centre_match_checking_fields = [
        'centre_name',
        'centre_name_of_investigator'
    ]
    centre_match_missing_file_name_prefix = 'missing'

    study_cancelled_meta_field_name = '$CANCELLED'

    # TODO: Train logistic regression model with ML-Approach
    study_cancelled_patterns = [
        # NOTE: Best predictor of cancellation
        r'\bcancel',
        # NOTE: r'\bdiscontinu' has many false positives; could be reduced by checking the occurence of the words:
        # study, studies, trial, PAS, PASS, PAES, MAH, market authorisation etc. in the same sentence
        r'\bdiscontinue(?!\s|s|rs)',
        # NOTE: False positives with r'\bterminat' in pregnancy related studies can be reduced by using the regex below
        r'\b(?<!pregnancy )(?<!elective )(?<!medical )(?<!induced )terminat',
        # NOTE: No matches found for halted, etc.
        r'\bhalt',
        # NOTE: r'\bsuspend' has one false positive, one unclear and one true positive
        r'\bsuspend(?!\s)',
        # NOTE: r'\bwithdr(?:a|e)w' has many false positives and matches withdrawal of meds as well as withdrawal reactions, withdrawal of consent, etc.
        # Cant exclude the word withdrawal because i.e. withdrawal of the MAH, withdrawal of the study
        r'\bwithdr(?:a|e)w(?!\S*\s+(?:reaction|symptom|rate|(?:of|from)\s+(?:\b\S+\b\s+)?(?:consent|treatment|drug|medication)))',
        # NOTE: No matches found for revoked etc.
        r'\brevok',
        # NOTE: The expression r'\babort' for words like abort has practically no relevance for study cancellation and is also used often in
        # pregnancy related studies. It can be improved in a similiar way to the termination term
        # r'\b(?<!spontaneous )(?<!medical )(?<!induced )(?<!elective termination/)abort',
        # NOTE: r'\binterrupt' only matches treatment interruption and the term "interrupted time series". Can be improved:
        # r'\binterrupt(?!\S*(?:\s+|-)time)',
        # NOTE: No matches found for abandoned etc.
        r'\babandon',
        # NOTE: r'\bstop' will match the word stop with false positives
        r'\bstop(?!\s)'
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
            "-dcf",
            "--detailed-cancel-fields",
            action="store_true",
            help="adds additional cancel fields"
        )

    def process_options(self, args, opts):
        PandasCommand.process_options(self, args, opts)

        def validate_path(path, command, file_extension='.xlsx'):
            if not path.is_file():
                raise UsageError(
                    f"Invalid {command} value, use a valid path to a file", print_help=False)

            if path.suffix != file_extension:
                raise UsageError(
                    f"Invalid {command} value, xlsx file expected", print_help=False)

        # Centre Matching
        self.centre_match_enabled = 'centre_match' in args
        self.check_centre_match = self.centre_match_enabled and opts.check_centre_match
        self.centre_match_path = Path(opts.centre_match_input or "")
        if self.centre_match_enabled:
            validate_path(self.centre_match_path, '-ci')

        # Cancel Detection
        self.cancel_enabled = 'cancel' in args
        self.detailed_cancel_fields = self.cancel_enabled and opts.detailed_cancel_fields

    def syntax(self):
        return "patch_name [options]"

    def short_desc(self):
        return "Patch the input file by matching the provided field_names"

    def run(self, args, opts):
        '''
        Performs different tasks based on arguments:
            centre_match        This will unify the centre_name columns with the help of a matching spreadsheet\n
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

            if not set(self.centre_match_fields).issubset(set(EU_PAS_Study.fields)):
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
                data.filter(like=self.matched_meta_field_name_prefix) \
                .apply(lambda x: ''.join([str(y) for y in x.values if isinstance(y, str)]), axis='columns')
            data.loc[
                data[centre_match_combined_field_name] == '',
                centre_match_combined_field_name
            ] = self.pd.NA

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

        if self.cancel_enabled:
            self.logger.info('Start cancel detection')
            data[self.study_cancelled_meta_field_name] = data['description'].str \
                .contains('|'.join(self.study_cancelled_patterns), case=False)

            data[self.updated_state_meta_field_name] = data['state']
            query = data[self.study_cancelled_meta_field_name].astype(bool) \
                & data[self.study_cancelled_meta_field_name].notna()
            data.loc[query, self.updated_state_meta_field_name] = 'Cancelled'
            self.logger.info('Cancel detection finished')

            # TODO: Extract all matches; useful for creating better regex
            if self.detailed_cancel_fields:
                self.logger.info('Start adding detailed cancel fields')

                # NOTE: Only extracts first match
                data[f'{self.study_cancelled_meta_field_name}_extracted_word'] = data['description'].str \
                    .extract('|'.join([fr'({x}\S*\b)' for x in self.study_cancelled_patterns]), flags=re.IGNORECASE) \
                    .apply(lambda x: '; '.join([str(y) for y in x.values if isinstance(y, str)]), axis='columns')

                # NOTE: Only extracts first matched sentence
                data[f'{self.study_cancelled_meta_field_name}_extracted_sentence'] = data['description'].str \
                    .extract('|'.join([
                        fr'(\b[^.!?]*{x}\S*\b[^.!?]*(?P<end{i}>[.!?]+)?(?(end{i})|$))'
                        for i, x in enumerate(self.study_cancelled_patterns)
                    ]), flags=re.IGNORECASE) \
                    .apply(lambda x: '; '.join([str(y) for y in x.values if not re.match(r'nan|[.!?]+', str(y))]), axis='columns')

                self.logger.info('Added detailed cancel fields')

        self.logger.info('Writing output data...')
        self.write_output(data, '_patched')
