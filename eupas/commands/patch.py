import json
import logging
from pathlib import Path
import re

from scrapy.exceptions import UsageError

from eupas.commands import PandasCommand
from eupas.items import EU_PAS_Study, EMA_RWD_Study


class Command(PandasCommand):

    commands = frozenset(['match', 'state', 'cancel'])

    matched_meta_field_name_prefix = '$MATCHED'

    match_map = {
        EU_PAS_Study: [
            'centre_name',
            'centre_name_of_investigator',
            # 'eu_pas_register_number'
        ],
        EMA_RWD_Study: [
            'lead_institution_encepp',
            'lead_institution_not_encepp',
            # 'eu_pas_register_number'
        ]
    }

    match_checking_map = {
        EU_PAS_Study: [
            'centre_name',
            'centre_name_of_investigator',
        ],
        EMA_RWD_Study: [
            'lead_institution_encepp',
            'lead_institution_not_encepp',
        ]
    }
    match_missing_file_name_prefix = 'missing'

    study_cancelled_meta_field_name = '$CANCELLED'

    # TODO: Train logistic regression model with ML-Approach
    study_cancelled_patterns = [
        # NOTE: Best predictor of cancellation
        r'\bcancel',
        # NOTE: r'\bdiscontinue(?!\s|s|rs)' has less false positives
        r'\bdiscontinu',
        # NOTE: False positives with r'\bterminat' in pregnancy related studies can be reduced by using the regex below
        r'\b(?<!pregnancy )(?<!elective )(?<!medical )(?<!induced )terminat',
        # NOTE: No matches found for halted, etc.
        r'\bhalt',
        # NOTE: r'\bsuspend(?!\s)' has less false positives
        r'\bsuspend',
        # NOTE: r'\bwithdr(?:a|e)w' has many false positives and matches withdrawal of meds as well as withdrawal reactions, withdrawal of consent, etc.
        # Cant exclude the word withdrawal because i.e. withdrawal of the MAH, withdrawal of the study
        r'\bwithdr(?:a|e)w(?!\S*\s+(?:reaction|symptom|rate|(?:of|from)\s+(?:\b\S+\b\s+)?(?:consent|treatment|drug|medication)))',
        # NOTE: No matches found for revoked etc.
        r'\brevok',
        # NOTE: The expression r'\babort' for words like abort has practically no relevance for study cancellation and is also used often in
        # pregnancy related studies. It can be improved in a similiar way to the termination term
        r'\b(?<!spontaneous )(?<!medical )(?<!induced )(?<!elective termination/)abort',
        # NOTE: r'\binterrupt' only matches treatment interruption and the term "interrupted time series". Can be improved:
        r'\binterrupt(?!\S*(?:\s+|-)time)',
        # NOTE: No matches found for abandoned etc.
        r'\babandon',
        # NOTE: r'\bstop(?!\s)' has less false positives
        r'\bstop',
        # NOTE: No matches found for called off
        r'\bcalled off',
        # NOTE: No matches found for ceased, etc.
        r'\bcease',
        # NOTE: Many words use the stem end; Only the verb end should be selected
        r'\bend(?:ed|s|ing)',
        # NOTE: No matches found for scrapped, etc.
        r'\bscrapp',
        # NOTE: No matches found for scrubbed, etc.
        r'\bscrubb',
        # NOTE: No matches found for forsaken, etc.
        r'\bforsak',
        # NOTE: Only one match
        r'\bdissolv'
    ]
    updated_state_meta_field_name = '$UPDATED_state'

    def add_options(self, parser):
        '''
        Adds custom options to the base pandas command.
        '''
        PandasCommand.add_options(self, parser)
        patch = parser.add_argument_group(title="Custom Patching Options")
        patch.add_argument(
            "-mi",
            "--match-input",
            metavar="FILE",
            default=None,
            help="path to the matching file"
        )
        patch.add_argument(
            "-mc",
            "--match-check",
            action="store_true",
            help="checks if all match_checking fields matched and sets the correct exitcode based on the result"
        )
        patch.add_argument(
            "--match-eupas",
            action="store_true",
            default=False,
            help="matches based on the fields of the EU PAS Register instead of the EMA RWD Catalogue"
        )
        patch.add_argument(
            "-ac",
            "--add-cancel-fields",
            action="store_true",
            help="adds additional cancel fields"
        )
        patch.add_argument(
            "-sc",
            "--save-cancel-samples",
            action="store_true",
            help="saves samples to check sensitivity of cancel field"
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

        # Matching
        self.matching_enabled = 'match' in args
        self.match_type = EU_PAS_Study if opts.match_eupas else EMA_RWD_Study
        self.match_checking_enabled = self.matching_enabled and opts.match_check
        self.match_input_path = Path(opts.match_input or "")
        if self.matching_enabled:
            validate_path(self.match_input_path, '-mi')

        # State Update
        self.update_state_enabled = 'state' in args

        # Regex Cancel
        self.cancel_enabled = 'cancel' in args
        self.add_cancel_fields_enabled = self.cancel_enabled and opts.add_cancel_fields
        self.save_cancel_samples_enabled = self.cancel_enabled and opts.save_cancel_samples

    def syntax(self):
        return "patch_name [options]"

    def short_desc(self):
        return "Patch the input file by matching the provided field_names"

    def run(self, args, opts):
        '''
        Performs different tasks based on arguments:
            match        This will unify the centre or institution columns with the help of a matching spreadsheet\n
            state        This will create a correct state column based on the dates provided\n
            cancel       This will find cancelled studies with a list of regex patterns
        '''
        import numpy as np
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

        if self.matching_enabled:
            self.logger.info('Start matching')

            match_fields = self.match_map[self.match_type]

            if not set(match_fields).issubset(set(self.match_type.fields)):
                raise UsageError(
                    "At least one match value isn't a valid field name", print_help=False)

            self.logger.info('\tReading matching data...')
            matching_data = self.pd.read_excel(
                self.match_input_path,
                sheet_name=match_fields,
                keep_default_na=False,
                na_values=self.na_values,
                na_filter=True
            )

            for field_name in match_fields:
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

            match_combined_field_name = f'{self.matched_meta_field_name_prefix}_combined_centre_name'

            data[match_combined_field_name] = \
                data.filter(like=self.matched_meta_field_name_prefix) \
                .apply(lambda x: ''.join([str(y) for y in x.values if isinstance(y, str)]), axis='columns')

            data.loc[
                data[match_combined_field_name] == '',
                match_combined_field_name
            ] = self.pd.NA

            self.logger.info('Matching finished')

            if self.match_checking_enabled:
                self.logger.info('Start match checking')

                not_matched = data.loc[
                    data[match_combined_field_name].isna()
                ]

                if not not_matched.empty:
                    check_match_data = {
                        field: not_matched.loc[
                            data[field].notna(), field
                        ].drop_duplicates().sort_values().tolist()
                        for field in self.match_checking_map[self.match_type]
                    }

                    with open(self.output_folder / f'{self.match_missing_file_name_prefix}_all.json', 'w', encoding='utf-8') as f:
                        json.dump(
                            check_match_data,
                            f,
                            indent='\t',
                            ensure_ascii=False
                        )

                    for field_name, missing in check_match_data.items():
                        with open(self.output_folder / f'{self.match_missing_file_name_prefix}_{field_name}.txt', 'w', encoding='utf-8') as f:
                            f.write('\n'.join(missing))

                    # NOTE: The pipeline should fail if new names have to be matched
                    self.exitcode = 1

                self.logger.info('Match checking finished')

        if self.update_state_enabled:
            self.logger.info('Start updating states')

            data = data.assign(**{
                self.updated_state_meta_field_name:
                    np.where(
                        data['final_report_date_actual'].notna(),
                        'Finalised',
                        np.where(
                            data['data_collection_date_actual'].notna(),
                            'Ongoing',
                            np.where(
                                data['funding_contract_date_planed'].notna()
                                | data['funding_contract_date_actual'].notna(),
                                'Planned',
                                self.pd.NA
                            )
                        )
                    ),
                f'{self.updated_state_meta_field_name}_eq_state': lambda x: x[self.updated_state_meta_field_name] == x['state']
            })

            self.logger.info('Finished updating states')

        if self.cancel_enabled:
            self.logger.info('Start regex cancel detection')

            data[f'{self.study_cancelled_meta_field_name}_REGEX'] = data['description'].str \
                .contains('|'.join(self.study_cancelled_patterns), case=False)
            data[f'{self.study_cancelled_meta_field_name}_MANUAL'] = data[f'{self.study_cancelled_meta_field_name}_REGEX']

            self.logger.info('Finished regex cancel detection')

            # TODO: Extract all matches; useful for creating better regex
            if self.add_cancel_fields_enabled:
                self.logger.info('Start adding detailed cancel fields')

                # NOTE: Only extracts first match
                data[f'{self.study_cancelled_meta_field_name}_REGEX_extracted_word'] = data['description'].str \
                    .extract('|'.join([fr'({x}\S*\b)' for x in self.study_cancelled_patterns]), flags=re.IGNORECASE) \
                    .apply(lambda x: '; '.join([str(y) for y in x.values if isinstance(y, str)]), axis='columns')

                # NOTE: Only extracts first matched sentence
                data[f'{self.study_cancelled_meta_field_name}_REGEX_extracted_sentence'] = data['description'].str \
                    .extract('|'.join([
                        fr'(\b[^.!?]*{x}\S*\b[^.!?]*(?P<end{i}>[.!?]+)?(?(end{i})|$))'
                        for i, x in enumerate(self.study_cancelled_patterns)
                    ]), flags=re.IGNORECASE) \
                    .apply(lambda x: '; '.join([str(y) for y in x.values if not re.match(r'nan|[.!?]+', str(y))]), axis='columns')

                self.logger.info('Finished adding detailed cancel fields')

            if self.save_cancel_samples_enabled:
                self.logger.info(
                    'Start generating samples to check sensitivity of cancel fields')
                samples = data.loc[
                    # NOTE: astype(bool) fills NA with True
                    ~data[f'{self.study_cancelled_meta_field_name}_REGEX']
                    .astype(bool)
                ].sample(
                    frac=0.05,
                    random_state=123  # NOTE: For reproducibility
                )
                self.write_output(samples, '_cancel_samples')
                self.logger.info('Finished generating samples')

        self.logger.info('Writing output data...')
        self.write_output(data, '_patched')
