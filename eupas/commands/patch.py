import logging
from pathlib import Path
import re

from scrapy.exceptions import UsageError

from eupas.commands import PandasCommand
from eupas.items import Study


class Command(PandasCommand):

    commands = frozenset(['match', 'cancel'])

    matched_meta_field_name_prefix = '$MATCHED'
    matching_fields = ['centre_name', 'centre_name_of_investigator']

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
            help="path to the matching file"
        )

    def process_options(self, args, opts):
        PandasCommand.process_options(self, args, opts)
        self.match_enabled = 'match' in args
        self.cancel_enabled = 'cancel' in args

        self.match_path = Path(opts.match_input or "")
        self.check_match = self.match_enabled and opts.check_matched
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
        if len(args) == 0:
            raise UsageError(
                "running 'scrapy patch' without additional arguments is not supported"
            )
        if not set(args).issubset(self.commands):
            raise UsageError(
                f"running 'scrapy patch' without one of these commands is not supported: {', '.join(self.commands)}"
            )

        self.logger = logging.getLogger()
        self.logger.info('Start patching')
        self.logger.info('Reading input data...')
        data = self.read_input()

        matched_combined_field_name = f'{self.matched_meta_field_name_prefix}_combined_name'
        if self.match_enabled:
            if not set(self.matching_fields).issubset(set(Study.fields)):
                raise UsageError(
                    "At least one patch value isn't a valid field name", print_help=False)

            self.logger.info('Reading matching data...')
            matching_data = self.pd.read_excel(
                self.match_path, sheet_name=self.matching_fields, header=None, keep_default_na=False, na_values=self.na_values, na_filter=True)

            self.logger.info('Start matching')
            for field_name in self.matching_fields:
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
                lambda x: ''.join([str(y) for y in x.values if str(y) != "nan"]), axis='columns')
            data.loc[data[matched_combined_field_name] ==
                     '', matched_combined_field_name] = self.pd.NA
            self.logger.info('Matching finished')

        if self.check_match:
            self.logger.info('Start match checking')
            not_matched = data.loc[data[matched_combined_field_name].isna()]
            type1 = sorted(
                list(set(not_matched.loc[data['centre_name'].notna(), 'centre_name'].values)))
            type2 = sorted(list(set(not_matched.loc[data['centre_name_of_investigator'].notna(
            ), 'centre_name_of_investigator'].values)))
            print(len(type1), type1)
            print(len(type2), type2)
            self.logger.info('Match checking finished')

        if self.cancel_enabled:
            self.logger.info('Start cancel detection')
            data[self.study_cancelled_meta_field_name] = data['description'].str.contains(
                '|'.join(self.study_cancelled_patterns), case=False)
            data[f'{self.study_cancelled_meta_field_name}_extracted_word'] = data['description'].str.extract('|'.join(
                [fr'({x}\S*\b)' for x in self.study_cancelled_patterns]), flags=re.IGNORECASE).apply(lambda x: '; '.join([str(y) for y in x.values if str(y) != "nan"]), axis='columns')
            data[f'{self.study_cancelled_meta_field_name}_extracted_sentence'] = data['description'].str.extract('|'.join(
                [fr'(\b[^.!?]*{x}\S*\b[^.!?]*(?P<end{i}>[.!?]+)?(?(end{i})|$))' for i, x in enumerate(self.study_cancelled_patterns)]), flags=re.IGNORECASE).apply(lambda x: '; '.join([str(y) for y in x.values if not re.match(r'nan|[.!?]+', str(y))]), axis='columns')

            data[self.updated_state_meta_field_name] = data['state']
            query = data[self.study_cancelled_meta_field_name].fillna(False)
            data.loc[query,
                     self.updated_state_meta_field_name] = 'Cancelled'
            self.logger.info('Cancel detection finished')

        self.logger.info('Writing output data...')
        self.write_output(data, '_patched')
