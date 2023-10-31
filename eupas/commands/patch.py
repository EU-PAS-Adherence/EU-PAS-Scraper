import logging
from pathlib import Path
import re

from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError

from eupas.items import Study


class Command(ScrapyCommand):

    requires_project = True

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
        ScrapyCommand.add_options(self, parser)
        patch = parser.add_argument_group(title="Custom Patching Options")
        patch.add_argument(
            "-i",
            "--input",
            metavar="FILE",
            default=None,
            help="path to the input file"
        )
        patch.add_argument(
            "-o",
            "--output",
            metavar="FOLDER",
            default=None,
            help="path to the output folder"
        )
        patch.add_argument(
            "-m",
            "--matchinput",
            metavar="FILE",
            default=None,
            help="path to the matching file"
        )

    def process_options(self, args, opts):
        ScrapyCommand.process_options(self, args, opts)
        self.match_enabled = 'match' in args
        self.cancel_enabled = 'cancel' in args

        self.input_path = Path(opts.input or "")
        if not self.input_path.is_file():
            raise UsageError(
                "Invalid -i value, use a valid path to a file", print_help=False)
        if self.input_path.suffix not in ['.csv', '.json', '.xlsx', '.xml']:
            raise UsageError(
                "Invalid -i value, file format not supported", print_help=False)

        self.output_folder = Path(opts.output or "")
        if not self.output_folder.is_dir():
            raise UsageError(
                "Invalid -i value, use a valid path to a folder", print_help=False)
        self.output_folder.mkdir(parents=True, exist_ok=True)

        self.match_path = Path(opts.matchinput or "")
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
        import pandas as pd

        self.logger.info('Reading input data...')
        input_data = None
        if self.input_path.suffix == '.csv':
            input_data = pd.read_csv(self.input_path)
        elif self.input_path.suffix == '.json':
            input_data = pd.read_json(self.input_path)
        elif self.input_path.suffix == '.xlsx':
            input_data = pd.read_excel(self.input_path).iloc[:, 1:]
            input_data.rename(
                columns=lambda x: '_'.join(
                    [word.lower() for word in x.split(' ')]) if x[0] != '$' else x,
                inplace=True
            )
        elif self.input_path.suffix == '.xml':
            input_data = pd.read_xml(self.input_path)

        # print(input_data)
        output_data = input_data
        if self.match_enabled:
            if not set(self.matching_fields).issubset(set(Study.fields)):
                raise UsageError(
                    "At least one patch value isn't a valid field name", print_help=False)

            self.logger.info('Reading matching data...')
            matching_data = pd.read_excel(
                self.match_path, sheet_name=self.matching_fields, header=None)

            self.logger.info('Start matching')
            for field_name in self.matching_fields:
                output_data = pd.merge(
                    # TODO: remove after deleting not applicable or change it so that only '' and None are
                    # Has to be done because of merging problems with the value not applicable
                    output_data.fillna(value={field_name: ''}),
                    matching_data[field_name].iloc[:, 1:].rename(columns={
                        1: f'{self.matched_meta_field_name_prefix}_{field_name}',
                        2: field_name
                    }),
                    how='left',
                    on=field_name,
                    # validate='m:1' Problem with not applicable!
                )
            output_data[f'{self.matched_meta_field_name_prefix}_combined_name'] = output_data[[f'{self.matched_meta_field_name_prefix}_{field_name}' for field_name in self.matching_fields]].apply(
                lambda x: ''.join([str(y) for y in x.values if str(y) != "nan"]), axis='columns')
            self.logger.info('Matching finished')

        if self.cancel_enabled:
            self.logger.info('Start cancel detection')
            output_data[self.study_cancelled_meta_field_name] = output_data['description'].str.contains(
                '|'.join(self.study_cancelled_patterns), case=False)
            output_data[f'{self.study_cancelled_meta_field_name}_extracted_word'] = output_data['description'].str.extract('|'.join(
                [fr'({x}\S*\b)' for x in self.study_cancelled_patterns]), flags=re.IGNORECASE).apply(lambda x: '; '.join([str(y) for y in x.values if str(y) != "nan"]), axis='columns')
            output_data[f'{self.study_cancelled_meta_field_name}_extracted_sentence'] = output_data['description'].str.extract('|'.join(
                [fr'(\b[^.!?]*{x}\S*\b[^.!?]*(?P<end{i}>[.!?]+)?(?(end{i})|$))' for i, x in enumerate(self.study_cancelled_patterns)]), flags=re.IGNORECASE).apply(lambda x: '; '.join([str(y) for y in x.values if not re.match(r'nan|[.!?]+', str(y))]), axis='columns')

            output_data[self.updated_state_meta_field_name] = output_data['state']
            query = output_data[self.study_cancelled_meta_field_name].fillna(
                False) == True
            output_data.loc[query,
                            self.updated_state_meta_field_name] = 'Cancelled'
            self.logger.info('Cancel detection finished')

        self.logger.info('Writing output data...')
        output_path = self.output_folder / self.input_path.name
        if output_path.suffix == '.csv':
            output_data.to_csv(output_path)
        elif output_path.suffix == '.json':
            output_data.to_json(
                output_path, orient='records', force_ascii=False)
        elif output_path.suffix == '.xlsx':
            output_data.rename(
                columns=lambda x: ' '.join(
                    [word.capitalize() for word in x.split('_')]) if x[0] != '$' else x,
                inplace=True
            )
            output_data.to_excel(output_path, sheet_name='PAS')
        elif output_path.suffix == '.xml':
            output_data.to_xml(output_path)
        self.logger.info('Patching finished')
