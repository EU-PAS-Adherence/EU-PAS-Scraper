from pathlib import Path

from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError


class PandasCommand(ScrapyCommand):

    import pandas as pd
    import numpy as np

    requires_project = True
    na_values = [
        "",
        "#N/A",
        "#N/A N/A",
        "#NA",
        "-1.#IND",
        "-1.#QNAN",
        "-NaN",
        "-nan",
        "1.#IND",
        "1.#QNAN",
        "<NA>",
        # "N/A",
        # "NA",
        "NULL",
        "NaN",
        "None",
        # "n/a",
        "nan",
        "null"
    ]

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        patch = parser.add_argument_group(title="Custom Pandas Options")
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

    def process_options(self, args, opts):
        ScrapyCommand.process_options(self, args, opts)

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

    def read_input(self):
        input_data = None
        if self.input_path.suffix == '.csv':
            input_data = self.pd.read_csv(
                self.input_path, keep_default_na=False, na_values=self.na_values, na_filter=True)
        elif self.input_path.suffix == '.json':
            input_data = self.pd.read_json(self.input_path)
        elif self.input_path.suffix == '.xlsx':
            input_data = self.pd.read_excel(
                self.input_path, keep_default_na=False, na_values=self.na_values, na_filter=True).iloc[:, 1:]
            input_data.rename(
                columns=lambda x: '_'.join(
                    [word.lower() for word in x.split(' ')]) if x[0] != '$' else x,
                inplace=True
            )
        elif self.input_path.suffix == '.xml':
            input_data = self.pd.read_xml(self.input_path)

        return input_data

    def write_output(self, data, output_suffix='_pandas'):
        output_path = self.output_folder / \
            f'{self.input_path.stem}{output_suffix}{self.input_path.suffix}'
        if output_path.suffix == '.csv':
            data.to_csv(output_path)
        elif output_path.suffix == '.json':
            data.to_json(
                output_path, orient='records', force_ascii=False)
        elif output_path.suffix == '.xlsx':
            data.rename(
                columns=lambda x: ' '.join(
                    [word.capitalize() for word in x.split('_')]) if x[0] != '$' else x,
                inplace=True
            )
            data.to_excel(output_path, sheet_name='PAS')
        elif output_path.suffix == '.xml':
            data.to_xml(output_path)
