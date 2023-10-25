from pathlib import Path

from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError

from eupas.items import Study


class Command(ScrapyCommand):

    requires_project = True
    matched_meta_field_name_prefix = '$MATCHED_'

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
            "-m",
            "--matchinput",
            metavar="FILE",
            default=None,
            help="path to the matching file"
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

        self.match_path = Path(opts.matchinput or "")
        if not self.match_path.is_file():
            raise UsageError(
                "Invalid -m value, use a valid path to a file", print_help=False)
        if self.match_path.suffix != '.xlsx':
            raise UsageError(
                "Invalid -m value, xlsx file expected", print_help=False)

        self.output_folder = Path(opts.output or "")
        if not self.output_folder.is_dir():
            raise UsageError(
                "Invalid -i value, use a valid path to a folder", print_help=False)
        self.output_folder.mkdir(parents=True, exist_ok=True)

    def syntax(self):
        return "matching_field_names [options]"

    def short_desc(self):
        return "Patch the input file by matching the provided field_names"

    def run(self, args, opts):
        if len(args) == 0:
            raise UsageError(
                "running 'scrapy patch' without additional arguments is not supported"
            )

        if not set(args).issubset(set(Study.fields)):
            raise UsageError(
                "At least one patch value isn't a valid field name", print_help=False)

        import pandas as pd
        matching_data = pd.read_excel(
            self.match_path, sheet_name=args, header=None)
        input_data = None
        if self.input_path.suffix == '.csv':
            input_data = pd.read_csv(self.input_path)
        elif self.input_path.suffix == '.json':
            input_data = pd.read_json(self.input_path)
        elif self.input_path.suffix == '.xlsx':
            input_data = pd.read_excel(self.input_path).iloc[:, 1:]
            input_data.rename(
                columns=lambda x: '_'.join(
                    [word.lower() for word in x.split(' ')]),
                inplace=True
            )
        elif self.input_path.suffix == '.xml':
            input_data = pd.read_xml(self.input_path)

        print(input_data)

        output_data = input_data
        for field_name in args:
            output_data = pd.merge(
                # TODO: remove after deleting not applicable or change it so that only '' and None are
                # Has to be done because of merging problems with the value not applicable
                output_data.fillna(value={field_name: ''}),
                matching_data[field_name].iloc[:, 1:].rename(columns={
                    1: f'{self.matched_meta_field_name_prefix}{field_name}',
                    2: field_name
                }),
                how='left',
                on=field_name,
                # validate='m:1' Problem with not applicable!
            )
        output_data[f'{self.matched_meta_field_name_prefix}_COMBINED'] = output_data[[f'{self.matched_meta_field_name_prefix}{field_name}' for field_name in args]].apply(lambda x: ''.join([str(y) for y in x.values if str(y) != "nan"]), axis='columns')

        output_path = self.output_folder / self.input_path.name
        if output_path.suffix == '.csv':
            output_data.to_csv(output_path)
        elif output_path.suffix == '.json':
            output_data.to_json(
                output_path, orient='records', force_ascii=False)
        elif output_path.suffix == '.xlsx':
            output_data.rename(
                columns=lambda x: ' '.join(
                    [word.capitalize() for word in x.split('_')]),
                inplace=True
            )
            output_data.to_excel(output_path)
        elif output_path.suffix == '.xml':
            output_data.to_xml(output_path)
