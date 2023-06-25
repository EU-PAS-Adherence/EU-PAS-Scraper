from difflib import SequenceMatcher, get_close_matches
import json
import logging
from pathlib import Path
import re

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError

from eupas.items import Study


class Command(ScrapyCommand):

    method = 1
    include_own_group_name = False

    requires_project = True

    junk_chars = '.,;\n\t'
    junk_words = frozenset([
        'inc', 'gmbh', 'ltd', 'limited', 'co', 'kg', 'spa', 'llc',
        'pharmaceuticals', 'pharma', 'therapeutics',
    ])

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        group = parser.add_argument_group(title="Custom Grouping Options")
        group.add_argument(
            "-i",
            "--input",
            metavar="FILE",
            default=None,
            help="path to the data"
        )
        group.add_argument(
            "-o",
            "--output",
            metavar="FOLDER",
            default=None,
            help="path to the output folder"
        )
        group.add_argument(
            "-c",
            "--cutoff",
            metavar="CUTOFF",
            default=0.5,
            help="cutoff value for grouping"
        )

    def process_options(self, args, opts):
        ScrapyCommand.process_options(self, args, opts)
        self.input_path = Path(opts.input or "")
        if not (self.input_path.exists() and self.input_path.is_file()):
            raise UsageError(
                "Invalid -i value, use a valid path to a file", print_help=False)
        if self.input_path.suffix != '.json':
            raise UsageError(
                "Invalid -i value, json file expected", print_help=False)

        self.output_folder = Path(opts.output or "")
        if self.output_folder.exists() and not self.output_folder.is_dir():
            raise UsageError(
                "Invalid -i value, use a valid path to a folder", print_help=False)
        self.output_folder.mkdir(parents=True, exist_ok=True)

        try:
            self.cutoff = float(opts.cutoff)
            assert self.cutoff >= 0 and self.cutoff <= 1
        except (ValueError, AssertionError) as e:
            raise UsageError(
                "Invalid -c value, use a valid float between 0 and 1", print_help=False) from e

    def syntax(self):
        return "field_names [options]"

    def short_desc(self):
        return "Group extracted row value"

    def serialize(self, s):
        return self.filter_multiple_spaces(self.filter_junk_words(self.filter_junk_chars(str(s).lower()))).strip()

    def filter_multiple_spaces(self, s):
        return re.sub(r'\s+', ' ', s)

    def filter_junk_chars(self, s):
        return re.sub(rf'[{self.junk_chars}]', '', s)

    def filter_junk_words(self, s):
        def sub_junk_words(m):
            return '' if m.group() in self.junk_words else m.group()
        return re.sub(r'\w+', sub_junk_words, s)

    def run(self, args, opts):
        if len(args) == 0:
            # args = Study.fields.keys()
            raise UsageError(
                "running 'scrapy group' without additional arguments is not supported"
            )

        if not set(args).issubset(set(Study.fields)):
            raise UsageError(
                "At least one -g value isn't a valid field name", print_help=False)

        with self.input_path.open('r') as f:
            self.studies = json.load(f)

        matcher = SequenceMatcher(isjunk=lambda x: x in self.junk_chars)

        class firstShowTuple(tuple):
            def __str__(self) -> str:
                return self.__getitem__(0).__str__()

        workbook = Workbook()

        logger = logging.getLogger()

        for i, field_name in enumerate(args):

            groups = {}
            sheet = workbook.create_sheet(field_name, i)

            logger.info(f'Grouping field: {field_name}')

            if self.method == 1:
                values = {
                    firstShowTuple(
                        (self.serialize(study[field_name]), str(study[field_name])))
                    for study in self.studies
                    if field_name in study
                }

                while len(values) > 1:
                    val = values.pop()
                    matches = get_close_matches(
                        val, values, n=len(values), cutoff=self.cutoff)
                    values.difference_update(matches)
                    if self.include_own_group_name:
                        matches.append(val)
                    groups.setdefault(val[1], sorted([m[1] for m in matches]))
                else:
                    val = values.pop()
                    groups.setdefault(
                        val[1], [val[1]] if self.include_own_group_name else [])

            elif self.method == 2:
                values = {
                    (self.serialize(study[field_name]), study[field_name])
                    for study in self.studies
                    if field_name in study
                }

                while len(values) > 1:
                    val = values.pop()
                    matcher.set_seq2(val[0])
                    matches = []
                    for other in values:
                        matcher.set_seq1(other[0])
                        if matcher.ratio() > self.cutoff:
                            matches.append(other)
                    values.difference_update(matches)
                    if self.include_own_group_name:
                        matches.append(val)
                    groups.setdefault(val[1], sorted([m[1] for m in matches]))
                else:
                    val = values.pop()
                    groups.setdefault(
                        val[1], [val[1]] if self.include_own_group_name else [])

            with open(f'{self.output_folder}/{field_name}.json', 'w') as f:
                json.dump(groups, f, indent='\t', sort_keys=True)

            for group_name, field_names in sorted(groups.items()):
                group_cell = WriteOnlyCell(value=group_name, ws=sheet)
                field_cells = [WriteOnlyCell(value=name, ws=sheet)
                               for name in field_names]
                sheet.append(
                    [group_cell, field_cells[0] if field_cells else ''])
                if len(field_cells) > 1:
                    start_row = sheet.max_row
                    for field_cell in field_cells[1:]:
                        sheet.append(['', field_cell])
                    sheet.merge_cells(
                        start_row=start_row, end_row=sheet.max_row, start_column=1, end_column=1)

        workbook.save(f'{self.output_folder}/groups.xlsx')
