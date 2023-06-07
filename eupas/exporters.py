# NOT DEFAULT
# Define your feed exports here
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/feed-exports.html

# Modified from:
#   https://github.com/jesuslosada/scrapy-xlsx/blob/master/scrapy_xlsx/exporters.py

from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell
from openpyxl.cell.cell import KNOWN_TYPES
from openpyxl.styles import Font
from scrapy.exporters import BaseItemExporter
from scrapy.spiders import Spider

from collections.abc import Iterable, Mapping
from datetime import date, datetime
import re
import sqlite3
from typing import List

from eupas.items import Study
from itemadapter.adapter import ItemAdapter


def uri_params(params, spider: Spider):
    is_filtered = spider.custom_settings.get('FILTER_STUDIES', False)
    return {
        **params,
        'spider_name': spider.name,
        'filter': 'filter' if is_filtered else 'all'
    }


class XlsxItemExporter(BaseItemExporter):

    def __init__(
        self,
        file,
        include_header_row=True,
        include_counter_column=True,
        join_multivalued='; ',
        default_value=None,
        sheet_name='PAS Studies',
        date_format='%Y-%m-%d',
        datetime_format='%Y-%m-%d %H:%M:%S',
        **kwargs
    ):
        self._configure(kwargs, dont_fail=True)

        self.file = file
        self.include_header_row = include_header_row
        self.include_counter_column = include_counter_column
        self.default_value = default_value
        self.seperator = join_multivalued
        self.headers_not_written = True
        self.date_format = date_format
        self.datetime_format = datetime_format

        self.workbook = Workbook(write_only=True)
        self.sheet = self.workbook.create_sheet(sheet_name, 0)
        self.counter = 1
        self.accent_font = Font(bold=True)

    def serialize_field(self, field, column_name, value):
        serializer = field.get('serializer', lambda x: x)
        return self._default_serializer(serializer(value))

    def _default_serializer(self, value):
        '''
        Provide a valid XLSX serialization for value.

        This method serializes the item fields trying to respect their type.
        Strings, numbers, booleans and dates are handled by openpyxl and they
        should appear with proper formatting in the output file. Other types
        are converted into a string. You can
        Individual scrapy.Item fields can provide a custom serializer too:
            my_field = Field(serializer=custom_serializer)
        '''

        if isinstance(value, KNOWN_TYPES):
            return value
        elif isinstance(value, Iterable):
            return self.seperator.join(map(str, value))
        elif isinstance(value, datetime):
            return value.strftime(self.datetime_format)
        elif isinstance(value, date):
            return value.strftime(self.date_format)
        else:
            return str(value)

    def export_item(self, item):
        if self.headers_not_written:
            self.headers_not_written = False
            self._write_headers_and_set_fields_to_export(item)

        fields = self._get_serialized_fields(
            item, default_value=self.default_value, include_empty=True
        )

        values = [value for _, value in fields]
        if self.include_counter_column:
            counterCell = WriteOnlyCell(value=self.counter, ws=self.sheet)
            counterCell.font = self.accent_font
            values = [counterCell] + values
            self.counter += 1
        self.sheet.append(values)

    def finish_exporting(self):
        # XXX: ideally, Scrapy would pass the filename and let the exporter
        # create the output file, however, it passes a file object already
        # open in 'append' mode, so this method ignores this file object and
        # only uses it to retrieve the filename.
        self.workbook.save(self.file.name)

    def _accentuate(self, values):
        values = [WriteOnlyCell(value=val, ws=self.sheet) for val in values]
        for val in values:
            val.font = self.accent_font
        return values

    def _snake_case_to_upper_case(self, names: List[str]):
        return [' '.join([word.capitalize() for word in name.split('_')]) for name in names]

    def _write_headers_and_set_fields_to_export(self, item):
        '''
        Write the header row using the field names of the first item.

        This method writes the header row using the field names of the first
        exported item or the values of the key-value-Mapping passed by the
        FEED_EXPORT_FIELDS setting.
        '''
        if self.fields_to_export is None:
            adapter = ItemAdapter(item)
            self.fields_to_export = dict(
                zip(list(adapter.field_names()), self._snake_case_to_upper_case(list(adapter.field_names()))))
        elif isinstance(self.fields_to_export, list):
            self.fields_to_export = dict(
                zip(self.fields_to_export, self._snake_case_to_upper_case(self.fields_to_export)))

        if self.include_header_row:
            if not isinstance(self.fields_to_export, Mapping):
                raise TypeError
            row = self._accentuate(list(self.fields_to_export.values()))
            if self.include_counter_column:
                row = [''] + row
            self.sheet.append(row)


class SQLiteItemExporter(BaseItemExporter):

    type_map = {
        str: 'TEXT',
        int: 'INTEGER',
        float: 'NUMERIC',
    }

    def __init__(
        self,
        file,
        join_multivalued='; ',
        default_value=None,
        db_name='study',
        date_format='%Y-%m-%d',
        datetime_format='%Y-%m-%d %H:%M:%S',
        **kwargs
    ):
        self._configure(kwargs, dont_fail=True)

        self.default_value = default_value
        self.seperator = join_multivalued
        self.date_format = date_format
        self.datetime_format = datetime_format
        self.db_name = db_name

        self.connection = sqlite3.connect(file.name)
        self.cursor = self.connection.cursor()

        self.regex = re.compile(r"[^a-z,A-Z,0-9,_]")
        self.table_created = False

    def serialize_field(self, field, column_name, value):
        serializer = field.get('serializer', lambda x: x)
        return self._default_serializer(serializer(value))

    def _default_serializer(self, value):
        '''
        Provide a valid SQL serialization for value.

        This method serializes the item fields trying to respect their type.
        Strings, numbers, booleans and dates are handled by openpyxl and they
        should appear with proper formatting in the output file. Other types
        are converted into a string. You can
        Individual scrapy.Item fields can provide a custom serializer too:
            my_field = Field(serializer=custom_serializer)
        '''

        if isinstance(value, Iterable) and not isinstance(value, str):
            return self.seperator.join(map(str, value))
        elif isinstance(value, datetime):
            return value.strftime(self.datetime_format)
        elif isinstance(value, date):
            return value.strftime(self.date_format)
        else:
            return value

    def export_item(self, item):

        fields = sorted(list(self._get_serialized_fields(
            item, default_value=self.default_value, include_empty=True
        )), key=lambda x: x[0])

        names = [self._get_sql_name(name) for name, _ in fields]
        values = [value for _, value in fields]

        if not self.table_created:
            self._create_table(names)
            self.table_created = True

        self.cursor.execute(self.insert_sql, [
            *values
        ])
        self.connection.commit()

    def _get_field_meta(self, field_name):
        try:
            meta = ItemAdapter.get_field_meta_from_class(Study, field_name)
        except KeyError:
            meta = {}
        return meta

    def _get_sql_name(self, field_name):
        return self._get_field_meta(field_name).get('sql_name', self.regex.sub('', field_name))

    def _create_table(self, names):
        sql_cols = []
        for name in names:
            meta = self._get_field_meta(name)
            primary = meta.get('primary_key', False)
            required = meta.get('required', False)
            sql_type = meta.get('sql_type', str)
            sql_name = self._get_sql_name(name)
            sql_cols.append(
                f'{sql_name} {self.type_map.get(sql_type, "BLOB")}{" PRIMARY KEY" if primary else ""}{" NOT NULL" if required else ""}')
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {self.db_name} ({', '.join(sql_cols)});")
        self.insert_sql = f"INSERT INTO {self.db_name} ({','.join(names)}) VALUES ({','.join(['?'] * len(names))});"

    def finish_exporting(self):
        self.connection.commit()
        self.connection.close()
