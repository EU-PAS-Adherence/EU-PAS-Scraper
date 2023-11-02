import logging
from pathlib import Path

from scrapy.exceptions import UsageError

from eupas.commands import PandasCommand


class Command(PandasCommand):

    group_by_field_name = '$MATCHED_combined_name'
    # group_by_field_name = ['$MATCHED_combined_name', '$UPDATED_state']

    array_fields = [
        'age_population', 'countries', 'data_sources_registered_with_encepp',
        'data_sources_not_registered_with_encepp', 'data_source_types', 
        'funding_other_names', 'funding_other_percentage', 'medical_conditions', 
        'primary_outcomes', 'references', 'scopes', 'secondary_outcomes', 
        'sex_population', 'study_design', 'substance_atc', 'substance_inn', 
        'other_documents_url', 'other_population'
    ]
    detail_split_fields = ['medical_conditions', 'primary_outcomes', 'secondary_outcomes']
    yes_eq_true_fields = ['collaboration_with_research_network', 'follow_up', 'medical_conditions', 'primary_outcomes', 'secondary_outcomes', 'uses_established_data_source']

    def add_options(self, parser):
        PandasCommand.add_options(self, parser)
        # patch = parser.add_argument_group(title="Custom Statistic Options")
        # patch.add_argument(
        #     "-m",
        #     "--matchinput",
        #     metavar="FILE",
        #     default=None,
        #     help="path to the matching file"
        # )

    def process_options(self, args, opts):
        PandasCommand.process_options(self, args, opts)
        # self.match_enabled = 'match' in args

    def syntax(self):
        return "[options]"

    def short_desc(self):
        return "Runs statistics with input file data."

    def preprocess(self, data: PandasCommand.pd.DataFrame):
        data.loc[data['$CANCELLED'].notna(), '$CANCELLED'] = data.loc[data['$CANCELLED'].notna(), '$CANCELLED'].astype(bool)

        for field in self.array_fields:
            data[field] = data[field].str.split('; ')
        
        for field in self.detail_split_fields:
            values = data[field]
            data[field] = values.str[0]
            data[f'{field}_details'] = values.str[1]

        for field in self.yes_eq_true_fields:
            data[field] = self.np.where(data[field] == 'Yes', True, False)

        req_by_reg = data['requested_by_regulator'].str.split(': ')
        data['requested_by_regulator'] = req_by_reg.str[0]
        data['requested_by_regulator_details'] = req_by_reg.str[1].str.split(', ')
        data.loc[data['requested_by_regulator'] == 'Yes', 'requested_by_regulator'] = True
        data.loc[data['requested_by_regulator'] == 'No', 'requested_by_regulator'] = False
        data.loc[data['requested_by_regulator'] == "Don't know", 'requested_by_regulator'] = self.pd.NA

        data.sort_index(axis=1, inplace=True)

        # print(data['$UPDATED_state'].str.get_dummies()['Planned'].sum())
        return data

    def run(self, args, opts):
        self.logger = logging.getLogger()
        self.logger.info('Start stats')
        self.logger.info('Reading input data...')
        data = self.preprocess(self.read_input())

        def test(x):
            state_sum = x.str.get_dummies().sum()
            return [state_sum.get(state, 0.0) for state in ['Cancelled', 'Finalised', 'Ongoing', 'Planned']]


        grouped = data.groupby(by=self.group_by_field_name, dropna=False)
        stats = grouped.agg(
            {
                '$CANCELLED': lambda x: x.fillna(0.0).astype(float).sum(),
                'eu_pas_register_number': 'count',
                # '$UPDATED_state': lambda x: list(x.str.get_dummies().sum()),
                # '$UPDATED_state': [lambda x: x.str.get_dummies().sum() for i in range(3)]
                '$UPDATED_state': test
            }
        )

        # for name, group in grouped:
        #     print(name)
        #     print(group)

        # print(grouped.count())

        self.logger.info('Writing output data...')
        self.write_output(data, '_statistics')
        self.write_output(stats, '_statistics_agg')
        # self.write_output(grouped.count(), '_statistics_count')
