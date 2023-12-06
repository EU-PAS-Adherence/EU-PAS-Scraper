import logging
# from pathlib import Path

# from scrapy.exceptions import UsageError

from eupas.commands import PandasCommand


class Command(PandasCommand):

    group_by_field_name = '$MATCHED_combined_name'

    str_dummy_fields = ['age_population', 'other_population']
    dummy_fields = ['$UPDATED_state', 'risk_management_plan']
    array_fields = [
        'age_population', 'countries', 'data_sources_registered_with_encepp',
        'data_sources_not_registered_with_encepp', 'data_source_types',
        'funding_other_names', 'funding_other_percentage', 'medical_conditions',
        'other_population', 'primary_outcomes', 'references', 'scopes',
        'secondary_outcomes', 'sex_population', 'study_design', 'substance_atc',
        'substance_inn', 'other_documents_url'
    ]
    array_detail_split_fields = ['medical_conditions',
                                 'primary_outcomes', 'secondary_outcomes']
    yes_eq_true_fields = ['collaboration_with_research_network', 'follow_up', 'medical_conditions',
                          'primary_outcomes', 'secondary_outcomes', 'uses_established_data_source']
    percentage_fields = ['funding_companies_percentage', 'funding_charities_percentage',
                         'funding_government_body_percentage', 'funding_research_councils_percentage',
                         'funding_eu_scheme_percentage']

    def syntax(self):
        return "[options]"

    def short_desc(self):
        return "Runs statistics with input file data."

    def preprocess(self, data):
        import numpy as np

        data['$CANCELLED'] = data['$CANCELLED'].astype(bool).fillna(False)
        self.logger.info(
            f'Excluding {data["$CANCELLED"].astype(int).sum()} cancelled studies...')
        data = data.loc[~data['$CANCELLED']]

        self.logger.info('Adding Dummies')
        for field in self.str_dummy_fields:
            field_dummies = data[field].str.get_dummies('; ').rename(
                columns=lambda x: f'{field}:{x}')
            data = PandasCommand.pd.concat([data, field_dummies], axis=1)

        for field in self.dummy_fields:
            field_dummies = PandasCommand.pd.get_dummies(
                data[field], prefix=f'{field}:')
            data = PandasCommand.pd.concat([data, field_dummies], axis=1)

        self.logger.info('Converting strings to arrays')
        for field in self.array_fields:
            data[field] = data[field].str.split('; ')

        self.logger.info('Splitting multivalue fields into seperate columns')
        for field in self.array_detail_split_fields:
            values = data[field]
            data[field] = values.str[0]
            data[f'{field}_details'] = values.str[1]

        req_by_reg = data['requested_by_regulator'].str.split(': ')
        data['requested_by_regulator'] = req_by_reg.str[0]
        data['requested_by_regulator_details'] = req_by_reg.str[1].str.split(
            ', ')

        self.logger.info('Converting strings to bools')
        for field in self.yes_eq_true_fields:
            data[field] = np.where(data[field] == 'Yes', True, False)

        data.loc[data['requested_by_regulator'] ==
                 'Yes', 'requested_by_regulator'] = True
        data.loc[data['requested_by_regulator'] ==
                 'No', 'requested_by_regulator'] = False
        data.loc[data['requested_by_regulator'] ==
                 "Don't know", 'requested_by_regulator'] = self.pd.NA

        self.logger.info('Filling number columns with default value 0.0')
        for field in self.percentage_fields:
            data[field].fillna(0.0, inplace=True)

        data['funding_other_percentage'] = data['funding_other_percentage'].apply(
            lambda x: list(map(float, x)) if isinstance(x, list) else [0.0])

        data.sort_index(axis=1, inplace=True)
        return data

    def create_categories(self, data):
        import numpy as np
        df = data.set_index('eu_pas_register_number')

        age_map = {
            'Preterm newborns': '<2 years',
            'Term newborns (0-27 days)': '<2 years',
            'Infants and toddlers (28 days - 23 months)': '<2 years',
            'Children (2 - 11 years)': '2-17 years',
            'Adolescents (12 - 17 years)': '2-17 years',
            'Adults (18 - 44 years)': '18-64 years',
            'Adults (45 - 64 years)': '18-64 years',
            'Adults (65 - 74 years)': '65+ years',
            'Adults (75 years and over)': '65+ years'
        }

        def get_funding_sources():
            companies = np.where(
                df.funding_companies_percentage > 0, 'Companies', '')
            charities = np.where(
                df.funding_charities_percentage > 0, 'Charities', '')
            government_bodies = np.where(
                df.funding_government_body_percentage > 0, 'Government Bodies', '')
            research_councils = np.where(
                df.funding_research_councils_percentage > 0, 'Research Councils', '')
            eu_schemes = np.where(
                df.funding_eu_scheme_percentage > 0, 'EU Schemes', '')
            other = np.where(
                df.funding_other_percentage.str[0] > 0, 'Other', '')
            return list(map(lambda x: x or PandasCommand.pd.NA, ['; '.join(filter(bool, x)) for x in zip(companies, charities, government_bodies,
                                                                                                         research_councils, eu_schemes, other)]))

        categories = df.loc[:, [
            'state', 'risk_management_plan', 'follow_up',
            'requested_by_regulator', 'collaboration_with_research_network',
            'country_type', 'medical_conditions', 'uses_established_data_source',
            'primary_outcomes', 'secondary_outcomes']]

        categories = categories.assign(
            registration_date=df['registration_date'].dt.year,
            study_type=df['study_type'].str.split(r'; |: ').str[0],
            countries=df['countries'].apply(len),
            number_of_subjects=df['number_of_subjects'].apply(
                lambda x: '<1000' if x < 1000 else '>10000' if x > 10000 else '1000-10000'
            ),
            age_population=df['age_population'].apply(
                lambda ages: '; '.join(sorted(list({age_map[x] for x in ages})))),
            sex_population=df['sex_population'].apply(
                lambda x: list(reversed(sorted(x)))).str.join(' and ')
        )
        return categories

    def create_grouped_agg(self, data):
        import numpy as np

        grouped = data.groupby(by=self.group_by_field_name, dropna=False)

        def set_sum(x: PandasCommand.pd.Series):
            return '; '.join(sorted(list(set(x.fillna('').apply(list).sum()))))

        def bool_sum(x: PandasCommand.pd.Series):
            return x.dropna().astype(float).sum()

        def setify(x: PandasCommand.pd.Series):
            return '; '.join(sorted(list(set(x.dropna().to_list()))))

        def mean_mean(x):
            return x.apply(np.mean).mean()

        str_dummie_agg = {col.split(':')[-1]: (col, bool_sum)
                          for col in data for field in self.str_dummy_fields if col.startswith(field) and ':' in col}

        dummie_agg = {col.split(':')[-1]: (col, bool_sum)
                      for col in data for field in self.dummy_fields if col.startswith(field) and ':' in col}

        percentage_agg = {
            f'mean_{col}': (col, 'mean') for col in self.percentage_fields
        }

        grouped_agg = grouped.agg(
            num_collabs_with_research_network=(
                'collaboration_with_research_network', bool_sum),
            which_countries=('countries', set_sum),
            num_with_follow_up=('follow_up', bool_sum),
            num_with_medical_conditions=('medical_conditions', bool_sum),
            which_medical_conditions=('medical_conditions_details', setify),
            min_number_of_subjects=('number_of_subjects', 'min'),
            max_number_of_subjects=('number_of_subjects', 'max'),
            mean_number_of_subjects=('number_of_subjects', 'mean'),
            median_number_of_subjects=('number_of_subjects', 'median'),
            num_with_primary_outcomes=('primary_outcomes', bool_sum),
            num_with_secondary_outcomes=('secondary_outcomes', bool_sum),
            num_requested_by_regulator=('requested_by_regulator', bool_sum),
            num_using_established_data_sources=(
                'uses_established_data_source', bool_sum),
            **str_dummie_agg,
            **dummie_agg,
            **percentage_agg,
            mean_other_percentage=('funding_other_percentage', mean_mean)
        )

        sizes = grouped.size().rename('num_studies')
        grouped_agg = grouped_agg.merge(
            sizes, left_index=True, right_index=True)

        return grouped_agg

    def run(self, args, opts):
        super().run(args, opts)

        self.logger = logging.getLogger()
        self.logger.info('Start stats')
        self.logger.info('Reading input data...')
        data = self.preprocess(self.read_input())
        categories = self.create_categories(data)
        grouped_agg = self.create_grouped_agg(data)

        import matplotlib as mpl
        import matplotlib.pyplot as plt
        mpl.style.use('bmh')
        date = data['registration_date'].dt.to_period('M')
        PandasCommand.pd.concat(
            [
                data.groupby(date).size().rename('studies'),
                data.groupby(date).size().cumsum().rename('cumulated studies')
            ], axis=1).plot(
            title='Frequency of studies by "Registration Date"',
            xlabel='Registration Date',
            ylabel='# of studies',
            subplots=True
        )
        plt.savefig(self.output_folder / 'registration_date_freq.png')

        self.logger.info('Writing output data...')
        self.write_output(data, '_statistics_preprocessed')
        self.write_output(categories, '_statistics_categories')
        self.write_output(grouped_agg, '_statistics_grouped_agg')
