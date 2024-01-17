from datetime import datetime
import logging

from eupas.commands import PandasCommand
from scrapy.exceptions import UsageError
from statsmodels.stats.proportion import proportion_confint


class Command(PandasCommand):

    group_by_field_name = '$MATCHED_combined_centre_name'

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

    multiple_categories_fields = ['age_population', 'data_source_types',
                                  'funded_by', 'other_population', 'scopes', 'sex_population', 'study_design']

    compare_datetime = None

    required_rmp = ['EU RMP category 1 (imposed as condition of marketing authorisation)',
                    'EU RMP category 2 (specific obligation of marketing authorisation)']

    def syntax(self):
        return "[options]"

    def short_desc(self):
        return "Runs statistics with input file data."

    def add_options(self, parser):
        PandasCommand.add_options(self, parser)
        statistics = parser.add_argument_group(title="Custom Eupas Options")
        statistics.add_argument(
            "-D",
            "--date",
            metavar="COMPARE_DATE",
            default=None,
            help="specifies the date to compare against",
        )

    def process_options(self, args, opts):
        PandasCommand.process_options(self, args, opts)
        import numpy as np
        if opts.date:
            try:
                self.compare_datetime = np.datetime64(
                    opts.date, 'm')
            except ValueError:
                raise UsageError(
                    'The date was not formatted correctly like this')
        else:
            self.compare_datetime = np.datetime64(datetime.utcnow(), 'm')

    def preprocess(self, data):
        import numpy as np

        # NOTE: Pandas reads boolean columns with NA Values as float
        # We need to fill na first because NA is True else
        data['$CANCELLED'] = data['$CANCELLED'].fillna(False).astype(bool)
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

        return data.sort_index(axis=1)

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

        scope_list = [
            'Risk assessment',
            'Effectiveness evaluation',
            'Drug utilisation study',
            'Disease epidemiology'
        ]

        data_source_list = [
            'Prospective patient-based data collection',
            'Disease/case registry',
            'Prescription event monitoring',
            'Administrative database, e.g. claims database',
            'Routine primary care electronic patient registry',
            'Exposure registry',
            'Pharmacy dispensing records',
            'Case-control surveillance',
            'Spontaneous reporting'
        ]

        study_design_list = [
            'Sentinel sites',
            'Intensive monitoring schemes',
            'Prescription event monitoring',
            'Cross-sectional study',
            'Cohort study',
            'Case-control study',
            'Case-series',
            'Case-crossover',
            'Self-controlled case series',
            'Drug utilisation study',
            'Pharmacokinetic study',
            'Pharmacodynamic study',
            'Drug interaction study',
            'Randomised controlled trial',
            'Non-randomised controlled trial'
        ]

        def get_funding_sources():
            funded_by_companies = df.funding_companies_percentage > 0
            funded_by_charities = df.funding_charities_percentage > 0
            funded_by_government_bodies = df.funding_government_body_percentage > 0
            fundes_by_research_councils = df.funding_research_councils_percentage > 0
            funded_by_eu_schemes = df.funding_eu_scheme_percentage > 0
            funded_by_other = df.funding_other_percentage.str[0] > 0

            companies = np.where(funded_by_companies, 'Companies', '')
            charities = np.where(funded_by_charities, 'Charities', '')
            government_bodies = np.where(
                funded_by_government_bodies, 'Government Bodies', '')
            research_councils = np.where(
                fundes_by_research_councils, 'Research Councils', '')
            eu_schemes = np.where(funded_by_eu_schemes, 'EU Schemes', '')
            other = np.where(funded_by_other, 'Other', '')

            num_sources = sum(map(lambda x: x.astype(int), [
                              funded_by_companies, funded_by_charities, funded_by_government_bodies, fundes_by_research_councils, funded_by_eu_schemes, funded_by_other]))
            multiple_funding_sources = np.where(num_sources > 1, True, False)
            return (list(map(lambda x: x or PandasCommand.pd.NA, ['; '.join(filter(bool, x)) for x in zip(companies, charities, government_bodies, research_councils, eu_schemes, other)])), multiple_funding_sources)

        categories = df.loc[:, [
            'state', 'risk_management_plan', 'follow_up',
            'requested_by_regulator', 'collaboration_with_research_network',
            'country_type', 'medical_conditions', 'uses_established_data_source',
            'primary_outcomes', 'secondary_outcomes', 'number_of_subjects',
            'data_collection_date_actual', 'final_report_date_actual']]

        funded_by, multiple_funding_sources = get_funding_sources()

        planned_duration = df.loc[df['final_report_date_planed'].notna()
                                  & df['data_collection_date_planed'].notna(), ['data_collection_date_planed', 'final_report_date_planed']].diff(axis='columns').iloc[:, -1]

        def get_quartiles(s, interpolation='linear'):
            one_quart, median, three_quart = s.quantile(
                [.25, .5, .75], interpolation=interpolation)
            result = np.where(s <= one_quart, 1, 0) \
                + np.where((s > one_quart) & (s <= median), 2, 0) \
                + np.where((s > median) & (s <= three_quart), 3, 0) \
                + np.where(s > three_quart, 4, 0)
            return np.where(result == 0, self.pd.NA, result)

        categories = categories.assign(
            registration_date=df['registration_date'].dt.year,
            study_type=df['study_type'].str.split(r'; |: ').str[0],
            number_of_countries=df['countries'].apply(len),
            number_of_countries_grouped=df['countries'].apply(
                lambda x: len(x) if len(x) < 3 else '3 or more'),
            countries_quartiles=lambda x: get_quartiles(
                x['number_of_countries']),
            number_of_subjects_grouped=df['number_of_subjects'].apply(
                lambda x: '<1000' if x < 1000 else '>10000' if x > 10000 else '1000-10000'
            ),
            number_of_subjects_quartiles=get_quartiles(
                df['number_of_subjects']),
            age_population=df['age_population'].apply(
                lambda ages: '; '.join(sorted(list({age_map[x] for x in ages})))),
            sex_population=df['sex_population'].apply(
                lambda x: list(reversed(sorted(x)))).str.join('; '),
            other_population=df['other_population'].apply(
                lambda x: list(sorted(x)) if isinstance(x, list) else x).str.join('; '),
            funded_by=funded_by,
            multiple_funding_sources=multiple_funding_sources,
            scopes=df['scopes'].apply(
                lambda scopes: '; '.join(sorted(list({x if x in scope_list else 'Other' for x in scopes})))),
            data_source_types=df['data_source_types'].apply(
                lambda sources: '; '.join(sorted(list({x if x in data_source_list else 'Other' for x in sources})))),
            study_design=df['study_design'].apply(
                lambda designs: '; '.join(sorted(list({x if x in study_design_list else 'Other' for x in designs})))),
            planned_duration=planned_duration,
            planned_duration_quartiles=lambda x: get_quartiles(
                x['planned_duration']),
            has_protocol=df['protocol_document_url'].notna(
            ) | df['latest_protocol_document_url'].notna(),
            has_result=df['result_document_url'].notna(
            ) | df['latest_result_document_url'].notna()
        )

        return categories.sort_index(axis=1)

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
        self.logger.info('Starting statistic script')
        self.logger.info(f'Pandas {self.pd.__version__}')
        self.logger.info('Reading input data...')
        data = self.preprocess(self.read_input())

        self.logger.info('Generating categories...')
        categories = self.create_categories(data)
        self.logger.info('Generating grouped aggregations...')
        grouped_agg = self.create_grouped_agg(data)

        self.logger.info('Generating and writing plots...')
        (self.output_folder / 'plots/').mkdir(parents=True, exist_ok=True)
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
        plt.savefig(self.output_folder / 'plots' /
                    'registration_date_freq.png')

        for col in ['number_of_countries', 'number_of_subjects']:
            plt.figure()
            categories[col].plot(
                kind='box', title=f'Boxplot of study categories by "{self.excel_name_converter(col)}"',)
            plt.savefig(self.output_folder / 'plots' / f'{col}_boxplot.png')

        plt.figure()
        categories['planned_duration'].map(lambda x: x.days).plot(
            kind='kde', title='Density of "Planned Duration"')
        plt.savefig(self.output_folder / 'plots' /
                    'planned_duration_density.png')

        self.logger.info('Writing output data...')
        self.write_output(data, '_statistics_preprocessed')
        self.write_output(categories, '_statistics_categories')
        self.write_output(grouped_agg, '_statistics_grouped_agg')

        categories_past_data_collection = categories[categories['data_collection_date_actual'].notna(
        ) & (categories['data_collection_date_actual'] <= self.compare_datetime)]

        categories_past_final_report = categories[categories['final_report_date_actual'].notna(
        ) & (categories['final_report_date_actual'] <= self.compare_datetime)]

        for df, suffix in [(categories, '_all'), (categories_past_data_collection, '_past_date_collection'), (categories_past_final_report, '_past_final_report')]:
            with self.pd.ExcelWriter(self.output_folder / f'{self.input_path.stem}_statistics_categories_described{suffix}.xlsx', engine='openpyxl') as writer:
                df.to_excel(
                    writer, sheet_name=f'categories{suffix}'[:31])
                df.describe().to_excel(
                    writer, sheet_name='numerical_descriptions')
                for col in sorted(set(df.columns) - {'number_of_countries', 'number_of_subjects', 'planned_duration', 'registration_date'}):

                    frequencies = df.loc[:, [col]].apply(lambda x: x.value_counts(
                        normalize=True) * 100).rename(columns={col: 'percentage'}).reset_index()

                    frequencies.to_excel(
                        writer, sheet_name=f'{col}_frequencies'[:31], index=False)

                    if col in self.multiple_categories_fields:
                        overall_frequencies = frequencies.assign(split=lambda x: x[col].str.split(
                            '; ')).explode('split').groupby('split')['percentage'].sum().reset_index().rename(columns={
                                'split': col,
                                'percentage': 'overall_percentage'})

                        overall_frequencies.to_excel(
                            writer, sheet_name=f'{col}_frequencies'[:31], index=False, startcol=3)

        import numpy as np
        categories_two_weeks_past_final_report = categories[categories['final_report_date_actual'].notna(
        ) & (categories['final_report_date_actual'] <= self.compare_datetime - np.timedelta64(14, 'D'))]

        for df, suffix in [(categories, '_all'), (categories_past_data_collection, '_past_date_collection'), (categories_two_weeks_past_final_report, '_two_weeks_past_final_report')]:
            with self.pd.ExcelWriter(self.output_folder / f'{self.input_path.stem}_statistics_categories_documents{suffix}.xlsx', engine='openpyxl') as writer:
                df.to_excel(
                    writer, sheet_name=f'categories{suffix}'[:31])

                for col in ['has_protocol', 'has_result']:

                    size_df = len(df.loc[:, [col]])
                    frequencies = df.loc[:, [col]].apply(lambda x: x.value_counts()) \
                        .rename(columns={col: 'absolute'}) \
                        .assign(
                            percentage=df.loc[:, [col]].apply(
                                lambda x: x.value_counts(normalize=True) * 100),
                            confidence_interval=lambda x: x['absolute'].apply(
                                lambda y: [z * 100 for z in proportion_confint(y, size_df, alpha=0.05, method='normal')])
                    ).reset_index()

                    frequencies.to_excel(
                        writer, sheet_name=col[:31], index=False)

                    size_df = len(
                        df.loc[df['risk_management_plan'].isin(self.required_rmp), [col]])
                    required_rmp_frequencies = df.loc[df['risk_management_plan'].isin(
                        self.required_rmp), [col]].apply(lambda x: x.value_counts()) \
                        .rename(columns={col: 'absolute'}) \
                        .assign(
                            percentage=df.loc[:, [col]].apply(
                                lambda x: x.value_counts(normalize=True) * 100),
                            confidence_interval=lambda x: x['absolute'].apply(
                                lambda y: [z * 100 for z in proportion_confint(y, size_df, alpha=0.05, method='normal')])
                    ).reset_index().rename(columns={col: f'required_{col}'})

                    required_rmp_frequencies.to_excel(
                        writer, sheet_name=col[:31], index=False, startrow=4)
