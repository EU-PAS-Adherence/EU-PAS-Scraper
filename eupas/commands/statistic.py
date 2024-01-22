from datetime import datetime
import logging

from eupas.commands import PandasCommand
from scrapy.exceptions import UsageError


class Command(PandasCommand):

    group_by_field_name = '$MATCHED_combined_centre_name'

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

    def preprocess(self, df):
        import numpy as np

        # NOTE: Pandas reads boolean columns with NA Values as float
        # We need to fill na first because NA is True else
        df['$CANCELLED'] = df['$CANCELLED'].fillna(False).astype(bool)
        self.logger.info(
            f'Excluding {df["$CANCELLED"].astype(int).sum()} cancelled studies...')
        df = df.loc[~df['$CANCELLED']]

        self.logger.info('Splitting strings to arrays')
        for field in self.array_fields:
            df[field] = df[field].str.split('; ')

        self.logger.info('Splitting multivalue fields into seperate columns')
        for field in self.array_detail_split_fields:
            values = df[field]
            df[field] = values.str[0]
            df[f'{field}_details'] = values.str[1]

        req_by_reg = df['requested_by_regulator'].str.split(': ')
        df['requested_by_regulator'] = req_by_reg.str[0]
        df['requested_by_regulator_details'] = req_by_reg.str[1].str.split(
            ', ')

        self.logger.info('Converting strings to bools')
        for field in self.yes_eq_true_fields:
            df[field] = np.where(df[field] == 'Yes', True, False)

        df.loc[df['requested_by_regulator'] ==
               'Yes', 'requested_by_regulator'] = True
        df.loc[df['requested_by_regulator'] ==
               'No', 'requested_by_regulator'] = False
        df.loc[df['requested_by_regulator'] ==
               "Don't know", 'requested_by_regulator'] = self.pd.NA

        self.logger.info('Filling number columns with default value 0.0')
        for field in self.percentage_fields:
            df[field].fillna(0.0, inplace=True)

        df['funding_other_percentage'] = df['funding_other_percentage'].apply(
            lambda x: list(map(float, x)) if isinstance(x, list) else [0.0])

        df = df.set_index('eu_pas_register_number')

        return df.sort_index(axis='columns')

    def create_categories(self, df):
        import numpy as np

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
            return (list(map(lambda x: x or self.pd.NA, ['; '.join(filter(bool, x)) for x in zip(companies, charities, government_bodies, research_councils, eu_schemes, other)])), multiple_funding_sources)

        categories = df.loc[:, [
            'state', 'risk_management_plan', 'follow_up',
            'requested_by_regulator', 'collaboration_with_research_network',
            'country_type', 'medical_conditions', 'uses_established_data_source',
            'primary_outcomes', 'secondary_outcomes', 'number_of_subjects']]

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
            number_of_countries_quartiles=lambda x: get_quartiles(
                x['number_of_countries']),
            number_of_subjects_grouped=df['number_of_subjects'].apply(
                lambda x:
                '<100' if x < 100 else
                '100-<500' if x < 500 else
                '500-<1000' if x < 1000 else
                '1000-10000' if x < 10000 else
                '>10000'
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
                x['planned_duration'])
        )

        return categories.sort_index(axis='columns')

    def create_grouped_agg(self, df):
        import numpy as np

        dummy_fields = ['state', 'risk_management_plan']
        dummies = self.pd.get_dummies(
            df[dummy_fields]).rename(columns=self.python_name_converter)

        grouped = df.assign(
            past_data_collection=lambda x: x['data_collection_date_actual'].notna() &
            (x['data_collection_date_actual'] <= self.compare_datetime),
            past_data_collection_has_protocol=lambda x: x['past_data_collection'] & x['has_protocol'],
            past_final_report=lambda x: x['final_report_date_actual'].notna() &
            (x['final_report_date_actual'] <= self.compare_datetime),
            past_final_report_has_protocol=lambda x: x['past_final_report'] & x['has_result']
        ).merge(dummies, left_index=True, right_index=True).groupby(by=self.group_by_field_name, dropna=False)

        def set_sum(x: PandasCommand.pd.Series):
            return len(set(x.dropna().apply(list).sum()))

        def bool_sum(x: PandasCommand.pd.Series):
            return x.dropna().astype(float).sum()

        def setify(x: PandasCommand.pd.Series):
            return '; '.join(sorted(list(set(x.dropna().apply(list).sum()))))

        def mean_mean(x):
            return x.apply(np.mean).mean()

        dummie_agg = {
            f'number_of_studies_with_{col}': (col, bool_sum) for col in dummies
        }

        percentage_agg = {
            f'mean_{col}': (col, 'mean') for col in self.percentage_fields
        }

        grouped_agg = grouped.agg(
            number_of_collaborations_with_research_network=(
                'collaboration_with_research_network', bool_sum),
            number_of_countries=('countries', set_sum),
            set_of_countries=('countries', setify),
            number_of_studies_with_follow_up=('follow_up', bool_sum),
            min_number_of_subjects=('number_of_subjects', 'min'),
            max_number_of_subjects=('number_of_subjects', 'max'),
            mean_number_of_subjects=('number_of_subjects', 'mean'),
            median_number_of_subjects=('number_of_subjects', 'median'),
            number_of_studies_with_primary_outcomes=(
                'primary_outcomes', bool_sum),
            number_of_studies_with_secondary_outcomes=(
                'secondary_outcomes', bool_sum),
            number_of_studies_requested_by_regulator=(
                'requested_by_regulator', bool_sum),
            number_of_studies_using_established_data_sources=(
                'uses_established_data_source', bool_sum),
            **dummie_agg,
            **percentage_agg,
            mean_other_percentage=('funding_other_percentage', mean_mean),
            number_of_studies_with_result=('has_result', bool_sum),
            number_of_studies_with_protocol=('has_protocol', bool_sum),
            number_of_studies_with_past_data_collection=(
                'past_data_collection', bool_sum),
            number_of_studies_with_past_data_collection_and_protocol=(
                'past_data_collection_has_protocol', bool_sum),
            number_of_studies_with_final_report=(
                'past_final_report', bool_sum),
            number_of_studies_with_past_final_report_and_protocol=(
                'past_final_report_has_protocol', bool_sum)
        )

        sizes = grouped.size().rename('num_studies')
        grouped_agg = grouped_agg.merge(
            sizes,
            left_index=True,
            right_index=True
        )
        return grouped_agg

    def create_dummies(self, df, drop_references=True):
        # import numpy as np

        dummy_without_na_drop_map = {
            'state': 'Finalised',
            'registration_date': 2010,  # NOTE: Choose other reference?
            'planned_duration_quartiles': 1,
            # 'study_type': 'Observational study',
            'collaboration_with_research_network': False,
            'country_type': 'National study',
            'number_of_countries_grouped': '3 or more',
            'number_of_countries_quartiles': 1,
            # 'funded_by': 'Pharmaceutical companies',
            'multiple_funding_sources': False,
            'medical_conditions': True,
            # 'age_population': '18-64 years',
            # 'sex_population': 'Male; Female',
            'number_of_subjects_grouped': '100-<500',
            'number_of_subjects_quartiles': 1,
            'uses_established_data_source': False,
            'follow_up': False,
            # 'scopes': 'Risk assessment',
            'primary_outcomes': False,
            'secondary_outcomes': False
        }

        dummy_with_na_drop_map = {
            'requested_by_regulator': False,
            'risk_management_plan': 'Not applicable',
            # 'other_population': str(np.nan),
        }

        dummy_drop_map = {
            **dummy_without_na_drop_map,
            **dummy_with_na_drop_map
        }

        prefix_sep = '__'
        dummies = self.pd.concat([
            self.pd.get_dummies(
                df[dummy_without_na_drop_map.keys()],
                prefix_sep=prefix_sep,
                columns=dummy_without_na_drop_map.keys()
            ),
            self.pd.get_dummies(
                df[dummy_with_na_drop_map.keys()],
                prefix_sep=prefix_sep,
                columns=dummy_with_na_drop_map.keys(),
                dummy_na=True
            )],
            axis='columns'
        )

        if drop_references:
            columns_to_drop = [
                col for col in dummies if str(dummy_drop_map[col.split(prefix_sep)[0]]) == col.split(prefix_sep)[-1]
            ]

            dummies.drop(columns=columns_to_drop, inplace=True)

        return dummies.rename(columns=self.python_name_converter)

    def univariate_lr(self, df, y):
        import statsmodels.formula.api as smf

        variables = {col.split('__')[0] for col in df.columns}
        col_var_map = {
            v: [col for col in df.columns if col.startswith(v) and '__' in col]
            for v in variables
        }
        col_var_map = {var: cols for var, cols in col_var_map.items() if cols}

        results = {}

        for var, cols in col_var_map.items():
            escaped_vars = [f'Q("{col}")' for col in cols]
            formula = f'{y} ~ {" + ".join(escaped_vars)}'
            self.logger.info(f'Running: {formula}')
            lr_result = smf.logit(formula, df).fit(warn_convergence=True)
            results.setdefault(var, lr_result)

        return results

    def run(self, args, opts):
        super().run(args, opts)
        import numpy as np
        import matplotlib as mpl
        import matplotlib.pyplot as plt
        from statsmodels.stats.proportion import proportion_confint

        self.logger = logging.getLogger()
        self.logger.info('Starting statistic script')
        self.logger.info(f'Pandas {self.pd.__version__}')
        self.logger.info('Reading input data...')
        data = self.preprocess(self.read_input())

        data = data.assign(
            has_protocol=data['protocol_document_url'].notna(
            ) | data['latest_protocol_document_url'].notna(),
            has_result=data['result_document_url'].notna(
            ) | data['latest_result_document_url'].notna()
        )

        self.logger.info('Generating categories...')
        categories = self.create_categories(data)
        categories = categories.merge(
            data.loc[:, [
                'data_collection_date_actual', 'final_report_date_actual',
                'has_protocol', 'has_result'
            ]],
            left_index=True,
            right_index=True
        )

        self.logger.info('Writing some preanalysis data...')
        self.write_output(data, '_statistics_preprocessed')
        self.write_output(categories, '_statistics_categories')

        categories_past_data_collection = categories[categories['data_collection_date_actual'].notna(
        ) & (categories['data_collection_date_actual'] <= self.compare_datetime)]

        categories_past_final_report = categories[categories['final_report_date_actual'].notna(
        ) & (categories['final_report_date_actual'] <= self.compare_datetime)]

        categories_two_weeks_past_final_report = categories[categories['final_report_date_actual'].notna(
        ) & (categories['final_report_date_actual'] <= self.compare_datetime - np.timedelta64(14, 'D'))]

        self.logger.info('Generating and writing part 1 of analysis...')
        for df, suffix in [
                (categories, '_all'),
                (categories_past_data_collection, '_past_date_collection'),
                (categories_past_final_report, '_past_final_report')]:

            with self.pd.ExcelWriter(self.output_folder / f'{self.input_path.stem}_statistics_categories_frequencies{suffix}.xlsx', engine='openpyxl') as writer:

                # Categories
                df.to_excel(
                    writer,
                    sheet_name=f'categories{suffix}'[:31]
                )

                # Description of all numerical fields
                # min max mean var etc.
                df.describe().to_excel(
                    writer,
                    sheet_name='numerical_descriptions'
                )

                # NOTE: Can exclude numerical columns with - {'number_of_countries', 'number_of_subjects', 'planned_duration', 'registration_date'}
                for col in sorted(set(df.columns)):

                    # Absolute and relative frequencies of categories
                    frequencies = self.pd.DataFrame().assign(
                        absolute=df.loc[:, [col]].apply(
                            lambda x: x.value_counts()),
                        percentage=df.loc[:, [col]].apply(
                            lambda x: x.value_counts(normalize=True) * 100)
                    ).reset_index()

                    frequencies.to_excel(
                        writer,
                        sheet_name=f'{col}_frequencies'[:31],
                        index=False
                    )

                    if col in self.multiple_categories_fields:

                        grouped_frequencies = frequencies \
                            .assign(split=lambda x: x[col].str.split('; ')) \
                            .explode('split') \
                            .groupby('split')

                        # Absolute and relative frequencies of subcategories
                        overall_frequencies = self.pd.DataFrame().assign(
                            overall_absolute=grouped_frequencies['absolute'].sum(
                            ),
                            overall_percentage=grouped_frequencies['percentage'].sum(
                            ),
                        ).reset_index().rename(columns={'split': col})

                        overall_frequencies.to_excel(
                            writer,
                            sheet_name=f'{col}_frequencies'[:31],
                            index=False,
                            startcol=4
                        )

        self.logger.info('Generating and writing part 2 of analysis...')
        for df, suffix in [
                (categories, '_all'),
                (categories_past_data_collection, '_past_date_collection'), (categories_two_weeks_past_final_report, '_two_weeks_past_final_report')]:

            with self.pd.ExcelWriter(self.output_folder / f'{self.input_path.stem}_statistics_categories_documents{suffix}.xlsx', engine='openpyxl') as writer:

                df.to_excel(
                    writer,
                    sheet_name=f'categories{suffix}'[:31]
                )

                for col in ['has_protocol', 'has_result']:

                    size_df = len(df.loc[:, [col]])
                    frequencies = df.loc[:, [col]] \
                        .apply(lambda x: x.value_counts()) \
                        .rename(columns={col: 'absolute'}) \
                        .assign(
                            percentage=df.loc[:, [col]].apply(
                                lambda x: x.value_counts(normalize=True) * 100),
                            confidence_interval=lambda x: x['absolute'].apply(
                                lambda y: [z * 100 for z in proportion_confint(y, size_df, alpha=0.05, method='beta')])
                    ).reset_index()

                    frequencies.to_excel(
                        writer,
                        sheet_name=col[:31],
                        index=False
                    )

                    # print(proportion_confint(
                    #     frequencies['absolute'].iloc[0], size_df, alpha=0.05, method='normal'))
                    # print(sp.stats.binomtest(
                    #     frequencies['absolute'].iloc[0], size_df, p=float(frequencies['absolute'].iloc[0]) / float(size_df)).proportion_ci())

                    size_df = len(
                        df.loc[df['risk_management_plan'].isin(self.required_rmp), [col]])
                    required_rmp_frequencies = df.loc[df['risk_management_plan'].isin(
                        self.required_rmp), [col]] \
                        .apply(lambda x: x.value_counts()) \
                        .rename(columns={col: 'absolute'}) \
                        .assign(
                            percentage=df.loc[:, [col]].apply(
                                lambda x: x.value_counts(normalize=True) * 100),
                            confidence_interval=lambda x: x['absolute'].apply(
                                lambda y: [z * 100 for z in proportion_confint(y, size_df, alpha=0.05, method='normal')])
                    ).reset_index().rename(columns={col: f'required_{col}'})

                    required_rmp_frequencies.to_excel(
                        writer,
                        sheet_name=col[:31],
                        index=False,
                        startrow=4
                    )

        self.logger.info('Generating and writing part 3 of analysis...')
        data_to_group = categories.merge(
            data.loc[:, [self.group_by_field_name, *self.percentage_fields,
                         'funding_other_percentage', 'countries']],
            left_index=True,
            right_index=True
        )
        grouped_agg = self.create_grouped_agg(data_to_group)
        self.write_output(grouped_agg, '_statistics_centre_all')

        for df, y_label, name in [
                (categories_past_data_collection, 'has_protocol', 'protocol'),
                (categories_two_weeks_past_final_report, 'has_result', 'results')]:

            self.logger.info(
                'Generating and writing dummies for logistic regression...')
            dummies = self.create_dummies(df)
            y = df.loc[:, [y_label]].astype(int)
            dummies_y = dummies.merge(
                y,
                left_index=True,
                right_index=True,
                how='right'
            )
            self.write_output(
                dummies_y, f'_statistics_categories_dummies_{name}')

            self.logger.info(
                'Generating and writing correlations for logistic regression...')
            dummies_all = self.create_dummies(df, drop_references=False)
            dummies_all_y = dummies_all.merge(
                y,
                left_index=True,
                right_index=True,
                how='right'
            )

            # import seaborn as sns
            # sns.set_theme(style="white")
            # corr = dummies_all_y.corr()
            # f, ax = plt.subplots(figsize=(11, 9))
            # mask = np.triu(np.ones_like(corr, dtype=bool))
            # cmap = sns.diverging_palette(230, 20, as_cmap=True)
            # sns.heatmap(corr, mask=mask, cmap=cmap, vmax=.3, center=0,
            #             square=True, linewidths=.5, cbar_kws={"shrink": .5})
            # f.savefig(f'test_{name}.png')

            correlations = dummies_all_y \
                .corr(method='pearson')[y_label] \
                .drop(y_label) \
                .rename(f'{y_label}_pearson_correlation_coefficient')
            self.write_output(
                correlations.to_frame(), f'_statistics_categories_dummies_correlations_{name}')

            self.logger.info(
                'Running univariate logistic regression and writing output...')
            results = self.univariate_lr(dummies_y, y_label)
            (self.output_folder /
             f'univariate_models/models/{name}/').mkdir(parents=True, exist_ok=True)
            (self.output_folder /
             f'univariate_models/summaries/{name}/').mkdir(parents=True, exist_ok=True)

            for file_name, model_result in results.items():
                model_result.save(
                    self.output_folder / 'univariate_models/models' / name / f'{file_name}.pickle')

                (self.output_folder / 'univariate_models/summaries' / name / f'{file_name}.txt') \
                    .write_text(model_result.summary().as_text())

                (self.output_folder / 'univariate_models/summaries' / name / f'{file_name}.html') \
                    .write_text(model_result.summary().as_html())

                (self.output_folder / 'univariate_models/summaries' / name / f'{file_name}.csv') \
                    .write_text(model_result.summary().as_csv())

        self.logger.info('Generating and writing plots...')
        (self.output_folder / 'plots/').mkdir(parents=True, exist_ok=True)
        mpl.style.use('bmh')
        date = data['registration_date'].dt.to_period('M')
        self.pd.concat(
            [
                data.groupby(date).size().rename('studies'),
                data.groupby(date).size().cumsum().rename('cumulated studies')
            ], axis='columns').plot(
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
