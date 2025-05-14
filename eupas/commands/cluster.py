from difflib import SequenceMatcher
import logging
import re
import unicodedata

from scrapy.exceptions import UsageError
from eupas.commands import PandasCommand


# NOTE: Was only used for initial company name matching. Most values were reassigned manually.
class Command(PandasCommand):

    junk_words = frozenset({
        'pharma', 'pharmaceuticals', 'therapeutics', 'international', 'group',
        'cro', 'kk', 'pvt', 'nhs foundation trust'
    })

    def add_options(self, parser):
        '''
        Adds custom options to the base pandas command.
        '''
        PandasCommand.add_options(self, parser)
        group = parser.add_argument_group(title="Custom Grouping Options")
        group.add_argument(
            "-c",
            "--cutoff",
            metavar="CUTOFF",
            default=0.6,
            help="cutoff value for grouping"
        )

    def process_options(self, args, opts):
        PandasCommand.process_options(self, args, opts)
        try:
            self.cutoff = float(opts.cutoff)
            assert self.cutoff >= 0 and self.cutoff <= 1
        except (ValueError, AssertionError) as e:
            raise UsageError(
                "Invalid -c value, use a valid float between 0 and 1", print_help=False) from e

    def syntax(self):
        return "field_names [options]"

    def short_desc(self):
        return "Cluster specified columns"

    def serialize(self, s, casefold=True):
        '''
        Serializes and cleans up string for clustering.
        '''
        def filter_multiple_spaces(s):
            return re.sub(r'\s+', ' ', s)

        def filter_junk_chars(s):
            return re.sub(r'[^\w ]', '', s)

        def sub_junk_words(m):
            from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
            return '' if m.group() in self.junk_words.union(ENGLISH_STOP_WORDS) else m.group()

        def filter_junk_words(s):
            import cleanco
            return cleanco.basename(re.sub(r'\w+', sub_junk_words, s), suffix=True, prefix=True, middle=True)

        return filter_multiple_spaces(
            filter_junk_words(
                filter_junk_chars(
                    unicodedata.normalize('NFKD', str(s).casefold() if casefold else str(s)).encode(
                        'ASCII', 'ignore').decode('UTF-8', 'ignore')
                ))).strip()

    def run(self, args, opts):
        '''
        Clusters the unique values of the columns specified in the arguments.
        '''
        super().run(args, opts)

        import numpy as np
        import pandas as pd
        from sklearn.cluster import AffinityPropagation

        if len(args) == 0:
            raise UsageError(
                "running 'scrapy cluster' without additional arguments is not supported"
            )

        self.logger = logging.getLogger()
        self.logger.info('Starting cluster script')
        self.logger.info(f'Pandas {pd.__version__}')
        self.logger.info('Reading and cleaning input data...')

        input = self.read_input()
        if not set(args).issubset(set(input.columns.values)):
            raise UsageError(
                "At least one cluster value isn't a valid field name", print_help=False)

        dfs = {
            field_name:
            input.loc[:, [field_name]]
                .rename(columns={field_name: 'manual'})
                .dropna()
                .drop_duplicates()
                .assign(original=lambda x: x['manual'], clean=lambda x: x['original'].map(self.serialize))
                .sort_values(by=['clean'])

            for field_name in args
        }

        # TODO: Improve clustering performance
        self.logger.info('Starting affinity propagation...')
        matcher = SequenceMatcher()
        AP = AffinityPropagation(
            affinity='precomputed', max_iter=1000, convergence_iter=4)
        for field_name, df in dfs.items():
            self.logger.info(f'Clustering {field_name}...')
            clean_values = df['clean'].to_list()
            similarity = np.identity(df.shape[0])

            for i in range(df.shape[0]):
                for j in range(i):
                    short, long = sorted([clean_values[i], clean_values[j]])
                    matcher.set_seqs(long, short)
                    similarity[i, j] = similarity[j, i] = matcher.ratio()

            def norm_range(x):
                return (x - self.cutoff) / (1 - self.cutoff)

            similarity = np.where(
                similarity < .8, norm_range(similarity), similarity)

            clusters = AP.fit_predict(similarity)
            dfs[field_name] = df.assign(
                clusters=clusters).sort_values(by=['clusters'])

        self.logger.info('Writing output data...')

        # DEBUG: export unique field_names only
        # for field_name, df in dfs.items():
        #     values = df[field_name].tolist()
        #     with open(self.output_folder / f'values_{field_name}.txt', 'w') as f:
        #         f.write('\n'.join(values))

        with pd.ExcelWriter(self.output_folder / 'clusters.xlsx', engine='openpyxl') as writer:
            for field_name, df in dfs.items():
                df.to_excel(writer, sheet_name=field_name, index=False)
