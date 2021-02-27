from ast import literal_eval
import pandas as pd
import Levenshtein
from collections import defaultdict

col_name_merch = 'plaid_dirty_name'
col_name_merch_category = 'plaid_category'
col_name_date = 'txn_date'

data = pd.read_csv('data/interview-data.csv', index_col=col_name_merch)
data[col_name_merch_category] = data[col_name_merch_category].apply(literal_eval)

dataset_len = len(data)

print('Length of the dataset:', dataset_len)

print('N Unique Merchs: ', len(data.index.unique()))

# first approach
dists = defaultdict(dict)

# if unique names are more than 3% values of the dataset consider it a unique merchant
# Dataset is being generated from the same distribution.
# it can reduce the complexity in further calculations.
um = []

# simple uniques
freqs = data.index.value_counts()
merchs_freq_gt_3_percent = freqs[(freqs / sum(freqs)) > 0.03]

remaining_data = data[~data.index.isin(merchs_freq_gt_3_percent.index)]
remaining_dataset_percent = round(len(remaining_data)/dataset_len * 100, 2)
print("Remaining Dataset length after clearing top 3% repeated merchs by name:", str(remaining_dataset_percent) + '%', 'of original dataset.')

len_unique_merchs = len(remaining_data.index.unique())
print("Unique merchants after clearing some very frequent merchs:", len_unique_merchs)

remaining_data.index = remaining_data.index.str.replace("\d+", ' ', regex=True)
remaining_data.index.unique()
data = data.explode(col_name_merch_category)
col_name_weekday = 'weekday'
data['weekday'] = pd.to_datetime(data[col_name_date]).dt.weekday

# applying leveinshtein ratio technique to determine accurate match

class GetTopMatches:

    def __init__(self, names):
        self.names = names

    def __iter__(self):
        for src in self.names:
            for dest in self.names:
                yield src, dest, self[src, dest]

    def __getitem__(self, tup):
        return self._calc_dist(*tup)

    def _calc_dist(self, src, dest):
        return Levenshtein.ratio(src, dest)

g = GetTopMatches(remaining_data.index)

ratio_dict = defaultdict(dict)

for src, dest, ratio in g:
    ratio_dict[src][dest] = ratio

lev_ratio_frame = pd.DataFrame(ratio_dict)

lev_ratio_frame = lev_ratio_frame.drop_duplicates()

traversed = []
uniques = []
for merch_name, ratios in lev_ratio_frame.iterrows():
    if merch_name not in traversed:
        unique_merch_list = ratios[ratios > 0.7].index.tolist()
        uniques.append(unique_merch_list[0])
        traversed.extend(unique_merch_list)

for unique in uniques + merchs_freq_gt_3_percent.index.tolist():
    print(unique)

print('Total Uniques =', len(uniques + merchs_freq_gt_3_percent.index.tolist()))
# print('After duplicated merchants removal. number of unique merchs', len_unique_merchs - reduction + len(merchs_freq_gt_3_percent))

