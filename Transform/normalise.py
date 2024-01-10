from transform_main import Transform
import pandas as pd
from functools import reduce

class Normalise(Transform):

    def __init__(self):
        super().__init__()

    def merge_dataframes(self):
        df_list_to_merge = [self.clean_txt(), self.clean_json(), self.clean_data_csvs(), self.clean_business_csvs(),
                            self.clean_applicants_csvs(), self.clean_engineering_csvs()]

        merged_df = pd.merge(df_list_to_merge[0], df_list_to_merge[1], on=['Forename', 'Lastname'], how='outer')

        for df in df_list_to_merge[2:]:
            merged_df = pd.merge(merged_df, df, on=['Forename', 'Lastname'], how='outer')

        return merged_df

test = Normalise()
pd.set_option('display.max_columns', None)
print(test.merge_dataframes())