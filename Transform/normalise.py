from transform_main import Transform
import pandas as pd


class Normalise(Transform):

    def __init__(self):
        super().__init__()

    def merge_courses(self):
        merge_courses_dfs = pd.merge(self.clean_data_csvs(), pd.merge(self.clean_engineering_csvs(), self.clean_business_csvs(),
        how='outer'), how='outer')
        return merge_courses_dfs

    def merge_dataframes(self):
        df_list_to_merge = [self.clean_txt(), self.clean_json(), self.merge_courses(), self.clean_applicants_csvs()]

        merged_df = pd.merge(df_list_to_merge[0], df_list_to_merge[1], on=['Forename', 'Lastname'], how='outer')

        for df in df_list_to_merge[2:]:
            merged_df = pd.merge(merged_df, df, on=['Forename', 'Lastname'], how='outer')

        return merged_df


test = Normalise()
pd.set_option('display.max_columns', None)
print(test.merge_dataframes())


