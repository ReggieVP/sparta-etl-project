import pandas as pd
from Extract.extract_main import Extract

class TransformCSV(Extract):
    def __init__(self):
        super().__init__()

    def transform_business_csvs(self):
        business_df_transform = self.calling_bucket_business()
        business_df_transform = business_df_transform.fillna(0)
        business_df_transform['Date'] = business_df_transform['Date'].apply(lambda x: x[3:])
        business_df_transform['Date'] = pd.to_datetime(business_df_transform['Date'])
        business_df_transform = business_df_transform.reset_index()
        business_df_transform = business_df_transform.drop(columns=['index'])

        if 'name' in business_df_transform.columns:
            split_names = business_df_transform['name'].str.rsplit(' ', n=1, expand=True)
            business_df_transform['forename'] = split_names[0]
            business_df_transform['lastname'] = split_names[1] if len(split_names.columns) > 1 else None
            business_df_transform.drop(columns=['name'], inplace=True)  # Drop 'name' column after splitting

        if 'trainer' in business_df_transform.columns:
            split_names = business_df_transform['trainer'].str.rsplit(' ', n=1, expand=True)
            business_df_transform['trainer_forename'] = split_names[0]
            business_df_transform['trainer_lastname'] = split_names[1] if len(split_names.columns) > 1 else None
            business_df_transform.drop(columns=['trainer'], inplace=True)  # Drop 'name' column after splitting


        return business_df_transform

    def transform_engineering_csvs(self):
        engineering_df_transform = self.calling_bucket_engineering()
        engineering_df_transform = engineering_df_transform.fillna(0)
        engineering_df_transform['Date'] = engineering_df_transform['Date'].apply(lambda x: x[3:])
        engineering_df_transform['Date'] = pd.to_datetime(engineering_df_transform['Date'])
        engineering_df_transform = engineering_df_transform.reset_index()
        engineering_df_transform = engineering_df_transform.drop(columns=['index'])

        if 'name' in engineering_df_transform.columns:
            split_names = engineering_df_transform['name'].str.rsplit(' ', n=1, expand=True)
            engineering_df_transform['forename'] = split_names[0]
            engineering_df_transform['lastname'] = split_names[1] if len(split_names.columns) > 1 else None
            engineering_df_transform.drop(columns=['name'], inplace=True)  # Drop 'name' column after splitting

        if 'trainer' in engineering_df_transform.columns:
            split_names = engineering_df_transform['trainer'].str.rsplit(' ', n=1, expand=True)
            engineering_df_transform['trainer_forename'] = split_names[0]
            engineering_df_transform['trainer_lastname'] = split_names[1] if len(split_names.columns) > 1 else None
            engineering_df_transform.drop(columns=['trainer'], inplace=True)  # Drop 'name' column after splitting

        return engineering_df_transform

    def transform_data_csvs(self):
        data_df_transform = self.calling_bucket_data()
        data_df_transform = data_df_transform.fillna(0)
        data_df_transform['Date'] = data_df_transform['Date'].apply(lambda x: x[3:])
        data_df_transform['Date'] = pd.to_datetime(data_df_transform['Date'])
        data_df_transform = data_df_transform.reset_index()
        data_df_transform = data_df_transform.drop(columns=['index'])

        if 'name' in data_df_transform.columns:
            split_names = data_df_transform['name'].str.rsplit(' ', n=1, expand=True)
            data_df_transform['forename'] = split_names[0]
            data_df_transform['lastname'] = split_names[1] if len(split_names.columns) > 1 else None
            data_df_transform.drop(columns=['name'], inplace=True)  # Drop 'name' column after splitting

        if 'trainer' in data_df_transform.columns:
            split_names = data_df_transform['trainer'].str.rsplit(' ', n=1, expand=True)
            data_df_transform['trainer_forename'] = split_names[0]
            data_df_transform['trainer_lastname'] = split_names[1] if len(split_names.columns) > 1 else None
            data_df_transform.drop(columns=['trainer'], inplace=True)  # Drop 'name' column after splitting

        return data_df_transform



pd.set_option("max_colwidth",None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

instance2 = TransformCSV()
print(instance2.transform_engineering_csvs())
print(instance2.transform_data_csvs())
print(instance2.transform_business_csvs())


