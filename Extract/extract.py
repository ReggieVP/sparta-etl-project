import boto3
import pandas as pd
import io
import json


class Extract:

    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = 'data-eng-250-final-project'
        self.paginator = self.s3_client.get_paginator('list_objects_v2')

    def calling_bucket_business(self):
        business_df = pd.DataFrame()
        date_list = []  # Store individual date values in a list
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix='Academy/')
        if 'Contents' in response:
            for obj in response['Contents']:
                print(obj['Key'])
                if 'Business' in obj['Key']:  # Check if 'Business' is in the object key
                    obj_key = obj['Key']
                    obj_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    bus_df = pd.read_csv(obj_content['Body'])
                    date = obj_key.split("_", 1)[1]
                    date = date.split(".")[0]
                    date_list += [date] * len(bus_df)  # Replicate date for each row in bus_df
                    business_df = pd.concat([business_df, bus_df])

        else:
            print("No objects found in the specified prefix.")

        business_df['Date'] = date_list[:len(business_df)]  # Assign dates to rows in business_df
        print(business_df)

    def calling_bucket_data(self):
        Data_df = pd.DataFrame()
        date_list = []  # Store individual date values in a list
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix='Academy/')
        if 'Contents' in response:
            for obj in response['Contents']:
                if 'Data' in obj['Key']:  # Check if 'Business' is in the object key
                    obj_key = obj['Key']
                    obj_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    da_df = pd.read_csv(obj_content['Body'])
                    date = obj_key.split("_", 1)[1]
                    date = date.split(".")[0]
                    Data_df['Date'] = date
                    date_list += [date] * len(da_df)  # Replicate date for each row in bus_df
                    Data_df = pd.concat([Data_df, da_df])

        else:
            print("No objects found in the specified prefix.")

        Data_df['Date'] = date_list[:len(Data_df)]
        print(Data_df)

    def calling_bucket_Engineering(self):
        Engineering_df = pd.DataFrame()
        date_list = []
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix='Academy/')
        if 'Contents' in response:
            for obj in response['Contents']:
                if 'Engineering' in obj['Key']:  # Check if 'Business' is in the object key
                    obj_key = obj['Key']
                    obj_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    Engineer_df = pd.read_csv(obj_content['Body'])
                    date = obj_key.split("_", 1)[1]
                    date = date.split(".")[0]
                    date_list += [date] * len(Engineer_df)
                    Engineering_df = pd.concat([Engineering_df, Engineer_df])

        else:
            print("No objects found in the specified prefix.")
        Engineering_df['Date'] = date_list[:len(Engineering_df)]
        print(Engineering_df)

    def calling_bucket_json(self):
        json_files_df = pd.DataFrame()
        for page in self.paginator.paginate(Bucket=self.bucket_name):

            for obj in page['Contents']:
                if '.json' in obj['Key']:
                    obj_key = obj['Key']
                    obj_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    json_file = json.load(obj_content['Body'])
                    json_df = pd.json_normalize(json_file)
                    json_df.fillna('na', inplace=True)
                    json_df.to_csv('talent.csv', index=False)

                    json_files_df =pd.concat([json_files_df,json_df])

        print(json_files_df)


    def calling_bucket_applicants(self):
        applicants_df = pd.DataFrame()
        date_list = []  # Store individual date values in a list
        for page in self.paginator.paginate(Bucket=self.bucket_name):
            for obj in page['Contents']:
                print(obj['Key'])
                if 'Applicants' in obj['Key']:  # Check if 'Business' is in the object key
                    obj_key = obj['Key']
                    obj_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    appli_df = pd.read_csv(obj_content['Body'])
                    date = obj_key.split("9", 1)[0]
                    date_list += [date] * len(appli_df)  # Replicate date for each row in bus_df
                    applicants_df = pd.concat([applicants_df, appli_df])

        else:
            print("No objects found in the specified prefix.")

        applicants_df['Date'] = date_list[:len(applicants_df)]  # Assign dates to rows in business_df
        print(applicants_df)


instance1 = Extract()
instance1.calling_bucket_json()




