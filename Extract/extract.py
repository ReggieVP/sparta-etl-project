import boto3
import pandas as pd
import io


class Extract:

    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')
        self.bucket_name = 'data-eng-250-final-project'

    def calling_bucket_business(self):
        business_df = pd.DataFrame()
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
                    business_df['Date'] = date
                    business_df = pd.concat([business_df, bus_df])

        else:
            print("No objects found in the specified prefix.")

        print(business_df)

    def calling_bucket_data(self):
        Data_df = pd.DataFrame()
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix='Academy/')
        if 'Contents' in response:
            for obj in response['Contents']:
                if 'Data' in obj['Key']:  # Check if 'Business' is in the object key
                    obj_key = obj['Key']
                    date = obj_key.split("_", 1)[1]
                    date = date.split(".")[0]
                    Data_df['Date'] = date
                    obj_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    da_df = pd.read_csv(obj_content['Body'])
                    Data_df = pd.concat([Data_df, da_df])

        else:
            print("No objects found in the specified prefix.")

        print(Data_df)

    def calling_bucket_Engineering(self):
        Engineering_df = pd.DataFrame()
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix='Academy/')
        if 'Contents' in response:
            for obj in response['Contents']:
                if 'Engineering' in obj['Key']:  # Check if 'Business' is in the object key
                    obj_key = obj['Key']
                    date = obj_key.split("_", 1)[1]
                    date = date.split(".")[0]
                    Engineering_df['Date'] = date
                    obj_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    Engineer_df = pd.read_csv(obj_content['Body'])
                    Engineering_df = pd.concat([Engineering_df, Engineer_df])

        else:
            print("No objects found in the specified prefix.")

        print(Engineering_df)

    def read_talent_json(self):
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix='Talent/')
        if 'Contents' in response:
            for obj in response['Contents']:
                obj_key = obj['Key']
                obj_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)



        print(obj['Key'])

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
instance1 = Extract()
instance1.calling_bucket_business()
instance1.calling_bucket_data()



