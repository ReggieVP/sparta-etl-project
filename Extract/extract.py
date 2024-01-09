import boto3
import pandas as pd
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
        data_df = pd.DataFrame()
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
                    data_df['Date'] = date
                    date_list += [date] * len(da_df)  # Replicate date for each row in bus_df
                    data_df = pd.concat([data_df, da_df])

        else:
            print("No objects found in the specified prefix.")

        data_df['Date'] = date_list[:len(data_df)]
        print(data_df)

    def calling_bucket_engineering(self):
        engineering_df = pd.DataFrame()
        date_list = []
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix='Academy/')
        if 'Contents' in response:
            for obj in response['Contents']:
                if 'Engineering' in obj['Key']:  # Check if 'Business' is in the object key
                    obj_key = obj['Key']
                    obj_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    engineer_df = pd.read_csv(obj_content['Body'])
                    date = obj_key.split("_", 1)[1]
                    date = date.split(".")[0]
                    date_list += [date] * len(engineer_df)
                    engineering_df = pd.concat([engineering_df, engineer_df])

        else:
            print("No objects found in the specified prefix.")
        engineering_df['Date'] = date_list[:len(engineering_df)]
        print(engineering_df)

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
        for page in self.paginator.paginate(Bucket=self.bucket_name):
            for obj in page['Contents']:
                if 'Applicants' in obj['Key']:  # Check if 'Business' is in the object key
                    obj_key = obj['Key']
                    obj_content = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj_key)
                    appli_df = pd.read_csv(obj_content['Body'])
                    applicants_df = pd.concat([applicants_df, appli_df])

        else:
            print("No objects found in the specified prefix.")

        print(applicants_df)

    def calling_bucket_txt(self):
        text_df = pd.DataFrame()
        for page in self.paginator.paginate(Bucket=self.bucket_name):
            for obj in page['Contents']:
                if '.txt' in obj['Key']:
                    obj_key = obj['Key']
                    s3_objects = self.s3_client.get_object(Bucket=self.bucket_name, Key='Talent/Sparta Day 16 January 2019.txt')
                    txt_df = pd.read_csv(s3_objects['Body'], sep='\t')

                    name = ((txt_df.columns[0]))
                    txt_df = txt_df.drop([0])
                    txt_df = pd.DataFrame(txt_df.iloc[:, 0].str.replace("-", ","))
                    txt_df= pd.DataFrame(txt_df.iloc[:, 0].str.split(",").tolist(),columns=["Name", "Psychometrics", "Presentation"])
                    txt_df.insert(3, "Date", name)
                    txt_df["Psychometrics"] = txt_df["Psychometrics"].str[17:19]
                    txt_df["Presentation"] = txt_df["Presentation"].str[15:17]
                    txt_df["Name"] = txt_df["Name"].str.upper().str.title()
                    txt_df["Date"] = txt_df["Date"]
                    txt_df["Date"] = pd.to_datetime(txt_df["Date"])
                    text_df = pd.concat([text_df, txt_df])

        print(text_df)




instance1 = Extract()
instance1.calling_bucket_business()
instance1.calling_bucket_engineering()
instance1.calling_bucket_data()
instance1.calling_bucket_json()
instance1.calling_bucket_applicants()
instance1.calling_bucket_txt()



