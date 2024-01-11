import pandas as pd
import datetime as dt
import re
from Extract.extract_main import Extract

class Transform(Extract):
    def __init__(self):
        super().__init__()

    def clean_txt(self):
        clean_txt_df = self.calling_bucket_txt()

        clean_txt_df = clean_txt_df.drop_duplicates()
        clean_txt_df = clean_txt_df.reset_index()
        clean_txt_df = clean_txt_df.drop(columns= ["index"])
        clean_txt_df["Presentation"] = round(clean_txt_df["Presentation"].astype(int)*100/32, 0).astype(int)
        clean_txt_df = clean_txt_df.rename(columns={"Psychometrics": "Psychometrics(%)", "Presentation": "Presentation(%)"})
        clean_txt_df = clean_txt_df.rename(columns={"Date": "Sparta_Day_Date"})

        if "Name" in clean_txt_df.columns:
            split_names = clean_txt_df["Name"].str.rsplit(' ', n=1, expand=True)
            clean_txt_df.insert(0,"Forename",split_names[0])
            clean_txt_df.insert(1,"Lastname",split_names[1])
            clean_txt_df.drop(columns=["Name"], inplace=True)  # Drop 'name' column after splitting

        return clean_txt_df


    def clean_json(self):
        clean_json_df = self.calling_bucket_json()

        clean_json_df = clean_json_df.reset_index()
        clean_json_df = clean_json_df.drop(columns = ["index"])

        clean_json_df = clean_json_df.drop(columns=["date"])
        clean_json_df.columns = clean_json_df.columns.str.upper().str.title()
        clean_json_df = clean_json_df.rename(columns={"Tech_Self_Score.C#": "C#", "Tech_Self_Score.Java": "Java",
                                                      "Tech_Self_Score.R": "R", "Tech_Self_Score.Javascript": "Javascript",
                                                      "Tech_Self_Score.Python": "Python","Tech_Self_Score.C++": "C++",
                                                      "Tech_Self_Score.Ruby": "Ruby","Tech_Self_Score.Spss": "SPSS",
                                                      "Tech_Self_Score.Php": "PHP"})

        if "Name" in clean_json_df.columns:
            split_names = clean_json_df["Name"].str.rsplit(' ', n=1, expand=True)
            clean_json_df.insert(0,"Forename",split_names[0])
            clean_json_df.insert(1,"Lastname",split_names[1])
            clean_json_df.drop(columns=["Name"], inplace=True)  # Drop 'name' column after splitting

        return clean_json_df


    def clean_business_csvs(self):
        clean_business_df = self.calling_bucket_business()

        clean_business_df.columns = clean_business_df.columns.str.upper().str.title()

        clean_business_df = clean_business_df.fillna(0)
        clean_business_df["Date"] = clean_business_df["Date"].apply(lambda x: x[3:])
        clean_business_df["Date"] = pd.to_datetime(clean_business_df["Date"])
        clean_business_df = clean_business_df.reset_index()
        clean_business_df = clean_business_df.drop(columns=["index"])
        clean_business_df = clean_business_df.rename(columns={"Date": "Start_Date"})
        clean_business_df["Course"] = "Business"

        if "Name" in clean_business_df.columns:
            split_names = clean_business_df["Name"].str.rsplit(' ', n=1, expand=True)
            clean_business_df.insert(0,"Forename",split_names[0])
            clean_business_df.insert(1,"Lastname",split_names[1])
            clean_business_df.drop(columns=["Name"], inplace=True)  # Drop 'name' column after splitting

        if "Trainer" in clean_business_df.columns:
            split_names = clean_business_df["Trainer"].str.rsplit(' ', n=1, expand=True)
            clean_business_df.insert(2,"Trainer_Forename",split_names[0])
            clean_business_df.insert(3,"Trainer_Lastname",split_names[1])
            clean_business_df.drop(columns=["Trainer"], inplace=True)  # Drop 'name' column after splitting

        return clean_business_df
    def clean_engineering_csvs(self):
        clean_engineering_df = self.calling_bucket_engineering()

        clean_engineering_df.columns = clean_engineering_df.columns.str.upper().str.title()

        clean_engineering_df = clean_engineering_df.fillna(0)
        clean_engineering_df["Date"] = clean_engineering_df["Date"].apply(lambda x: x[3:])
        clean_engineering_df["Date"] = pd.to_datetime(clean_engineering_df["Date"])
        clean_engineering_df = clean_engineering_df.reset_index()
        clean_engineering_df = clean_engineering_df.drop(columns=["index"])
        clean_engineering_df = clean_engineering_df.rename(columns={"Date": "Start_Date"})
        clean_engineering_df["Course"] = "Engineering"

        if "Name" in clean_engineering_df.columns:
            split_names = clean_engineering_df["Name"].str.rsplit(' ', n=1, expand=True)
            clean_engineering_df.insert(0,"Forename",split_names[0])
            clean_engineering_df.insert(1,"Lastname",split_names[1])
            clean_engineering_df.drop(columns=["Name"], inplace=True)  # Drop 'name' column after splitting

        if "Trainer" in clean_engineering_df.columns:
            split_names = clean_engineering_df["Trainer"].str.rsplit(' ', n=1, expand=True)
            clean_engineering_df.insert(2,"Trainer_Forename",split_names[0])
            clean_engineering_df.insert(3,"Trainer_Lastname",split_names[1])
            clean_engineering_df.drop(columns=["Trainer"], inplace=True)  # Drop 'name' column after splitting

        return clean_engineering_df
    def clean_data_csvs(self):
        clean_data_df = self.calling_bucket_data()

        clean_data_df.columns = clean_data_df.columns.str.upper().str.title()

        clean_data_df = clean_data_df.fillna(0)
        clean_data_df["Date"] = clean_data_df["Date"].apply(lambda x: x[3:])
        clean_data_df["Date"] = pd.to_datetime(clean_data_df["Date"])
        clean_data_df = clean_data_df.reset_index()
        clean_data_df = clean_data_df.drop(columns=['index'])
        clean_data_df = clean_data_df.rename(columns={"Date": "Start_Date"})
        clean_data_df["Course"] = "Data"

        if "Name" in clean_data_df.columns:
            split_names = clean_data_df["Name"].str.rsplit(' ', n=1, expand=True)
            clean_data_df.insert(0,"Forename",split_names[0])
            clean_data_df.insert(1,"Lastname",split_names[1])
            clean_data_df.drop(columns=["Name"], inplace=True)  # Drop 'name' column after splitting

        if "Trainer" in clean_data_df.columns:
            split_names = clean_data_df["Trainer"].str.rsplit(' ', n=1, expand=True)
            clean_data_df.insert(2,"Trainer_Forename",split_names[0])
            clean_data_df.insert(3,"Trainer_Lastname",split_names[1])
            clean_data_df.drop(columns=["Trainer"], inplace=True)  # Drop 'name' column after splitting

        return clean_data_df
    
    def clean_applicants_csvs(self):
        clean_applicants_df = self.calling_bucket_applicants()

        clean_applicants_df = clean_applicants_df.reset_index()
        clean_applicants_df = clean_applicants_df.drop(columns=["index"])

        clean_applicants_df["phone_number"] = clean_applicants_df["phone_number"].str.replace("(", "")
        clean_applicants_df["phone_number"] = clean_applicants_df["phone_number"].str.replace(")", "")
        clean_applicants_df["phone_number"] = clean_applicants_df["phone_number"].str.replace("-", "")
        clean_applicants_df["phone_number"] = clean_applicants_df["phone_number"].str.replace(" ", "")

        clean_applicants_df.columns = clean_applicants_df.columns.str.upper().str.title()

        clean_applicants_df["Invited_Date"] = clean_applicants_df["Invited_Date"].fillna(0).astype(int)
        clean_applicants_df["Month"] = clean_applicants_df["Month"].str.upper().str.title()
        clean_applicants_df["Month"] = clean_applicants_df["Month"].str.replace("Sept", "September")
        clean_applicants_df["Date"] = clean_applicants_df["Invited_Date"].astype(int).astype(str) + " " + clean_applicants_df["Month"]
        clean_applicants_df["Date"] = pd.to_datetime(clean_applicants_df["Date"])
        clean_applicants_df = clean_applicants_df.rename(columns={"Date": "Application_Date"})

        clean_applicants_df = clean_applicants_df.drop(columns=["Invited_Date", "Month", "Id"])
        clean_applicants_df["Dob"] = pd.to_datetime(clean_applicants_df["Dob"], format="%d/%m/%Y")

        if "Name" in clean_applicants_df.columns:
            split_names = clean_applicants_df["Name"].str.rsplit(' ', n=1, expand=True)
            clean_applicants_df.insert(0,"Forename",split_names[0])
            clean_applicants_df.insert(1,"Lastname",split_names[1])
            clean_applicants_df.drop(columns=["Name"], inplace=True)  # Drop 'name' column after splitting

        if "Invited_By" in clean_applicants_df.columns:
            split_names = clean_applicants_df["Invited_By"].str.rsplit(' ', n=1, expand=True)
            clean_applicants_df.insert(2,"Talent_Forename",split_names[0])
            clean_applicants_df.insert(3,"Talent_Lastname",split_names[1])
            clean_applicants_df.drop(columns=["Invited_By"], inplace=True)  # Drop 'name' column after splitting

        return clean_applicants_df



pd.set_option('display.max_columns', None)


