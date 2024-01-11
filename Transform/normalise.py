from transform_main import Transform
import pandas as pd
import numpy as np

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

        merged_df.insert(0, "Student_ID", range(1, 1 + len(merged_df)))

        merged_df = merged_df.sort_values('Uni')
        merged_df['University_ID'] = (merged_df.groupby(['Uni']).cumcount()==0).astype(int)
        merged_df['University_ID'] = merged_df['University_ID'].cumsum()

        merged_df = merged_df.sort_values('Degree')
        merged_df['Grade_ID'] = (merged_df.groupby(['Degree']).cumcount() == 0).astype(int)
        merged_df['Grade_ID'] = merged_df['Grade_ID'].cumsum()

        merged_df = merged_df.sort_values('Postcode')
        merged_df['Postcode_ID'] = (merged_df.groupby(['Postcode']).cumcount() == 0).astype(int)
        merged_df['Postcode_ID'] = merged_df['Postcode_ID'].cumsum()

        merged_df = merged_df.sort_values('Gender')
        merged_df['Gender_ID'] = (merged_df.groupby(['Gender']).cumcount() == 0).astype(int)
        merged_df['Gender_ID'] = merged_df['Gender_ID'].cumsum()

        merged_df = merged_df.sort_values('City')
        merged_df['City_ID'] = (merged_df.groupby(['City']).cumcount() == 0).astype(int)
        merged_df['City_ID'] = merged_df['City_ID'].cumsum()

        merged_df = merged_df.sort_values('Address')
        merged_df['Address_ID'] = (merged_df.groupby(['Address']).cumcount() == 0).astype(int)
        merged_df['Address_ID'] = merged_df['Address_ID'].cumsum()

        merged_df = merged_df.sort_values('Course')
        merged_df['Course_ID'] = (merged_df.groupby(['Course']).cumcount() == 0).astype(int)
        merged_df['Course_ID'] = merged_df['Course_ID'].cumsum()

        merged_df = merged_df.sort_values(['Talent_Forename', 'Talent_Lastname'])
        merged_df['Talent_Team_ID'] = (merged_df.groupby(['Talent_Forename', 'Talent_Lastname']).cumcount() == 0).astype(int)
        merged_df['Talent_Team_ID'] = merged_df['Talent_Team_ID'].cumsum()

        merged_df = merged_df.sort_values(['Trainer_Forename', 'Trainer_Lastname'])
        merged_df['Trainer_ID'] = (merged_df.groupby(['Trainer_Forename', 'Trainer_Lastname']).cumcount() == 0).astype(int)
        merged_df['Trainer_ID'] = merged_df['Trainer_ID'].cumsum()

        return merged_df

    def weakness_table(self):
        merged_df = self.merge_dataframes()
        weakness_df = pd.DataFrame(columns=['Weaknesses_ID', 'Weaknesses'])
        # Appending each weakness to a list, ignoring NaN values
        weakness_list = []
        for person_weakness in merged_df["Weaknesses"]:
            if isinstance(person_weakness, list):
                for weakness in person_weakness:
                    weakness_list.append(weakness)
        # Converting the list into a set, so duplicates are removed
        weakness_list = list(set(weakness_list))
        weakness_df = pd.DataFrame(data=weakness_list, columns=['Weaknesses'])
        weakness_df.insert(0, "Weaknesses_ID", range(1, 1 + len(weakness_df)))
        return weakness_df

    def weakness_junction_df(self):
        merged_df = self.merge_dataframes()
        weakness_df = self.weakness_table()
        student_weakness_df = pd.DataFrame(columns=["Student_Id", "Weaknesses_ID"])
        for row in merged_df["Student_ID"]:
            if isinstance(merged_df["Weaknesses"][row - 1], list):
                word_list = merged_df["Weaknesses"][row - 1]
                for word in word_list:
                    student_weakness_df.loc[len(student_weakness_df.index)] = [row, weakness_df["Weaknesses_ID"][
                        weakness_df.index[weakness_df["Weaknesses"] == word][0]]]
        return student_weakness_df

    def strength_table(self):
        merged_df = self.merge_dataframes()
        strength_df = pd.DataFrame(columns=['Strengths_ID', 'Strengths'])
        # Appending each weakness to a list, ignoring NaN values
        strength_list = []
        for person_strength in merged_df["Strengths"]:
            if isinstance(person_strength, list):
                for strength in person_strength:
                    strength_list.append(strength)
        # Converting the list into a set, so duplicates are removed
        strength_list = list(set(strength_list))
        strength_df = pd.DataFrame(data=strength_list, columns=['Strengths'])
        strength_df.insert(0, "Strengths_ID", range(1, 1 + len(strength_df)))
        return strength_df

    def strength_junction_table(self):
        merged_df = self.merge_dataframes()
        strength_df = self.strength_table()
        student_strength_df = pd.DataFrame(columns=["Student_Id", "Strengths_ID"])
        for row in merged_df["Student_ID"]:
            if isinstance(merged_df["Strengths"][row - 1], list):
                word_list = merged_df["Strengths"][row - 1]
                for word in word_list:
                    student_strength_df.loc[len(student_strength_df.index)] = [row, strength_df["Strengths_ID"][
                        strength_df.index[strength_df["Strengths"] == word][0]]]
        return student_strength_df

    def tech_score_table(self):
        merged_df = self.merge_dataframes()
        selected_columns = ['C#', 'Java', 'R', 'Javascript', 'Python', 'C++', 'Ruby', 'SPSS', 'PHP']
        cols = {'Language_ID': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                'Language': merged_df[selected_columns].columns.tolist()}

        language_df = pd.DataFrame(data=cols)
        #print(language_df)


        tech_score_df = pd.DataFrame(columns=["Student_Id", "Langauge_ID", "Score"])
        for row in merged_df["Student_ID"]:
            for columns in language_df["Language"]:
                if not np.isnan(merged_df[columns][row - 1]):
                    tech_score_df.loc[len(tech_score_df.index)] = [row, language_df["Language_ID"]\
                        [language_df.index[language_df["Language"] == columns][0]], merged_df[columns][row - 1]]

        tech_score_df.astype(int)



test = Normalise()
pd.set_option('display.max_columns', None)
print(test.merge_dataframes())




