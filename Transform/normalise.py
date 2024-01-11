from Transform.transform_main import Transform
import pandas as pd
import numpy as np


class Normalise(Transform):

    def __init__(self):
        super().__init__()
    def merge_courses(self):
        merge_courses_dfs = pd.merge(self.clean_data_csvs(),
                                     pd.merge(self.clean_engineering_csvs(), self.clean_business_csvs(),
                                              how='outer'), how='outer')
        return merge_courses_dfs

    def merge_dataframes(self):
        df_list_to_merge = [self.clean_txt(), self.clean_json(), self.merge_courses(), self.clean_applicants_csvs()]

        merged_df = pd.merge(df_list_to_merge[0], df_list_to_merge[1], on=['Forename', 'Lastname'], how='outer')

        for df in df_list_to_merge[2:]:
            merged_df = pd.merge(merged_df, df, on=['Forename', 'Lastname'], how='outer')

        merged_df.insert(0, "Student_ID", range(1, 1 + len(merged_df)))

        merged_df = merged_df.sort_values('Uni')
        merged_df['University_ID'] = (merged_df.groupby(['Uni']).cumcount() == 0).astype(int)
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
        merged_df['Talent_Team_ID'] = (
                    merged_df.groupby(['Talent_Forename', 'Talent_Lastname']).cumcount() == 0).astype(int)
        merged_df['Talent_Team_ID'] = merged_df['Talent_Team_ID'].cumsum()

        merged_df = merged_df.sort_values(['Trainer_Forename', 'Trainer_Lastname'])
        merged_df['Trainer_ID'] = (merged_df.groupby(['Trainer_Forename', 'Trainer_Lastname']).cumcount() == 0).astype(
            int)
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

    def tech_table(self):
        merged_df = self.merge_dataframes()
        selected_columns = ['C#', 'Java', 'R', 'Javascript', 'Python', 'C++', 'Ruby', 'SPSS', 'PHP']
        cols = {'Language_ID': [1, 2, 3, 4, 5, 6, 7, 8, 9],
                'Language': merged_df[selected_columns].columns.tolist()}

        language_df = pd.DataFrame(data=cols)
        return language_df

    def tech_score_table(self):
        merged_df = self.merge_dataframes()
        language_df = self.tech_table()
        tech_score_df = pd.DataFrame(columns=["Student_Id", "Langauge_ID", "Score"])
        for row in merged_df["Student_ID"]:
            for columns in language_df["Language"]:
                if not np.isnan(merged_df[columns][row - 1]):
                    tech_score_df.loc[len(tech_score_df.index)] = [row, language_df["Language_ID"] \
                        [language_df.index[language_df["Language"] == columns][0]], merged_df[columns][row - 1]]
        tech_score_df = tech_score_df.astype(int)
        return tech_score_df

    def trainers_table(self):
        merged_df = self.merge_dataframes()
        trainers = merged_df[["Trainer_ID", "Trainer_Forename", "Trainer_Lastname"]].drop_duplicates().sort_values(
            by=["Trainer_ID"]).copy()
        return trainers

    def courses_table(self):
        merged_df = self.merge_dataframes()
        courses = merged_df[["Course_ID", "Course", "Trainer_ID"]].drop_duplicates().sort_values(
            by=["Course_ID"]).copy()
        return courses

    def city_table(self):
        merged_df = self.merge_dataframes()
        city_df = merged_df[['City_ID', 'City']].copy()
        city_df = city_df.drop_duplicates()
        city_df = city_df.sort_values(by='City_ID')
        city_df = city_df.dropna()

        return city_df

    def postcode_table(self):
        merged_df = self.merge_dataframes()
        postcode_df = merged_df[['Postcode_ID', 'Postcode']].copy()
        postcode_df = postcode_df.drop_duplicates()
        postcode_df = postcode_df.sort_values(by='Postcode_ID')
        postcode_df = postcode_df.dropna()
        return postcode_df

    def address_table(self):
        merged_df = self.merge_dataframes()
        address_df = merged_df[["Address_ID", "Student_ID", "Address", "City_ID", "Postcode_ID"]]
        address_df = address_df.drop_duplicates()
        address_df = address_df.sort_values(by='Address_ID')
        address_df = address_df.dropna()
        return address_df

    def university_table(self):
        merged_df = self.merge_dataframes()
        university = merged_df[["University_ID", "Uni"]].drop_duplicates().sort_values(
            by=["University_ID"]).copy()
        university = university.dropna()

        return university

    def grade_table(self):
        merged_df = self.merge_dataframes()
        grades = merged_df[["Grade_ID", "Degree"]].drop_duplicates().sort_values(by=["Grade_ID"]).copy()

        return grades

    def gender_table(self):
        merged_df = self.merge_dataframes()
        gender = merged_df[["Gender_ID", "Gender"]].drop_duplicates().sort_values(by=["Gender_ID"]).copy()

        return gender

    def education(self):
        merged_df = self.merge_dataframes()
        education = merged_df[["Student_ID", "University_ID", "Grade_ID"]].drop_duplicates().sort_values(
            by=["Student_ID"]).copy()

        return education

    def talent_team(self):
        merged_df = self.merge_dataframes()
        talent_team = merged_df[
            ["Talent_Team_ID", "Talent_Forename", "Talent_Lastname"]].drop_duplicates().sort_values(
            by=["Talent_Team_ID"]).copy()

        return talent_team

    def metrics_table(self):
        metrics = {'metric_id': [1, 2, 3, 4, 5, 6],
                   'metric': ['Analytic', 'Independent', 'Determined', 'Professional', 'Studious', 'Imaginative']}
        metrics_df = pd.DataFrame(data=metrics)

        return metrics_df

    def weeks_table(self):
        weeks = {'week_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                 'week_number': ['W1', 'W2', 'W3', 'W4', 'W5', 'W6', 'W7', 'W8', 'W9', 'W10']}
        weeks_df = pd.DataFrame(data=weeks)

        return weeks_df

    def weekly_scores_table(self):
        merged_df = self.merge_dataframes()
        metrics_df = self.metrics_table()
        weeks_df = self.weeks_table()
        subset_weeks = merged_df.iloc[:, [0] + list(range(24, 84))]
        weekly_scores = {"Student_ID": [], "Metric_ID": [], "Week_ID": [], "Score": []}

        student_id_list = []
        metric_id_list = []
        week_id_list = []
        score_list = []

        for index, row in subset_weeks.iterrows():
            metric_id = 0
            week_id = 0
            student_id = row["Student_ID"]
            for col in subset_weeks.columns[1:]:
                col_name = col.split("_")
                if col_name[0] != "Student_ID" and col_name[0] in metrics_df["metric"].values:
                    metric_index = metrics_df.index[metrics_df["metric"] == col_name[0]][0]
                    metric_id = metrics_df._get_value(metric_index, "metric_id")
                    week_index = weeks_df.index[weeks_df["week_number"] == col_name[1]][0]
                    week_id = weeks_df._get_value(week_index, "week_id")
                    score = row[col]
                    student_id_list.append(student_id)
                    metric_id_list.append(metric_id)
                    week_id_list.append(week_id)
                    score_list.append(score)

        weekly_scores["Student_ID"] = student_id_list
        weekly_scores["Metric_ID"] = metric_id_list
        weekly_scores["Week_ID"] = week_id_list
        weekly_scores["Score"] = score_list

        weekly_scores_df = pd.DataFrame(weekly_scores)

        return weekly_scores_df

    def precourses(self):
        merged_df = self.merge_dataframes()
        precourse = merged_df[["Student_ID", "Psychometrics(%)", "Presentation(%)", "Course_Interest",
                               "Result", "Sparta_Day_Date", "Application_Date"]].copy()
        precourse.insert(0, "Precourse_ID", range(1, 1 + len(precourse)))
        precourse = precourse.drop_duplicates()
        precourse = precourse.sort_values(by=["Precourse_ID"])
        return precourse

    def students(self):
        merged_df = self.merge_dataframes()
        student = merged_df[
                ["Student_ID", "Forename", "Lastname", "Dob", "Gender", "Email", "Address_ID", "Phone_Number",
                 "Self_Development", "Geo_Flex", "Financial_Support_Self", "Course_ID", "Talent_Team_ID",
                 "Start_Date"]].drop_duplicates().sort_values(by=["Student_ID"]).copy()
        student = student.dropna()

        return student


#test = Normalise()
#pd.set_option('display.max_columns', None)

#print(test.precourses())
