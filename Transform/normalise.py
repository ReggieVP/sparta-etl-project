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

        merged_df.insert(0, "Student_ID", range(1, 1 + len(merged_df)))

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


test = Normalise()
pd.set_option('display.max_columns', None)
print(test.merge_dataframes())




