
import unittest
from unittest.mock import patch, MagicMock
from io import BytesIO
import pandas as pd
from Transform.transform_main import Transform
import datetime as dt

class TestTransform(unittest.TestCase):
    #------------------------------------------------------------------------
    # Test for txt transform to check dataframe formatted correctly
    #------------------------------------------------------------------------
    def test_txt_transform(self):
    # The TXT Transform changes column names and returns the data
    # This test will check if the column Sparta_Day_Date is present and checks that the column Name
    # has been delete
        transformer = Transform()
        result_df = transformer.clean_txt()

        column_names = result_df.columns.tolist()

        self.assertIn('Sparta_Day_Date', column_names)
        self.assertNotIn('Name', column_names)

    # ------------------------------------------------------------------------
    # Test for JSON transform to check dataframe formatted correctly
    # ------------------------------------------------------------------------

    def test_json_transform(self):
    # The JSON Transform changes column names and returns the data
    # This test will check if two of the columns have chnged to Java & PHP
    # And check the number of rows is as expected
        transformer = Transform()
        result_df = transformer.clean_json()

        column_names = result_df.columns.tolist()
        df_len = len(result_df)

        self.assertIn('Java', column_names)
        self.assertIn('PHP', column_names)
        self.assertEqual(df_len, 3105)

    # ------------------------------------------------------------------------
    # Test for business transform to check dataframe formatted correctly
    # ------------------------------------------------------------------------

    def test_business_transform(self):
    # The Business Transform changes column names and returns the data
    # This test will check if the column Start_Date is present and checks that the column Name
    # has been delete
        transformer = Transform()
        result_df = transformer.clean_business_csvs()

        column_names = result_df.columns.tolist()

        self.assertIn('Start_Date', column_names)
        self.assertNotIn('Name', column_names)

    # ------------------------------------------------------------------------
    # Test for engineering transform to check dataframe formatted correctly
    # ------------------------------------------------------------------------

    def test_engineering_transform(self):
    # The Engineering Transform changes column names and returns the data
    # This test will check if the column Start_Date is present and checks that the column Name
    # has been deleted
        transformer = Transform()
        result_df = transformer.clean_engineering_csvs()

        column_names = result_df.columns.tolist()

        self.assertIn('Start_Date', column_names)
        self.assertNotIn('Name', column_names)

    # ------------------------------------------------------------------------
    # Test for data transform to check dataframe formatted correctly
    # ------------------------------------------------------------------------

    def test_data_transform(self):
    # The Data Transform changes column names and returns the data
    # This test will check if the column Start_Date is present and checks that the column Name
    # has been deleted
        transformer = Transform()
        result_df = transformer.clean_data_csvs()

        column_names = result_df.columns.tolist()

        self.assertIn('Start_Date', column_names)
        self.assertNotIn('Name', column_names)

    # ------------------------------------------------------------------------
    # Test for applicants transform to check dataframe formatted correctly
    # ------------------------------------------------------------------------

    def test_applicants_transform(self):
    # The Applicants Transform changes column names and returns the data
    # This test will check if the columns Invited_Date and  Name
    # have been deleted
        transformer = Transform()
        result_df = transformer.clean_data_csvs()

        column_names = result_df.columns.tolist()

        self.assertNotIn('Invited_Date', column_names)
        self.assertNotIn('Name', column_names)

if __name__ == '__main__':
    unittest.main()
