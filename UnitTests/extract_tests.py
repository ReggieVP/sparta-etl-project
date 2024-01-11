import unittest
from unittest.mock import patch, Mock
from io import BytesIO
import pandas as pd
from Extract.extract_main import Extract


class TestDataProcessor(unittest.TestCase):

    #------------------------------------------------------------------------
    # Test for business csv extract to check dataframe formatted correctly
    #------------------------------------------------------------------------
    @patch('Extract.extract_main.boto3.client')
    def test_calling_bucket_business_returns_correct_dataframe(self, mock_s3_client):

        csv_content = b'Name,Value,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40\n'\
                      b'Gustaf Lude,Gregor Gomez,6,4,1,1,2,3,1,1,1,2,1,6,6,2,2,3,3,2,5,1,3,7,6,2,6,8,4,8,1,8,7,3,5,8,8,7,8,5,8,8\n'

        mock_s3 = Mock()
        mock_s3.get_object.return_value = {'Body': BytesIO(csv_content)}
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'Academy/Business_20_2019-02-11.csv'}]
        }
        mock_s3_client.return_value = mock_s3

        extractor = Extract()

        result_df = extractor.calling_bucket_business()
        print("Result")
        print(result_df)
        expected_data = {
            'Name': ['Gustaf Lude'],
            'Value': ['Gregor Gomez'],
            '1': [6], '2': [4], '3': [1], '4': [1], '5': [2], '6': [3],
            '7': [1], '8': [1], '9': [1], '10': [2], '11': [1], '12': [6],
            '13': [6], '14': [2], '15': [2], '16': [3], '17': [3], '18': [2],
            '19': [5], '20': [1], '21': [3], '22': [7], '23': [6], '24': [2],
            '25': [6], '26': [8], '27': [4], '28': [8], '29': [1], '30': [8],
            '31': [7], '32': [3], '33': [5], '34': [8], '35': [8], '36': [7],
            '37': [8], '38': [5], '39': [8], '40': [8], 'Date': ['20_2019-02-11']
        }
        expected_df = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df)

    #------------------------------------------------------------------------
    # Test for Data csv extract to check dataframe formatted correctly
    #------------------------------------------------------------------------
    @patch('Extract.extract_main.boto3.client')
    def test_calling_bucket_data_returns_correct_dataframe(self, mock_s3_client):
            """
            This method tests whether the calling_bucket_data function of the Extract class correctly retrieves and
            processes data from an AWS S3 bucket, returning the expected DataFrame.
            """
            csv_content = b'Name,Value,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60\n'\
                          b'Emmey Wadforth,Lucy Foster,4,1,7,3,2,1,1,1,3,2,1,4,2,3,2,2,2,1,8,5,4,3,8,5,2,4,8,6,7,3,3,2,4,1,4,4,8,8,7,8,8,4,5,7,5,5,8,8,5,6,8,8,8,8,8,7,8,8,8,8\n'


            mock_s3 = Mock()
            mock_s3.get_object.return_value = {'Body': BytesIO(csv_content)}
            mock_s3.list_objects_v2.return_value = {
                'Contents': [{'Key': 'Academy/Data_31_2019-05-20.csv'}]
            }
            mock_s3_client.return_value = mock_s3

            extractor = Extract()

            result_df = extractor.calling_bucket_data()
            print("Result")
            print(result_df)
            expected_data = {
                'Date': ['31_2019-05-20'],
                'Name': ['Emmey Wadforth'],
                'Value': ['Lucy Foster'],
                '1': [4.0], '2': [1.0], '3': [7.0], '4': [3.0], '5': [2.0],'6': [1.0], '7': [1.0],'8': [1.0],'9': [3.0],'10': [2.0],
                '11': [1.0],'12': [4.0],'13': [2.0],'14': [3.0], '15': [2.0],'16': [2.0],'17': [2.0],'18': [1.0],
                '19': [8.0],'20': [5.0],'21': [4.0],'22': [3.0], '23': [8.0],'24': [5.0],'25': [2.0],'26': [4.0],
                '27': [8.0],'28': [6.0],'29': [7.0],'30': [3.0], '31': [3.0],'32': [2.0], '33': [4.0],'34': [1.0],
                '35': [4.0],'36': [4.0], '37': [8.0],'38': [8.0], '39': [7.0],'40': [8.0], '41': [8.0],'42': [4.0],
                '43': [5.0],'44': [7.0], '45': [5.0],'46': [5.0], '47': [8.0],'48': [8.0], '49': [5.0],'50': [6.0],
                '51': [8.0],'52': [8.0], '53': [8.0],'54': [8.0], '55': [8.0],'56': [7.0], '57': [8.0],'58': [8.0], '59': [8.0], '60': [8.0]}

            expected_df = pd.DataFrame(expected_data)

            pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df)

    #------------------------------------------------------------------------
    # Test for Engineering csv extract to check dataframe formatted correctly
    #------------------------------------------------------------------------

    @patch('Extract.extract_main.boto3.client')
    def test_calling_bucket_engineering_returns_correct_dataframe(self, mock_s3_client):
            """
            This method tests whether the calling_bucket_engineering function of the Extract class correctly retrieves and
            processes data from an AWS S3 bucket, returning the expected DataFrame.
            """
            csv_content = b'Name,Value,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48\n'\
                          b'Giustina Catford,Martina Meadows,1,1,4,4,2,4,1,7,1,1,1,3,2,5,6,3,4,2,6,6,3,8,6,6,8,8,8,5,5,4,5,6,7,3,7,4,8,8,8,8,8,8,8,6,8,6,4,8\n'

            mock_s3 = Mock()
            mock_s3.get_object.return_value = {'Body': BytesIO(csv_content)}
            mock_s3.list_objects_v2.return_value = {
                'Contents': [{'Key': 'Academy/Engineering_22_2019-07-22.csv'}]
            }
            mock_s3_client.return_value = mock_s3

            extractor = Extract()

            result_df = extractor.calling_bucket_engineering()

            expected_data = {
                'Name': ['Giustina Catford'],
                'Value': ['Martina Meadows'],
                '1': [1], '2': [1], '3': [4], '4': [4], '5': [2], '6': [4], '7': [1], '8': [7], '9': [1], '10': [1], '11': [1], '12': [3],
                '13': [2], '14': [5], '15': [6], '16': [3], '17': [4], '18': [2], '19': [6], '20': [6], '21': [3], '22': [8], '23': [6],
                '24': [6], '25': [8], '26': [8], '27': [8], '28': [5], '29': [5], '30': [4], '31': [5], '32': [6], '33': [7], '34': [3],
                '35': [7], '36': [4], '37': [8], '38': [8], '39': [8], '40': [8], '41': [8], '42': [8], '43': [8], '44': [6], '45': [8],
                '46': [6], '47': [4], '48': [8], 'Date': ['22_2019-07-22']}

            expected_df = pd.DataFrame(expected_data)

            pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df)

    #------------------------------------------------------------------------
    # Test for TXT extract to check dataframe columns are correct
    #------------------------------------------------------------------------
    def test_calling_bucket_txt_returns_correct_dataframe(self):
        extractor = Extract()

        result_df = extractor.calling_bucket_txt()

        column_names = result_df.columns.tolist()
        expected_data = ["Name", "Psychometrics", "Presentation", "Date"]

        self.assertEqual(expected_data, column_names)
    #------------------------------------------------------------------------
    # Test for JSON extract to check dataframe columns are correct
    #------------------------------------------------------------------------
    def test_calling_bucket_json_returns_correct_dataframe(self):
        extractor = Extract()

        result_df = extractor.calling_bucket_json()

        column_names = result_df.columns.tolist()
        expected_data = ['name','date','strengths','weaknesses','self_development','geo_flex','financial_support_self','result','course_interest',
                         'tech_self_score.C#','tech_self_score.Java','tech_self_score.R','tech_self_score.JavaScript','tech_self_score.Python',
                         'tech_self_score.C++','tech_self_score.Ruby','tech_self_score.SPSS','tech_self_score.PHP']

        self.assertEqual(expected_data, column_names)

    # ------------------------------------------------------------------------
    # Test for applicants extract to check dataframe columns are correct
    # ------------------------------------------------------------------------
    def test_calling_bucket_applicants_returns_correct_dataframe(self):
        extractor = Extract()

        result_df = extractor.calling_bucket_applicants()

        column_names = result_df.columns.tolist()
        expected_data = ['id','name','gender','dob','email','city','address','postcode','phone_number',
                         'uni','degree','invited_date','month','invited_by']

        self.assertEqual(expected_data, column_names)
if __name__ == '__main__':
    unittest.main()

