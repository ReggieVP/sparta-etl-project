import unittest
from unittest.mock import patch, Mock
from io import BytesIO
import pandas as pd
from Extract.extract_main import Extract


class TestDataProcessor(unittest.TestCase):
    @patch('Extract.extract_main.boto3.client')
    def test_calling_bucket_business_returns_correct_dataframe(self, mock_s3_client):

        # csv_content = b'Name,Value,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,Date\n'\
        #               b'Gustaf Lude,Gregor Gomez,6,4,1,1,2,3,1,1,1,2,1,6,6,2,2,3,3,2,5,1,3,7,6,2,6,8,4,8,1,8,7,3,5,8,8,7,8,5,8,8,8,6,8,7,8,8,7,8,20_2019-02-11\n'

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
        print("Expected")
        print(expected_df)
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df)

if __name__ == '__main__':
    unittest.main()