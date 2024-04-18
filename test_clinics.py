import unittest
from unittest.mock import patch
import pandas as pd
from datetime import datetime
from main import fetch_data, save_to_parquet
import os

class TestClinicalTrialsAPI(unittest.TestCase):
    @patch('main.re.get')  # Mock the API call
    def test_fetch_data(self, mock_get):
        # create a mock API response
        mock_response_content = (
            "NCT Number,Study Title,Study URL\n"
            "NCT00072579,Sargramostim in Treating Patients,www.text.com\n"
        )
        mock_response = mock_response_content.encode('utf-8')
        mock_get.return_value.content = mock_response
        # call the fetch_data function
        df, _ = fetch_data()
        # verify that the DataFrame has the expected columns
        expected_columns = ["NCT Number", "Study Title", "Study URL"]
        self.assertListEqual(list(df.columns), expected_columns)
        # verify that the DataFrame contains the expected number of rows
        expected_row_count = 1 
        self.assertEqual(len(df), expected_row_count)

    def test_save_to_parquet(self):
        # create a mock DataFrame with sample data
        mock_data = {
            "NCT Number": ["NCT00072579"],
            "Study Title": ["Sargramostim in Treating Patients..."]

        }
        df = pd.DataFrame(mock_data)
        # call the save_to_parquet function
        pipeline_run_id = "mock_pipeline_id"
        pipeline_start_timestamp = datetime.now().isoformat()
        save_to_parquet(df, pipeline_run_id, pipeline_start_timestamp)
        # verify that the parquet file was created with the expected filename
        expected_filename = f"data/cl_run_{pipeline_run_id}.parquet"
        # check if the file exists 
        if os.path.exists(expected_filename):
            # file exists, so the test passes 
            # then delete the file after verification so it will not interefere with actual run results
            os.remove(expected_filename)
        else:
            self.fail(f"File '{expected_filename}' does not exist.")

if __name__ == '__main__':
    unittest.main()