import math
import uuid
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests as re

# API url
url = 'https://clinicaltrials.gov/api/v2/studies'

# Define initial parameters for the api call
params = {
    "pageSize": 1000,
    "format": "csv",
    "countTotal": "true"
}

def fetch_data() -> tuple[pd.DataFrame, str]:
    '''
    Function that fetches data from the Get call and returns a df and next page token
    '''
    # call the api with initial parameters
    response = re.get(url, params=params)
    # Convert the response to utf-8 string object
    csv_content = StringIO(response.content.decode('utf-8'))
    # Read response text into dataframe with delimiter and No header as it will be added separetly
    df = pd.read_csv(csv_content, sep=',', index_col= None)
    return df, response.headers['x-next-page-token']


def save_to_parquet(data_frame: pd.DataFrame, pipeline_run_id: str, pipeline_start_timestamp: datetime) -> None:
    '''
    Function to save the retrieved data into parquet files
    it will add to the dataframe the pipeline run id & timestamp columns then saves it as parquet file
    '''
    data_frame['pipeline_run_id'] = pipeline_run_id
    data_frame['pipeline_start_timestamp'] = pipeline_start_timestamp
    # create /data directory if not exisit
    Path("./data").mkdir(parents=True, exist_ok=True)
    # write df to parquet format using pyarrow and gzip compression
    data_frame.to_parquet(f"data/cl_run_{pipeline_run_id}.parquet",
                          engine='pyarrow', compression='gzip')


def read_parquet() -> pd.DataFrame:
    '''
    Extra function to read all the written data in the data directory
    '''
    df = pd.read_parquet('data/', engine='pyarrow')
    return df


def pipeline_run(change_date: datetime = None) -> None:
    '''
    This is the pipeline run trigger function, if no change_date passed the pipline will pull all studies
    If change_date is provided, the pipeline will pull only studies after the specified date
    Date should be in this format: yyyy-MM-dd example: '2020-01-18'
    '''
    if re.get(url, params=params).status_code == 200:
        if change_date:
                params["filter.advanced"] = f"AREA[LastUpdatePostDate]RANGE[{change_date},MAX]"
        # get total count
        total_count = re.get(url, params=params).headers['x-total-count']
        # get column headers
        api_columns = fetch_data()[0].columns.to_list()
        # loop into the range total_count/1000 as we display 1000 results per call
        for i in range(math.ceil(int(total_count)/1000)):
            pipeline_run_id = str(uuid.uuid4())
            pipeline_start_timestamp = datetime.now().isoformat()
            # check if there is a next page to loop into
            if 'x-next-page-token' in re.get(url, params=params).headers:
                df, next_page = fetch_data()
                # assign columns headers
                df.columns = api_columns
                # set next page token
                params["pageToken"] = next_page
                save_to_parquet(df, pipeline_run_id, pipeline_start_timestamp)
    else:
        print(f"Error fetching data: {re.get(url, params=params).status_code}")


if __name__ == "__main__":
    '''
    Default run: asks for user input choice and runs pipeline accordingly
    '''
    change_date = input("Enter the date (YYYY-MM-DD) to only fetch studies changed from onwards (leave blank for all studies): ")
    # only changed studies run
    if change_date:
        try:
            # verify user input is in YYYY-MM-DD format
            if bool(datetime.strptime(change_date, "%Y-%m-%d")) == True:
                print(f"Fetching studies changed from {change_date} onwards...")
                pipeline_run(change_date)
                print("Data fetched and stored in data directory successfully.")
        except Exception as error:
            print("Please enter data in correct format (YYYY-MM-DD) example: 2024-04-10 " + str(error))
    # full run
    else:
        print("Fetching all studies...")
        pipeline_run()
        print("Data fetched and stored in data directory successfully.")
        