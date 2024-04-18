# Introduction 
This project is designed to fetch data from (https://clinicaltrials.gov/) and stores it in parquet files format.

For more information about the API, visit (https://clinicaltrials.gov/data-api/api)

### Data fetching can be done in two ways:
- Fetch all studies: if you don't specifiy a date the pipeline will fetch all studies from the API.
- Partial fetch: if you specify a date the pipeline will fetch only studies that have changed after the specified date.


### Data storage:
- The pipeline will store the data automatically to the data directory in parquet files.
- Data is stored in batches, each file includes 1000 rows (the max we can get from each API call).
- Files name includes the pipeline_run_id for more clarification.
- Files will include extra 2 columns: pipeline_run_id , pipeline_start_timestamp and can be used for later requirements.


# Getting Started
To get started please follow these steps:
1. Create a virtual environment

        python -m venv env

2. Install dependencies

        pip install -r requirements.txt


# Build and Test
How to use:
1. Run unit tests

        python -m unittest

2. Run pipeline

        python main.py

Here you'll be asked to enter date in "YYYY-MM-DD" format for partial run or press enter to go for a full run.

If pipeline run succeeds you will read this message: "Data fetched and stored in data directory successfully." and the parquet files can be found in /data directory

