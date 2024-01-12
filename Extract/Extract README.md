# Extract Folder

## Overview

The **Extract folder** in the Sparta ETL project contains a script and data sources specifically dedicated to the extraction phase of the ETL process. 
This phase focuses on retrieving raw data from Amazon S3, with a particular emphasis on Sparta data related to courses (data, business, and engineering) and candidate details.

## The Process:

- The Python code provided in **extract_main.py** defines a class named Extract designed for an ETL (Extract, Transform, Load) process on data stored in an Amazon S3 bucket. 
- The class encapsulates methods for extracting and processing different types of data, including 'Business' data from CSV files, JSON files, 'Applicants' data from CSV files, and text files. 
- The class utilises the boto3 library for AWS interactions, json for handling JSON data, and pandas for data manipulation. 
- Each method within the class focuses on a specific data type, with consistent naming conventions for resulting DataFrames. 
- The code is structured in modules, making it adaptable for handling additional data types and serves as part of a larger ETL workflow. 
- The extraction process involves reading data from the S3 bucket, performing necessary data manipulations, and printing the resulting DataFrames.

## Contents

1. **extract_main.py:**
   - This Python script is responsible for connecting to the specified Amazon S3 bucket and extracting relevant data files.
   - It utilizes the boto3 library for AWS interactions and ensures secure access using AWS credentials.

2. **data sources:**
   - The data extracted is in json, csv and txt format. The raw data files can be found in the folder paths Amazon S3/Buckets/data-eng-250-final-project/Talent/
and Amazon S3/Buckets/data-eng-250-final-project/Academy/.

   - Note you will need access to these Buckets to be able to access the data.

## Usage

1. **Configure AWS Credentials:**
   - Provide your AWS access key ID and secret access key to gain access to Amazon S3.

2. **S3 Bucket Information:**
   - Specify the name of the S3 bucket containing the Sparta data in the `data-eng-250-final-project`.
3. **Import Boto3**
   - First pip install boto3
   ```bash
   pip install boto3 pandas
   ```
   
   - Import boto3
   ```bash
   import boto3
   ```
5. **Run the Extraction Script:**
   - Execute the **Extract** class within **extract_main.py** script to initiate the extraction process.
   
   ```bash
   python extract_main.py
   ```
## Functionality

### Class: Extract

#### Methods:

1. **`calling_bucket_business`:**
    - Extracts 'Business' data from CSV files with the 'Business' prefix in the S3 bucket. Concatenates the data and adds a 'Date' column based on the file name.

2. **`calling_bucket_data`:**
    - Extracts 'Data' data from CSV files with the 'Data' prefix in the S3 bucket. Concatenates the data and adds a 'Date' column based on the file name.

3. **`calling_bucket_engineering`:**
    - Extracts 'Engineering' data from CSV files with the 'Engineering' prefix in the S3 bucket. Concatenates the data and adds a 'Date' column based on the file name.

4. **`calling_bucket_json`:**
    - Extracts JSON files from the S3 bucket. Normalizes the JSON data into a pandas DataFrame.

5. **`calling_bucket_applicants`:**
    - Extracts 'Applicants' related data from CSV files with the 'Applicants' prefix in the S3 bucket. Concatenates the data.

6. **`calling_bucket_txt`:**
    - Extracts text files from the S3 bucket. Processes the data and creates a DataFrame with columns 'Name,' 'Psychometrics,' 'Presentation,' and 'Date.'
