# Extract Folder

## Overview

The **extract folder** in the Sparta ETL project contains a script and data sources specifically dedicated to the extraction phase of the ETL process. 
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
   - The data extracted is in json, csv and txt format. The raw data files can be found in this folder.

## Usage

1. **Configure AWS Credentials:**
   - Provide your AWS access key ID and secret access key to gain access to Amazon S3.

2. **S3 Bucket Information:**
   - Specify the name of the S3 bucket containing the Sparta data in the `data-eng-250-final-project`.

3. **Run the Extraction Script:**
   - Execute the **extract_main.py** script to initiate the extraction process.
   
   ```bash
   python extract_script.py
   ```
