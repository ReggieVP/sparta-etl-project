# Data 250 ETL Project

## Overview

This README provides an overview of the ETL (Extract, Transform, Load) project conducted by the Data 250 team. The project focuses on extracting data from Amazon S3, specifically Sparta data related to courses (data, business, and engineering) and candidate details. The team comprises 12 members collaborating on Python-based ETL processes.

## Requirements

Ensure you have the following prerequisites installed to run the ETL project successfully:

- Python 3.x
- Required Python libraries (boto3, pandas, etc.)
- AWS credentials with read access to the specified S3 bucket
- Access to the target database with appropriate permissions

## Setup Instructions

### 1. Clone the project repository to your local machine:

   git clone https://github.com/ReggieVP/sparta-etl-project.git

### 2. Install the required Python libraries:

   pip install -r requirements.txt

### 3. Configure AWS credentials and other parameters in the designated configuration files.

### 4. Run the scripts:
   
- extract_main.py
- transform_main.py
- load.py

## ETL Process

The ETL process consists of the following steps:

1. **Extraction:** Data is extracted from Amazon S3, focusing on Sparta data related to courses (data, business, and engineering) and candidate details.

2. **Transformation:** Raw data undergoes transformation as needed, including cleaning, formatting, and enriching, to make it suitable for analysis and storage.

3. **Loading:** Transformed data is loaded into the target database or storage system, ensuring proper indexing and organization.

4. **Data Visualisation:** The final data will be visualised using Microsft Power BI. It will be presented in an accessible, colour blind-friendly format 

## Collaboration

The Data 250 team collaborated on this project using version control (Git) and followed Agile and Scrum best practices for code review, collaboration and documentation.

## Collaborators

- Scrum Master: Sebsatian Manley 
- Product Manager: Georgia Grayland
- Anushkriti Sarvesh (GitHub Link)
- Aymen Shallal (GitHub Link)
- Jesse Rolfe (GitHub Link)
- Katsiaryna Graham (GitHub Link)
- Medan Grant-Anderson and (GitHub Link)
- Rebecca Hill (GitHub Link)
- Richard Van Parys (GitHub Link)
- Richard Elegbe-Williams (GitHub Link)
- Tolani Oladimeji (GitHub Link)
- William Watt (GitHub Link)
