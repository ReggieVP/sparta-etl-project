# Data 250 ETL Project

## Overview

This README provides an overview of the ETL (Extract, Transform, Load) project conducted by the Data 250 team. The project focuses on extracting data from Amazon S3, specifically Sparta Global data related to courses (data, business, and engineering) and candidate details. The team comprises 12 members (see list below) collaborating on Python-based ETL processes. 

Throughout this project, we were able to implement scrum and agile ideology principles. This enabled us to work in a structured and organized manner. Daily sprints were set out with a sprint review of the previous day's sprint, a sprint planning session immediately after this to implement feedback in the sprint review and plan the day ahead, and a sprint retrospective at the end of the day. By doing this, we were iteratively improving our working life-cycle and able to produce a functioning product at the end of each sprint. 

Participating in this project allowed us to appreciate some of the key areas of the product development lifecycle including:
1. The importance of sprint planning.
2. Taking action on the sprint review from non-technical and technical stakeholders.
3. Organising within our teams to collaborate using git.
4. For the scrum master, leading a team effectively and comminuting the tasks in an order of importance.
5. For the product owner, relaying suggestions and requirements from the stakeholders to the team in the form of actionable tasks. 

## Requirements

Ensure you have the following prerequisites installed to run the ETL project successfully:

- Python 3.11
- Required Python libraries: pandas, boto3, json, numpy.
- AWS credentials with read access to the specified S3 bucket. Those within the Sparta Global company will be able to request access to this. 
- Access to the target database with appropriate permissions

## Setup Instructions

### 1. Clone the project repository to your local machine:

   `$ git clone https://github.com/ReggieVP/sparta-etl-project.git`

### 2. Install the required Python libraries:

  ` pip install -r requirements.txt`

### 3. Configure AWS credentials and other parameters in the designated configuration files.

For further information on how to to this, please follow the guidelines here: 

### 4. Run the scripts:
   
- extract_main.py
- transform_main.py
- normalise.py
- -no-name-yet.py

## ETL Process

The ETL process consists of the following steps:

1. **Extraction:** Data is extracted from Amazon S3, focusing on Sparta Global data related to courses (data, business, and engineering) and candidate details. There are three types of file:
- .csv
- .txt
- .json

Each of these file types required different methods to extract the data, and load it into a pandas dataframe for transformation. 

3. **Transformation:** Raw data undergoes transformation as needed, including cleaning, formatting, and enriching, to make it suitable for analysis and storage.

4. **Loading:** Transformed data is loaded into the target database or storage system, ensuring proper indexing and organization.

5. **Data Visualisation:** The final data will be visualised using Microsft Power BI. It will be presented in an accessible, colour blind-friendly format 

## Collaboration

The Data 250 team collaborated on this project using version control (Git) and followed Agile and Scrum best practices for code review, collaboration and documentation.

## Collaborators

- Scrum Master: Sebsatian Manley 
- Product Manager: Georgia Grayland
- Repository Owner: Richard Van Parys
- Anushkriti Sarvesh (GitHub Link)
- Aymen Shallal (GitHub Link)
- Jesse Rolfe (GitHub Link)
- Katsiaryna Graham (GitHub Link)
- Medan Grant-Anderson and (GitHub Link)
- Rebecca Hill (GitHub Link)
- Richard Elegbe-Williams (GitHub Link)
- Tolani Oladimeji (GitHub Link)
- William Watt (GitHub Link)
