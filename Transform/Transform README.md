# Transform Folder
---
## Overview
The Transform folder in the Sparta ETL project contains a script and data sources specifically dedicated to the transformation phase of the ETL pipeline. This phase focuses on taking the extracted data from the previous phase, and transforming it into a format and structre that both meets the clients needs and is ready for loading and data visualisation. This step also includes abstracting the data into 3rd Normal form, and data cleaning. 

## The Process:
- The Python code provided in **transform_main.py** defines a class named Transform designed for an ETL (Extract, Transform, Load) process stored on data stored in an Amazon S3 bucket.
- The class has mutliple methods aimed at transforming the extracted data.
- The extracted data was in different file formats (txt, csv, json), therefore mutliple different methods had to be utilised to transform the data into a format appropriate for normalisation and visualisation.
- The methods in the class include data cleaning methods, where null values were taken into consideration across the dataset and dealt with appropriately for the context. See further docuementation on the data cleaning process below.
- Other methods include those of separating name columns, date formatting, and proper casting.
- Once this process had been finished, all resulting data frames from the extract process were transformed ready to be loaded into a Relational Database. 

## Contents:
1. **transform_main.py**:
  - This Python file is responsible for transforming the extracted data from Amazon S3 that was turned into appropriate DataFrames in the Extract phase.
  - It Utilises python libraries and adta cleaning methods to ensure there are no missing values in the data, data types across the dataset are correct, and that tables are in a format that is appropriate for the load phase. 


## Usage:
1. **Ensure the dataset has been loaded correctly**
   
3. **Run the transformation Script**:
   - Execute the **transform_main.py** script to initiate the extraction process.
     
     ```python transform_main.py```

--- 
## Data Cleaning Documentation
- The data cleaning process required different methods to be adopted in order to ensure the data was in appropriate formats.
- Therefore different methods were adopted and different challenges presented as extracted data was not uniform, and there were many missing values.

**Below are the different issues faced, and the strategies taken to overcome them:**

### Course csv Files:
#### Issues: 
- Null values for scores of students who have left the academy at a certain point. 
- Date contains the course prefix and not in the correct format. 

#### Solution:  
- Impute 0 for null values - there are no zero values in the data set so this will indicate that the student left the academy. 
- Remove the course prefix and use to_datetime method to convert the date into a standard date format throughout all the files. 

 ### Individual JSON Files:
#### Issues: 
- Null values in tech self-score for different tech stacks between individuals 
- Redundant date is present. 

#### Solutions: 
- Null values kept as this indicates no score was given, rather than a score of 0. 
- Redundant date removed.

### Applicant csv Files:
#### Issues: 
- Phone number not in standard format. 
- Post code not in standard format.
- Invited date and invited month & year in separate columns and not in the standard date format.

#### Solutions:
- Formatted the phone number to present in the correct format.
- Formatted the postcode to be in the correct order and format.
- Combined date and invited month & year into one column, converted it to datetime and yy-mm-dd format to be uniform across all tables.

### Sparta Day txt Files:
#### Issues: 
- Psychometric and presentation scores were out of a total score meaning it would be harder to visualise this data. 
- Columns do not have correct column headers.
- Date is not in the standard format.

#### Solution: 
- Psychometric and presentation scores extracted, leaving only the values.
- Columns names given to each column – for example Name, Psychometric score, Presentation Score.
- Data seprated into a DataFrame so that the key information is easily accessible. 


