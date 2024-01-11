# Transform Folder
---
## Overview
The Transform folder in the Sparta ETL project contains a script and data sources specifically dedicated to the transformation phase of the ETL pipeline. This phase focuses on taking the extracted data from the previous phase, and transforming it into a format and structre that both meets the clinets needs and is ready for loading and data visualisation. This step also includes abstracting the data into 3rd Normal form, and data cleaning. 

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

