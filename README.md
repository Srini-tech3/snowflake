Snowflake Data Loading Guide
This guide provides step-by-step instructions to load data from your local system and AWS S3 into Snowflake, supporting CSV, XML, and JSON file formats.

Prerequisites
SnowSQL installed for command-line interaction with Snowflake or access to Snowflake Web UI.
AWS S3 Bucket (if loading from S3).
Necessary privileges to create stages, tables, and run the COPY INTO command in Snowflake.
1. Loading Data from Local System
Step 1: Create a Target Table in Snowflake
Before loading the data, create a table in Snowflake to store the data from the file. Modify the table structure based on your file format.

Example for CSV File:
sql
Copy code
CREATE OR REPLACE TABLE my_table_csv (
    id INTEGER,
    name STRING,
    age INTEGER
);
Example for JSON File:
sql
Copy code
CREATE OR REPLACE TABLE my_table_json (
    data VARIANT
);
Example for XML File:
sql
Copy code
CREATE OR REPLACE TABLE my_table_xml (
    data VARIANT
);
Step 2: Create a Named Stage (Optional)
You can create a named stage if you plan to reuse it for multiple data loads. Alternatively, Snowflake's temporary stages can also be used without this step.

sql
Copy code
CREATE OR REPLACE STAGE my_stage;
Step 3: Put File into Snowflake Stage
Use the PUT command to upload your local file to the Snowflake stage.

bash
Copy code
snowsql -q "PUT file://path_to_your_local_file.csv @my_stage"
Step 4: Copy Data into the Table
Use the COPY INTO command to load the data from the stage into the Snowflake table.

For CSV:
sql
Copy code
COPY INTO my_table_csv
FROM @my_stage/file_name.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
For JSON:
sql
Copy code
COPY INTO my_table_json
FROM @my_stage/file_name.json
FILE_FORMAT = (TYPE = JSON);
For XML:
sql
Copy code
COPY INTO my_table_xml
FROM @my_stage/file_name.xml
FILE_FORMAT = (TYPE = XML);
2. Loading Data from AWS S3
Step 1: Create an External Stage
Create an external stage that points to your AWS S3 bucket.

sql
Copy code
CREATE OR REPLACE STAGE my_s3_stage
URL='s3://your-bucket-name/'
CREDENTIALS=(AWS_KEY_ID='your_aws_access_key' AWS_SECRET_KEY='your_aws_secret_key');
Step 2: Create a Target Table in Snowflake
Follow the same table creation steps as above for CSV, JSON, or XML formats.

Step 3: Copy Data from S3 into the Table
For CSV:
sql
Copy code
COPY INTO my_table_csv
FROM @my_s3_stage/path_to_csv_file.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
For JSON:
sql
Copy code
COPY INTO my_table_json
FROM @my_s3_stage/path_to_json_file.json
FILE_FORMAT = (TYPE = JSON);
For XML:
sql
Copy code
COPY INTO my_table_xml
FROM @my_s3_stage/path_to_xml_file.xml
FILE_FORMAT = (TYPE = XML);
3. File Formats in Snowflake
If you're loading files with specific formats (e.g., different delimiters, compression, etc.), you can create a custom file format:

sql
Copy code
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;
4. Monitoring and Verifying the Load
Check the Load History:
After loading the data, you can verify the success or failure of the COPY command by checking the load history.

sql
Copy code
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
5. Error Handling
If the data loading process encounters any errors, you can view detailed error messages by querying the load history or specifying error-handling options in the COPY INTO command.

Example: Loading with error tolerance:

sql
Copy code
COPY INTO my_table_csv
FROM @my_stage/file_name.csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

Conclusion
You have successfully learned how to load data from both a local system and AWS S3 into Snowflake using CSV, XML, and JSON formats. Be sure to adjust the table structures and file formats based on your specific data needs.
