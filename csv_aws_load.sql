//Loading CSV file to snowflake table from AWS S3//
CREATE
OR REPLACE STORAGE INTEGRATION s3_int TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = S3 ENABLED = TRUE STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::277707130077:role/s3_snowflake_file_ingestion' STORAGE_ALLOWED_LOCATIONS = (
    's3://awssnowsss/csv/',
    's3://awssnowsss/json/',
    's3://awssnowsss/xml/'
) COMMENT = 'Integration with aws s3 buckets';
DESC INTEGRATION s3_int;
//STEP 1: Create a file format -
CREATE
OR REPLACE FILE FORMAT all_file_format.csv1 TYPE = 'csv' SKIP_HEADER = 1 FIELD_DELIMITER = ',';
//STEP 2: Create a stage and upload the csv file from AWS S3-
CREATE
OR REPLACE STAGE external_stage.csv_stage1 URL = 's3://awssnowsss/csv/' STORAGE_INTEGRATION = s3_int FILE_FORMAT = all_file_format.csv1;
LIST @external_stage.csv_stage1;
//STEP 3: Create a Table to input the source data-
CREATE
OR REPLACE TABLE all_tables.bpl_top_scorers (
    Rank NUMBER PRIMARY KEY,
    Player VARCHAR2(30),
    Team VARCHAR2(30),
    Goals NUMBER,
    Penalties NUMBER,
    Minutes NUMBER,
    Matches NUMBER,
    Country VARCHAR2(30)
);
//STEP 4: COPY the data into the table -
COPY INTO all_tables.bpl_top_scorers
FROM
    @external_stage.csv_stage1;
//Finally the emp_info table with all information -
SELECT
    *
FROM
    all_tables.bpl_top_scorers;
