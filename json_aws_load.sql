//Loading JSON file to snowflake table from AWS S3//
//STEP 1: Create a file format -
CREATE
OR REPLACE FILE FORMAT all_file_format.json2 TYPE = 'json';
//STEP 2: Create a stage and upload the csv file from AWS S3-
CREATE
OR REPLACE STAGE external_stage.json_stage1 URL = 's3://awssnowsss/json/' STORAGE_INTEGRATION = s3_int FILE_FORMAT = all_file_format.json2;
LIST @external_stage.json_stage1;
//STEP 3:Create a stage table with source data-
CREATE
OR REPLACE TABLE stage_tables.stg_player(json_data VARIANT);
//STEP 4: Copy the source data into stage table -
COPY INTO stage_tables.stg_player
FROM
    @external_stage.json_stage1;
SELECT
    *
FROM
    stage_tables.stg_player;
//STEP 5: Creating a final table and inserting the data to the table -
    CREATE
    OR REPLACE TABLE all_tables.Cricket_player_info AS
SELECT
    value:"name"::VARCHAR2(100) as name,
    value:"team"::VARCHAR2(100) as team,
    value:"battingStyle"::VARCHAR2(100) as batting_Style,
    value:"bowlingStyle"::VARCHAR2(100) as bowling_Style,
    value:"playingRole"::VARCHAR2(100) as playing_Role
FROM
    stage_tables.stg_player,
    LATERAL FLATTEN(input => json_data);
//Finally the emp_info table with all information -
SELECT
    *
FROM
    all_tables.Cricket_player_info;
