//Loading XML file to snowflake table from AWS S3//
//STEP 1: Create a file format -
CREATE
OR REPLACE FILE FORMAT practice.all_file_format.xml2 TYPE = 'xml';
//STEP 2: Create a stage -
CREATE
OR REPLACE STAGE all_stages.xml_stage2 URL = 's3://awssnowsss/xml/' STORAGE_INTEGRATION = s3_int FILE_FORMAT = all_file_format.xml2;
LIST @all_stages.xml_stage2;
//STEP 3:Create a stage table with source data-
CREATE
OR REPLACE TABLE stage_tables.stg_movies(xml_data VARIANT);
SELECT
    *
FROM
    stage_tables.stg_movies;
//STEP 4: Copy the source data into stage table -
    COPY INTO stage_tables.stg_movies
FROM
    @all_stages.xml_stage2;
//STEP 5: Creating a final table and inserting the data to the table -
    CREATE
    OR REPLACE TABLE all_tables.best_movies AS
SELECT
    XMLGET(mo.value, 'title'): "$"::VARCHAR2(30) AS movie_title,
    XMLGET(mo.value, 'year'): "$"::NUMBER AS movie_year,
    XMLGET(mo.value, 'genre'): "$"::VARCHAR2(30) AS movie_genre,
    XMLGET(mo.value, 'rating'): "$"::NUMBER(5, 1) AS movie_rating
FROM
    stage_tables.stg_movies,
    LATERAL FLATTEN(to_array(xml_data: "$")) AS mo;
//Finally the emp_info table with all information -
SELECT
    *
FROM
    all_tables.best_movies;
