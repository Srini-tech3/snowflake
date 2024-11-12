//Loading XML file to snowflake table//
CREATE SCHEMA IF NOT EXISTS all_file_format;
CREATE SCHEMA IF NOT EXISTS all_stages;
CREATE SCHEMA IF NOT EXISTS stage_tables;
CREATE SCHEMA IF NOT EXISTS all_tables;
//STEP 1: Create a file format -
CREATE
OR REPLACE FILE FORMAT practice.all_file_format.xml1 TYPE = 'xml';
//STEP 2: Create a stage -
CREATE
OR REPLACE STAGE practice.all_stages.xml_stage1 FILE_FORMAT = practice.all_file_format.xml1;
//STEP 3: Using snowsql CLI upload the source file in stage -
srini #COMPUTE_WH@PRACTICE.ALL_STAGES>put file://C:\Users\srini\OneDrive\Desktop\Srini\Snowflake\files\emp.xml @xml_stage1;
//STEP 4:Create a stage table with source -
CREATE
OR REPLACE TABLE practice.stage_tables.stg_emp(xml_data VARIANT);
//STEP 5: Copy the source data into stage table -
COPY INTO stage_tables.stg_emp
FROM
    @practice.all_stages.xml_stage1;
//STEP 6: Create a final table for source information -
    CREATE
    OR REPLACE TABLE all_tables.emp_info (
        employeeID NUMBER PRIMARY KEY,
        firstName VARCHAR2(100),
        lastName VARCHAR2(100),
        position VARCHAR2(100),
        department VARCHAR2(100),
        salary NUMBER,
        dateOfJoining DATE,
        email VARCHAR2(100),
        address VARCHAR2(100)
    );
//STEP 7: Insert the data into the table -
INSERT INTO
    all_tables.emp_info
SELECT
    XMLGET(em.value, 'employeeID'): "$"::NUMBER AS employeeID,
    XMLGET(em.value, 'firstName'): "$"::VARCHAR2(100) AS firstName,
    XMLGET(em.value, 'lastName'): "$"::VARCHAR2(100) AS lastName,
    XMLGET(em.value, 'position'): "$"::VARCHAR2(100) AS position,
    XMLGET(em.value, 'department'): "$"::VARCHAR2(100) AS department,
    XMLGET(em.value, 'salary'): "$"::NUMBER AS salary,
    XMLGET(em.value, 'dateOfJoining'): "$"::DATE AS dateOfJoining,
    XMLGET(em.value, 'email'): "$"::VARCHAR2(100) AS email,
    (
        XMLGET(addr.value, 'street'): "$" || ', ' || XMLGET(addr.value, 'city'): "$" || ', ' || XMLGET(addr.value, 'state'): "$" || ', ' || XMLGET(addr.value, 'postalCode'): "$"
    )::VARCHAR2(200) AS address
FROM
    stage_tables.stg_emp,
    LATERAL FLATTEN(to_array(xml_data: "$")) AS em,
    LATERAL FLATTEN(to_array(em.value: "$")) AS addr
WHERE
    addr.value LIKE '<address>%';
//Finally the emp_info table with all information -
SELECT
    *
FROM
    all_tables.emp_info;
