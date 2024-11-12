//Loading XML file to snowflake table//
//STEP 1: Create a file format -
CREATE
OR REPLACE FILE FORMAT practice.all_file_format.json1 TYPE = 'JSON';
//STEP 2: Create a stage -
CREATE
OR REPLACE STAGE practice.all_stages.json_stage1 FILE_FORMAT = practice.all_file_format.json1;
//STEP 3: Using snowsql CLI upload the source file in stage -
srini #COMPUTE_WH@PRACTICE.ALL_STAGES>put file://C:\Users\srini\OneDrive\Desktop\Srini\Snowflake\files\data.json @json_stage1;
//STEP 4:Create a stage table with source -
CREATE
OR REPLACE TABLE stage_tables.stg_mobile(json_data VARIANT);
//STEP 5: Copy the source data into stage table -
COPY INTO stage_tables.stg_mobile
FROM
    @all_stages.json_stage1;
SELECT
    *
FROM
    stage_tables.stg_mobile;
//STEP 6: Creating a final table and inserting the data to the table -
    CREATE
    OR REPLACE TABLE all_tables.mobile_info AS
SELECT
    json_data:phone_brand::VARCHAR2(100) as phone_brand,
    json_data:phone_model::VARCHAR2(100) as phone_model,
    COALESCE(json_data:specs.Network. "4G bands", 'No')::VARCHAR2(300) as band_4G,
    COALESCE(json_data:specs.Network.GPRS, 'No')::VARCHAR2(100) as GPRS,
    COALESCE(json_data:specs.Network.EDGE, 'No')::VARCHAR2(100) as EDGE,
    json_data:specs.Display.Type::VARCHAR2(100) as Display_type,
    json_data:specs.Display.Size::VARCHAR2(100) as Display_Size,
    json_data:specs.Display.Resolution::VARCHAR2(100) as Display_Resolution,
    json_data:specs.Platform.OS::VARCHAR2(100) as OS,
    json_data:specs.Memory. "Card slot"::VARCHAR2(100) as memory_slot,
    json_data:specs.Memory.Internal::VARCHAR2(100) as internal_memory,
    json_data:specs.Sound. "3.5mm jack"::VARCHAR2(100) as Headphone_3_5mm,
    json_data:specs.Battery.Type::VARCHAR2(100) as Battery,
    json_data:specs.Launch.Announced::VARCHAR2(100) as Announced,
    json_data:specs.Launch.Status::VARCHAR2(100) as Status,
    json_data:specs.Misc.Colors::VARCHAR2(100) as Colors,
FROM
    stage_tables.stg_mobile;
//Finally the emp_info table with all information -
SELECT
    *
FROM
    all_tables.mobile_info;
