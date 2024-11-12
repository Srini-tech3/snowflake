//STEP 1: Create a file format -
CREATE OR REPLACE FILE FORMAT my_csv
TYPE = 'csv'
field_delimiter = ',';

//STEP 2: Create a stage -
CREATE OR REPLACE stage my_stage
file_format = 'my_csv';

list @my_stage;

//STEP 3: Using snowsql CLI upload the source file in stage - 
srini#COMPUTE_WH@DEMO.ALL_STAGES>put file://C:\Users\srini\OneDrive\Desktop\Srini\Snowflake\files\supply_chain_data.csv @my_stage;

//STEP 4: Create a final table for source information - 
CREATE OR REPLACE TABLE supply_chain_table
(
    Product_type VARCHAR2(30),
    SKU VARCHAR2(30),
    Price NUMBER(10,2),
    Availability NUMBER(10,2),
    Number_of_products_sold NUMBER(10,2),
    Revenue_generated NUMBER(10,2),
    Customer_demographics VARCHAR2(30),
    Stock_levels NUMBER(10,2),
    Lead_times NUMBER(10,2),
    Order_quantities NUMBER(10,2),
    Shipping_times NUMBER(10,2),
    Shipping_carriers VARCHAR2(30),
    Shipping_costs NUMBER(10,2),
    Supplier_name VARCHAR2(30),
    Location VARCHAR2(30), 
    Lead_time NUMBER(10,2),
    Production_volumes NUMBER(10,2),
    Manufacturing_lead_time NUMBER(10,2),
    Manufacturing_costs NUMBER(10,2),
    Inspection_results VARCHAR2(30),
    Defect_rates NUMBER(10,2),
    Transportation_modes VARCHAR2(30),
    Routes VARCHAR2(30),
    Costs NUMBER(10,2)
);

//STEP 5: Copy the source data into table -
COPY INTO supply_chain_table
FROM @my_stage/supply_chain_data.csv;

//Finally the emp_info table with all information -
select * from supply_chain_table;
 
