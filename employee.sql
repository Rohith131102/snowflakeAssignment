

-- Question - 1 
CREATE or REPLACE ROLE Admin;   -- creating role admin
CREATE or REPLACE ROLE PII;     -- creating role PII
CREATE or REPLACE ROLE DEVELOPER;   -- creating role developer

-- Granting roles so as to replicate the given hierchy
GRANT ROLE Admin to ROLE ACCCOUNTADMIN;
GRANT ROLE PII to ROLE ACCCOUNTADMIN;
GRANT ROLE DEVELOPER to ROLE ADMIN;

-- using role ACCCOUNTADMIN and granting privileges
USE ROLE ACCCOUNTADMIN;
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE ADMIN;
GRANT CREATE DATABASE on ACCOUNT to ROLE Admin; 

-- Question - 2
-- Create an M-sized warehouse using the accountadmin role with name as assignment_wh and use it for all the queries
-- creating warehouse
CREATE or REPLACE WAREHOUSE assignment_wh with warehouse_size = Medium;

-- Question - 3 : Switch to the admin role
USE ROLE Admin;

-- Question - 4  : Create a database assignment_db
CREATE or REPLACE DATABASE assignment_db;

-- Question - 5 : Create a schema my_schema
CREATE SCHEMA my_schema;

-- Question - 6
-- Create a table using any sample csv
CREATE or REPLACE table employee (
ID integer,
firstname varchar(100),
lastname varchar(100),
email varchar(100),
phoneNumber varchar(10),
city varchar(50),
etl_ts timestamp default current_timestamp(),    -- for getting the time at which the record is getting inserted
etl_by varchar default 'snowsight',   -- for getting application name from which the record was inserted
filename varchar  -- for getting the name of the file USEd to insert data into the table.
);

-- Question - 8
-- Load the file into an external and internal stage

-- We created a file format called csv_format which holds data of format csv
CREATE OR REPLACE FILE FORMAT csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1; 

CREATE STAGE internal_stage file_format = csv_format;

/*
putting into staging area from local using snowsql
put file:///users/rohith_boodireddy/Downloads/employeesf.csv @internal_stage;
*/

LIST @internal_stage;



-- granting all on integration to role admin for s3 integration
GRANT ALL ON INTEGRATION s3_integration TO ROLE admin;

-- creating a storage integration object called s3_integration with holding s3 storage provider.
CREATE or REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::366068070173:ROLE/admin_s3'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snow-empdata/data/employeesf.csv');

-- Describing Integration object to arrange a relationship between aws and snowflake
desc integration s3_integration;

-- creating a external stage called external_stage with holding my_csv_format file format and s3 bucket url.
CREATE OR REPLACE STAGE external_stage URL = 's3://snow-empdata/data/employeesf.csv' STORAGE_INTEGRATION = s3_integration FILE_FORMAT = csv_format;

LIST @external_stage;

-- Question - 9
-- Load data into the tables using copy into statements. In one table load from the internal stage and in another from the external.

-- Copying table employee_int and employee_ext for loading employee data from internal stage and external stage
CREATE or REPLACE TABLE employee_int LIKE employee;
CREATE or REPLACE TABLE employee_ext LIKE employee;

-- Copying data into respective table from corresponding stages and we are fetching the table data using metadata function.
COPY INTO employee_int(ID, firstname, lastname, email, phonenumber, city, filename)
FROM (SELECT $1, $2, $3, $4, $5, $6, METADATA$FILENAME FROM @internal_stage/employeesf.csv.gz)
FILE_FORMAT = csv_format
ON_ERROR = CONTINUE;

COPY INTO employee_ext(ID, firstname, lastname, email, phonenumber, city, filename)
FROM (SELECT $1,$2,$3,$4,$5,$6,metadata$filename FROM @external_stage)
FILE_FORMAT = csv_format
ON_ERROR = CONTINUE;

SELECT * FROM employee_int LIMIT 10;
SELECT * FROM employee_ext LIMIT 10;

-- Question - 7
-- Creating variant version using parse_json directly from the internal stage created earlier
CREATE OR REPLACE TABLE employees_variant
AS (
SELECT PARSE_JSON('{
    "ID": ' || t.$1 || ',
    "First_Name": "' || t.$2 || '",
    "Last_Name": "' || t.$3 || '",
    "Email": "' || t.$4 || '",
    "Department": " '|| t.$5 || ' ",
    "Contact_no": "'|| t.$6 || '",
    "City": "'|| t.$7 || '",
  }') AS employee_data
  FROM @internal_stage (pattern => '.*employeesf.*') t
);

select * from employees_variant;

-- Question - 10
-- Upload any parquet file to the stage location and infer the schema of the file
    -- We created a file format called parquet_format which holds data of format parquet
CREATE FILE FORMAT my_parquet_format
  TYPE = PARQUET;

-- We created a stage called parquet_stage with holding parquet_format file format.
CREATE STAGE parquet_int file_format=parquet_format;

/*
putting employeesf.parquet in staging area from local using snowsql
put file://~/Desktop/employeesf.parquet @parquet_stage;
*/

LIST @parquet_int;
    
 -- Query to Infer about the schema
SELECT * FROM TABLE( INFER_SCHEMA(
            LOCATION => '@parquet_int',
            FILE_FORMAT => 'parquet_format'
        )
    );

-- Question - 11
-- Run a select query on the staged parquet file without loading it to a snowflake table
SELECT * FROM @parquet_int/employeesf.parquet;


-- Question - 12
-- Add masking policy to the PII columns such that fields like email, phone number, etc. show as **masked** to a user with the developer role. If the role is PII the value of these columns should be visible
-- Creating masking policy for given constraints.
CREATE OR REPLACE MASKING POLICY email_mask AS (VAL string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('PII') THEN VAL
    ELSE '**masked**'
  END;


CREATE OR REPLACE MASKING POLICY contact_mask AS (VAL string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('PII') THEN VAL
    ELSE '**masked**'
  END;


-- Applying those policies to table by altering them
ALTER TABLE IF EXISTS employee_int MODIFY Email SET MASKING POLICY email_mask;

ALTER TABLE IF EXISTS employee_ext MODIFY Email SET MASKING POLICY email_mask;

ALTER TABLE IF EXISTS employee_ext MODIFY phonenumber SET MASKING POLICY contact_mask;

ALTER TABLE IF EXISTS employee_int MODIFY phonenumber SET MASKING POLICY contact_mask;


-- Displaying data from Admin view
SELECT * FROM employee_int LIMIT 10;
SELECT * FROM employee_ext LIMIT 10;


USE ROLE ACCOUNTADMIN;
-- Granting required previlages to role PII
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_int TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_ext TO ROLE PII;

-- using the role PII
USE ROLE PII;

-- Displaying data from PII role
SELECT * FROM employee_int LIMIT 10;
SELECT * FROM employee_ext LIMIT 10;

-- Switching the role 
USE ROLE ACCOUNTADMIN;
-- Granting required previlages to role DEVELOPER
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_int TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_ext TO ROLE DEVELOPER;

-- using the role DEVELOPER
USE ROLE DEVELOPER; 

-- Displaying data from PII role
SELECT * FROM employee_int LIMIT 10;
SELECT * FROM employee_ext LIMIT 10;






