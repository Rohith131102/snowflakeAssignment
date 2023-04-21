


CREATE or REPLACE ROLE Admin;
CREATE or REPLACE ROLE PII;
GRANT ROLE Admin to ROLE ACCCOUNTADMIN;
GRANT ROLE PII to ROLE ACCCOUNTADMIN;
CREATE or REPLACE ROLE DEVELOPER;
GRANT ROLE DEVELOPER to ROLE ADMIN;
USE ROLE Admin;
USE ROLE ACCCOUNTADMIN;

CREATE or REPLACE WAREHOUSE assignment_wh with warehouse_size = Medium;



GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE ADMIN;
GRANT CREATE DATABASE on ACCOUNT to ROLE Admin; 
USE ROLE Admin;
CREATE or REPLACE DATABASE assignment_db;
CREATE SCHEMA my_schema;


CREATE or REPLACE table employee (
ID integer,
firstname varchar(100),
lastname varchar(100),
email varchar(100),
phoneNumber varchar(10),
city varchar(50),
etl_ts timestamp default current_timestamp(),
    -- for getting the time at which the record is getting inserted
etl_by varchar default 'snowsight',
    -- for getting application name from which the record was inserted
filename varchar -- for getting the name of the file USEd to insert data into the table.
);

CREATE STAGE internal_stage;

-- put file:///USErs/rohith_boodireddy/Downloads/employeesf.csv @internal_stage;

current ROLE;
LIST @internal_stage;

CREATE STAGE external_stage;

GRANT ALL ON INTEGRATION s3_integration TO ROLE admin;

CREATE or REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::366068070173:ROLE/admin_s3'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snow-empdata/data/employeesf.csv');


desc integration s3_integration;

CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;

CREATE OR REPLACE STAGE external_stage URL = 's3://snow-empdata/data/employeesf.csv' STORAGE_INTEGRATION = s3_integration FILE_FORMAT = my_csv_format;

LIST @external_stage;


SHOW ROLES;
USE ROLE Admin;


CREATE or REPLACE TABLE employee_int LIKE employee;
CREATE or REPLACE TABLE employee_ext LIKE employee;






COPY INTO employee_int(ID, firstname, lastname, email, phonenumber, city, filename)
FROM (SELECT $1, $2, $3, $4, $5, $6, METADATA$FILENAME FROM @internal_stage/employeesf.csv.gz)
FILE_FORMAT = my_csv_format
ON_ERROR = CONTINUE;


COPY INTO employee_ext(ID, firstname, lastname, email, phonenumber, city, filename)
FROM (SELECT $1,$2,$3,$4,$5,$6,metadata$filename FROM @external_stage)
FILE_FORMAT = my_csv_format
ON_ERROR = CONTINUE;


    
SELECT * FROM employee_int;





LIST @parquet_int;



SELECT * FROM TABLE( INFER_SCHEMA(
            LOCATION => '@parquet_int',
            FILE_FORMAT => 'my_parquet_format'
        )
    );

SELECT * FROM @parquet_int/employeesf.parquet;

CREATE FILE FORMAT my_parquet_format
  TYPE = PARQUET;



CREATE STAGE parquet_int file_format=my_parquet_format;



SELECT * FROM employee_int;







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


ALTER TABLE IF EXISTS employee_int MODIFY Email SET MASKING POLICY email_mask;

ALTER TABLE IF EXISTS employee_ext MODIFY Email SET MASKING POLICY email_mask;

ALTER TABLE IF EXISTS employee_ext MODIFY phonenumber SET MASKING POLICY contact_mask;

ALTER TABLE IF EXISTS employee_int MODIFY phonenumber SET MASKING POLICY contact_mask;



SELECT * FROM employee_int LIMIT 10;
SELECT * FROM employee_ext LIMIT 10;


USE ROLE ACCOUNTADMIN;
-- Granting required previlages to role developer
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_int TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_ext TO ROLE PII;
USE ROLE PII; -- using the role PII

SELECT * FROM employee_int LIMIT 10;
SELECT * FROM employee_ext LIMIT 10;

-- Switching the role 
USE ROLE ACCOUNTADMIN;
-- Granting required previlages to role developer
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA ASSIGNMENT_DB.MY_SCHEMA TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_int TO ROLE DEVELOPER;
GRANT SELECT ON TABLE assignment_db.my_schema.employee_ext TO ROLE DEVELOPER;
USE ROLE DEVELOPER; -- using the role Developer

SELECT * FROM employee_int LIMIT 10;
SELECT * FROM employee_ext LIMIT 10;






