# peer learning document
## Mohan's Approach
Link - https://github.com/mohangundluri2510/AWS-Assignment
- used ACCOUNTADMIN role, which is the built-in role that has the highest level of privileges in Snowflake.
- Created three roles: Admin, Developer, and PII. Admin is granted the Developer role, and PII is granted the ACCOUNTADMIN role.
- The assignment_db database is dropped if it exists, and a new warehouse assignment_wh is created using the ACCOUNTADMIN role.
- The Admin role is granted privileges to assignment_wh and CREATE DATABASE permissions on the account level.
```
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE Admin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE Admin;
```

- Switched to the Admin role, creates a new database assignment_db, and creates a new schema my_schema within assignment_db.
- Created a variant table
- used the below command to move the file from local to table stage.
`PUT file:///Users/mohangundluri/Downloads/data.json @%JSON_TABLE`  
- Moved the file from table stage to table.
-  A new external stage External_stage is created to load data from an S3 bucket using AWS key id and AWS secret key
```
CREATE STAGE External_stage
    URL='s3://employee-data-bucket-1/'
    CREDENTIALS=(AWS_KEY_ID='' AWS_SECRET_KEY='');
```
- Two new tables are created: External_Employee_Table and Internal_Employee_Table. The former is used to load data from the External_stage while the latter is used to load data from an internal stage
```
CREATE OR REPLACE TABLE External_Employee_Table(
    Name VARCHAR(50) NOT NULL,
    Phone VARCHAR(15) NOT NULL,
    Email VARCHAR(100) NOT NULL,
    Country VARCHAR(20) NOT NULL,
    Address VARCHAR(100) NOT NULL,
    etl_ts timestamp default current_timestamp() ,
    etl_by VARCHAR(100) default 'SNOW_SIGHT',
    file_name VARCHAR(100)
);
```
- Created a file format for a CSV file and defining the delimiter and header settings.
```
CREATE OR REPLACE FILE FORMAT CSV_FILE_FORMAT
    TYPE='CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;
 ```
- Copied data from an external stage into a table named External_Employee_Table using the CSV file format.
```
COPY INTO External_Employee_Table(Name, Phone, Email, Country, Address, file_name)
FROM (SELECT $1 AS Name , $2 AS Phone, $3 AS Email, $4 AS Country , $5 AS Address, METADATA$FILENAME AS file_name FROM @External_stage(
    FILE_FORMAT => CSV_FILE_FORMAT
));
```
- Copied data from an internal stage into a table named Internal_Employee_Table using the CSV file format.
- Created a file format for a Parquet file and a stage named Internal_stage_for_parquet using the Parquet file format then schema is inferred.
-  A masking policy is created for the email and phone columns in the External_Employee_Table.
- Privileges are granted to the PII role for the warehouse, database, and tables in the schema.
- Data is selected from the Parquet file using the PII role.
- A new user called Developer1 is created with the default role of Developer and the default warehouse of 'assignment_wh'.
```
CREATE USER IF NOT EXISTS Developer1 
PASSWORD='Developer1' DEFAULT_ROLE =' Developer'  
DEFAULT_WAREHOUSE = 'assignment_wh'  
MUST_CHANGE_PASSWORD = FALSE;
```
- The Developer role is granted to the Developer1 user.
```
GRANT ROLE Developer TO USER Developer1;
```
- Data is selected from the Parquet file using the Developer1 user and masking can be seen.

## Aswat Bisht's Approach
Link - https://github.com/bisht-ash/Snowflake
- Defined three new roles: Admin, PII, and Developer.
- Granted Admin and PII roles to the existing role ACCCOUNTADMIN, and grant the Developer role to the Admin role.
- Created data warehouse with   WAREHOUSE_SIZE =MEDIUM and then altered to WAREHOUSE_SIZE = SMALL
- Granted all privileges on the warehouse to the Admin role and allow the Admin role to create databases in the account.
- Created a new database called assignment_db and a schema called my_schema inside that database.
- Created a table emp 
```
CREATE OR REPLACE TABLE emp
(
EMPLOYEE_ID NUMBER,
FIRST_NAME STRING,
LAST_NAME STRING,
EMAIL STRING,
PHONE_NUMBER STRING,
HIRE_DATE STRING,
JOB_ID STRING,
SALARY NUMBER,
COMMISSION_PCT STRING,
MANAGER_ID STRING,
DEPARTMENT_ID STRING
)
```
- Created a stage named my_int_stage and loaded the data into internal stage.
- Loaded into stage using snowsql put
```
put file://~/Desktop/employees.csv @my_int_stage;
```
- Created a my_csv_format with format = CSV
- He has set up storage integration with AWS S3 and made the trust relationship between snowflake and AWS
- Created a Variant table using PARSE_JSON and Checked whether data is inserted or not
```
SELECT X:First_name::string from emp_V;
```
- Defined  Parquet file format called my_parquet_format.
- uploaded a .parquet to s3 bucket and Infered the schema of the parquet file.
- Random query on parquet 
```
SELECT $1:first_name::varchar NAME,
       $1:email::varchar EMAIL
FROM @my_ext_stage/userdata1.parquet (FILE_FORMAT=>'my_parquet_format' );
```
- Defined two masking policies for email and salary and applied to roles as per requirement
- Granted privileges to the PII and Developer roles to access the warehouse, database, schema, and tables.
- Switched between roles to test access to the emp table.

