# snowflakeAssignment

### Questions
1. Create roles as per the below-mentioned hierarchy. Accountadmin already exists in Snowflake
<img width="397" alt="Screenshot 2023-05-24 at 11 19 07 PM" src="https://github.com/Rohith131102/snowflakeAssignment/assets/123619674/753134c9-c86f-425e-a483-d888f82d8e9b">

2. Create an M-sized warehouse using the accountadmin role, name -> assignment_wh and use it for all the queries

3. Switch to the admin role

4. Create a database assignment_db

5. Create a schema my_schema

6. Create a table using any sample csv. You can get 1 by googling for sample csv’s. Preferably search for a sample employee dataset so that you have PII related columns else you can consider any column as PII 

7.  Also, create a variant version of this dataset 

8.  Load the file into an external and internal stage

9.  Load data into the tables using copy into statements. In one table load from the internal stage and in another from the external 

10.  Upload any parquet file to the stage location and infer the schema of the file

11.  Run a select query on the staged parquet file without loading it to a snowflake table

12.  Add masking policy to the PII columns such that fields like email,phone number, etc. show as **masked** to a user with the developer role. If the role is PII the value of these columns should be visible 



Adding comments for every solution in the employee.sql file

Documentation followed - https://docs.snowflake.com/

for s3 integration - https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration

for masking policies - https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy

for variant table - https://docs.snowflake.com/en/user-guide/semistructured-considerations

for granting privileges - https://docs.snowflake.com/en/sql-reference/sql/grant-privilege

