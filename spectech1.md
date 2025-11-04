we working with mysql ver 8.0
i want to create app for backup database with node js for user friendly , user can input select query for backup , and can select db1 or db2 connection based on .env file
method backup using generate csv files, with using query 
for export used this query
SELECT column1, column2, column3
INTO OUTFILE '/path/to/your/file.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
FROM your_table_name;

and for restore used this query
LOAD DATA INFILE '/path/to/your/file.csv'
INTO TABLE your_table_name
FIELDS TERMINATED BY ','
ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES; -- Use this if your CSV has a header row