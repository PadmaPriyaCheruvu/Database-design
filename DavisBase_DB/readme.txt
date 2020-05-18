After unzipping the assignment, execute the following commands from the DavisBase folder on the command line
1. javac DavisBase.java
2. java DavisBase

This displays davisbase prompt will appear and commands can be executed.

Available commands and usage---


CREATE COMMAND 
CREATE TABLE table_name (
    column1 datatype,
    column2 datatype,
    column3 datatype,
   ....
); 

Example - create table employee (emp_id int primary key,name char, salary int);

====================================
INSERT COMMAND 
INSERT INTO table_name (column1, column2, column3, ...)
VALUES (value1, value2, value3, ...); 

Example - insert into employee (emp_id,name,salary) values (1,John,100000); 

====================================
SELECT COMMAND 
SELECT column1, column2, ...
FROM table_name WHERE <Condition>;

Example - select * from employee
where emp_id = 1; 
====================================
DROP COMMAND 
DROP TABLE TABLE_NAME 

Example - Drop table employee;
====================================
SHOW TABLES;

Shows all the available tables in DavisBase
====================================
EXIT

Exit from DavisBase
====================================
HELP;

Displays all the possible commands and their usage.
====================================
