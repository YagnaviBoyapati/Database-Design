# Database Engine
 The goal of this project is to design and develop a simple, lightweight Relational Database Management System (RDBMS) from scratch.
 
 
Project: Building simple relational database management system (RDBMS). 
## Dependencies:
This project was built and tested on Java SE >18.0.1, this may still work for olderversions.
## Setup and usage:
Download the and unzip the file to the location of your choice, open the terminal/command prompt, set the file location in terminal using ‘cd <destination>’ command. Type the commands in the terminal to compile and execute the code.
 >> javac *.java 
 >> java TeamPauli
You will see a prompt ‘davisbase’ appear in the output. To see the list of commands that you want to type in davisbase, you can issue the command help. 
Point to be noted that all identifiers in DavisBase are case insensitive. So, while typing down the command, you can type the query either in lower case or higher case.
Other than help, we recommend trying out these commands to test the SQL engine:
1.    show databases;
2.    use <tablename>;
3.    show tables;
4.    help;
5.    select * from davisbase_columns
6.    select * from davisbase_columns where table_name="davisbase_tables";
7.    CREATE TABLE <tablename> (<give values>);
8.    insert into <tablename> values (<give values>);
9.    update <tablenmae> set <given values> where <condition>;
10.    delete from <tablename>;
11.    drop table <tablename>;
12.    exit;
## Assumptions:
1.    When updating string values, values of same length should be given, else system can behave erratically.
2.    only '=' is supported when using conditions on TEXT columns.
3.    Supported Data Types: Byte, Short, Integer, Long, Float, Double, Date, DateTime, String.
4.    Only one condition in SELECT, UPDATE and DELETE command is supported.
5.    Page size is fixed to 512 bytes.
6.    Explicitly NULL value cannot be set to a column in INSERT query.

