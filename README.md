# Mini-SQL-Engine
A mini sql engine which will run a subset of SQL queries using command line interface.
### Built using Python, itertools, regex.

### Dataset:
1. csv files for tables.
- If a file is : File1.csv , the table name would be File1
- There will be no tab separation or space separation.
- Both csv file type cases taken care of : the one where values are in double quotes and the one where values are without quotes.
2. All the elements in files would be only INTEGERS
3. A file named: metadata.txt (note the extension) would be available which will have the following structure for each table:
-<begin_table>
-<table_name>
-<attribute1>
-....
-<attributeN>
-<end_table>
4. Column names are unique among all the tables. So column names are not preceded by table names in SQL queries.

### Queries:
1. Project Columns(could be any number of columns) from one or more tables :
- Usage : Select * from table_name;
- Usage : Select col1, col2 from table_name;
2. Aggregate functions: Simple aggregate functions on a single column.
Sum, average, max, min and count :
- Usage : Select max(col1) from table_name;
- Select/project with distinct from one table: (distinct of a pair of values indicates the pair should be distinct), Usage : Select distinct col1, col2 from table_name;
4. Select with WHERE from one or more tables :
- Usage : Select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20;
- In the where queries, there would be a maximum of one AND/OR operator with no NOT operators.
- Relational operators include "< , >, <=, >=, =".
5. Select/Project Columns(could be any number of columns) from table using “group by”:
- Usage : Select col1, COUNT(col2) from table_name group by col1.
- In the group by queries, Sum/Average/Max/Min/Count can be used as aggregate functions.
6. Select/Project Columns(could be any number of columns) from table in ascending/descending order according to a column using “order by”:
- Usage : Select col1,col2 from table_name order by col1 ASC|DESC.
- At max only one column can be used to sort the rows.
- Query can have multiple tables in it.

### Relevant Details:
- Multiple error handling checks are put to deal with erroneous queries.
- To run the engine, use : bash run.sh "Query";
where Query can be all the permutations and combinations of SQL that MySQL permits using the built clauses.
