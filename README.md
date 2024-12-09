# DSCI551 Final Project - CHATDB
## Minghua Zhang


| col 3 is      | right-aligned | $1600 |
| col 2 is      | centered      |   $12 |
| zebra stripes | are neat      |    $1 |

CHATDB runs at the terminal line. To begin, one can type 'python project.py' 

Then user can follow the prompt to use existing database or using existing databases. 

Suppose the user choose to use the existing databases and choose 'coffeeshop' as the database to explore

User now could choose to explore data (1) or jump to query (2) or back to last level (3)

For query(2), user can type anything now to give prompt to generate SQL queries 

For example: 

 a. 'sample query'   -- generating example SQL queries based on current database
 
 b. 'sample query with groupby'  -- generating example SQL queries using a specific language structure - GroupBy
 
 c. 'find highest unit_price by product_category' -- natural language answering - find the maximum unit price for each product category

 After generating the queries, users can choose if they want to execute the queries that are just generated. 
 
 If they do, they then choose one of the queries to print out actual result of the query.

For uploading new database, user can choose to upload 'usedcar.csv' and 'gym.csv' in the begining level and create corresponding new databases
