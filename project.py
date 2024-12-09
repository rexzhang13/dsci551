import numpy as np
import pandas as pd
import random
import nltk
from nltk.tokenize import word_tokenize
import mysql.connector


def is_number(token):
    try:
        float(token)
        return True
    except ValueError:
        return False



def connect_to_database(database_name):
   connection = mysql.connector.connect(
            host='localhost',    
            database= database_name,   
            user='root',          
            password='Zmh201130?' 
        )
        
   if connection.is_connected():
        print("Successfully connected to database")

   return connection

def process_tables(connection):
   dataframes = {}

   if connection.is_connected():
      
      cursor = connection.cursor()

      cursor.execute("SHOW TABLES")
      tables = cursor.fetchall()

      for (table_name,) in tables:
         
         cursor.execute(f"SELECT * FROM {table_name}")
         rows = cursor.fetchall()

         column_names = [d[0] for d in cursor.description]

         df = pd.DataFrame(rows, columns=column_names)
         dataframes[table_name] = df

   return dataframes

def upload_table(connection, file_name):

   if connection.is_connected():
      data = pd.read_csv(file_name)
       
      query_columns_types = []
      for column_name, type in zip(data.columns, data.dtypes):
         if 'int' in str(type):
            query_columns_types.append(f"{column_name} INT")
         elif 'float' in str(type):
            query_columns_types.append(f"{column_name} FLOAT")
         else:
            query_columns_types.append(f"{column_name} VARCHAR(255)")


      cursor = connection.cursor()
      #create the table with the attribute 
      q = f"CREATE TABLE {file_name.split('.')[0]} ({','.join(query_columns_types)})"
      cursor.execute(q)

      #columns = ', '.join(data.columns)
      placeholders = ', '.join(['%s'] * len(data.columns))
      query = f"INSERT INTO {file_name.split('.')[0]} VALUES ({placeholders})"

      for _, row in data.iterrows():
         cursor.execute(query, tuple(row))

      connection.commit()

   return connection



   


############### PATTERNS ##################

def group_by(table, function, A, B):
   #Select function A GroupBy B
   #A ==> numerical
   #B ==> categorical
   nlr = f'find the {function} of {A} by {B} '

   pattern = f"SELECT {B}, {function}({A}) FROM {table} GROUP BY {B}"



   return [pattern, nlr]

def where(table, A, value, typ):
   nlr = f'find {A} where it equals to {value} '

   if typ == 'str':
     pattern = f"SELECT *  FROM {table} WHERE {A} = '{value}' "
   else: 
     pattern = f"SELECT *  FROM {table} WHERE {A} = {value} "

   return [pattern, nlr]

def order_by(table, A):

   nlr = f'Order the data by {A} '

   pattern = f"SELECT *  FROM {table} ORDER BY {A} "

   return [pattern, nlr]

def having(table, function, A, B, value, typ):
   nlr = f'find the {function} of {A} by {B} when the {function} of {A} equals to {value}  '

   if typ == 'str':
     pattern = f"SELECT {B} , {function}({A}) FROM {table} GROUP BY {B} HAVING {function}({A}) = '{value}'"
   else:
     pattern = f"SELECT {B} , {function}({A}) FROM {table} GROUP BY {B} HAVING {function}({A}) = {value}"

   return [pattern, nlr]

   
#############################################

def process_user_input(input):
   ##see if the user's input is asking for one of the following three kinds of questions:
   ##1. sample query 
   ##2. sample query with specific kind ：group by, having, order by, where.....
   ##3. Natural Language Answering 

   keywords1 = ['sample', 'example','samples', 'examples',]
   keywords2 = ['groupby', 'having', 'orderby', 'where', 'min', 'max', 'sum', 'count', 'average']
   tokens = word_tokenize(input.lower())


   if any(k in tokens for k in keywords1):
      if any(k in tokens for k in keywords2):
         return 2
      else:
         return 1
   else:
      return 3



def sample_data(connection):
   if connection.is_connected():
      cursor = connection.cursor()

      cursor.execute("SHOW TABLES")
      tables = cursor.fetchall()


      for (table_name,) in tables:
        

        query = f"SELECT * FROM {table_name} LIMIT 5"
        cursor.execute(query)
        data = cursor.fetchall()
        
        column_names = [d[0] for d in cursor.description]
        


        #data_by_table[table_name] = data


        print(f"\n Keys and Some Sample Data from Table '{table_name}':")
        print(column_names)
        for row in data:
          print(row)

def table_attr(dataframe):   
    numberical_attr = []
    categorical_attr = [] 
    
    i = 0
    for attr in dataframe.columns:
        value = dataframe.iloc[0, i]

        #if it is a string ==> categorical value
        if type(value) == str:
            categorical_attr.append(attr)
        #if it is a int or float ==> numerical value
        elif type(value) == int or float:
            numberical_attr.append(attr)   
        i += 1


    return numberical_attr, categorical_attr


def sample_query(dataframes):
   #group by, having, order by, where
   f = ['MAX', 'MIN', 'COUNT', 'SUM', 'AVG']

   result = []
   
   #for each table in the database
   for table_name, df in dataframes.items():
      numberical_attr, categorical_attr = table_attr(df)

      if len(numberical_attr) != 0 and len(categorical_attr) != 0:
        A = random.choice(numberical_attr)
        B = random.choice(categorical_attr)
        function = random.choice(f)
        
        #group by 
        result.append(group_by(table_name, function, A, B)[0])
        result.append(group_by(table_name, function, A, B)[1])

        #having
        A = random.choice(numberical_attr)
        B = random.choice(categorical_attr)
        value = random.choice(df[A].tolist())
        function = random.choice(f)
        typ = ' '


        if A in categorical_attr:
          typ = 'str'

        result.append(having(table_name, function, A, B, value, typ)[0])
        result.append(having(table_name, function, A, B, value, typ)[1])



      #where
      A = random.choice(df.columns.tolist()) 
      value = random.choice(df[A].tolist())
      typ = ' '
      if A in categorical_attr:
          typ = 'str'

      result.append(where(table_name, A, value, typ)[0])
      result.append(where(table_name, A, value, typ)[1])
   

      

      #order by
      A = random.choice(df.columns.tolist()) 
      result.append(order_by(table_name, A)[0])
      result.append(order_by(table_name, A)[1])

      
   
   return result


#pattern matching for query with specific keywords
def keywords(input):

   tokens = word_tokenize(input.lower())

   if 'groupby' in tokens:
      if 'min' in tokens:
         return ['groupby', 'MIN']
      elif 'max' in tokens:
         return ['groupby', 'MAX']
      elif 'average' in  tokens:
         return ['groupby', 'AVG']
      elif 'count' in tokens:
         return ['groupby', 'COUNT']
      elif 'sum' in tokens:
         return ['groupby', 'SUM']


   elif 'orderby' in tokens:
      return ['orderby']
   
   elif 'having' in tokens:
      return ['having']
   
   elif 'where' in tokens:
      return ['where']



def sample_query_with_keyword(dataframes,input):

   keyword = keywords(input)

   f = ['MAX', 'MIN', 'COUNT', 'SUM', 'AVERAGE']
   result = []

   #for each table in the database
   for table_name, df in dataframes.items():
      numberical_attr, categorical_attr = table_attr(df)

   #if len(numberical_attr) != 0 and len(categorical_attr) != 0:
   
      if keyword[0] == 'groupby':
         f = keyword[1]
         A = random.choice(numberical_attr)
         B = random.choice(categorical_attr)
         result.append(group_by(table_name, f, A, B) [0])
         result.append(group_by(table_name, f, A, B) [1])

      elif keyword[0] == 'orderby':
         A = random.choice(df.columns.to_list())
         result.append(order_by(table_name, A)[0])
         result.append(order_by(table_name, A)[1])

      elif keyword[0] == 'having':
         function = random.choice(f)
         A = random.choice(numberical_attr)
         B = random.choice(categorical_attr)
         value = random.choice(df[A].tolist())
         typ = ' '

         if A in categorical_attr:
          typ = 'str'
         result.append(having(table_name, function, A, B, value, typ )[0])
         result.append(having(table_name, function, A, B, value, typ )[1])

      elif keyword[0] == 'where':
         A = random.choice(df.columns.tolist()) 
         value = random.choice(df[A].tolist())
         typ = ' '

         if A in categorical_attr:
           typ = 'str'
         result.append(where(table_name, A, value, typ )[0])
         result.append(where(table_name, A, value, typ )[1])

         
   return result

         
               
               

      


def nl_keywords(dataframes,input):   
   input_attr = []
   input_func = []
   

   #pre-defined functional keywords
   function_keywords = ['highest', 'largest', 'biggest', 'most', 'maximum',
                          'lowest', 'smallest', 'least', 'minimum',
                          'total', 'sum',
                          'average', 'mean',
                          'count', 'number of',
                          'group', 'by', 'cluster', 'partition',
                           'order', 'arrange',
                           'when', 'where', 'equal',
                           'having', 'where'
                            ]

  

   tokens = word_tokenize(input.lower())
   tokens_unlower = word_tokenize(input)
   value = tokens[-1]
   if 'to' in tokens:
     index = tokens.index('to')
     value = ' '.join(tokens_unlower[index + 1:])

   #extract the functional keywords and attribute names from input 
   for t in tokens:
      if t in function_keywords:
         input_func.append(t)

   
   for _, df in dataframes.items():
     for t in tokens_unlower:
      if t in df.columns:
         input_attr.append(t)

   for t in tokens:
      if is_number(t):
         value = t


   return input_func, input_attr, value
   




def query_nl(dataframes,table, input):
   #possible natural language representations of MYSQL keywords
   # NL                                         MYSQL
   # highest, largest, biggest, most, maximum    MAX
   # lowest, smallest, least, minimum            MIN
   # total, sum                                  SUM
   # average, mean                              AVERAGE
   # count, number of                           COUNT              
   # group, by, cluster, partition             GROUPBY
   # order, arrange                             ORDERBY
   # when, where, equal to,                      WHERE
   # having, where                            HAVING


   #extract the functional keywords, attribute names, and possible numberical value (for where and having ) from input 
   input_func, input_attr, value = nl_keywords(dataframes,input)

   for _, df in dataframes.items():
      _, categorical_attr = table_attr(df)
   

   #NL: function <numerical A> by <categorical B> 
   if any(i in ['by'] for i in input_func ): 
      #function <numerical A> by <categorical B>  Having 
      if any(i in ['having', 'where', 'equal'] for i in input_func):
        if any(i in ['highest', 'largest', 'biggest', 'most', 'maximum'] for i in input_func ):
            return having(table, 'MAX',input_attr[0] , input_attr[1], value, ' ')
        elif any(i in ['lowest', 'smallest', 'least', 'minimum'] for i in input_func ):
            return having(table, 'MIN',input_attr[0] , input_attr[1], value, ' ')
        elif any(i in ['total', 'sum'] for i in input_func ):
            return having(table, 'MIN',input_attr[0] , input_attr[1], value, ' ')
        elif any(i in ['average', 'mean'] for i in input_func ):
            return having(table, 'AVG',input_attr[0] , input_attr[1], value, ' ')
        elif any(i in ['count', 'number of'] for i in input_func ):
            return having(table, 'COUNT',input_attr[0] , input_attr[1], value, ' ')

      
      else:
         #SELECT MAX <numerical A> FROM  TABLE  GROUPBY <categorical B> 
         if any(i in ['highest', 'largest', 'biggest', 'most', 'maximum'] for i in input_func ):
            return group_by(table, 'MAX',input_attr[0] , input_attr[1])
         elif any(i in ['lowest', 'smallest', 'least', 'minimum'] for i in input_func ):
            return group_by(table, 'MIN',input_attr[0] , input_attr[1])
         elif any(i in ['total', 'sum'] for i in input_func ):
            return group_by(table, 'MIN',input_attr[0] , input_attr[1])
         elif any(i in ['average', 'mean'] for i in input_func ):
            return group_by(table, 'AVG',input_attr[0] , input_attr[1])
         elif any(i in ['count', 'number of'] for i in input_func ):
            return group_by(table, 'COUNT',input_attr[0] , input_attr[1])
   
   if any(i in ['order', 'arrange'] for i in input_func ): 
      return order_by(table, input_attr[0])

   if any(i in ['when', 'where', 'equal'] for i in input_func ):
      if input_attr[0] in categorical_attr:
        return where(table, input_attr[0], value, 'str')
      else:
        return where(table, input_attr[0], value, ' ')






  


def execute(connection, query):

   cursor = connection.cursor()
   cursor.execute(query)
   result = cursor.fetchall()

   return result
   


               
         
   
  
         

       
    
      
     
   





if __name__ == "__main__":
  
  current_database = 1
  current_connection = 1 
  current_tables = 1
  db_list = ['coffeeshop']


  keywords_sample_query = ['sample', 'example', 'query', 'queries']


  print("Welcome to ChatDB!")
  print("Before inputing your query for ChatDB, please choose if you want to focus on one of the existing databases or upload a new database?")

  

  while True:
    print("1. Add New Database")
    print("2. Existing Database")

    choice = input("Please choose one to get start  ").strip()


    if choice == '1':
      db_name = input("Please name your own database   ")

      connection = mysql.connector.connect(
            host= 'localhost',
            user= 'root',
            password= 'Zmh201130?'
        )

      if connection.is_connected():

         cursor = connection.cursor()
         cursor.execute(f"CREATE DATABASE {db_name}")

      
      current_connection = mysql.connector.connect(
            host= 'localhost',
            user= 'root',
            database = db_name,
            password= 'Zmh201130?'
        )


      file_name = input("Now a new empty database is created! Now upload your file by typing the name of the file  ")
      upload_table(current_connection, file_name)
      
      while True:
         file_name = input("Now the file is uplaod into the database! You can add other files or back to menu by typing E   ")
         if file_name == 'E':
            break
         else: 
            upload_table(current_connection,  file_name)


      current_table = process_tables(current_connection)
      current_database = db_name
      db_list.append(db_name)



    elif choice == '2':
      print("Here are the existing databases:  ")
      for db in db_list:
         print(db)


      current = input("Choose one database to get start  ")

      current_database = current 
      current_connection = connect_to_database(current_database)
      current_tables = process_tables(current_connection)

  
      while True:
         print("You have selected the database! Right now do you want to take a look at the data or jump into querying the database?")
         print("1. Explore the data by showing tables, attributes and their first few rows from each table ")
         print("2. Querying ")
         print("3. Back to last level  ")
         a = input("Select One   ").strip()

         if a == '1':
            sample_data(current_connection)

         elif a == '2':
            inp = input("Input your request/query, you can ask for sample queries with the specific language structure or just ask CHATDB in natural language    ")
            
            #identify the kind of user's question
            r = process_user_input(inp)
            
            #sample query
            if r == 1:
              result = sample_query(current_tables)
              i = 0
              while i < len(result):
                 print(result[i])
                 print(result[i+1])
                 print('\n')
                 i += 2

            #sample query with specific function
            elif r == 2:
               result1 = sample_query_with_keyword(current_tables,inp)
               result2 = sample_query_with_keyword(current_tables,inp)
               result = result1 + result2
               i = 0
               while i < len(result):
                 print(result[i])
                 print(result[i+1])
                 print('\n')
                 i += 2


            elif r == 3:
               result = query_nl(current_tables, current_database, inp)
               i = 0
               while i < len(result):
                 print(result[i])
                 print(result[i+1])
                 print('\n')
                 i += 2
            

            #Ask user if they want to execute the generated query 
            ex = input('Do you want to execute the query/queries that just generated?  [Y/N]')
            if ex == 'Y':
               # 1 & 2
               if isinstance(result, list):
                  queries = []
                  i = 0
                  m = 1
                  while i < len(result):

                     print(str(m) + '  ' +  result[i + 1])
                     print(str(m) + '  ' +  result[i])
                     
                     queries.append(result[i])
                     i += 2
                     m += 1

                  
                  n = input('which one do you want to execute? Input the number   ')
                  print(execute(current_connection, queries[int(n)-1]))
               # 3
               else:
                  print(execute(current_connection, result[0]))

         elif a == '3':
            break

               

                  
               


         

    


  
         


      
      