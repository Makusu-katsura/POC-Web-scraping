# This program are develop to make web scraping for educational purpose only.
# The idea is to extract Product ID from Product Name save to database for web scraping
import pyodbc 
import re
conn  = pyodbc.connect('DRIVER={SQL Server};'
                      'Server=localhost;'
                      'Database=test;'
                     'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute("SELECT a.* from (select DISTINCT [Product Name] from  XXX ) a order by [Product Name]")

sql_batch = ''
count=0
sql_array = []
for product_name in cursor.fetchall():
            name = product_name[0]
            # find pattern of word using regular expression to find a word contains 10 digit ex. '1457896325' or 'B0XXXXXXXX'
            id= re.findall(r'[A-Z0-9]{10}', name)
            if len(id)>1:
               print(id)
               # find pattern of word using regular expression to find a word start with 'B0' and end with anything 
               # but still has 10 digit ex 'B0XXXXXXXX'
               ProductID = re.findall(r'\bB0.{8}\b',str(id))
               if len(set(ProductID)) <= 1:
                    print(ProductID)
                    if ProductID == []: 
                         continue
                    ProductID=ProductID[0] 
               else:  
                    # because sometimes we got a words that are in a pattern but it's not what we want 
                    # so we assuming that next index would be the one we want 
                    ProductID =ProductID[1]
               # because we use library re when result come out it's inside [''] so we need to remove them
               id =id[id.index(str(ProductID).removeprefix('[\'').removesuffix('\']'))]
               print(id)
          
            # because we use library re when result come out it's inside [''] so we need to remove them
            ProductID = str(id).removeprefix('[\'').removesuffix('\']')
            # if we can't find a word in pattern let database save it as empty string
            if(ProductID=='[]'):
                 ProductID = ''
            sql = 'INSERT INTO dbo.XXX ([Product Name], ProductID) VALUES('+name+', '+ProductID+');\n'
            sql_batch+= sql
            ob = (name,ProductID)

            sql_array.append(ob)
            count+=1
            print(count)

cursor.executemany('''INSERT INTO dbo.XXX_Scraping ([Product Name], ProductID) VALUES(?, ?)''',sql_array)
cursor.commit()
cursor.close()


