#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2
import pandas as pd
from sqlalchemy import create_engine


# In[2]:


try:
    with psycopg2.connect(
        dbname="Projects",
        user="postgres",
        password="Varadeep",
        host="localhost",
        port="9369"
    ) as connection:
        with connection.cursor() as cursor:
            data=pd.read_csv("V:/Projects/1.Data Analytics/3.Campaign Strategy Dashboard/data_drug.csv")
            engine = create_engine("postgresql://postgres:Varadeep@localhost:9369/Projects")
            data.to_sql("drugs_data",con=engine,if_exists="append",chunksize=1000)
except psycopg2.Error as e:
    print("Error:", e)
else:
    print("Data succesfully copied to DataBase")
finally:
    if connection:
        connection.close()

