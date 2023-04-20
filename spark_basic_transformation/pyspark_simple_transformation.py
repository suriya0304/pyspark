#!/usr/bin/env python
# coding: utf-8

# In[4]:


import findspark
import os


# In[5]:


findspark.init("/home/suriya/pySpark/spark-3.3.2-bin-hadoop3")


# In[6]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ### Absolute path of data

# In[7]:


cur_dir=os.getcwd()
rel_path='data.csv'
abs_path=os.path.join(cur_dir,rel_path)
abs_path


# ### Create spark session

# In[8]:


spark=SparkSession.builder.appName('simple transformation').master('local').getOrCreate()


# ### Generating df from a csv file

# In[9]:


df=spark.read.csv(path=abs_path,header=True,inferSchema=True)


# In[10]:


df=df.withColumn("name",F.concat(F.col('lname'),F.lit(','),F.col('fname')))


# In[11]:


df.head(5)


# ### Connection properties to PG

# In[12]:


dbConnectionUrl = "jdbc:postgresql://localhost:5432/spark_test"


# In[13]:


prop = {"driver":"org.postgresql.Driver", "user":"postgres", "password":"test"}


# ### Connect and Write to db

# In[16]:


df.write.jdbc(mode='overwrite', url=dbConnectionUrl, table="name", properties=prop)


# In[ ]:




