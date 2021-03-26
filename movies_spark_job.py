#!/usr/bin/env python
# coding: utf-8

# In[76]:

import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession


# Need to make declaration in SparkContext() when submit pyspark job
sc = SparkContext()
spark = SQLContext(sc)
type(spark)

# In[77]:


movies_data_1 = spark.read.csv("gs://bigdata-etl-2_flights/qoala-query-result/000000000000.csv")


# In[78]:


movies_data_1 = spark.read.options(delimiter=';', header='True').csv("gs://bigdata-etl-2_flights/qoala-query-result")


# In[83]:


movies_data_1.registerTempTable("movies_data_1")


# In[84]:


spark.sql("select count(*) from movies_data_1").show()


# In[85]:


movies_1 = spark.sql("select * from movies_data_1")


# In[86]:


movies_data_2 = spark.read.options(delimiter=';', header='True').csv("gs://bigdata-etl-2_flights/qoala-query-result-2")


# In[87]:


movies_data_2.registerTempTable("movies_data_2")


# In[88]:


spark.sql("select count(*) from movies_data_2").show()


# In[89]:


movies_2 = spark.sql("select * from movies_data_2")


# In[90]:


movies_data_3 = spark.read.options(delimiter=';', header='True').csv("gs://bigdata-etl-2_flights/qoala-query-result-3")


# In[91]:


movies_data_3.registerTempTable("movies_data_3")


# In[92]:


spark.sql("select count(*) from movies_data_3").show()


# In[93]:


movies_3 = spark.sql("select * from movies_data_3")


# In[94]:


from datetime import date

current_date = date.today()

file_name = str(current_date)

bucket_name = "gs://bigdata-etl-2_flights"


# In[95]:


output_movies_ata_1 = bucket_name+"/movies_data_output/"+file_name+"_datamart_1"
output_movies_ata_2 = bucket_name+"/movies_data_output/"+file_name+"_datamart_2"
output_movies_ata_3 = bucket_name+"/movies_data_output/"+file_name+"_datamart_3"


# In[97]:


movies_1.coalesce(1).write.format("json").save(output_movies_ata_1)
movies_2.coalesce(1).write.format("json").save(output_movies_ata_2)
movies_3.coalesce(1).write.format("json").save(output_movies_ata_3)


# In[ ]:




