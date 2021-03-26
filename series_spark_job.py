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





# In[78]:


series_data_1 = spark.read.options(delimiter=';', header='True').csv("gs://bigdata-etl-2_flights/qoala-query-result-5")


# In[83]:


series_data_1.registerTempTable("series_data_1")


# In[84]:


spark.sql("select count(*) from series_data_1").show()


# In[85]:


series_1 = spark.sql("select * from series_data_1")


# In[86]:


series_data_2 = spark.read.options(delimiter=';', header='True').csv("gs://bigdata-etl-2_flights/qoala-query-result-4")


# In[87]:


series_data_2.registerTempTable("series_data_2")


# In[88]:


spark.sql("select count(*) from series_data_2").show()


# In[89]:


series_2 = spark.sql("select * from series_data_2")


from datetime import date

current_date = date.today()

file_name = str(current_date)

bucket_name = "gs://bigdata-etl-2_flights"


# In[95]:


output_movies_ata_1 = bucket_name+"/series_data_output/"+file_name+"_datamart_1"
output_movies_ata_2 = bucket_name+"/series_data_output/"+file_name+"_datamart_2"


# In[97]:


series_1.coalesce(1).write.format("json").save(output_movies_ata_1)
series_2.coalesce(1).write.format("json").save(output_movies_ata_2)


# In[ ]:




