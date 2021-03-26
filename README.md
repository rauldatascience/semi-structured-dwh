# Project Problem

I was asked to do a data warehousing process with semi-structured data (nested json) and I got the data from the Kaggle platform via the following url here https://
www.kaggle.com/edgartanaka1/tmdb-movies-and-series. 

In this case, i was also asked for making data pipelines. This will makes DA/BI easier to query or consume the dataset.


# Project Preparation

In this case, i use this following tools:
1. BigQuery as Data Warehouse (Schedulling Query) and as a datamart for DA/BI
2. Google Cloud Storage (Source)
3. Dataproc ephermal-cluster as a workflow
4. Apache Spark (computing if needed)
5. Apache Airflow (Composer) as an orchestration tools

Here is the figure of my architecture of movies and series dataset

![alt text](https://github.com/rauldatascience/semi-structured-dwh/blob/main/output_merge/flow.jpg?raw=true)

# Project Step Documentation

1. Downloading the dataset from kaggle platform
2. Merge each json files (in this case i use 10000 files of movies and series). for merging the json files, refer to `read_and_merge_files.py`.
3. Store the result to the GCS
4. Create dataset and table in BigQuery with auto-detection schema. This will performs a nested JSON into BigQuery.
5. Make 5 querys for 5 datamart with schedulling query service. The output file is CSV that generate the CSV result to GCS. For the documentation query please refer 
6. Create ephermal cluster dataproc with apache airflow, please refer to this file `dag_movies.py` and `dag_series.py`
7. In the DAG's we also make a spark job, incase we are going to make a huge data computation. We also deleting the cluster automatically for bill security aspect.
8. The result will stored in BigQuery as a new table of Datamart. So, DA/BI can access easily there.

## Movies DAG's

![alt text](https://github.com/rauldatascience/semi-structured-dwh/blob/main/output_merge/movie_airflow.png?raw=true)

## Series DAG's

![alt text](https://github.com/rauldatascience/semi-structured-dwh/blob/main/output_merge/series_airflow.png?raw=true)




