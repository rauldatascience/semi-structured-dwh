from datetime import datetime,timedelta , date 
from airflow import models,DAG 
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator,DataProcPySparkOperator,DataprocClusterDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators import BashOperator 
from airflow.models import *
from airflow.utils.trigger_rule import TriggerRule


current_date = str(date.today())

BUCKET = "gs://bigdata-etl-2_flights"

PROJECT_ID = "bigdata-etl-2"

PYSPARK_JOB = BUCKET + "/spark-job/series_spark_job.py"

DEFAULT_DAG_ARGS = {
    'owner':"airflow",
    'depends_on_past' : False,
    "start_date":datetime.utcnow(),
    "email_on_failure":False,
    "email_on_retry":False,
    "retries": 1,
    "retry_delay":timedelta(minutes=5),
    "project_id":PROJECT_ID,
    "scheduled_interval":"0 0 * * *"
}

with DAG("series_etl",default_args=DEFAULT_DAG_ARGS) as dag : 

    create_cluster = DataprocClusterCreateOperator(

        task_id ="create_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-1",
        worker_machine_type="n1-standard-2",
        num_workers=2,
        region="asia-southeast2",
        zone ="asia-southeast2-a"
    )

    submit_pyspark = DataProcPySparkOperator(
        task_id = "run_pyspark_etl",
        main = PYSPARK_JOB,
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="asia-southeast2"
    )

    bq_load_series_1 = GoogleCloudStorageToBigQueryOperator(

        task_id = "bq_load_series_1",
        bucket='bigdata-etl-2_flights',
        source_objects=["series_data_output/"+current_date+"_datamart_1/*.json"],
        destination_project_dataset_table= f'{PROJECT_ID}:qoala_test.series_datamart_1',
        autodetect = True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0,
    )

    bq_load_series_2 = GoogleCloudStorageToBigQueryOperator(

        task_id = "bq_load_series_2",
        bucket='bigdata-etl-2_flights',
        source_objects=["series_data_output/"+current_date+"_datamart_2/*.json"],
        destination_project_dataset_table= f'{PROJECT_ID}:qoala_test.series_datamart_2',
        autodetect = True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0,
    )

    delete_cluster = DataprocClusterDeleteOperator(

        task_id ="delete_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="asia-southeast2",
        trigger_rule = TriggerRule.ALL_DONE
    )

    delete_tranformed_files = BashOperator(
        task_id = "delete_tranformed_files",
        bash_command = "gsutil -m rm -r " +BUCKET + "/series_data_output/*"
    )

    create_cluster.dag = dag

    create_cluster.set_downstream(submit_pyspark)

    submit_pyspark.set_downstream(bq_load_series_1)

    bq_load_series_1.set_downstream(bq_load_series_2)

    bq_load_series_2.set_downstream(delete_cluster)
    
    delete_cluster.set_downstream(delete_tranformed_files)
