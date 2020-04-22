from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, \
    DataProcPySparkOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from datetime import datetime, timedelta
from operators import InternationalFootballDataSetToDataLake

start_date = datetime(2020, 4, 20)

default_args = {
    'start_date': start_date,
    'depends_on_past': False
}

dag = DAG("internaltional_football_result", default_args=default_args, schedule_interval=None)

cluster_name = 'etl-int-football-{{ ds }}'
gcs_football_bucket = Variable.get('gcs_football_bucket')  # int_football_bucket
gcp_conn = "google_cloud_connection"
region = Variable.get('gc_region')
start_int_football_pipeline = DummyOperator(task_id="StartPipeline", dag=dag)

create_dataproc_cluster = DataprocClusterCreateOperator(
    task_id='create_dataproc_cluster',
    cluster_name=cluster_name,
    project_id=Variable.get('gc_project_id'),
    gcp_conn_id=gcp_conn,
    num_workers=2,
    num_masters=1,
    image_version='preview',
    master_machine_type='n1-standard-2',
    worker_machine_type='n1-standard-2',
    worker_disk_size=50,
    master_disk_size=50,
    region=region,
    storage_bucket=gcs_football_bucket,
    dag=dag
)

international_football_dataset_to_datalake = InternationalFootballDataSetToDataLake(
    task_id="international_football_dataset_to_datalake",
    name="martj42/international-football-results-from-1872-to-2017",
    destination_path="/airflow/datasources/catalog/csv", dag=dag
)

upload_cleaning_spark_job_to_gcs = FileToGoogleCloudStorageOperator(task_id='upload_cleaning_spark_job_to_gcs',
                                                                    src='/airflow/spark-jobs/football_dataset_cleaner.py',
                                                                    dst='spark_jobs/football_dataset_cleaner.py',
                                                                    bucket=gcs_football_bucket,
                                                                    google_cloud_storage_conn_id=gcp_conn,
                                                                    dag=dag)

spark_code_path = 'gs://' + gcs_football_bucket + '/spark_jobs/football_dataset_cleaner.py'
submit_cleaning_spark_job = DataProcPySparkOperator(
    task_id='submit_cleaning_spark_job',
    main=spark_code_path,
    cluster_name=cluster_name,
    job_name='football_dataset_cleaner',
    region=region,
    arguments=['gs://int_football_bucket/data_lake/football/results.csv',
               'gs://int_football_bucket/staging/football/results.parquet'],
    gcp_conn_id=gcp_conn,
    dag=dag
)

end_int_football_pipeline = DummyOperator(task_id="EndPipeline", dag=dag)

start_int_football_pipeline >> [create_dataproc_cluster, international_football_dataset_to_datalake,
                                upload_cleaning_spark_job_to_gcs]
[create_dataproc_cluster, international_football_dataset_to_datalake,
 upload_cleaning_spark_job_to_gcs] >> submit_cleaning_spark_job
submit_cleaning_spark_job >> end_int_football_pipeline
