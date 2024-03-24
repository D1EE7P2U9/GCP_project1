from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

default_args = {
    'owner': 'Deepak',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 3, 25),
}

dag = DAG(
    'gcs_to_bigquery',
    default_args=default_args,
    description='A DAG to upload files from GCS to BigQuery',
    schedule_interval=timedelta(days=1),  # You can adjust the schedule interval as needed
)


upload_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id='gcs_bq',
    bucket='you_bucket_name',
    source_objects=[path_to_your_csv],  # Specify the file you want to upload
    destination_project_dataset_table='your_bq_table',  # Specify your BigQuery table
    # Define the schema of your table if needed
    schema_fields=[  
        {'name': 'id', 'type': 'STRING'},
        {'name': 'name', 'type': 'STRING'},
        {'name': 'item', 'type': 'STRING'},
    ],
    create_disposition='CREATE_IF_NEEDED',  # Create the table if it doesn't exist
    write_disposition='WRITE_APPEND',  # Append data to the table
    dag=dag,
)


upload_to_bigquery
