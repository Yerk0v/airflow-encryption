from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from encryption_dag import task_encrypt_and_upload
from decrypt_transform_dag import task_decrypt_transform_and_upload

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define el DAG maestro
dag = DAG(
    'ETL_Final',
    default_args=default_args,
    description='Master DAG to run encryption, upload, decryption, transformation, and upload tasks',
    schedule_interval=None,
)

# Define el operador para encriptar y subir
encrypt_upload_task = PythonOperator(
    task_id='encrypt_and_upload',
    provide_context=True,
    python_callable=task_encrypt_and_upload,
    op_kwargs={
        'bucket_name': 'golden-cheetah-datasets',
        'source_blob': 'datasets/athletes.csv',
        'dest_blob': 'datasets/athletes.csv.enc',
    },
    dag=dag,
)

# Define el operador para desencriptar, transformar y subir
decrypt_transform_upload_task = PythonOperator(
    task_id='decrypt_transform_and_upload',
    provide_context=True,
    python_callable=task_decrypt_transform_and_upload,
    op_kwargs={
        'bucket_name': 'golden-cheetah-datasets',
        'source_blob': 'datasets/athletes.csv.enc',
        'dataset_id': 'final_data',
        'table_id': 'athletes_encrypted',
    },
    dag=dag,
)

# Define la secuencia de ejecución de tareas
encrypt_upload_task >> decrypt_transform_upload_task

