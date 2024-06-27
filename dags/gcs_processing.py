from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import csv

# Función para leer, transformar y escribir datos en GCS
def etl_process(bucket_name, source_blob, dest_blob, **kwargs):
    # Especifica la ruta completa al archivo de clave de servicio JSON
    key_path = "/opt/airflow/dags/keys/credentials.json"

    # Crea un GCSHook con las credenciales del archivo de clave de servicio
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default', key_path=key_path)

    # Leer datos desde GCS
    source_data = gcs_hook.download(bucket_name, source_blob)
    source_data_str = source_data.decode('utf-8')
    
    # Ejemplo simple de transformación: convertir texto a mayúsculas
    transformed_data_str = source_data_str.upper()
    transformed_data = transformed_data_str.encode('utf-8')
    
    # Escribir datos transformados de nuevo a GCS
    gcs_hook.upload(bucket_name, dest_blob, transformed_data)

    return transformed_data_str

# Función para imprimir las dos primeras columnas en el log
def print_first_two_columns(transformed_data_str, **kwargs):
    # Convertir los datos transformados de texto a una lista de filas
    csv_reader = csv.reader(transformed_data_str.splitlines())
    rows = list(csv_reader)

    # Imprimir las dos primeras columnas de cada fila
    for row in rows:
        if len(row) >= 2:
            print(f"First column: {row[0]}, Second column: {row[1]}")
        else:
            print("Row does not have enough columns")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gcs_processing',
    default_args=default_args,
    description='Un ETL directo con Airflow y GCS sin almacenamiento local',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Tarea para ejecutar el proceso ETL
    etl_task = PythonOperator(
        task_id='etl_process',
        python_callable=etl_process,
        op_kwargs={
            'bucket_name': 'golden-cheetah-datasets',
            'source_blob': 'datasets/athletes.csv',
            'dest_blob': 'transformed/athletes_uppercase.csv',
        },
    )

    # Nueva tarea para imprimir las dos primeras columnas
    print_columns_task = PythonOperator(
        task_id='print_first_two_columns',
        python_callable=print_first_two_columns,
        op_args=[ "{{ task_instance.xcom_pull(task_ids='etl_process') }}" ],
    )

    # Definir la secuencia de las tareas
    etl_task >> print_columns_task
