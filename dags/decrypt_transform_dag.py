from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from google.cloud import kms, bigquery
from datetime import datetime
import logging
import pandas as pd
import io
import base64

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'decrypt_transform_dag',
    default_args=default_args,
    description='DAG for decrypting, transforming and uploading a CSV file to BigQuery',
    schedule_interval=None,
)

# Configuración de KMS
project_id = 'boreal-forest-427103-q9'
location_id = 'global'
key_ring_id = 'athletes-key'
crypto_key_id = 'python-key'
key_name = f'projects/{project_id}/locations/{location_id}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}'

key_path = "/opt/airflow/dags/keys/credentials.json"

# Inicializar cliente de KMS
kms_client = kms.KeyManagementServiceClient.from_service_account_json(key_path)

def decrypt_data(ciphertext, key_name):
    decoded_ciphertext = base64.b64decode(ciphertext)
    response = kms_client.decrypt(request={'name': key_name, 'ciphertext': decoded_ciphertext})
    plaintext = response.plaintext
    return plaintext


def encrypt_data(data, key_name):
    response = kms_client.encrypt(request={'name': key_name, 'plaintext': data})
    return base64.b64encode(response.ciphertext).decode('utf-8')

def transform_data(data):
    df = pd.read_csv(io.BytesIO(data))
    columnas_a_mantener = ['name', 'age', 'gender', 'activities', 'bike', 'run', 'swim', 'other', 'weightkg', 'adress', '240s_peak_wpk', '420s_peak_wpk', '720s_peak_wpk']
    df = df[columnas_a_mantener]
    df.dropna(inplace=True)
    
    # Encriptar columnas sensibles
    columnas_sensibles = ['name', 'adress']
    for columna in columnas_sensibles:
        df[columna] = df[columna].apply(lambda x: encrypt_data(x.encode(), key_name))
    
    return df


def task_decrypt_transform_and_upload(bucket_name, source_blob, dataset_id, table_id, **kwargs):
    try:
        logging.info("Starting decryption and transformation process...")

        # Crea un GCSHook con las credenciales del archivo de clave de servicio
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default', key_path=key_path)

        # Descargar el archivo encriptado desde GCS
        ciphertext = gcs_hook.download(bucket_name, source_blob)
        logging.info(f"File downloaded from GCS: {source_blob}")

        # Desencriptar los datos del archivo
        plaintext = decrypt_data(ciphertext, key_name)
        
        # Transformar los datos
        transformed_data = transform_data(plaintext)

        # Inicializar cliente de BigQuery
        bq_client = bigquery.Client.from_service_account_json(key_path, project=project_id)

        # Nombre completo de la tabla en BigQuery (proyecto.dataset.tabla)
        table_name = f"{project_id}.{dataset_id}.{table_id}"

        # Cargar datos en BigQuery
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # Opcional: Truncar tabla antes de carga
        job = bq_client.load_table_from_dataframe(
            dataframe=transformed_data,
            destination=table_name,
            job_config=job_config
        )
        job.result()

        logging.info(f"Data uploaded to BigQuery: {table_name}")

    except Exception as e:
        logging.error("An error occurred during the decryption, transformation and upload process", exc_info=True)
        raise


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

decrypt_transform_upload_task
