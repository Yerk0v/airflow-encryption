from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import kms
import base64
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'encryption_dag',
    default_args=default_args,
    description='DAG for encrypting and uploading a CSV file to GCS',
    schedule_interval=None,
)

# Configuración de KMS
project_id = 'boreal-forest-427103-q9'
location_id = 'global'
key_ring_id = 'athletes-key'
crypto_key_id = 'python-key'
key_name = f'projects/{project_id}/locations/{location_id}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}'

# Especifica la ruta completa al archivo de clave de servicio JSON
key_path = "/opt/airflow/dags/keys/credentials.json"

# Inicializar cliente de KMS
kms_client = kms.KeyManagementServiceClient.from_service_account_json(key_path)

def encrypt_data(data, key_name):
    response = kms_client.encrypt(request={'name': key_name, 'plaintext': data})
    ciphertext = base64.b64encode(response.ciphertext).decode('utf-8')
    logging.info(f"Data encrypted successfully with key: {key_name}")
    return ciphertext

def task_encrypt_and_upload(bucket_name, source_blob, dest_blob, **kwargs):
    try:
        logging.info("Starting encryption process...")
        
        # Crea un GCSHook con las credenciales del archivo de clave de servicio
        gcs_hook = GCSHook(gcp_conn_id='google_cloud_default', key_path=key_path)
        
        # Descargar el archivo CSV desde GCS
        source_data = gcs_hook.download(bucket_name, source_blob)
        logging.info(f"File downloaded from GCS: {source_blob}")
        
        # Encriptar los datos del archivo CSV
        ciphertext = encrypt_data(source_data, key_name)
        logging.info(f"File encrypted successfully")
        
        # Subir el archivo encriptado a GCS utilizando upload_from_string
        gcs_hook.upload(bucket_name, dest_blob, data=ciphertext)
        logging.info(f"File uploaded to GCS: {dest_blob}")
        
        # Eliminar el archivo original no encriptado
        gcs_hook.delete(bucket_name, source_blob)
        logging.info(f"Original file deleted from GCS: {source_blob}")

    except Exception as e:
        logging.error("An error occurred during the encryption and upload process", exc_info=True)
        raise

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

encrypt_upload_task
