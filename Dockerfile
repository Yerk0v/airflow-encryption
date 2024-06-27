FROM apache/airflow:2.7.0

USER root

COPY requirements.txt .
COPY dags /opt/airflow/dags
COPY keys /opt/airflow/dags/keys

RUN pip install --no-cache-dir -r requirements.txt

USER airflow

# Copiar los DAGs al directorio de Airflow
COPY dags /opt/airflow/dags

# Copiar el archivo de credenciales JSON
COPY keys /opt/airflow/dags/keys