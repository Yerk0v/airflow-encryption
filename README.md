# ETL Project for Encrypting and Decrypting Sensitive Cyclist Data with Airflow and GCP

![Representación del proyecto](imagen.png)

This project uses Docker, Airflow, and Google Cloud Platform (GCP) services to encrypt and decrypt sensitive cyclist data. Data encryption is crucial for protecting user privacy and complying with data protection regulations.

## Description

The project performs the following tasks:

1. **Encriptación:** Encrypts the original cyclist data to protect their personal information.
2. **Carga a Google Cloud Storage:** Stores the encrypted data in a Google Cloud Storage bucket.
3. **Eliminación de datos no encriptados:** Deletes the original unencrypted files to ensure data security.
4. **Transformación y encriptación selectiva:** Performs data transformations (e.g., removing null values) and then encrypts only the sensitive columns (such as name and address).
5. **Carga a BigQuery:** Loads the transformed and encrypted data to a Data Warehouse in BigQuery for analysis and visualization.

## Tecnologías utilizadas

* **Python:** Main programming language.
* **Docker:** To create an isolated and reproducible runtime environment.
* **Airflow:** To orchestrate and schedule the ETL workflow tasks.
* **Google Cloud Storage:** To store the encrypted data.
* **BigQuery:** To store and analyze the transformed data in a DataWarehouse.

## Instalación y uso

[Video sobre Instrucciones detalladas sobre cómo configurar y ejecutar el proyecto]


