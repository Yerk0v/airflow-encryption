# ETL Project for Encrypting and Decrypting Sensitive Cyclist Data with Airflow and GCP

![Representación del proyecto](imagen.png)

This project uses Docker, Airflow, and Google Cloud Platform (GCP) services to encrypt and decrypt sensitive cyclist data. Data encryption is crucial for protecting user privacy and complying with data protection regulations.

## Description

The project performs the following tasks:

1. **Encryption:** Encrypts the original cyclist data to protect their personal information.
2. **Upload to Google Cloud Storage:** Stores the encrypted data in a Google Cloud Storage bucket.
3. **Deletion of unencrypted data:** Deletes the original unencrypted files to ensure data security.
4. **Transformation and selective encryption:** Performs data transformations (e.g., removing null values) and then encrypts only the sensitive columns (such as name and address).
5. **Load to BigQuery:** Loads the transformed and encrypted data to a Data Warehouse in BigQuery for analysis and visualization.

## Technologies Used

* **Python:** Main programming language.
* **Docker:** To create an isolated and reproducible runtime environment.
* **Airflow:** To orchestrate and schedule the ETL workflow tasks.
* **Google Cloud Storage:** To store the encrypted data.
* **BigQuery:** To store and analyze the transformed data in a DataWarehouse.

## Installation and Use

[Video with detailed instructions on how to set up and run the project]


