# Proyecto ETL para encriptar, descencriptar datos sensibles de ciclistas.

### Este proyecto utiliza Docker, Airflow y servicios de Google Cloud Platform. En resumen, encripta los datos, los carga a un bucket de GCS y elimina el archivo que no está encriptado. Luego, manipula este archivo transformandolo (en este caso, se eliminan nulos) y se encriptan solamente las columnas sensibles (name y address). Finalmente, los carga a un DataWarehouse ubicado en BigQuery. 

