from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from datetime import datetime
import boto3
import pandas as pd

# Definir el DAG
dag = DAG(
    'etl_dynamodb_a_s3',
    description='Proceso ETL de DynamoDB a S3',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Función para obtener credenciales de AWS desde Airflow
def get_aws_credentials():
    conn = BaseHook.get_connection("aws_credentials")
    aws_credentials = {
        "access_key": conn.login,
        "secret_access_key": conn.password,
        "session_token": conn.extra_dejson.get("aws_session_token"),
    }
    return aws_credentials

# Tarea para extraer datos de DynamoDB
@task(dag=dag)
def extract_from_dynamodb():
    aws_credentials = get_aws_credentials()
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=aws_credentials["access_key"],
        aws_secret_access_key=aws_credentials["secret_access_key"],
        aws_session_token=aws_credentials["session_token"]
    )
    table_name = 'fabricantes'
    table = dynamodb.Table(table_name)
    
    # Escanear los datos de la tabla
    response = table.scan()
    data = response['Items']

    # Convertir los datos a un DataFrame
    df = pd.DataFrame(data)
    df.to_csv('/tmp/extracted_data.csv', index=False)
    print(df)
    print("Tarea 1 - Extraer datos de DynamoDB")

# Tarea para transformar los datos
@task(dag=dag)
def transform_data():
    df = pd.read_csv('/tmp/extracted_data.csv')
    # Realiza tus transformaciones aquí
    # Ejemplo: Convertir los nombres de las columnas a minúsculas
    df.columns = [col.lower() for col in df.columns]
    # Ordenar los datos por la columna 'id' (si existe)
    if 'id' in df.columns:
        df = df.sort_values(by='id')
    df.to_csv('/tmp/transformed_data.csv', index=False)
    print(df)
    print("Tarea 2 - Transformar datos")

# Tarea para cargar los datos en S3
@task(dag=dag)
def load_to_s3():
    aws_credentials = get_aws_credentials()
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_credentials["access_key"],
        aws_secret_access_key=aws_credentials["secret_access_key"],
        aws_session_token=aws_credentials["session_token"]
    )
    bucket_name = 'stewartmaquera'
    file_name = '/tmp/transformed_data.csv'
    s3.upload_file(file_name, bucket_name, 'transformed_data.csv')
    print("Tarea 3 - Cargar csv a S3")

# Establecer las dependencias entre tareas
extract_task = extract_from_dynamodb()
transform_task = transform_data()
load_task = load_to_s3()

extract_task >> transform_task >> load_task
