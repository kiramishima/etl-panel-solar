from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime as dt, timedelta
from airflow.decorators import task
from extract import get_files_name_by_group
from transform import load_data_to_pandas
from load import load_data_to_mongo

default_args = {
    'owner': 'irc_404',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="etl_dag_v5",
    description="DAG para el proceso ETL",
    start_date=dt(2023, 6, 1, 2), # Lo iniciara el 1ero de Junio a las 2AM
    schedule_interval="@monthly" # Ejecute cada mes
) as dag:

    task1 = PythonOperator(
        task_id="Listar_Archivos_CSV_Agrupados_Planta",
        python_callable= get_files_name_by_group,
        show_return_value_in_logs=True
    )
    
    task2 = PythonOperator(
        task_id="Procesar_Datos",
        python_callable=load_data_to_pandas,
        show_return_value_in_logs=True
    )

    task3 = PythonOperator(
        task_id="Cargar_A_MongoDB",
        python_callable=load_data_to_mongo,
        show_return_value_in_logs=True
    )

    task1 >> task2 >> task3