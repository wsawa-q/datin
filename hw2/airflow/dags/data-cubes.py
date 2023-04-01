from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import os

import population as p
import care_providers as cp

# The actual tasks defined here will run in a different context from the
# context of this script. Different tasks run on different workers at
# different points in time, which means that this script cannot be used to
# cross communicate between tasks. Note that for this purpose we have a more
# advanced feature called XComs.

root = './airflow/'
output_path = root + "output/"
input_path = root + "input/"

if not os.path.exists(output_path):
    os.makedirs(output_path)

def calculate_population():
    hashmap = p.county_codelist_create(input_path)
    data_as_csv = p.load_csv_file_as_object(input_path + "population.csv")
    data_cube = p.as_data_cube(data_as_csv, hashmap)
    f = open(output_path + "population.ttl", "w")
    f.write(data_cube.serialize(format="ttl"))
    f.close()
    
def calculate_health_care():
    data_as_csv = cp.load_csv_file_as_object(input_path + "care_providers.csv")
    for data in data_as_csv:
        data['measure'] = cp.hashmap[(data['Obec'], data['DruhPece'])]
    data_cube = cp.as_data_cube(data_as_csv)
    f = open(output_path + "health_care.ttl", "w")
    f.write(data_cube.serialize(format="ttl"))
    f.close()
 
dag_args = {
    "email": ["test@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    'retry_delay': timedelta(minutes=15)
}

with DAG(
    dag_id="data-cubes",
    default_args=dag_args,
    start_date=datetime(2023, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=["NDBI046"],    
) as dag:
        
    task01 = PythonOperator(
        task_id="population",
        python_callable=calculate_population
    )
    
    task02 = PythonOperator(
        task_id="care_providers",
        python_callable=calculate_health_care
    )

    task01, task02
