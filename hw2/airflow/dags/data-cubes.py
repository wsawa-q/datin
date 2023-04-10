from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import os
import requests

import population as p
import care_providers as cp

# The actual tasks defined here will run in a different context from the
# context of this script. Different tasks run on different workers at
# different points in time, which means that this script cannot be used to
# cross communicate between tasks. Note that for this purpose we have a more
# advanced feature called XComs.

root = "./"
output_path = os.path.join(root, "output/")
input_path = os.path.join(root, "input/")

if not os.path.exists(output_path):
    os.makedirs(output_path)

if not os.path.exists(input_path):
    os.makedirs(input_path)


def download_csv(url, name):
    response = requests.get(url, verify=False)
    response.raise_for_status()

    with open(os.path.join(input_path, name), "wb") as f:
        f.write(response.content)


def calculate_population():
    population = os.path.join(input_path, "population.csv")
    county_codelist = os.path.join(input_path, "county_codelist.csv")
    hashmap = p.county_codelist_create(county_codelist)
    data_as_csv = p.load_csv_file_as_object(population)
    data_cube = p.as_data_cube(data_as_csv, hashmap)
    f = open(os.path.join(output_path, "population.ttl"), "w")
    f.write(data_cube.serialize(format="ttl"))
    f.close()


def calculate_health_care():
    providers = os.path.join(input_path, "care_providers.csv")
    data_as_csv = cp.load_csv_file_as_object(providers)
    for data in data_as_csv:
        data['measure'] = cp.hashmap[(data['Obec'], data['DruhPece'])]
    data_cube = cp.as_data_cube(data_as_csv)
    f = open(os.path.join(output_path, "health_care.ttl"), "w")
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
        task_id="download_population",
        python_callable=download_csv,
        op_args=[
            "https://www.czso.cz/documents/10180/184344914/130141-22data2021.csv", "population.csv"]
    )

    task02 = PythonOperator(
        task_id="download_county_codelist",
        python_callable=download_csv,
        op_args=["https://skoda.projekty.ms.mff.cuni.cz/ndbi046/seminars/02/%C4%8D%C3%ADseln%C3%ADk-okres%C5%AF-vazba-101-nad%C5%99%C3%ADzen%C3%BD.csv", "county_codelist.csv"]
    )

    task03 = PythonOperator(
        task_id="download_care_providers",
        python_callable=download_csv,
        op_args=["https://data.mzcr.cz/distribuce/63/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv", "care_providers.csv"]
    )

    task04 = PythonOperator(
        task_id="population",
        python_callable=calculate_population
    )

    task05 = PythonOperator(
        task_id="care_providers",
        python_callable=calculate_health_care
    )

    [task01, task02] >> task04, task03 >> task05
