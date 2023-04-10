# System requirements #
The system requirements for running this Python code are:

- Python 3.x installed on the system
- csv and os libraries installed
- rdflib library installed 
- airflow library installed


# Installation requirements #

```
pip install rdflib
pip install csv
pip install airflow
```
Additional installation instruction for Apach AirFlow you can find [here](https://airflow.apache.org/docs/apache-airflow/stable/start.html)


# Folders Description #

## airflow ##

### dags ###

 Contents python scripts: 
 * *data-cubes.py* for creating DAG in Apach AirFlow
 * *care_providers.py* and *population.py* from previous HW (script description you can find in folder <b>hw1</b>)

### input ###

Folder is generated automatically by Python when you run a script if the folder has not already been created. <br>

Contents csv files: 
* *care_providers.csv*
* *country_codelist.csv* 
* *population.csv*

### output ### 

Folder is generated automatically by Python when you run a script if the folder has not already been created. <br>

Contents ttl files:
* *health_care.ttl*
* *population.ttl* 


# File Description #

## data-cubes.py ##

### Run ###
```
python data-cubes.py
```

### Input ###
- have files named *population.py* and *care_providers.py* located in the same folder as the script *data-cubes.py*


### Output ###
* *population.ttl*
* *health_care.ttl*
