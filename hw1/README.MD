# System requirements #
The system requirements for running this Python code are:

- Python 3.x installed on the system
- csv and os libraries installed
- rdflib library installed
- SPARQLWrapper library installed

# Installation requirements #

```
pip install rdflib
pip install csv
pip install SPARQLWrapper
```

# File Description #

## care_providers.py ##

### Run ###
```
python care_providers.py
```

### Input ###
- have a file named <i>care_providers.csv</i> in the same folder as the script <i>care_providers.py</i>


### Output ###
- <i>care_providers.ttl</i>

## population.py ##

### Run ###
```
python population.py
```

### Input ###
- have a file named <i>population.csv</i> in the same folder as the script <i>population.py</i>


### Output ###
- <i>population.ttl</i>

## queries.py ##

### Run ###
```
python queries.py
```

### Input ###
- make queries for files <i>population.ttl</i> and <i>care_providers.ttl</i>