import rdflib.graph as g
import os
from SPARQLWrapper import SPARQLWrapper, JSON

absolute_path = os.path.dirname(__file__)

def get_population_ttl():
    graph = g.Graph()
    graph.parse(absolute_path + '/population.ttl', format='ttl')
    
    return graph


def get_health_care_ttl():
    graph = g.Graph()
    graph.parse(absolute_path + '/care_providers.ttl', format='ttl')
    
    return graph

def queries(ttl: g):
    queries_dir = os.path.join(absolute_path, "SPARQL")
    for file in os.listdir(queries_dir):
        with open(os.path.join(queries_dir, file), "r") as f:
            result = ttl.query(f.read())
            print(file, end=":\t")
            print(bool(result))
            

def main():
    print("Population queries:")
    population = get_population_ttl()
    queries(population)

    print()

    print("Health care queries:")
    health_care = get_health_care_ttl()
    queries(health_care)
    

if __name__ == '__main__':
    main()