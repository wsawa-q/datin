import csv
import os
from datetime import datetime

from rdflib import Graph, BNode, Literal, Namespace, URIRef
from rdflib.namespace import QB, RDF, XSD, SKOS, DCTERMS, FOAF

NS = Namespace("https://sawa.github.io/ontology#")
NSR = Namespace("https://sawa.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SDMX_DIMENSION = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")

absolute_path = os.path.dirname(__file__)

def main():
    hashmap = county_codelist_create()
    data_as_csv = load_csv_file_as_object(absolute_path + "/population.csv")
    data_cube = as_data_cube(data_as_csv, hashmap)
    f = open(absolute_path + "/population.ttl", "w")
    f.write(data_cube.serialize(format="ttl"))
    f.close()


def county_codelist_create():
    result = {}
    with open(absolute_path + "/county_codelist.csv", "r") as stream:
        reader = csv.reader(stream)
        next(reader)
        for line in reader:
            result[line[8]] = line[4]
    return result


def load_csv_file_as_object(file_path: str):
    result = []
    with open(file_path, "r") as stream:
        reader = csv.reader(stream)
        header = next(reader)  # Skip header
        for line in reader:
            if line[2] == 'DEM0004' and line[5] == '101':
                result.append({key: value for key, value in zip(header, line)})
    return result


def as_data_cube(data, hashmap):
    result = Graph()
    dimensions = create_dimensions(result)
    measures = create_measure(result)
    structure = create_structure(result, dimensions, measures)
    dataset = create_dataset(result, structure)
    create_observations(result, dataset, data, hashmap)
    return result


def create_dimensions(collector: Graph):

    county = NS.county
    collector.add((county, RDF.type, RDFS.Property))
    collector.add((county, RDF.type, QB.DimensionProperty))
    collector.add((county, RDFS.label, Literal("Okres", lang="cs")))
    collector.add((county, RDFS.label, Literal("County", lang="en")))
    collector.add((county, RDFS.range, XSD.string))
    collector.add((county, RDFS.subPropertyOf, SDMX_DIMENSION.refArea))
    collector.add((county, QB.concept, SDMX_DIMENSION.refArea))

    region = NS.region
    collector.add((region, RDF.type, RDFS.Property))
    collector.add((region, RDF.type, QB.DimensionProperty))
    collector.add((region, RDFS.label, Literal("Kraj", lang="cs")))
    collector.add((region, RDFS.label, Literal("Region", lang="en")))
    collector.add((region, RDFS.range, XSD.string))
    collector.add((region, RDFS.subPropertyOf, SDMX_DIMENSION.refArea))
    collector.add((region, QB.concept, SDMX_DIMENSION.refArea))

    collector.add((county, SKOS.prefLabel, Literal("Okres", lang="cs")))
    collector.add((county, SKOS.prefLabel, Literal("County", lang="en")))

    collector.add((region, SKOS.prefLabel, Literal("Kraj", lang="cs")))
    collector.add((region, SKOS.prefLabel, Literal("Region", lang="en")))

    return [county, region]


def create_measure(collector: Graph):

    measure = NS.measure
    collector.add((measure, RDF.type, RDFS.Property))
    collector.add((measure, RDF.type, QB.MeasureProperty))
    collector.add((measure, RDFS.label, Literal("StredniHodnota", lang="cs")))
    collector.add((measure, RDFS.label, Literal("Mean", lang="en")))
    collector.add((measure, RDFS.range, XSD.integer))
    collector.add((measure, RDFS.subPropertyOf, SDMX_MEASURE.obsValue))
    collector.add((measure, QB.concept, SDMX_MEASURE.obsValue))

    return [measure]


def create_structure(collector: Graph, dimensions, measures):

    structure = NS.structure
    collector.add((structure, RDF.type, QB.structure))

    for dimension in dimensions:
        component = BNode()
        collector.add((structure, QB.component, component))
        collector.add((component, QB.dimension, dimension))

    for measure in measures:
        component = BNode()
        collector.add((structure, QB.component, component))
        collector.add((component, QB.measure, measure))

    return structure


def create_dataset(collector: Graph, structure):

    dataset = NSR.dataCubeInstance
    collector.add((dataset, RDF.type, QB.DataSet))
    collector.add((dataset, RDFS.label, Literal(
        "Population 2021", lang="en")))
    collector.add((dataset, QB.structure, structure))

    publisher = NSR["publisher"]
    collector.add((publisher, RDF.type, FOAF.Organization))
    collector.add((publisher, FOAF.name, Literal("Publisher", lang="en")))
    collector.add((publisher, FOAF.name, Literal("Vydavatel", lang="cs")))
    collector.add((dataset, DCTERMS.publisher, publisher))

    license = URIRef("https://example.com/license")
    collector.add((dataset, DCTERMS.license, license))

    issued = Literal(datetime.now().strftime("%Y-%m-%d"), datatype=XSD.date)
    collector.add((dataset, DCTERMS.issued, issued))

    modified = Literal(datetime.now().strftime("%Y-%m-%d"), datatype=XSD.date)
    collector.add((dataset, DCTERMS.modified, modified))

    return dataset


def create_observations(collector: Graph, dataset, data, hashmap):
    for index, row in enumerate(data):
        resource = NSR["observation-" + str(index).zfill(3)]
        create_observation(collector, dataset, resource, row, hashmap)


def create_observation(collector: Graph, dataset, resource, data, hashmap):
    county_uri = NSR[data["vuzemi_kod"]]
    region_uri = NSR[hashmap[data["vuzemi_kod"]]]

    if (county_uri, RDF.type, SKOS.Concept) not in collector:
        collector.add((county_uri, RDF.type, SKOS.Concept))
        collector.add((county_uri, SKOS.prefLabel, Literal(
            data["vuzemi_txt"], lang="cs")))
        collector.add((county_uri, SKOS.prefLabel, Literal(
            data["vuzemi_txt"], lang="en")))
    
    if (region_uri, RDF.type, SKOS.Concept) not in collector:
        collector.add((region_uri, RDF.type, SKOS.Concept))
        collector.add((region_uri, SKOS.prefLabel, Literal(
            hashmap[data["vuzemi_kod"]], lang="cs")))
        collector.add((region_uri, SKOS.prefLabel, Literal(
            hashmap[data["vuzemi_kod"]], lang="en")))

    collector.add((resource, RDF.type, QB.Observation))
    collector.add((resource, QB.dataSet, dataset))
    if data['vuzemi_cis'] == "101":
        collector.add((resource, NS.county, county_uri))
        collector.add((resource, NS.region, region_uri))
    collector.add((resource, NS.measure, Literal(
        data["hodnota"], datatype=XSD.integer)))


if __name__ == "__main__":
    main()