import csv
import wget

from rdflib import Graph, BNode, Literal, Namespace
from rdflib.namespace import QB, RDF, XSD

NS = Namespace("https://sawa.github.io/ontology#")
NSR = Namespace("https://sawa.github.io/resources/")
RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
SDMX_DIMENSION = Namespace("http://purl.org/linked-data/sdmx/2009/dimension#")
SDMX_MEASURE = Namespace("http://purl.org/linked-data/sdmx/2009/measure#")

hashmap = {}

def load_csv_file_as_object(file_path: str):
    result = []
    with open(file_path, "r", errors='ignore') as stream:
        reader = csv.reader(stream)
        header = next(reader)  # Skip header
        for line in reader:
            if (line[5], line[29]) in hashmap.keys():
                hashmap[(line[5], line[29])] += 1
            else:
                hashmap[(line[5], line[29])] = 1

            result.append({key: value for key, value in zip(header, line)})
    return result


def as_data_cube(data):
    result = Graph()
    dimensions = create_dimensions(result)
    measures = create_measure(result)
    structure = create_structure(result, dimensions, measures)
    dataset = create_dataset(result, structure)
    create_observations(result, dataset, data)
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
    collector.add((region, RDFS.label, Literal("Reference Area", lang="en")))
    collector.add((region, RDFS.subPropertyOf, SDMX_DIMENSION.refArea))
    collector.add((region, QB.concept, SDMX_DIMENSION.refArea))

    collector.add((region, RDFS.range, XSD.string))

    field_of_care = NS.field_of_care
    collector.add((field_of_care, RDF.type, RDFS.Property))
    collector.add((field_of_care, RDF.type, QB.DimensionProperty))
    collector.add((field_of_care, RDFS.label, Literal("OborPece", lang="cs")))
    collector.add((field_of_care, RDFS.label,
                  Literal("FieldOfCare", lang="en")))
    collector.add((field_of_care, RDFS.range, XSD.string))
    collector.add((field_of_care, RDFS.subPropertyOf,
                  SDMX_DIMENSION.occupation))
    collector.add((field_of_care, QB.concept, SDMX_DIMENSION.occupation))

    return [county, region, field_of_care]


def create_measure(collector: Graph):

    measure = NS.measure
    collector.add((measure, RDF.type, RDFS.Property))
    collector.add((measure, RDF.type, QB.MeasureProperty))
    collector.add((measure, RDFS.label, Literal("Pocet", lang="cs")))
    collector.add((measure, RDFS.label, Literal("Measure", lang="en")))
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
        "Care Providers", lang="en")))
    collector.add((dataset, QB.structure, structure))

    return dataset


def create_observations(collector: Graph, dataset, data):
    for index, row in enumerate(data):
        resource = NSR["observation-" + str(index).zfill(3)]
        create_observation(collector, dataset, resource, row)


def create_observation(collector: Graph, dataset, resource, data):
    collector.add((resource, RDF.type, QB.Observation))
    collector.add((resource, QB.dataSet, dataset))
    collector.add((resource, NS.county, Literal(data["Okres"], lang="cs")))
    collector.add((resource, NS.region, Literal(data["Kraj"], lang="cs")))
    collector.add((resource, NS.field_of_care, Literal(
        data["NazevZarizeni"], lang="cs")))
    collector.add((resource, NS.measure, Literal(
        data["measure"], datatype=XSD.integer)))
