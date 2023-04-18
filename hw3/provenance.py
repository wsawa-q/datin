from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, PROV, XSD
import os

prov = Namespace("http://www.w3.org/ns/prov#")
g = Graph()

ds1 = URIRef("https://data.gov.cz/zdroj/datové-sady/00024341/aa4c99d9f1480cca59807389cf88d4dc")
ds2 = URIRef("https://data.gov.cz/zdroj/datové-sady/00025593/12032e1445fd74fa08da79b14137fc29")
person = URIRef("https://sawa.github.io/person")
software = URIRef("https://sawa.github.io/software")
usage = URIRef("https://sawa.github.io/usage")
association = URIRef("https://sawa.github.io/association")
custom_role = Namespace("https://sawa.github.io/roles/")

g.add((ds1, RDF.type, PROV.Entity))
g.add((ds2, RDF.type, PROV.Entity))

activity = URIRef("https://sawa.github.io/activity")
g.add((activity, RDF.type, PROV.Activity))
g.add((activity, PROV.startedAtTime, Literal("2023-04-17T00:00:00", datatype=XSD.dateTime)))

g.add((person, RDF.type, PROV.Person))
g.add((software, RDF.type, PROV.SoftwareAgent))

g.add((activity, PROV.qualifiedUsage, usage))
g.add((activity, PROV.qualifiedAssociation, association))
g.add((usage, RDF.type, PROV.Usage))
g.add((association, RDF.type, PROV.Association))
g.add((usage, PROV.entity, ds1))
g.add((usage, PROV.entity, ds2))
g.add((association, PROV.agent, person))
g.add((association, PROV.agent, software))

g.add((usage, custom_role.role1, Literal("data_analyst")))
g.add((association, custom_role.role2, Literal("software_tool")))

output_dir = os.path.dirname(os.path.abspath(__file__))
output_file = os.path.join(output_dir, "provenance.trig")
g.serialize(output_file, format="trig")
