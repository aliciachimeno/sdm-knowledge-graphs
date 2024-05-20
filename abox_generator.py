# SDM Project 2. Knowledge Graphs
# ABOX generator
import pandas as pd  # for handling csv and csv contents
from rdflib import Graph, Literal, RDF, URIRef, Namespace  # basic RDF handling
from rdflib.namespace import FOAF, XSD  # most common namespaces
import urllib.parse  # for parsing strings to URI's

pd.read_csv('Edge_paper_volumes.csv')

BASEURL = 'http://SDM.org/Lab2'

g = Graph()