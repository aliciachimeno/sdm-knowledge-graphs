# SDM Project 2. Knowledge Graphs
# TBOX generator
from rdflib import Graph, Literal, Namespace
from rdflib.namespace import RDF, RDFS

NAMESPACE = "https://example.org/SDM/Lab2"

n = Namespace(NAMESPACE)
g = Graph()

def create_property(property, domain, range):
    g.add((property, RDF.type, RDF.Property))
    g.add((property, RDFS.domain, domain))
    g.add((property, RDFS.range, range))

def create_subproperty(subproperty, property, domain, range):
    g.add((subproperty, RDFS.subPropertyOf, property))
    g.add((subproperty, RDFS.domain, domain))
    g.add((subproperty, RDFS.range, range))


# Classes
author = n.author; reviewer = n.reviewer

paper = n.paper
review = n.review
affiliation = n.affiliation
keyword = n.keyword
venue = n.venue; journal = n.journal; conference = n.conference
compilation = n.compilation; volume = n.volume; edition = n.edition

# Properties
writes = n.writes; is_corresponding_author = n.is_corresponding_author
writes_r = n.writes_r
about = n.about
belongs_to_a = n.belongs_to_a
relates_to = n.relates_to
cites = n.cites
published_in_c = n.published_in_c; published_in_v = n.published_in_v; published_in_e = n.published_in_e
belongs_to_v = n.belongs_to_v; belongs_to_c = n.belongs_to_c; belongs_to_j = n.belongs_to_j


# Triples
g.add((reviewer, RDFS.subClassOf, author))
g.add((journal, RDFS.subClassOf, venue))
g.add((conference, RDFS.subClassOf, venue))
g.add((volume, RDFS.subClassOf, compilation))
g.add((edition, RDFS.subClassOf, compilation))

create_property(writes, author, paper)
create_subproperty(is_corresponding_author, writes, author, paper)

create_property(writes_r, reviewer, review)

create_property(about, review, paper)

create_property(belongs_to_a, author, affiliation)

create_property(relates_to, paper, review)

create_property(cites, paper, paper)

create_property(published_in_c, paper, compilation)
create_subproperty(published_in_v, published_in_c, paper, volume)
create_subproperty(published_in_e, published_in_c, paper, edition)

create_property(belongs_to_v, compilation, venue)
create_subproperty(belongs_to_j, belongs_to_v, volume, journal)
create_subproperty(belongs_to_c, belongs_to_v, edition, conference)


g.serialize(destination="TBOX.ttl")