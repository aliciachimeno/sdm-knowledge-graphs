# SDM Project 2. Knowledge Graphs
# TBOX generator
from rdflib import Graph, Namespace
from rdflib.namespace import RDF, RDFS, XSD
import os
import os.path as op


class TBOXGenerator():

    default_ttl_path = op.join(os.getcwd(), 'output', 'TBOX.ttl')

    def __init__(self, baseURL='http://SDM.org/Lab2/', ttl_path=default_ttl_path):
        n = Namespace(baseURL)
        self.g = Graph()
        self.g.bind('', n)

        print('Generating TBOX...')

        # Classes
        print('Asserting DatatypeProperties and subclasses...')
        author = n.author
        reviewer = n.reviewer
        self.create_property(n.name_author, author, XSD.string)
        self.g.add((reviewer, RDFS.subClassOf, author))

        paper = n.paper
        self.create_property(n.pages, paper, XSD.integer)
        self.create_property(n.DOI, paper, XSD.string)
        self.create_property(n.abstract, paper, XSD.string)
        self.create_property(n.name_paper, paper, XSD.string)

        review = n.review
        self.create_property(n.approves, paper, XSD.string)
        self.create_property(n.content, paper, XSD.string)

        affiliation = n.affiliation
        self.create_property(n.name_affiliation, affiliation, XSD.string)
        self.create_property(n.type, affiliation, XSD.string)

        keyword = n.keyword
        self.create_property(n.name_keyword, keyword, XSD.string)

        venue = n.venue
        self.create_property(n.name_venue, venue, XSD.string)
        journal = n.journal
        self.g.add((journal, RDFS.subClassOf, venue))
        conference = n.conference
        self.g.add((conference, RDFS.subClassOf, venue))

        compilation = n.compilation
        self.create_property(n.name_compilation, compilation, XSD.string)
        self.create_property(n.year, compilation, XSD.integer)
        volume = n.volume
        self.g.add((volume, RDFS.subClassOf, compilation))
        edition = n.edition
        self.create_property(n.location, edition, XSD.string)
        self.g.add((edition, RDFS.subClassOf, compilation))
        print('DatatypeProperties and subclasses asserted!')

        # Object Properties
        print('Asserting ObjectProperties...')
        self.create_property(n.writes, author, paper)
        self.create_subproperty(
            n.is_corresponding_author, n.writes, author, paper)

        self.create_property(n.writes_r, reviewer, review)

        self.create_property(n.about, review, paper)

        self.create_property(n.belongs_to_a, author, affiliation)

        self.create_property(n.relates_to, paper, review)

        self.create_property(n.cites, paper, paper)

        self.create_property(n.published_in_c, paper, compilation)
        self.create_subproperty(
            n.published_in_v, n.published_in_c, paper, volume)
        self.create_subproperty(
            n.published_in_e, n.published_in_c, paper, edition)

        self.create_property(n.belongs_to_v, compilation, venue)
        self.create_subproperty(
            n.belongs_to_j, n.belongs_to_v, volume, journal)
        self.create_subproperty(
            n.belongs_to_c, n.belongs_to_v, edition, conference)
        print('ObjectProperties asserted!')

        # Generate .ttl
        print('Serializing TBOX...')
        self.g.serialize(destination=ttl_path)
        print('TBOX serialized!')

        print('TBOX generated!')
        return None

    def create_property(self, property, domain, range):
        self.g.add((property, RDF.type, RDF.Property))
        self.g.add((property, RDFS.domain, domain))
        self.g.add((property, RDFS.range, range))

    def create_subproperty(self, subproperty, property, domain, range):
        self.g.add((subproperty, RDFS.subPropertyOf, property))
        self.g.add((subproperty, RDFS.domain, domain))
        self.g.add((subproperty, RDFS.range, range))


if __name__ == '__main__':
    TBOXGenerator()
