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
        self.create_property(n.authorHasName, author, XSD.string)
        self.g.add((reviewer, RDFS.subClassOf, author))
        self.create_property(n.reviewerHasName, reviewer, XSD.string)

        paper = n.paper

        review = n.review

        affiliation = n.affiliation
        self.create_property(n.affiliationHasType, affiliation, XSD.string)
        self.create_property(n.affiliationHasName, affiliation, XSD.string)

        keyword = n.keyword

        venue = n.venue
        journal = n.journal
        conference = n.conference
        self.create_property(n.conferenceHasName, conference, XSD.string)
        self.g.add((journal, RDFS.subClassOf, venue))
        self.g.add((conference, RDFS.subClassOf, venue))

        compilation = n.compilation
        volume = n.volume
        edition = n.edition
        self.g.add((volume, RDFS.subClassOf, compilation))
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
