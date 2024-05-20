# SDM Project 2. Knowledge Graphs
# ABOX generator
from pandas import read_csv  # for handling csv and csv contents
from rdflib import Graph, Namespace, Literal, URIRef  # basic RDF handling
import os
import os.path as op

class ABOXGenerator():

    default_ttl_path = op.join(os.getcwd(), 'output', 'ABOX.ttl')

    def __init__(self, baseURL='http://SDM.org/Lab2/', ttl_path=default_ttl_path):

        n = Namespace(baseURL)
        g = Graph()
        g.bind('', n)

        print('Generating ABOX...')

        ######## Node generation test ########
        cwd = os.getcwd()
        data_path = op.join(cwd, 'data')
        nodes_path = op.join(data_path, 'nodes')

        df_affiliation = read_csv(
            op.join(nodes_path, 'Node_affiliation.csv')
            , sep=',', header=0)
        
        print(df_affiliation.head())
        print('Columns:', df_affiliation.columns.to_list())

        for index, aff in df_affiliation.iterrows():
            if index >= 10: break
            node_uri  = URIRef(n + ('affiliation_' + str(index)))
            g.add((node_uri, n.affiliationHasType, Literal(aff['Type'])))
            g.add((node_uri, n.affiliationHasName, Literal(aff['Affiliation'])))

        df_author = read_csv(
            op.join(nodes_path, 'Node_author.csv')
            , sep=',', header=0)
        print(df_author.head())
        print('Columns:', df_author.columns.to_list())

        for index, auth in df_author.iterrows():
            if index >= 10: break
            node_uri = URIRef(n + ('author_' + str(index)))
            g.add((node_uri, n.authorHasName, Literal(auth['author'])))
        ######################################


        ######## Edge generation test ########
        edges_path = op.join(data_path, 'edges')

        df_aff_auth = read_csv(
            op.join()
        )
        ######################################
        
        # Generate .ttl
        print('Serializing ABOX...')
        g.serialize(destination=ttl_path)
        print('ABOX serialized!')

        print('ABOX generated!')
        return None

if __name__ == '__main__':
    ABOXGenerator()