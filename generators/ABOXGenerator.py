# SDM Project 2. Knowledge Graphs
# ABOX generator
from pandas import read_csv  # for handling csv and csv contents
from rdflib import Graph, Namespace, Literal, URIRef  # basic RDF handling
import os
import os.path as op


class ABOXGenerator():

    default_ttl_path = op.join(os.getcwd(), 'output', 'ABOX.ttl')

    def __init__(self, baseURL='http://SDM.org/Lab2/', ttl_path=default_ttl_path):

        self.n = Namespace(baseURL)
        self.g = Graph()
        self.g.bind('', self.n)

        print('Generating ABOX...')

        # Node assertion
        cwd = os.getcwd()
        data_path = op.join(cwd, 'data')
        nodes_path = op.join(data_path, 'nodes')

        df_affiliation = self.load_clean_csv(
            op.join(nodes_path, 'Node_affiliation.csv'))
        self.assert_node(df_affiliation
                         , {'affiliation': 'Affiliation'}
                         , {'affiliationHasType': 'Type'
                            , 'affiliationHasName': 'Affiliation'}
                            )

        df_author = self.load_clean_csv(
            op.join(nodes_path, 'Node_author.csv'))
        self.assert_node(df_author
                         , {'author': 'author'}
                         , {'authorHasName': 'author'})

        # Edges generation
        edges_path = op.join(data_path, 'edges')

        df_aff_auth = self.load_clean_csv(
            op.join(edges_path, 'Edge_affiliation_author.csv'))
        
        for _, edge in df_aff_auth.iterrows():
            auth_uri = URIRef(self.n + 'author$' + edge['author'])
            aff_uri = URIRef(self.n + 'affiliation$' + edge['Affiliation'])
            self.g.add((auth_uri, self.n.belongs_to_a, aff_uri))
        ######################################

        # Generate .ttl
        print('Serializing ABOX...')
        self.g.serialize(destination=ttl_path)
        print('ABOX serialized!')

        print('ABOX generated!')
        return None
    
    def load_clean_csv(self, path):
        df = read_csv(path, sep=',', header=0)
        for c in df.columns:
            df[c] = df[c].str.replace(' ', '_')
        return df

    def assert_node(self, df, id, properties):
        for _, node in df.iterrows():
            urn, column = next(iter(id.items()))
            node_uri = URIRef(self.n + urn + '$' + node[column])
            for property, p_column in properties.items():
                property_uri = URIRef(self.n + property)
                self.g.add((node_uri, property_uri, Literal(node[p_column])))
    
    def assert_edge(self, df, ids):
        

if __name__ == '__main__':
    ABOXGenerator()
