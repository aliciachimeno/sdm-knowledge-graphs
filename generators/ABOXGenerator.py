# SDM Project 2. Knowledge Graphs
# ABOX generator
from pandas import read_csv, DataFrame  # for handling csv and csv contents
from rdflib import Graph, Namespace, Literal # basic RDF handling
import os
import os.path as op


class ABOXGenerator():

    default_ttl_path = op.join(os.getcwd(), 'output', 'ABOX.ttl')

    def __init__(self, baseURL='http://SDM.org/Lab2/', ttl_path=default_ttl_path):

        print('Generating ABOX...')

        self.n = Namespace(baseURL)
        self.g = Graph()
        self.g.bind('', self.n)

        cwd = os.getcwd()
        data_path = op.join(cwd, 'data')
        nodes_path = op.join(data_path, 'nodes')
        edges_path = op.join(data_path, 'edges')

        # Assert Nodes
        print('Asserting nodes...')

        df_review = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_author_reviews.csv'))
        # Reviewers are authors who have written at least one review
        reviewers = df_review['author'].drop_duplicates()
        self.assert_nodes(DataFrame(reviewers), {'reviewer': 'author'}, {
            'name_author': 'author'})

        df_author = self.load_clean_csv(
            op.join(nodes_path, 'Node_author.csv'))
        df_author = df_author[~df_author.isin(
            # Reviewer is a subclass of Author, so we skip them
            reviewers.to_list())].drop_duplicates()
        self.assert_nodes(df_author, {'author': 'author'}, {
            'name_author': 'author'})

        df_paper = self.load_clean_csv(
            op.join(nodes_path, 'Node_paper.csv'))
        self.assert_nodes(df_paper, {'paper': 'paper_title'}, {
                          'pages': 'pages', 'DOI': 'doi', 'abstract': 'abstract', 'name_paper': 'paper_title'})

        # Reviews
        df = df_review
        properties = {'approves': 'approves', 'content': 'content'}
        for _, node in df.iterrows():
            node_uri = self.n.term('review' + '_' +
                              str(node['id_paper']) + str(node['author']))
            for property, p_column in properties.items():
                property_uri = self.n.term(property)
                self.g.add((node_uri, property_uri, Literal(node[p_column])))

        df_affiliation = self.load_clean_csv(
            op.join(nodes_path, 'Node_affiliation.csv'))
        self.assert_nodes(df_affiliation, {'affiliation': 'Affiliation'}, {
                          'type': 'Type', 'name_affiliation': 'Affiliation'})

        df_keyword = self.load_clean_csv(
            op.join(nodes_path, 'Node_keywords.csv'))
        self.assert_nodes(df_keyword, {'keyword': 'Node_keywords'}, {
                          'name_keyword': 'Node_keywords'})

        df_journal = self.load_clean_csv(
            op.join(nodes_path, 'Node_journals.csv'))
        self.assert_nodes(df_journal, {'journal': 'x'}, {'name_venue': 'x'})

        df_conference = self.load_clean_csv(
            op.join(nodes_path, 'Node_conference.csv'))
        self.assert_nodes(df_conference, {'conference': 'conference'}, {
                          'name_venue': 'conference'})

        df_volume = self.load_clean_csv(
            op.join(nodes_path, 'Node_volumes.csv'))
        self.assert_nodes(df_volume, {'volume': 'volume'}, {
                          'name_compilation': 'volume', 'year': 'year'})

        df_edition = self.load_clean_csv(
            op.join(nodes_path, 'Node_edition.csv'))
        self.assert_nodes(df_edition, {'edition': 'edition'}, {
                          'name_compilation': 'edition', 'year': 'year', 'location': 'location'})
        print('Nodes asserted!')

        # Assert Properties
        print('Asserting properties...')
        df_paper_auth = self.load_clean_csv(
            op.join(edges_path, 'Edge_papers_author.csv'))
        df_paper_auth = df_paper_auth.merge(df_paper.loc[:, ['id_paper', 'paper_title']],
                                            how='left', on='id_paper')

        # writes
        df_writes = df_paper_auth[~df_paper_auth['main_author']]
        df_writes = df_writes.loc[:, ['paper_title', 'author']]
        self.assert_properties(
            df_writes, {'author': 'author', 'paper': 'paper_title'}, 'writes')

        # is_corresponding_author
        df_corresponding_author = df_paper_auth[df_paper_auth['main_author']]
        df_corresponding_author = df_corresponding_author.loc[:, [
            'paper_title', 'author']]
        self.assert_properties(
            df_corresponding_author, {'author': 'author', 'paper': 'paper_title'}, 'is_corresponding_author')

        #  writes_r
        df_writes_r = df_review.loc[:, ['id_paper', 'author']]
        for _, edge in df_writes_r.iterrows():
            subject_uri = self.n.term('author' +
                                 '_' + str(edge['author']))
            object_uri = self.n.term('review' +
                                '_' + str(edge['id_paper']) + str(edge['author']))
            property_uri = self.n.writes_r
            self.g.add((subject_uri, property_uri, object_uri))

        # about
        df_about = df_writes_r.merge(
            df_paper.loc[:, ['id_paper', 'paper_title']], how='left', on='id_paper')
        for _, edge in df_about.iterrows():
            subject_uri = self.n.term('review' +
                                 '_' + str(edge['id_paper']) + str(edge['author']))
            object_uri = self.n.term('paper' +
                                '_' + edge['paper_title'])
            property_uri = self.n.about
            self.g.add((subject_uri, property_uri, object_uri))

        # belongs_to_a
        df_aff_auth = self.load_clean_csv(
            op.join(edges_path, 'Edge_affiliation_author.csv'))
        self.assert_properties(
            df_aff_auth, {'author': 'author', 'affiliation': 'Affiliation'}, 'belongs_to_a')

        # relates_to
        df_paper_keyw = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_keywords.csv'))
        df_paper_keyw = df_paper_keyw.merge(
            df_paper.loc[:, ['id_paper', 'paper_title']], how='left', on='id_paper')
        df_paper_keyw = df_paper_keyw.loc[:, ['paper_title', 'keywords']]
        self.assert_properties(
            df_paper_keyw, {'paper': 'paper_title', 'keyword': 'keywords'}, 'relates_to')

        # cites
        df_cites = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_paper.csv'))
        df_cites = df_cites.merge(
            df_paper.loc[:, ['id_paper', 'paper_title']], how='left', on='id_paper')
        df_cites = df_cites.loc[:, ['paper_title', 'cites_value']].rename(
            columns={'paper_title': 'paper_subject'})
        df_cites = df_cites.merge(df_paper.loc[:, [
                                  'id_paper', 'paper_title']], how='left', left_on='cites_value', right_on='id_paper')
        df_cites = df_cites.loc[:, ['paper_subject', 'paper_title']].rename(
            columns={'paper_title': 'paper_object'})
        for _, edge in df_cites.iterrows():
            subject_uri = self.n.term('paper' +
                                 '_' + str(edge['paper_subject']))
            object_uri = self.n.term('paper' +
                                '_' + str(edge['paper_object']))
            property_uri = self.n.cites
            self.g.add((subject_uri, property_uri, object_uri))

        # published_in_v
        df_paper_vol = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_volumes.csv'))
        df_paper_vol = df_paper_vol.merge(
            df_paper.loc[:, ['id_paper', 'paper_title']], how='left', on='id_paper')
        df_published_in_v = df_paper_vol.loc[:, ['paper_title', 'id_volume']]
        self.assert_properties(df_published_in_v, {
                               'paper': 'paper_title', 'volume': 'id_volume'}, 'published_in_v')

        # published_in_e
        df_paper_ed = self.load_clean_csv(
            op.join(edges_path, 'Edge_papers_edition.csv'))
        df_paper_ed = df_paper_ed.merge(df_paper.loc[:, [
                                        'id_paper', 'paper_title']], how='left', on='id_paper')
        df_paper_ed = df_paper_ed.merge(
            df_edition.loc[:, ['ref_edition', 'edition']], how='left', on='ref_edition')
        df_published_in_e = df_paper_ed.loc[:, ['paper_title', 'edition']]
        self.assert_properties(df_published_in_e, {
                               'paper': 'paper_title', 'edition': 'edition'}, 'published_in_e')

        # belongs_to_j
        df_vol_journal = self.load_clean_csv(
            op.join(edges_path, 'Edge_volumes_journal.csv'))
        self.assert_properties(
            df_vol_journal, {'volume': 'id_volume', 'journal': 'journal'}, 'belongs_to_j')

        # belongs_to_c
        df_ed_conf = self.load_clean_csv(
            op.join(edges_path, 'Edge_edition_conference.csv'))
        df_ed_conf = df_ed_conf.merge(
            df_edition.loc[:, ['ref_edition', 'edition']], how='left', on='ref_edition')
        df_belongs_to_c = df_ed_conf.loc[:, ['edition', 'conference']]
        self.assert_properties(df_belongs_to_c, {
                               'edition': 'edition', 'conference': 'conference'}, 'belongs_to_c')

        print('Properties asserted!')

        # Generate .ttl
        print('Serializing ABOX...')
        self.g.serialize(destination=ttl_path)
        print('ABOX serialized!')

        print('ABOX generated!')
        return None

    def load_clean_csv(self, path):
        df = read_csv(path, sep=',', header=0)
        bad_characters = [' ', '"', '!', '.', ':', '\'', ',', '?',
                          '@', '|', '/', '+', '&', '[', ']', '*', '$', '=', 'Ã', '¨', '¢', 'µ']
        for c in df.select_dtypes(include=['object']).columns:
            # Could be improved to only affect id columns that appear in URNs
            for char in bad_characters:
                df[c] = df[c].str.replace(char, '_')
            # look for more...
        return df

    def assert_nodes(self, df, id, properties):
        urn = next(iter(id))
        for _, node in df.iterrows():
            node_uri = self.n.term(str(urn) + '_' + str(node[id[urn]]))
            for property, p_column in properties.items():
                property_uri = self.n.term(property)
                self.g.add((node_uri, property_uri, Literal(node[p_column])))

    def assert_properties(self, df, ids, property):
        ids_iterator = iter(ids)
        subj_urn = next(ids_iterator)
        obj_urn = next(ids_iterator)
        for _, edge in df.iterrows():
            subject_uri = self.n.term(str(subj_urn) +
                                      '_' + str(edge[ids[subj_urn]]))
            object_uri = self.n.term(str(obj_urn) +
                                     '_' + str(edge[ids[obj_urn]]))
            property_uri = self.n.term(property)
            self.g.add((subject_uri, property_uri, object_uri))


if __name__ == '__main__':
    ABOXGenerator()
