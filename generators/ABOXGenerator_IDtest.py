# SDM Project 2. Knowledge Graphs
# ABOX generator
from pandas import read_csv, DataFrame, isna, Series  # for handling csv and csv contents
from rdflib import Graph, Namespace, Literal  # basic RDF handling
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

        # After cleaning ids, some can be repeated. We keep track of them to generate unique URNs
        self.repeated_ids = {}
        self.n_digits = {}

        # Assert Nodes
        print('Asserting nodes...')

        df_review = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_author_reviews.csv'), ['id_paper', 'author'])
        # Reviewers are authors who have written at least one review
        reviewers = df_review['urn2'].drop_duplicates()
        self.reviewers = reviewers.to_list()
        self.assert_nodes(DataFrame(reviewers), {'reviewer': 'urn2'}, {
            'name_author': 'author'})

        df_author = self.load_clean_csv(
            op.join(nodes_path, 'Node_author.csv'), 'author')
        df_author = df_author[~df_author.isin(
            # Reviewer is a subclass of Author, so we skip them
            self.reviewers)].drop_duplicates()
        self.assert_nodes(df_author, {'author': 'urn1'}, {
            'name_author': 'author'})

        df_paper = self.load_clean_csv(
            op.join(nodes_path, 'Node_paper.csv'), 'paper_title')
        self.assert_nodes(df_paper, {'paper': 'urn1'}, {
                          'pages': 'pages', 'DOI': 'doi', 'abstract': 'abstract', 'name_paper': 'paper_title'})

        # Reviews
        df = df_review
        properties = {'approves': 'approves', 'content': 'content'}
        for _, node in df.iterrows():
            node_uri = self.n.term('review' + '_' +
                                   str(node['urn1']) + str(node['urn2']))
            for property, p_column in properties.items():
                # If the property is not na, assert it
                if not isna(node[p_column]):
                    property_uri = self.n.term(property)
                    self.g.add((node_uri, property_uri, Literal(node[p_column])))

        df_affiliation = self.load_clean_csv(
            op.join(nodes_path, 'Node_affiliation.csv'), 'Affiliation')
        self.assert_nodes(df_affiliation, {'affiliation': 'urn1'}, {
                          'type': 'Type', 'name_affiliation': 'Affiliation'})

        df_keyword = self.load_clean_csv(
            op.join(nodes_path, 'Node_keywords.csv'), 'Node_keywords')
        self.assert_nodes(df_keyword, {'keyword': 'urn1'}, {
                          'name_keyword': 'Node_keywords'})

        df_journal = self.load_clean_csv(
            op.join(nodes_path, 'Node_journals.csv'), 'x')
        self.assert_nodes(df_journal, {'journal': 'urn1'}, {'name_venue': 'x'})

        df_conference = self.load_clean_csv(
            op.join(nodes_path, 'Node_conference.csv'), 'conference')
        self.assert_nodes(df_conference, {'conference': 'urn1'}, {
                          'name_venue': 'conference'})

        df_volume = self.load_clean_csv(
            op.join(nodes_path, 'Node_volumes.csv'), 'volume')
        self.assert_nodes(df_volume, {'volume': 'urn1'}, {
                          'name_compilation': 'volume', 'year': 'year'})

        df_edition = self.load_clean_csv(
            op.join(nodes_path, 'Node_edition.csv'), 'edition')
        self.assert_nodes(df_edition, {'edition': 'urn1'}, {
                          'name_compilation': 'edition', 'year': 'year', 'location': 'location'})

        df_community = self.load_clean_csv(
            op.join(nodes_path, 'Node_community.csv'), 'community')
        self.assert_nodes(df_community, {'community': 'urn1'}, {
                          'name_community': 'community'})
        print('Nodes asserted!')

        # Assert Properties
        print('Asserting properties...')
        df_paper_auth = self.load_clean_csv(
            op.join(edges_path, 'Edge_papers_author.csv'), ['id_paper', 'author'])
        df_paper_auth = df_paper_auth.loc[:, ['id_paper', 'urn2', 'main_author']]
        df_paper_auth = df_paper_auth.merge(df_paper.loc[:, ['id_paper', 'urn1']],
                                            how='left', on='id_paper')

        # writes
        df_writes = df_paper_auth[~df_paper_auth['main_author']]
        df_writes = df_writes.loc[:, ['urn1', 'urn2']]
        self.assert_properties(
            df_writes, {'author': 'urn2', 'paper': 'urn1'}, 'writes')

        # is_corresponding_author
        df_corresponding_author = df_paper_auth[df_paper_auth['main_author']]
        df_corresponding_author = df_corresponding_author.loc[:, [
            'urn1', 'urn2']]
        self.assert_properties(
            df_corresponding_author, {'author': 'urn2', 'paper': 'urn1'}, 'is_corresponding_author')

        #  writes_r
        df_writes_r = df_review.loc[:, ['urn1', 'urn2']]
        for _, edge in df_writes_r.iterrows():
            # If subject is reviewer, change urn from author to reviewer
            subj_urn = 'reviewer' if edge['urn2'] in self.reviewers else 'author'
            subject_uri = self.n.term(subj_urn +
                                      '_' + str(edge['urn2']))
            object_uri = self.n.term('review' +
                                     '_' + str(edge['urn1']) + str(edge['urn2']))
            property_uri = self.n.writes_r
            self.g.add((subject_uri, property_uri, object_uri))

        # about
        df_about = df_writes_r.loc[:, ['id_paper', 'urn1', 'urn2']].rename(columns={'urn1': 'id_paper_fixed'})
        df_about = df_about.merge(
            df_paper.loc[:, ['id_paper', 'urn1']], how='left', on='id_paper')
        for _, edge in df_about.iterrows():
            subject_uri = self.n.term('review' +
                                      '_' + str(edge['id_paper_fixed']) + str(edge['urn2']))
            object_uri = self.n.term('paper' +
                                     '_' + edge['urn1'])
            property_uri = self.n.about
            self.g.add((subject_uri, property_uri, object_uri))

        # belongs_to_a
        df_aff_auth = self.load_clean_csv(
            op.join(edges_path, 'Edge_affiliation_author.csv'), ['author', 'Affiliation'])
        self.assert_properties(
            df_aff_auth, {'author': 'urn1', 'affiliation': 'urn2'}, 'belongs_to_a')

        # relates_to
        df_paper_keyw = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_keywords.csv'), ['id_paper', 'keywords'])
        df_paper_keyw = df_paper_keyw.loc[:, ['id_paper', 'urn2']]
        df_paper_keyw = df_paper_keyw.merge(
            df_paper.loc[:, ['id_paper', 'urn1']], how='left', on='id_paper')
        self.assert_properties(
            df_paper_keyw, {'paper': 'urn1', 'keyword': 'urn2'}, 'relates_to')

        # cites
        df_cites = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_paper.csv'), ['id_paper', 'cites_value'])
        df_cites = df_cites.merge(
            df_paper.loc[:, ['id_paper', 'urn1']], how='left', on='id_paper')
        df_cites = df_cites.loc[:, ['urn1', 'cites_value']].rename(
            columns={'urn1': 'paper_subject'})
        df_cites = df_cites.merge(df_paper.loc[:, [
                                  'id_paper', 'urn2']], how='left', left_on='cites_value', right_on='id_paper')
        df_cites = df_cites.loc[:, ['paper_subject', 'urn2']].rename(
            columns={'urn2': 'paper_object'})
        for _, edge in df_cites.iterrows():
            subject_uri = self.n.term('paper' +
                                      '_' + str(edge['paper_subject']))
            object_uri = self.n.term('paper' +
                                     '_' + str(edge['paper_object']))
            property_uri = self.n.cites
            self.g.add((subject_uri, property_uri, object_uri))

        # published_in_v
        df_paper_vol = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_volumes.csv'), ['id_paper', 'id_volume'])
        df_paper_vol = df_paper_vol.merge(
            df_paper.loc[:, ['id_paper', 'urn1']], how='left', on='id_paper')
        df_published_in_v = df_paper_vol.loc[:, ['urn1', 'id_volume']]
        self.assert_properties(df_published_in_v, {
                               'paper': 'paper_title', 'volume': 'id_volume'}, 'published_in_v')

        # published_in_e
        df_paper_ed = self.load_clean_csv(
            op.join(edges_path, 'Edge_papers_edition.csv'), ['id_paper', 'ref_edition'])
        df_paper_ed = df_paper_ed.merge(df_paper.loc[:, [
                                        'id_paper', 'paper_title']], how='left', on='id_paper')
        df_paper_ed = df_paper_ed.merge(
            df_edition.loc[:, ['ref_edition', 'edition']], how='left', on='ref_edition')
        df_published_in_e = df_paper_ed.loc[:, ['paper_title', 'edition']]
        self.assert_properties(df_published_in_e, {
                               'paper': 'paper_title', 'edition': 'edition'}, 'published_in_e')

        # belongs_to_j
        df_vol_journal = self.load_clean_csv(
            op.join(edges_path, 'Edge_volumes_journal.csv'), ['id_volume', 'journal'])
        self.assert_properties(
            df_vol_journal, {'volume': 'id_volume', 'journal': 'journal'}, 'belongs_to_j')

        # belongs_to_c
        df_ed_conf = self.load_clean_csv(
            op.join(edges_path, 'Edge_edition_conference.csv'), ['ref_edition', 'conference'])
        df_ed_conf = df_ed_conf.merge(
            df_edition.loc[:, ['ref_edition', 'edition']], how='left', on='ref_edition')
        df_belongs_to_c = df_ed_conf.loc[:, ['edition', 'conference']]
        self.assert_properties(df_belongs_to_c, {
                               'edition': 'edition', 'conference': 'conference'}, 'belongs_to_c')

        # c_in
        df_conf_comm = self.load_clean_csv(
            op.join(edges_path, 'Edge_conference_community.csv'), ['conference', 'community']
        )
        self.assert_properties(
            df_conf_comm, {'conference': 'conference', 'community': 'community'}, 'c_in')

        # j_in
        df_journal_comm = self.load_clean_csv(
            op.join(edges_path, 'Edge_journal_community.csv'), ['journal', 'community']
        )
        self.assert_properties(
            df_journal_comm, {'journal': 'journal', 'community': 'community'}, 'j_in')

        print('Properties asserted!')

        # Generate .ttl
        print('Serializing ABOX...')
        self.g.serialize(destination=ttl_path)
        print('ABOX serialized!')

        print('ABOX generated!')
        return None

    def load_clean_csv(self, path, ids):
        df = read_csv(path, sep=',', header=0)
        # Remove rows with missing id value(s)
        df.loc[:, ids].dropna(inplace=True)
        # Assert id uniqueness
        df.loc[:, ids].drop_duplicates(inplace=True)
        return df

    def assert_nodes(self, df, id, properties):
        # clean id
        urn = next(iter(id))
        id_col = id[urn]
        if df[id_col].dtype == 'object':
            bad_characters = [' ', '"', '!', '.', ':', '\'', ',', '?',
                          '@', '|', '/', '+', '&', '[', ']', '*', '$', '=', 'Ã', '¨', '¢', 'µ']
            id_col = 'clean_' + id_col
            df[id_col] = df[id[urn]]
            for char in bad_characters:
                df[id_col] = df[id_col].str.replace(char, '_')

        # Find repeated ids
        df_counts = df[id_col].value_counts()
        df_repeated_counts = df_counts[df_counts > 1]
        self.n_digits[urn] = df_repeated_counts.max() / 10

        # Assert nodes triples with not repeated ids
        not_repeated_ids = set(df_counts[df_counts == 1].index)
        df_not_repeated = df[df[id_col].isin(not_repeated_ids)]
        for _, node in df_not_repeated.iterrows():
            node_uri = self.n.term(str(urn) + '0'*self.n_digits[urn] + str(node[id_col]))
            for property, p_column in properties.items():
                # If the property is not nan, assert it
                if not isna(node[p_column]):
                    property_uri = self.n.term(property)
                    self.g.add((node_uri, property_uri,
                               Literal(node[p_column])))
        
        # Assert nodes triples with repeated ids
        id_count = Series(data=0, index = df_repeated_counts.index)
        repeated_ids = set(df_repeated_counts.index)
        df_repeated = df[df[id_col].isin(repeated_ids)]
        for _, node in df_repeated:
            id_count[node[id_col]] += 1
            node_uri = self.n.term(
                str(urn) + format(id_count[node[id_col]], f'0{self.n_digits[urn]}d') + str(node[id_col]))
            for property, p_column in properties.items():
                # If the property is not nan, assert it
                if not isna(node[p_column]):
                    property_uri = self.n.term(property)
                    self.g.add((node_uri, property_uri,
                               Literal(node[p_column])))

    def assert_properties(self, df, ids, property):
        ids_iterator = iter(ids)
        subj_urn_param = next(ids_iterator)
        obj_urn_param = next(ids_iterator)
        # clean id
        urn = next(iter(id))
        id_col = id[urn]
        if df[id_col].dtype == 'object':
            bad_characters = [' ', '"', '!', '.', ':', '\'', ',', '?',
                              '@', '|', '/', '+', '&', '[', ']', '*', '$', '=', 'Ã', '¨', '¢', 'µ']
            id_col = 'clean_' + id_col
            df[id_col] = df[id[urn]]
            for char in bad_characters:
                df[id_col] = df[id_col].str.replace(char, '_')

        #  Find repeated ids
        df_counts = df[id_col].value_counts()
        self.repeated_ids[urn] = df_counts[df_counts > 1]
        n_digits = self.repeated_ids[urn].max() / 10

        # Assert nodes triples with not repeated ids
        not_repeated_ids = set(df_counts[df_counts == 1].index)
        df_not_repeated = df[df[id_col].isin(not_repeated_ids)]
        for _, node in df_not_repeated.iterrows():
            node_uri = self.n.term(str(urn) + '0'*self.n_digits[urn] + str(node[id_col]))
            for property, p_column in properties.items():
                # If the property is not nan, assert it
                if not isna(node[p_column]):
                    property_uri = self.n.term(property)
                    self.g.add((node_uri, property_uri,
                               Literal(node[p_column])))

        # Assert nodes triples with repeated ids
        id_count = Series(data=0, index=self.repeated_ids[urn].index)
        repeated_ids = set(self.repeated_ids[urn].index)
        df_repeated = df[df[id_col].isin(repeated_ids)]
        for _, node in df_repeated:
            id_count[node[id_col]] += 1
            node_uri = self.n.term(
                str(urn) + format(id_count[node[id_col]], f'0{self.n_digits[urn]}d') + str(node[id_col]))
            for property, p_column in properties.items():
                # If the property is not nan, assert it
                if not isna(node[p_column]):
                    property_uri = self.n.term(property)
                    self.g.add((node_uri, property_uri,
                               Literal(node[p_column])))
        for _, edge in df.iterrows():
            # If subject or object is reviewer, change urn from author to reviewer
            subj_urn = 'reviewer' if subj_urn_param == 'author'\
                and edge[ids['author']] in self.reviewers else subj_urn_param
            obj_urn = 'reviewer' if obj_urn_param == 'author'\
                and edge[ids['author']] in self.reviewers else obj_urn_param
            subject_uri = self.n.term(str(subj_urn) +
                                      '_' + str(edge[ids[subj_urn_param]]))
            object_uri = self.n.term(str(obj_urn) +
                                     '_' + str(edge[ids[obj_urn_param]]))
            property_uri = self.n.term(property)
            self.g.add((subject_uri, property_uri, object_uri))


if __name__ == '__main__':
    ABOXGenerator()
