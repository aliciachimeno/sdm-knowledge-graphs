# SDM Project 2. Knowledge Graphs
# ABOX generator
# for handling csv and csv contents
from pandas import read_csv, DataFrame, isna, Series
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

        ############ Assert Nodes ############
        print('Asserting nodes...')

        # paper, author, reviewer
        df_paper = self.load_clean_csv(
            op.join(nodes_path, 'Node_paper.csv'), ['csv_id_paper', 'name_paper', 'DOI', 'abstract', 'pages'], ['csv_id_paper'])
        df_paper = self.generate_urn(df_paper, 'paper')
        self.assert_nodes(df_paper, 'paper', [
                          'name_paper', 'DOI', 'abstract', 'pages'])

        df_author = self.load_clean_csv(
            op.join(nodes_path, 'Node_author.csv'), ['name_author'])

        df_review = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_author_reviews.csv'), ['csv_id_paper', 'name_author', 'content', 'approves'], ['csv_id_paper', 'name_author'])

        # Reviewers are authors who have written at least one review
        self.reviewers = df_review['name_author'].unique()
        self.df_reviewer = DataFrame(self.reviewers, columns=['name_author'])
        self.df_reviewer = self.generate_urn(self.df_reviewer, 'reviewer')
        self.assert_nodes(self.df_reviewer, 'reviewer', ['name_author'])

        # Reviewer is a subclass of Author, so we skip them
        df_author = df_author[~df_author['name_author']
                              .isin(self.reviewers)]
        self.df_author = self.generate_urn(df_author, 'author')
        self.assert_nodes(self.df_author, 'author', ['name_author'])

        # Reviews
        df_review = self.generate_urn(df_review, 'review')
        self.assert_nodes(df_review, 'review', ['content', 'approves'])

        # affiliations
        df_affiliation = self.load_clean_csv(
            op.join(nodes_path, 'Node_affiliation.csv'), ['type', 'name_affiliation'], ['name_affiliation'])
        df_affiliation = self.generate_urn(df_affiliation, 'affiliation')
        self.assert_nodes(df_affiliation, 'affiliation',
                          ['type', 'name_affiliation'])

        # keywords
        df_keyword = self.load_clean_csv(
            op.join(nodes_path, 'Node_keywords.csv'), ['name_keyword'])
        df_keyword = self.generate_urn(df_keyword, 'keyword')
        self.assert_nodes(df_keyword, 'keyword', ['name_keyword'])

        # journals
        df_journal = self.load_clean_csv(
            op.join(nodes_path, 'Node_journals.csv'), ['name_venue'])
        df_journal = self.generate_urn(df_journal, 'journal')
        self.assert_nodes(df_journal, 'journal', ['name_venue'])

        # conferences
        df_conference = self.load_clean_csv(
            op.join(nodes_path, 'Node_conference.csv'), ['name_venue'])
        df_conference = self.generate_urn(df_conference, 'conference')
        self.assert_nodes(df_conference, 'conference', ['name_venue'])

        # volumes
        df_volume = self.load_clean_csv(
            op.join(nodes_path, 'Node_volumes.csv'), ['name_compilation', 'year'], ['name_compilation'])
        df_volume = self.generate_urn(df_volume, 'volume')
        self.assert_nodes(df_volume, 'volume', ['name_compilation', 'year'])

        # editions
        df_edition = self.load_clean_csv(
            op.join(nodes_path, 'Node_edition.csv'), ['csv_id_edition', 'name_compilation', 'edition_num', 'year', 'location'], ['csv_id_edition'])
        df_edition = self.generate_urn(df_edition, 'edition')
        self.assert_nodes(df_edition, 'edition', [
                          'name_compilation', 'year', 'location'])

        # communities
        df_community = self.load_clean_csv(
            op.join(nodes_path, 'Node_community.csv'), ['name_community'])
        df_community = self.generate_urn(df_community, 'community')
        self.assert_nodes(df_community, 'community', ['name_community'])
        print('Nodes asserted!')

        ########### Assert Properties ###########
        print('Asserting properties...')
        df_paper_auth = self.load_clean_csv(
            op.join(edges_path, 'Edge_papers_author.csv'), ['csv_id_paper', 'name_author', 'main_author'], ['csv_id_paper', 'name_author'])
        df_paper_auth = df_paper_auth.merge(df_paper.loc[:, ['paper', 'csv_id_paper']],
                                            how='left', on='csv_id_paper')
        self.df_paper_auth = df_paper_auth
        self.df_paper_auth['author'] = self.df_paper_auth['name_author'].apply(
            self.get_author_urn)

        # writes
        df_writes = df_paper_auth[~df_paper_auth['main_author']]
        self.assert_properties(
            df_writes, 'author', 'writes', 'paper')

        # is_corresponding_author
        df_corresponding_author = df_paper_auth[df_paper_auth['main_author']]
        self.assert_properties(
            df_corresponding_author, 'author', 'is_corresponding_author', 'paper')

        #  writes_r
        df_writes_r = df_review.merge(
            self.df_reviewer, how='left', on='name_author')
        self.assert_properties(df_writes_r, 'reviewer', 'writes_r', 'review')

        # about
        df_about = df_review.merge(df_paper, how='left', on='csv_id_paper')
        self.assert_properties(df_about, 'review', 'about', 'paper')

        # belongs_to_a
        df_aff_auth = self.load_clean_csv(
            op.join(edges_path, 'Edge_affiliation_author.csv'), ['name_author', 'name_affiliation'])
        df_aff_auth = df_aff_auth.merge(
            df_affiliation, how='left', on='name_affiliation')
        df_aff_auth['author'] = df_aff_auth['name_author'].apply(
            self.get_author_urn)
        self.assert_properties(
            df_aff_auth, 'author', 'belongs_to_a', 'affiliation')

        # relates_to
        df_paper_keyw = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_keywords.csv'), ['csv_id_paper', 'name_keyword'])
        df_paper_keyw = df_paper_keyw.merge(
            df_paper.loc[:, ['csv_id_paper', 'paper']], how='left', on='csv_id_paper')
        df_paper_keyw = df_paper_keyw.merge(
            df_keyword, how='left', on='name_keyword')
        self.assert_properties(df_paper_keyw, 'paper', 'relates_to', 'keyword')

        # cites
        df_cites = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_paper.csv'), ['subject_csv_id_paper', 'object_csv_id_paper'])
        df_cites = df_cites.merge(df_paper.loc[:, ['csv_id_paper', 'paper']], how='left', left_on='subject_csv_id_paper', right_on='csv_id_paper')\
            .rename(columns={'paper': 'subject_paper'})
        df_cites = df_cites.merge(df_paper.loc[:, ['csv_id_paper', 'paper']], how='left', left_on='object_csv_id_paper', right_on='csv_id_paper')\
            .rename(columns={'paper': 'object_paper'})
        self.assert_properties(df_cites, 'subject_paper',
                               'cites', 'object_paper')

        # published_in_v
        df_paper_vol = self.load_clean_csv(
            op.join(edges_path, 'Edge_paper_volumes.csv'), ['csv_id_paper', 'name_compilation', 'short_volume'], ['csv_id_paper', 'name_compilation'])
        df_paper_vol = df_paper_vol.merge(
            df_paper.loc[:, ['csv_id_paper', 'paper']], how='left', on='csv_id_paper')
        df_paper_vol = df_paper_vol.merge(
            df_volume.loc[:, ['name_compilation', 'volume']], how='left', on='name_compilation')
        self.assert_properties(df_paper_vol, 'paper',
                               'published_in_v', 'volume')

        # published_in_e
        df_paper_ed = self.load_clean_csv(
            op.join(edges_path, 'Edge_papers_edition.csv'), ['csv_id_paper', 'csv_id_edition'])
        df_paper_ed = df_paper_ed.merge(df_paper.loc[:, [
                                        'csv_id_paper', 'paper']], how='left', on='csv_id_paper')
        df_paper_ed = df_paper_ed.merge(
            df_edition.loc[:, ['csv_id_edition', 'edition']], how='left', on='csv_id_edition')
        self.assert_properties(df_paper_ed, 'paper',
                               'published_in_e', 'edition')

        # belongs_to_j
        df_vol_journal = self.load_clean_csv(
            op.join(edges_path, 'Edge_volumes_journal.csv'), ['name_compilation', 'name_venue'])
        df_vol_journal = df_vol_journal.merge(
            df_volume.loc[:, ['name_compilation', 'volume']], how='left', on='name_compilation')
        df_vol_journal = df_vol_journal.merge(
            df_journal, how='left', on='name_venue')
        self.assert_properties(
            df_vol_journal, 'volume', 'belongs_to_j', 'journal')

        # belongs_to_c
        df_ed_conf = self.load_clean_csv(
            op.join(edges_path, 'Edge_edition_conference.csv'), ['csv_id_edition', 'name_venue'])
        df_ed_conf = df_ed_conf.merge(
            df_edition.loc[:, ['csv_id_edition', 'edition']], how='left', on='csv_id_edition')
        df_ed_conf = df_ed_conf.merge(
            df_conference, how='left', on='name_venue')
        self.assert_properties(df_ed_conf, 'edition',
                               'belongs_to_c', 'conference')

        # c_in
        df_conf_comm = self.load_clean_csv(
            op.join(edges_path, 'Edge_conference_community.csv'), ['name_venue', 'name_community'])
        df_conf_comm = df_conf_comm.merge(
            df_conference, how='left', on='name_venue')
        df_conf_comm = df_conf_comm.merge(
            df_community, how='left', on='name_community')
        self.assert_properties(
            df_conf_comm, 'conference', 'c_in', 'community')

        # j_in
        df_journal_comm = self.load_clean_csv(
            op.join(edges_path, 'Edge_journal_community.csv'), ['name_venue', 'name_community'])
        df_journal_comm = df_journal_comm.merge(
            df_journal, how='left', on='name_venue')
        df_journal_comm = df_journal_comm.merge(
            df_community, how='left', on='name_community')
        self.assert_properties(
            df_journal_comm, 'journal', 'j_in', 'community')

        print('Properties asserted!')

        ########## Generate .ttl ############
        print('Serializing ABOX...')
        self.g.serialize(destination=ttl_path)
        print('ABOX serialized!')

        print('ABOX generated!')
        return None

    def load_clean_csv(self, path, columns, ids=None):
        if not ids:
            ids = columns
        df = read_csv(path, sep=',', header=0, names=columns)
        # Remove rows with missing id value(s)
        df.loc[:, ids].dropna(inplace=True)
        # Assert id uniqueness
        df.loc[:, ids].drop_duplicates(inplace=True)
        return df

    def assert_nodes(self, df, id, properties):
        for _, node in df.iterrows():
            node_uri = self.n.term(node[id])
            for property in properties:
                # If the property is not nan, assert it
                if not isna(node[property]):
                    property_uri = self.n.term(property)
                    self.g.add((node_uri, property_uri,
                               Literal(node[property])))

    def assert_properties(self, df, subject_id, property, object_id):
        for _, edge in df.iterrows():
            subject_uri = self.n.term(edge[subject_id])
            object_uri = self.n.term(edge[object_id])
            property_uri = self.n.term(property)
            self.g.add((subject_uri, property_uri, object_uri))

    def generate_urn(self, df, id):
        df[id] = [id + str(i) for i in range(len(df))]
        return df

    def get_author_urn(self, author):
        if author in self.reviewers:
            return self.df_reviewer\
                .loc[self.df_reviewer['name_author'] == author, 'reviewer'].iloc[0]
        return self.df_author.loc[self.df_author['name_author'] == author, 'author'].iloc[0]


if __name__ == '__main__':
    ABOXGenerator()
