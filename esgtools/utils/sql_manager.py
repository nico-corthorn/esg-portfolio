
import os
import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def get_config_db():
    config_db = {}
    config_db['dbname'] = os.environ.get('POSTGRES_DB_NAME')
    config_db['username'] = os.environ.get('POSTGRES_USER')
    config_db['password'] = os.environ.get('POSTGRES_PASSWORD')
    config_db['host'] = os.environ.get('POSTGRES_HOST')
    config_db['port'] = os.environ.get('POSTGRES_PORT')
    return config_db


class ManagerSQL:

    def __init__(self, sql_params=None):
        """
            Parameters
            ----------
            sql_params: dict or None
                Should have the same key-value structure as the dictionary returned by Secrets Manager
        """

        # Basic db parameters
        if sql_params is None:
            sql_params = get_config_db()
        db_name = sql_params['dbname']
        user = sql_params['username']
        password = sql_params['password']
        host = sql_params['host']
        port = sql_params['port']

        # Connexion for downloading data
        self.cnxn = psycopg2.connect(database=db_name, user=user, password=password, host=host,
                                     port=port)
        self.cursor = self.cnxn.cursor()

        # Connexion for uploading data
        self.db_url = 'postgresql://' + user + ':' + password + '@' + host + '/' + db_name
        self.con = create_engine(self.db_url)

    def select(self, table):
        """ Returns table as DataFrame. """
        sql = 'select * from '+table
        df = pd.read_sql(sql, self.cnxn)
        return df

    def select_query(self, query):
        """ Returns query output as DataFrame. """
        df = pd.read_sql(query, self.cnxn)
        return df

    def select_column_list(self, column, table):
        """ Returns column values as list. """
        sql = 'select '+column+' from '+table+' order by '+column
        df = pd.read_sql(sql, self.cnxn)
        lst = [element for element in df[column]]
        return lst

    def select_distinct_column_list(self, column, table):
        """ Returns unique values of column as list. """
        sql = 'select distinct '+column+' from '+table+' order by '+column
        df = pd.read_sql(sql, self.cnxn)
        lst = [element for element in df[column]]
        return lst

    def select_distinct_indexed_column_list(self, column, table):
        sql = \
            """
            WITH RECURSIVE t AS (
            SELECT MIN({0}) AS {0} FROM {1}
            UNION ALL
            SELECT (SELECT MIN({0}) FROM {1} WHERE {0} > t.{0})
            FROM t WHERE t.{0} IS NOT NULL
            )
            SELECT {0} FROM t WHERE {0} IS NOT NULL
            UNION ALL
            SELECT NULL WHERE EXISTS(SELECT 1 FROM {1} WHERE {0} IS NULL)
            """.format(column, table)
        df = pd.read_sql(sql, self.cnxn)
        lst = [element for element in df[column]]
        return lst

    def select_as_dictionary(self, column_key, column_value, table):
        """ Return dictionary column_key: column_value, column_key must have unique values. """
        sql = 'select '+column_key+', '+column_value+' from '+table
        df = pd.read_sql(sql, self.cnxn)
        assert df[column_key].is_unique, "Column {} doesn't have unique values.".format(column_key)
        return df.set_index(column_key).to_dict()[column_value]

    def upload_df(self, table, df, chunk_size=1):
        """ Uploads data frame to table. Appends information. """
        df.to_sql(name=table, con=self.con, if_exists='append', index=False, chunksize=chunk_size)

    def upload_df_sequential(self, table, df):
        """ Uploads data frame to table. Appends information. """
        for i in range(df.shape[0]):
            df.loc[[df.index[i]], :].to_sql(name=table, con=self.con, if_exists='append', index=False)

    def upload_df_chunks(self, table, df, chunk_size=1000):
        """ 
            Uploads data frame to table. Appends information. 
            DataFrame index must have been reseted previously.
        """
        ind = list(df.index)
        chunks = [ind[i:i + chunk_size] for i in range(0, len(ind), chunk_size)]
        for chunk in chunks:
            df.loc[chunk, :].to_sql(name=table, con=self.con, if_exists='append', index=False)

    def upload_df_2(self, table, df):
        k = len(df.columns)
        query = "insert into {0} ({1}) values ({2})".format(table, ",".join(df.columns), ",".join(["?"]*k))
        for i in range(df.shape[0]):
            values = tuple([i for i in df.loc[df.index[i], :]])
            self.cursor.execute(query, values)
            self.con.commit()

    def query(self, query):
        """ Executes query. Doesn't return anything.
            Intended for customized delete queries. """
        self.cursor.execute(query)
        self.cnxn.commit()

    def clean_table(self, table):
        """ Delete all information from the table. """
        self.cursor.execute('delete from ' + table)
        self.cnxn.commit()
