# noqa: E501 inspired by https://github.com/hyrise/index_selection_evaluation/blob/ca1dc87e20fe64f0ef962492597b77cd1916b828/selection/dbms/postgres_dbms.py
import constants
import logging
from sqlalchemy import *
from typing import List, Dict, Tuple


class Connector():
    def __init__(self):

        cargs = {
            'dbname':constants.DB_NAME,
            'user':constants.DB_USER,
            'password':constants.DB_PASS,
            'host':constants.DB_HOST
        }
        self._engine = create_engine("postgresql+psycopg2://", connect_args=cargs)
        self._connection = self._engine.connect()
        self._connection.execution_options(isolation_level="AUTOCOMMIT")
        
        
        logging.debug(
            f"Connected to {constants.DB_NAME} as {constants.DB_USER}")

        self.refresh_stats()

    def close(self):
        self._connection.close()
        logging.debug(
            f"Disconnected from {constants.DB_NAME} as {constants.DB_USER}")

    def refresh_stats(self):
        self._connection.execute("ANALYZE;")
        self._metadata = MetaData()
        self._metadata.reflect(self._connection)

    def get_table_info(self, refresh = False) -> Dict[str, List[str]]:
        if refresh:
            self._metadata = MetaData()
            self._metadata.reflect(self._connection)

        info = {
            name: [c.name for c in table.columns] 
            for name, table in self._metadata.tables.items()
        }
        return info

    def get_index_info(self, refresh = False) -> List[Tuple[str, str, List[str]]]:
        if refresh:
            self._metadata = MetaData()
            self._metadata.reflect(self._connection)

        info = []
        for table in self._metadata.sorted_tables:
            for index in table.indexes:
                cols = [c.name for c in index.columns]
                info.append((
                    index.name,
                    table.name,
                    cols))
        return info

    # TODO(Mike): Convert to ORM
    # def get_unused_indexes(self):
    #     query = '''
    #     SELECT s.relname AS tablename,
    #         s.indexrelname AS indexname,
    #         pg_relation_size(s.indexrelid) AS index_size
    #     FROM pg_catalog.pg_stat_user_indexes s
    #     JOIN pg_catalog.pg_index i ON s.indexrelid = i.indexrelid
    #     WHERE s.idx_scan = 0      -- has never been scanned
    #         AND 0 <>ALL (i.indkey)  -- no index column is an expression
    #         AND s.schemaname = 'public'
    #         AND NOT i.indisunique   -- is not a UNIQUE index
    #         AND NOT EXISTS          -- does not enforce a constraint
    #             (SELECT 1 FROM pg_catalog.pg_constraint c
    #             WHERE c.conindid = s.indexrelid)
    #     ORDER BY pg_relation_size(s.indexrelid) DESC;
    #     '''
    #     return self._connection.execute(query)

    # BEGIN knob interactions
    def get_config(self, name):
        # TODO(Mike): Add error checking (throw err if knob does not exists)
        query = f"SELECT setting, unit FROM pg_settings WHERE name = '{name}';"
        return list(self._connection.execute(query))[0]
    # We should not have knob actions here.
    def get_categorical_type_with_values(self, name):
        # TODO(Mike): Add error checking (throw err if knob does not exists)
        query = f"SELECT vartype, enumvals FROM pg_settings WHERE name = '{name}';"
        return list(self._connection.execute(query))[0]

    # END knob interactions
