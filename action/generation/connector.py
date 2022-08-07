# noqa: E501 inspired by https://github.com/hyrise/index_selection_evaluation/blob/ca1dc87e20fe64f0ef962492597b77cd1916b828/selection/dbms/postgres_dbms.py
import logging
from typing import Dict, List, Tuple

import sqlalchemy as sa


class Connector:
    def __init__(self, dbname=None, user=None, password=None, host=None):

        cargs = {"dbname": dbname, "user": user, "password": password, "host": host}

        self.dbname = dbname
        self.user = user

        self._engine = sa.create_engine("postgresql+psycopg2://", connect_args=cargs)
        self._connection = self._engine.connect()
        self._connection.execution_options(isolation_level="AUTOCOMMIT")

        logging.debug(f"Connected to {self.dbname} as {self.user}")

        self.refresh_stats()

        # reflect pg_settings for knob table schema
        self._catalog_meta = sa.MetaData()
        self._catalog_meta.reflect(self._connection, schema="pg_catalog", views=True, only=["pg_settings"])

    def close(self):
        self._connection.close()
        logging.debug(f"Disconnected from {self.dbname} as {self.user}")

    def refresh_stats(self):
        self._connection.execute("ANALYZE;")
        self._metadata = sa.MetaData()
        self._metadata.reflect(self._connection)

    def get_table_info(self, refresh=False) -> Dict[str, List[str]]:
        if refresh:
            self.refresh_stats()

        info = {name: [c.name for c in table.columns] for name, table in self._metadata.tables.items()}
        return info

    def get_index_info(self, refresh=False) -> List[Tuple[str, str, List[str]]]:
        if refresh:
            self.refresh_stats()

        info = []
        for table in self._metadata.sorted_tables:
            for index in table.indexes:
                cols = [c.name for c in index.columns]
                info.append((index.name, table.name, cols))
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

    def get_config(self, name):
        # TODO(Mike): Add error checking (throw err if knob does not exists)
        pg_settings = self._catalog_meta.tables["pg_catalog.pg_settings"]
        stmt = sa.select(pg_settings).where(pg_settings.c.name == name)
        try:
            return self._connection.execute(stmt).first()
        except Exception as e:
            raise ValueError(f"{name} is not a valid knob ({e})")
