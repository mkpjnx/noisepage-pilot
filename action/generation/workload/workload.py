import re
import time
from collections import defaultdict
from typing import List

import numpy as np
import pandas as pd
import pglast
import pglast.visitors
from connector import Connector

PGLAST_CLAUSES = ["whereClause", "groupClause","sortClause"]

_PG_LOG_COLUMNS: List[str] = [
    "log_time",
    "user_name",
    "database_name",
    "process_id",
    "connection_from",
    "session_id",
    "session_line_num",
    "command_tag",
    "session_start_time",
    "virtual_transaction_id",
    "transaction_id",
    "error_severity",
    "sql_state_code",
    "message",
    "detail",
    "hint",
    "internal_query",
    "internal_query_pos",
    "context",
    "query",
    "query_pos",
    "location",
    "application_name",
    "backend_type",
]


# general log parsing functions
def _extract_query(message_series):
    """
    Extract SQL queries from the CSVLOG's message column.

    Parameters
    ----------
    message_series : pd.Series
        A series corresponding to the message column of a CSVLOG file.

    Returns
    -------
    query : pd.Series
        A str-typed series containing the queries from the log.
    """
    simple = r"statement: ((?:DELETE|INSERT|SELECT|UPDATE).*)"
    extended = r"execute .+: ((?:DELETE|INSERT|SELECT|UPDATE).*)"
    regex = f"(?:{simple})|(?:{extended})"
    query = message_series.str.extract(regex, flags=re.IGNORECASE)
    # Combine the capture groups for simple and extended query protocol.
    query = query[0].fillna(query[1])
    query.fillna("", inplace=True)
    return query.astype(str)


def _parse_csv_log(file):
    """
    Extract queries from workload csv file and return df with fingerprint
    and corresponding queries
    """
    df = pd.read_csv(
        file,
        names=_PG_LOG_COLUMNS,
        # parse_dates=["log_time", "session_start_time"],
        usecols=[
            # "log_time",
            # "session_start_time",
            # "command_tag",
            "message",
            # "detail",
        ],
        header=None,
        index_col=False,
    )

    # filter out empty messages
    df = df[df["message"] != ""]
    # df['detail'].fillna("", inplace=True)
    # extract queries and toss commits, sets, etc.
    df["queries"] = _extract_query(df["message"])
    df = df[df["queries"] != ""]
    return df[["queries"]]


def _find_colrefs(node: pglast.node.Node):
    """
    Find all column refs by scanning through a pglast node
    """
    if node is pglast.Missing:
        return []
    colrefs = []
    for subnode in node.traverse():
        if type(subnode) is pglast.node.Scalar:
            continue
        if type(subnode.ast_node) is pglast.ast.ColumnRef:
            colref = tuple([n.val.value for n in subnode.fields if type(n.ast_node) == pglast.ast.String])
            if len(colref) > 0:
                colrefs.append(colref)
    return colrefs


def _get_all_colrefs(sql, table_cols):
    """
    Get all column refs from a sql statement which appear in
    WHERE and GROUP BYs

    Attempt to resolve aliases for table refs
    """
    tree = pglast.parse_sql(sql)

    aliases = {}
    clause_refs = {clause:[] for clause in PGLAST_CLAUSES}
    referenced_tables = pglast.visitors.referenced_relations(tree)

    # mine the AST for aliases and colrefs
    for node in pglast.node.Node(tree).traverse():
        if type(node) is pglast.node.Scalar:
            continue
        for clause in PGLAST_CLAUSES:
            if clause in node.attribute_names:
                clause_refs[clause]  += _find_colrefs(node[clause])
        if "alias" in node.attribute_names and "relname" in node.attribute_names:
            if node.alias is pglast.Missing or node.relname is pglast.Missing:
                continue
            if node.alias.aliasname.value in aliases:
                print("UH OH, double alias")
            aliases[node.alias.aliasname.value] = node.relname.value
    # resolve aliases and figure out actual table col refs
    # print(sql, clause_refs)
    for clause, refs in clause_refs.items():
        clause_refs[clause] = _resolve_colref_aliases(refs, aliases, referenced_tables, table_cols)

    return clause_refs


# parse_colref_aliases returns set of colrefs with resolved table names
def _resolve_colref_aliases(raw_colrefs, aliases, referenced_tables, table_cols):
    potential_colrefs = []
    for c in raw_colrefs:
        potential_tables = []
        p_col = None

        # The colref does not specify table, assume this col could be in
        # any of the referenced tables
        if len(c) == 1:
            p_col = c[0]
            potential_tables = [t for t in referenced_tables if t in table_cols and p_col in table_cols[t]]

        # The colref does specify table, also attempt to resolve alias
        if len(c) == 2:
            t = c[0]
            p_col = c[1]
            if t not in table_cols and t in aliases:
                t = aliases[t]
            potential_tables = [t]

        # Only add the table,col pair if it exists in the schema
        potential_colrefs += [
            (p_t, p_col) for p_t in potential_tables if (p_t in table_cols and p_col in table_cols[p_t])
        ]
    return set(potential_colrefs)


def _aggregate_templates(df, table_cols, percent_threshold=1):
    """
    Aggregate queries into templates based on pglast
    Only retain most common queries up to {percent_threshold} of the workload
    """

    df["fingerprint"] = df["queries"].apply(pglast.parser.fingerprint)
    aggregated = (
        df[["queries", "fingerprint"]]
        .groupby("fingerprint")
        .agg([pd.DataFrame.sample, "count"])["queries"]
        .sort_values("count", ascending=False)
    )
    aggregated["fraction"] = aggregated["count"] / aggregated["count"].sum()
    aggregated["cumsum"] = aggregated["fraction"].cumsum()
    filtered = pd.DataFrame(aggregated[aggregated["cumsum"] <= percent_threshold])

    # get column refs
    filtered["clause_refs"] = filtered["sample"].apply(
        _get_all_colrefs,
        args=(table_cols,))

    return filtered[["sample", "count", "cumsum", "clause_refs"]]


def get_workload_colrefs(filtered, table_cols, clauses):
    # TODO: when to filter by clause?
    # TODO: is 
    tables = set(table_cols.keys())
    table_colrefs_joint_counts = {k: defaultdict(np.uint64) for k in tables}


    for _, row in filtered.iterrows():
        for table in tables:
            cols_for_table = []
            for clause in clauses:
                refs = row["clause_refs"][clause]
                cols_for_table += [col for (tab, col) in refs if tab == table]
            if len(cols_for_table) == 0:
                continue
            joint_ref = tuple(set(cols_for_table))
            table_colrefs_joint_counts[table][joint_ref] += row["count"]

    return table_colrefs_joint_counts


class Workload:
    def __init__(self, workload_csv_file_name: str, db_connector: Connector):
        self.file_name = workload_csv_file_name
        self.conn = db_connector

        # TODO: cleaner format to store these?
        self._parse()

    def _parse(self):
        # execute log parsing for workload, store each type of col_refs in a per-table basis
        ts = time.time()
        self.table_cols = self.conn.get_table_info()
        self.parsed = _parse_csv_log(self.file_name)
        print(f"\tparsed\t{time.time() - ts}")

        ts = time.time()
        self.filtered = _aggregate_templates(self.parsed, self.table_cols)
        print(f"\ttemplatize\t{time.time() - ts}")
        self.filtered.to_csv("test.csv")

    def export_sample(self, sample_size=500, output=None):
        sample_size = min(sample_size, len(self.parsed["queries"]))
        with open(output, "w") as file:
            queries = self.parsed["queries"].sample(sample_size).values
            print(";\n".join(queries), file=file)

    def get_colrefs(self, clauses = PGLAST_CLAUSES):
        clauses = clauses if clauses is not None else PGLAST_CLAUSES
        return get_workload_colrefs(self.filtered, self.table_cols, clauses)
