import copy
import itertools
from collections import defaultdict

import numpy as np
from connector import Connector
from rules.index_action import CreateIndexAction, DropIndexAction, Index
from workload.workload import Workload

from action import ActionGenerator


class DropIndexGenerator(ActionGenerator):
    """
    For each existing index, yield a DROP INDEX statement.
    """

    def __init__(self, connector: Connector, **kwargs) -> None:
        ActionGenerator.__init__(self)
        self.indexes = connector.get_index_info()

    def get_action(self):
        for ind in self.indexes:
            target = Index(ind[1], ind[2], override_name=ind[0])
            yield DropIndexAction(target)


class ExhaustiveIndexGenerator(ActionGenerator):
    """
    For each query, this action generator produces a CREATE INDEX statement
    for each table's columns.
    """

    def __init__(self, connector, max_width=1, **kwargs) -> None:
        ActionGenerator.__init__(self)
        table_info = connector.get_table_info()
        self.tables = list(table_info.keys())
        joint_refs = {k: defaultdict(lambda: defaultdict(np.uint64)) for k in self.tables}
        for (table, cols) in table_info.items():
            joint_refs[table][tuple(cols)] = 1
        self.joint_refs = joint_refs
        self.max_width = max_width

    def _iter_table_widths(self, table, width):
        col_perms = set()
        for cols in self.joint_refs[table]:
            for perm in itertools.permutations(cols, width):
                col_perms.add(perm)
                target = Index(table, perm)
                yield CreateIndexAction(target)

    def get_action(self):
        for table in self.tables:
            for width in range(1, self.max_width + 1):
                for action in self._iter_table_widths(table, width):
                    yield action

    def items(self):
        for table in self.tables:
            for width in range(1, self.max_width + 1):
                for action in self._iter_table_widths(table, width):
                    yield action


class WorkloadIndexGenerator(ActionGenerator):
    """
    For each query, this action generator produces a CREATE INDEX statement
    for each table's columns which appears together.
    """

    # TODO: change this to take workload object instead of joint_refs
    def __init__(self, workload: Workload, max_width=1, **kwargs) -> None:
        ActionGenerator.__init__(self)
        joint_refs = workload.get_where_colrefs()
        self.tables = list(joint_refs.keys())
        self.joint_refs = joint_refs
        self.actions_per_table = [len(joint_refs[t]) for t in self.tables]
        # Prefix sum of actions per table
        self.max_width = max_width

    def _iter_table_widths(self, table, width):
        col_perms = set()
        for cols in self.joint_refs[table]:
            for perm in itertools.permutations(cols, width):
                if perm in col_perms:
                    continue
                col_perms.add(perm)
                target = Index(table, perm)
                yield CreateIndexAction(target)

    def get_action(self):
        for table in self.tables:
            for width in range(1, self.max_width + 1):
                for action in self._iter_table_widths(table, width):
                    yield action

    def items(self):
        for table in self.tables:
            for width in range(1, self.max_width + 1):
                for action in self._iter_table_widths(table, width):
                    yield action


class TypedIndexGenerator(ActionGenerator):
    """
    For each query, this action generator produces a CREATE INDEX statement
    for each table's columns which appears together.
    """

    def __init__(self, upstream: ActionGenerator, types, **kwargs) -> None:
        ActionGenerator.__init__(self)
        self.upstream = upstream
        self.types = types

    def __iter__(self) -> str:
        for orig_action in self.upstream.generate_all():
            for using in self.types:
                new_action = copy.deepcopy(orig_action)
                new_action.using = using
                yield new_action
