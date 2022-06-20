from action import Action

from pglast import ast, stream
from pglast.enums.parsenodes import *


class CreateIndexAction(Action):
    def __init__(self, table, cols, using=None):
        Action.__init__(self)
        self.table = table
        self.cols = cols
        self.using = using

    def index_name(self):
        colnames = [c.replace("_", "") for c in self.cols]
        return f'idx_{self.table}_{"_".join(colnames)}'

    def _to_sql(self):
        index_name = self.index_name()

        self.ast = ast.IndexStmt(
            idxname=index_name,
            relation=ast.RangeVar(relname=self.table, inh=True),
            accessMethod='btree' if self.using is None else self.using,
            indexParams=tuple(
                [
                    ast.IndexElem(
                        col,
                        ordering=SortByDir.SORTBY_DEFAULT,
                        nulls_ordering=SortByNulls.SORTBY_NULLS_DEFAULT,
                    ) for col in self.cols]
            ),
            idxcomment=None,
            if_not_exists=True,
        )
        return stream.RawStream(semicolon_after_last_statement=True)(self.ast)


class DropIndexAction(Action):
    def __init__(self, idxname, cascade=False):
        Action.__init__(self)
        self.idxname = idxname
        self.cascade = cascade

    def _to_sql(self):

        self.ast = ast.DropStmt(
            objects=[self.idxname],
            removeType=ObjectType.OBJECT_INDEX,
            behavior=DropBehavior.DROP_CASCADE if self.cascade else DropBehavior.DROP_RESTRICT,
            missing_ok=True,
        )
        return stream.RawStream(semicolon_after_last_statement=True)(self.ast)
