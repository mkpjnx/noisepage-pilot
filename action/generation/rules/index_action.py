import pglast.enums.parsenodes as pnodes
from pglast import ast, stream

from action import Action, Configuration


class Index(Configuration):
    def __init__(self, table, cols, using=None, override_name=None):
        self._table = table
        self._cols = cols
        self._using = using
        self._override_name = override_name

    @property
    def identifier(self):
        columns_string = ",".join(map(str, self._cols))
        using_string = "" if self._using is None else "USING {self.using}"
        return f"Index {self.index_name} on {self._table}({columns_string}){using_string}"

    @property
    def table(self):
        return self._table

    @property
    def cols(self):
        return self._cols

    @property
    def using(self):
        return self._using

    @property
    def index_name(self):
        if self._override_name is not None:
            return self._override_name
        colnames = [c.replace("_", "") for c in self._cols]
        return f'idx_{self._table}_{"_".join(colnames)}'


class CreateIndexAction(Action):
    def __init__(self, target: Index):
        Action.__init__(self, target)

    def to_json(self):
        return {"type": "CreateIndex", "target": self.target.identifier}

    def to_sql(self):
        index = self.target
        index_name = index.index_name

        self.ast = ast.IndexStmt(
            idxname=index_name,
            relation=ast.RangeVar(relname=index.table, inh=True),
            accessMethod="btree" if index.using is None else index.using,
            indexParams=tuple(
                [
                    ast.IndexElem(
                        col,
                        ordering=pnodes.SortByDir.SORTBY_DEFAULT,
                        nulls_ordering=pnodes.SortByNulls.SORTBY_NULLS_DEFAULT,
                    )
                    for col in index.cols
                ]
            ),
            idxcomment=None,
            if_not_exists=True,
        )
        return stream.RawStream(semicolon_after_last_statement=True)(self.ast)


class DropIndexAction(Action):
    def __init__(self, target: Index, cascade=False):
        Action.__init__(self, target)
        self.cascade = cascade

    def to_json(self):
        return {"type": "DropIndex", "target": self.target.identifier, "cascade": False}

    def to_sql(self):
        self.ast = ast.DropStmt(
            objects=[self.target.index_name],
            removeType=pnodes.ObjectType.OBJECT_INDEX,
            behavior=pnodes.DropBehavior.DROP_CASCADE if self.cascade else pnodes.DropBehavior.DROP_RESTRICT,
            missing_ok=True,
        )
        return stream.RawStream(semicolon_after_last_statement=True)(self.ast)
