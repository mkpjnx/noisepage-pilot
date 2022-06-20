from action import Action

from pglast import ast, stream
from pglast.enums.parsenodes import *

class KnobAction(Action):
    def __init__(self, name, setting=None, alterSystem=False):
        Action.__init__(self)
        self.name = name
        self.setting = setting
        self.alterSystem = alterSystem

    def _to_sql(self):
        setArg = None
        if (type(self.setting) == int):
            setArg = [ast.A_Const(ast.Integer(self.setting))]
        if (type(self.setting) == float):
            setArg = [ast.A_Const(ast.Float(str(self.setting)))]
        if (type(self.setting) == bool):
            setArg = [ast.A_Const(ast.String('t' if self.setting else 'f'))]
        if (type(self.setting) == str):
            setArg = [ast.A_Const(ast.String(self.setting))]

        setKind = VariableSetKind.VAR_SET_DEFAULT if setArg is None else VariableSetKind.VAR_SET_VALUE
        self.ast = ast.VariableSetStmt(
            kind=setKind,
            name=self.name,
            args=setArg
        )
        sqlstr = stream.RawStream(semicolon_after_last_statement=True)(self.ast)
        return f'ALTER SYSTEM {sqlstr}' if self.alterSystem else sqlstr
