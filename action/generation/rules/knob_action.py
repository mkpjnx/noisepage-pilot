import pglast.enums.parsenodes as pnodes
from pglast import ast, stream

from action import Action, Configuration


class Knob(Configuration):
    def __init__(self, name, precision = 4):
        self._name = name
        self._precision = precision

    @property
    def identifier(self):
        return f"Knob({self._name})"

    @property
    def name(self):
        return self._name
    
    @property
    def precision(self):
        return self._precision


class KnobAction(Action):
    def __init__(self, target: Knob, setting=None, alterSystem=False):
        Action.__init__(self, target)
        self.alterSystem = alterSystem
        if self.target.precision is not None and type(setting) == float:
            self.setting = round(setting, self.target.precision)
        else:
            self.setting = setting

    def to_json(self):
        return {
            "type": "KnobAction",
            "target": self.target.identifier,
            "val": self.setting,
            "alter_system": self.alterSystem,
        }

    def to_sql(self):
        setArg = None
        if type(self.setting) == int:
            setArg = [ast.A_Const(ast.Integer(self.setting))]
        if type(self.setting) == float:
            setArg = [ast.A_Const(ast.Float(str(self.setting)))]
        if type(self.setting) == bool:
            setArg = [ast.A_Const(ast.String("t" if self.setting else "f"))]
        if type(self.setting) == str:
            setArg = [ast.A_Const(ast.String(self.setting))]

        setKind = pnodes.VariableSetKind.VAR_SET_DEFAULT if setArg is None else pnodes.VariableSetKind.VAR_SET_VALUE
        self.ast = ast.VariableSetStmt(kind=setKind, name=self.target.name, args=setArg)
        sqlstr = stream.RawStream(semicolon_after_last_statement=True)(self.ast)
        return f"ALTER SYSTEM {sqlstr}" if self.alterSystem else sqlstr
