import copy
import itertools

from action import ActionGenerator
from action import Action

from connector import Connector

from pglast import ast, stream
from pglast.enums.parsenodes import *

from enum import Enum


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


class KnobType(Enum):
    DELTA = auto()
    PCT = auto()
    ABSOLUTE = auto()
    POW2 = auto()


class NumericalKnobGenerator(ActionGenerator):
    '''
    Create a ALTER SYSTEM stmt for a given knob name and numerical range
    '''

    def __init__(
        self,
        connector: Connector,
        knob_name: str,
        mode: KnobType = KnobType.PCT,
        min_val: float = 0.1,
        max_val: float = 5,
        interval: float = 0.1,
        **kwargs
    ):
        ActionGenerator.__init__(self)
        self.connector = connector
        self.mode = mode
        self.min_val = min_val
        self.max_val = max_val
        self.interval = interval

        knob = connector.get_config(knob_name)

        if knob['vartype'] not in ['integer', 'real']:
            raise TypeError(
                f"{knob} ({knob['vartype']}) is not a numerical knob (i.e. real or integer)")

        self.valType = float if knob['vartype'] == 'real' else int
        self.cur_val = self.valType(knob['setting'])
        self.knob = knob

    def __iter__(self):
        val = self.cur_val
        change = self.min_val
        while change <= self.max_val:
            new_val = val
            if self.mode == KnobType.PCT:
                new_val = val * change
            elif self.mode == KnobType.DELTA:
                new_val = val + change
            elif self.mode == KnobType.ABSOLUTE:
                new_val = change
            elif self.mode == KnobType.POW2:
                new_val = 2**change
            else:
                new_val = None

            # Check legality:
            if new_val > self.valType(self.knob['max_val']):
                raise ValueError(
                    f"max_val exceeds legal limit ({self.knob['max_val']}) for {self.knob['name']}")

            if new_val < self.valType(self.knob['min_val']):
                raise ValueError(
                    f"min_val exceeds legal limit ({self.knob['min_val']}) for {self.knob['name']}")

            yield KnobAction(self.knob['name'], new_val)
            change += self.interval
