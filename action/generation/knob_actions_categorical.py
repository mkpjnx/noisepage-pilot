from knob_actions import KnobAction
from action import ActionGenerator
from connector import Connector
from typing import List


class CategoricalKnobGenerator(ActionGenerator):
    '''
    Create a ALTER SYSTEM stmt for a given knob name and numerical range
    '''

    def __init__(
            self,
            connector: Connector,
            knob_name: str,
            values: List[str],
            alterSystem=False,
            **kwargs
    ):
        ActionGenerator.__init__(self)
        self.connector = connector
        self.knob_name = knob_name
        self.type = type
        self.generate_values = []
        self.alterSystem = alterSystem
        self.illegal_options = []
        self.illegal_knob = False

        # validity check
        knob = connector.get_config(knob_name)
        vartype = knob['vartype']
        legal_enumvals = knob['enumvals']

        if vartype not in ['bool', 'enum']:
            raise TypeError(f"{knob_name} ({vartype}) is not a categorical knob (i.e. bool or enum)")

        if vartype == 'bool':
            legal_enumvals = {True, False}  # strings or booleans are ok
        for val in values:
            if val not in legal_enumvals:
                # TODO: raise error or keep going?
                pass
            else:
                self.generate_values.append(val)


    def __iter__(self):
        for val in self.generate_values:
            yield KnobAction(self.knob_name, val, self.alterSystem)

    # def __eq__(self, other):
    #     return self.name == other.name and self.generate_values == other.generate_values and self.alterSystem == other.alterSystem


if __name__ == "__main__":
    enable = CategoricalKnobGenerator(Connector(), 'enable_seqscan', ['off', True])  # local
    actions = list(enable)
    for a in actions:
        print(a)

    wal_alter = CategoricalKnobGenerator(Connector(), 'wal_level', ['minimal', 'replica'], alterSystem=True)  # global
    actions = list(wal_alter)
    for a in actions:
        print(a)
