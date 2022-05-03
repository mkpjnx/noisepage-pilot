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
            name: str,
            values: List[str],
            alterSystem=False
    ):
        ActionGenerator.__init__(self)
        self.connector = connector
        self.name = name
        self.type = type
        self.generate_values = []
        self.alterSystem = alterSystem
        self.illegal_options = []
        self.illegal_knob = False

        # validity check
        try:
            vartype, legal_enumvals = connector.get_categorical_type_with_values(name)  # return 'enum', ['minimal', 'replica', 'logical']
        except IndexError as e:
            print("error in categorical args of {}, index name not found!".format(name))
            self.illegal_knob = True
            return

        if vartype not in ['bool', 'enum']:
            print("vartype: {} not in ['bool', 'enum']".format(vartype))
            raise TypeError("wrong config: knob {} type not in ['bool', 'enum']".format(name))

        if vartype == 'bool':
            legal_enumvals = {'on', 'off', True, False}  # strings or booleans are ok
        for val in values:
            if val not in legal_enumvals:
                # TODO: raise error or keep going?
                self.illegal_options.append(val)
            else:
                self.generate_values.append(str(val))


    def __iter__(self):
        for val in self.generate_values:
            yield KnobAction(self.name, val, self.alterSystem)

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
