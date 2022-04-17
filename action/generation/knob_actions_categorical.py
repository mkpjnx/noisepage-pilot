from knob_actions import KnobAction
from action import ActionGenerator
from connector import Connector


class CategoricalKnobGenerator(ActionGenerator):
    '''
    Create a ALTER SYSTEM stmt for a given knob name and numerical range
    '''

    def __init__(
            self,
            connector: Connector,
            name: str,
            values: list[str],
            alterSystem=False
    ):
        ActionGenerator.__init__(self)
        self.connector = connector
        self.name = name
        self.type = type
        self.generate_values = []
        self.alterSystem = alterSystem

        # validity check
        def get_type_with_values(name):
            # TODO(Mike): Add error checking (throw err if knob does not exists)
            query = f"SELECT vartype, enumvals FROM pg_settings WHERE name = '{name}';"
            return connector.exec_commit(query)[0]

        try:
            vartype, legal_enumvals = get_type_with_values(name)  # return 'enum', ['minimal', 'replica', 'logical']

            if vartype not in ['bool', 'enum']:
                print("vartype: {} not in ['bool', 'enum']".format(vartype))
                # TODO: raise error?
            if vartype == 'bool':
                legal_enumvals = {'on', 'off', True, False}  # strings or booleans are ok
            for val in values:
                if val not in legal_enumvals:
                    # TODO: raise error?
                    continue
                self.generate_values.append(str(val))
        except Exception as e:
            print("error in categorical args of {} : ".format(name), e)

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
