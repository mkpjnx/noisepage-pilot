from operator import index
from typing import List
import yaml

from action import ActionGenerator
from connector import Connector
import index_actions
import knob_actions
import knob_actions_categorical
from workload import Workload


def parse_config(file: str, conn: Connector) -> List[ActionGenerator]:
    # To deduplicate exact same generators_map with same attrs, can use set()
    generators_map = {} # generator name - generator
    with open(file, 'r') as f:
        config = yaml.safe_load(f)
        for generator in config['Generators']:
            gen_type = list(generator.keys())[0]
            gen_args = generator[gen_type]
            # TODO: remove this when all generators_map are implemented
            print(gen_args)
            if not 'Name' in gen_args:
                raise Exception('All generators_map must have a name')
            if gen_args['Name'] in generators_map:
                raise Exception('Duplicated generator name {}'.format(gen_args['Name']))
            if gen_type == "DropIndexGenerator":
                # TODO: pass in specified input indexes to drop generator
                # inds = gen_args['Indexes']
                generators_map[gen_args['Name']] = index_actions.DropIndexGenerator(conn)
            elif gen_type == "ExhaustiveIndexGenerator":
                maxwidth = int(gen_args['MaxWidth'])
                generators_map[gen_args['Name']] = index_actions.ExhaustiveIndexGenerator(
                    conn, maxwidth)
            elif gen_type == "WorkloadIndexGenerator":
                if not 'WorkloadFile' in gen_args:
                    raise Exception(
                        'WorkloadIndexGenerator must have an associated file')
                workload = Workload(gen_args['WorkloadFile'], conn)
                maxwidth = int(gen_args['MaxWidth'])
                generators_map[gen_args['Name']] = index_actions.WorkloadIndexGenerator(
                    workload, maxwidth)
            elif gen_type == "TypedIndexGenerator":
                # TODO: pass in specified types to drop generator
                # types = gen_args['Types']
                if not 'Upstream' in gen_args:
                    raise Exception(
                        'TypedIndexGenerator must have an upstream generator')
                upstream = gen_args['Upstream']
                if not upstream in generators_map:
                    raise Exception(
                        'Upstream generator {} not found'.format(upstream))
                generators_map[gen_args['Name']
                           ] = index_actions.TypedIndexGenerator(generators_map[upstream])
            elif gen_type == "NumericalKnobGenerator":
                if not 'Knob' in gen_args:
                    raise Exception(
                        'NumericalKnobGenerator must have a specified knob')
                knob_name = gen_args['Knob']
                knob_type = knob_actions.KnobType[gen_args['Type']]
                minVal = float(gen_args['Min'])
                maxVal = float(gen_args['Max'])
                interval = float(gen_args['Interval'])
                generators_map[gen_args['Name']] = knob_actions.NumericalKnobGenerator(
                    conn, knob_name, knob_type, minVal, maxVal, interval)
            elif gen_type == "CategoricalKnobGenerator":
                if not 'Knob' in gen_args:
                    raise Exception(
                        'CategoricalKnobGenerator must have a specified knob')
                knob_name = gen_args['Knob']
                if not 'Values' in gen_args:
                    raise Exception(
                        'CategoricalKnobGenerator must have specified knob values')
                values = gen_args['Values']
                generators_map[gen_args['Name']] = knob_actions_categorical.CategoricalKnobGenerator(
                    conn, knob_name, values)
    return generators_map


if __name__ == "__main__":
    generators_map = parse_config('temp.yaml', Connector())
    for _, gen in generators_map.items():
        for action in gen:
            print(str(action))
