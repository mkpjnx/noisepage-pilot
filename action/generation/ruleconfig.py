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
    # To deduplicate exact same generators with same attrs, can use generators = set()
    generators_map = {}
    with open(file, 'r') as f:
        config = yaml.safe_load(f)
        for generator in config['Generators']:
            gen_type = list(generator.keys())[0]
            gen_args = generator[gen_type]
            # TODO: remove this when all generators are implemented
            print(gen_args)
            if not gen_args.has_key('Name'):
                raise Exception('All generators must have a name')
            if gen_args['Name'] in generators_map.keys():
                raise Exception('Duplicated generator name {}'.format(gen_args['Name']))
            if gen_type == "DropIndexGenerator":
                # TODO: pass in specified input indexes to drop generator
                # inds = gen_args['Indexes']
                generators[gen_args['Name']] = index_actions.DropIndexGenerator(conn)
            elif gen_type == "ExhaustiveIndexGenerator":
                maxwidth = int(gen_args['MaxWidth'])
                generators[gen_args['Name']] = index_actions.ExhaustiveIndexGenerator(
                    conn, maxwidth)
            elif gen_type == "WorkloadIndexGenerator":
                if not gen_args.has_key('WorkloadFile'):
                    raise Exception(
                        'WorkloadIndexGenerator must have an associated file')
                workload = Workload(gen_args['WorkloadFile'], conn)
                maxwidth = int(gen_args['MaxWidth'])
                generators[gen_args['Name']] = index_actions.WorkloadIndexGenerator(
                    workload, maxwidth)
            elif gen_type == "TypedIndexGenerator":
                # TODO: pass in specified types to drop generator
                # types = gen_args['Types']
                if not gen_args.has_key('Upstream'):
                    raise Exception(
                        'TypedIndexGenerator must have an upstream generator')
                upstream = gen_args['Upstream']
                if not generators.has_key(upstream):
                    raise Exception(
                        'Upstream generator {} not found'.format(upstream))
                generators[gen_args['Name']
                           ] = index_actions.TypedIndexGenerator(upstream)
            elif gen_type == "NumericalKnobGenerator":
                if not gen_args.has_key('Knob'):
                    raise Exception(
                        'NumericalKnobGenerator must have a specified knob')
                knob_name = gen_args['Knob']
                knob_type = knob_actions.KnobType[gen_args['Type']]
                minVal = float(gen_args['Min'])
                maxVal = float(gen_args['Max'])
                interval = float(gen_args['Interval'])
                generators[gen_args['Name']] = knob_actions.NumericalKnobGenerator(
                    conn, knob_name, knob_type, minVal, maxVal, interval)
            elif gen_type == "CategoricalKnobGenerator":
                if not gen_args.has_key('Knob'):
                    raise Exception(
                        'CategoricalKnobGenerator must have a specified knob')
                knob_name = gen_args['Knob']
                if not gen_args.has_key('Values'):
                    raise Exception(
                        'CategoricalKnobGenerator must have specified knob values')
                values = gen_args['Values']
                generators[gen_args['Name']] = knob_actions_categorical.CategoricalKnobGenerator(
                    conn, knob_name, values)
    return generators


if __name__ == "__main__":
    generators = parse_config('temp.yaml', Connector())
    for _, gen in generators:
        for action in gen:
            print(str(action))
