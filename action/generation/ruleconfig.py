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
    generators_map = {}  # generator name - generator
    with open(file, 'r') as f:
        config = yaml.safe_load(f)
        for generator in config['Generators']:
            gen_type = generator['generator']

            gen_args = generator['args']
            gen_args = gen_args if gen_args is not None else {}

            gen_name = generator['name']

            new_generator = None
            gen_args['connector'] = conn

            # TODO: remove this when all generators_map are implemented
            print(generator)
            if gen_name in generators_map:
                raise Exception('Duplicated generator name {}'.format(gen_name))
            # Construct args
            if gen_type == "DropIndexGenerator":
                new_generator = index_actions.DropIndexGenerator(**gen_args)
            elif gen_type == "ExhaustiveIndexGenerator":
                new_generator = index_actions.ExhaustiveIndexGenerator(**gen_args)
            elif gen_type == "WorkloadIndexGenerator":
                gen_args['workload'] = Workload(gen_args['workload'], conn)
                new_generator = index_actions.WorkloadIndexGenerator(**gen_args)
            elif gen_type == "TypedIndexGenerator":
                upstream = gen_args['upstream']
                if not gen_args['upstream'] in generators_map:
                    raise Exception(
                        'Upstream generator {} not found'.format(upstream))
                gen_args['upstream'] = generators_map[upstream]
                new_generator = index_actions.TypedIndexGenerator(**gen_args)
            elif gen_type == "NumericalKnobGenerator":
                gen_args['mode'] = knob_actions.KnobType[gen_args['mode']]
                new_generator = knob_actions.NumericalKnobGenerator(**gen_args)
            elif gen_type == "CategoricalKnobGenerator":
                new_generator = knob_actions_categorical.CategoricalKnobGenerator(
                    **gen_args)
            generators_map[gen_name] = new_generator
    return generators_map


if __name__ == "__main__":
    generators_map = parse_config('temp.yaml', Connector())
    for _, gen in generators_map.items():
        for action in gen:
            print(str(action))
