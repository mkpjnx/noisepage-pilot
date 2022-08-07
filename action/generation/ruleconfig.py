from typing import List, Dict
import yaml

from action import ActionGenerator
from connector import Connector

import rules

from workload import Workload


def parse_config(file: str, conn: Connector) -> Dict[str, ActionGenerator]:
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

            if 'upstream' in gen_args:
                upstream = gen_args['upstream']
                if not gen_args['upstream'] in generators_map:
                    raise Exception(
                        'Upstream generator {} not found'.format(upstream))
                gen_args['upstream'] = generators_map[upstream]

            if 'workload' in gen_args:
                gen_args['workload'] = Workload(gen_args['workload'], conn)
            # TODO: remove this when all generators_map are implemented

            print(generator)
            if gen_name in generators_map:
                raise Exception('Duplicated generator name {}'.format(gen_name))

            new_generator = rules.__dict__[gen_type](**gen_args)
            generators_map[gen_name] = new_generator
    return generators_map
