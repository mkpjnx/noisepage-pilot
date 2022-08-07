from typing import List, Dict
from plumbum import cli
import yaml, json

from action import Action

from connector import Connector
from workload import Workload
from action import ActionGenerator

import rules

class RuleEngine:
    def __init__(self, config, conn: Connector):
        self.config = config
        self._conn = conn

        self.config_map = {}
        self.action_map = {}

        self.instantiate_generators()

    @property
    def connector(self):
        return self._conn

    def add_action(self, action: Action, gen_name):
        key = None

        # Check if relevant target config exists
        if action.target.identifier not in self.config_map:
            self.config_map[action.target.identifier] = action.target
            self.action_map[action.target] = []
        
        # Set the action's config to an existing one
        config = self.config_map[action.target.identifier] 
        action._target = config

        # Add action to relevant data
        # TODO: check for duplicates
        self.action_map[config].append(action)
        return

    def export(self):
        return {str(k):[a.to_json() for a in v] for k,v in self.action_map.items()}

    def instantiate_generators(self):
        # To deduplicate exact same generators_map with same attrs, can use set()
        self.generators_map = {}  # generator name - generator

        for generator in self.config['Generators']:
            print(generator)

            gen_type = generator['generator']
            gen_name = generator['name']

            gen_args = generator['args']
            gen_args = {} if gen_args is None else gen_args
            
            gen_args['connector'] = self.connector

            new_generator = None

            if 'upstream' in gen_args:
                upstream_name = gen_args['upstream']
                if upstream_name not in self.generators_map:
                    raise Exception(
                        'Upstream generator {} not found'.format(upstream_name))
                gen_args['upstream'] = self.generators_map[upstream_name]

            if 'workload' in gen_args:
                gen_args['workload'] = Workload(gen_args['workload'], self.connector)

            if gen_name in self.generators_map:
                raise Exception('Duplicated generator name {}'.format(gen_name))

            # Instantiate generator from installed rules
            new_generator = rules.__dict__[gen_type](**gen_args)
            self.generators_map[gen_name] = new_generator

    def run_generator(self, output_path):
        for gen_name, gen in self.generators_map.items():
            for action in gen.get_action():
                self.add_action(action, gen_name)

        with open(output_path, "w") as f:
            converted = self.export()
            print(json.dumps(converted,indent=4))
            json.dump(converted, fp = f, indent=4)

class GenerateActions(cli.Application):
    output_sql = cli.SwitchAttr("--output-sql", str, mandatory=True)
    config_file = cli.SwitchAttr("--config-file", str, mandatory=True)

    def main(self):
        with open(self.config_file) as f:
            config = yaml.safe_load(f)
        conn = Connector(config['Connector'])
        engine = RuleEngine(config, conn)
        engine.run_generator(self.output_sql)

if __name__ == "__main__":
    GenerateActions.run()
