import json
import time

import rules
import yaml
from connector import Connector
from plumbum import cli
from workload.workload import Workload

from action import Action


class RuleEngine:
    def __init__(self, config, conn: Connector, workload_map={}):
        self.config = config
        self._conn = conn
        self.workload_map = workload_map

        self.config_map = {}
        self.action_map = {}

        self.instantiate_generators()

    @property
    def connector(self):
        return self._conn

    def add_action(self, action: Action, gen_name):
        # key = None

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
        return {str(k): [a.to_json() for a in v] for k, v in self.action_map.items()}

    def instantiate_generators(self):
        # To deduplicate exact same generators_map with same attrs, can use set()
        self.generators_map = {}  # generator name - generator

        for generator in self.config["Generators"]:
            print(generator)

            gen_type = generator["generator"]
            gen_name = generator["name"]

            gen_args = generator["args"]
            gen_args = {} if gen_args is None else gen_args

            gen_args["connector"] = self.connector

            new_generator = None

            if "upstream" in gen_args:
                upstream_name = gen_args["upstream"]
                if upstream_name not in self.generators_map:
                    raise Exception("Upstream generator {} not found".format(upstream_name))
                gen_args["upstream"] = self.generators_map[upstream_name]

            if "workload" in gen_args:
                fname = gen_args["workload"]
                if fname not in self.workload_map:
                    workload = Workload(gen_args["workload"], self.connector)
                    self.workload_map[fname] = workload
                gen_args["workload"] = self.workload_map[fname]

            if gen_name in self.generators_map:
                raise Exception("Duplicated generator name {}".format(gen_name))

            # Instantiate generator from installed rules
            new_generator = rules.__dict__[gen_type](**gen_args)
            self.generators_map[gen_name] = new_generator

    def run_generator(self, output_path):
        for gen_name, gen in self.generators_map.items():
            ts = time.time()
            for action in gen.get_action():
                self.add_action(action, gen_name)
            print(f"{gen_name} ({time.time() - ts}s)")

        with open(output_path, "w") as f:
            converted = self.export()
            json.dump(converted, fp=f, indent=4)


class GenerateActions(cli.Application):
    output_sql = cli.SwitchAttr("--output-sql", str, mandatory=True)
    config_file = cli.SwitchAttr("--config-file", str, mandatory=True)

    def main(self):
        with open(self.config_file) as f:
            config = yaml.safe_load(f)
        print(config["Connector"])
        conn = Connector(**config["Connector"])

        workload_map = {}
        if "Workload" in config:
            w = Workload(config["Workload"]["csvlog"], conn)
            w.export_sample(**config["Workload"]["export"])

            workload_map[config["Workload"]["csvlog"]] = w

        engine = RuleEngine(config, conn, workload_map)
        engine.run_generator(self.output_sql)


if __name__ == "__main__":
    GenerateActions.run()
