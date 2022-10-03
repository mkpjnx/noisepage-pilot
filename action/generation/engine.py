import json
import time

import rules
import yaml
from connector import Connector
from plumbum import cli
from workload.workload import Workload

import action


class ActionCatalog:
    def __init__(self):
        self.map_ident_config = {}
        self.action_map = {}
    
    def add_action(self, action: action.Action, gen_name):
        # key = None

        # Check if relevant target config exists
        if action.target.identifier not in self.map_ident_config:
            self.map_ident_config[action.target.identifier] = action.target
            self.action_map[action.target.identifier] = {'actions':[]}

        # Set the action's config to an existing one
        config = self.map_ident_config[action.target.identifier]
        action._target = config

        # Add action to relevant data (avoid dupes)
        if action not in self.action_map[config.identifier]['actions']:
            self.action_map[config.identifier]['actions'].append(action)
        return

    def export(self):
        return json.loads(action.JSONEncoder().encode(self.action_map))

class RuleEngine:
    def __init__(self, config, conn: Connector, workload_map={}):
        self.config = config
        self._conn = conn
        self.workload_map = workload_map

        self.action_catalogue = ActionCatalog()

        self.instantiate_generators()

    @property
    def connector(self):
        return self._conn

    def instantiate_generators(self):
        # To deduplicate exact same generators_map with same attrs, can use set()
        self.generators_map = {}  # generator name - generator

        for generator in self.config["Generators"]:

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
                self.action_catalogue.add_action(action, gen_name)
            print(f"\t{gen_name}\t{time.time() - ts}")
            
        ts = time.time()
        with open(output_path, "w") as f:
            converted = self.action_catalogue.export()
            json.dump(converted, fp=f, indent=4)
        print(f"\twrite_actions\t{time.time() - ts}")


class GenerateActions(cli.Application):
    output_name = cli.SwitchAttr(['-o','--output-name'], str, mandatory=True)
    config_file = cli.SwitchAttr(['-c','--config-file'], str, mandatory=True)

    def main(self):
        start_ts = time.time()
        ts = time.time()
        with open(self.config_file) as f:
            config = yaml.safe_load(f)
        print(f"load_config \t{time.time() - ts}")
        ts = time.time()

        conn = Connector(**config["Connector"])
        print(f"connected\t{time.time() - ts}")
        ts = time.time()

        workload_map = {}

        if "Workloads" in config:
            for wkload in config['Workloads']:
                print(f"{wkload['name']}: parsing csvlog {wkload['csvlog']}")
                w = Workload(wkload["csvlog"], conn)
                if 'export' in wkload:
                    w.export_sample(**wkload["export"])
                workload_map[wkload['name']] = w

        print(f"workload_process \t{time.time() - ts}")
        ts = time.time()

        engine = RuleEngine(config, conn, workload_map)

        ts = time.time()
        engine.run_generator(self.output_name)
        print(f"run_engine \t{time.time() - ts}")
        print(f"TOTAL\t{time.time() - start_ts}")

if __name__ == "__main__":
    GenerateActions.run()
