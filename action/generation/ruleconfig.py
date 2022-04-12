from operator import index
from typing import List
import yaml

from action import ActionGenerator
from connector import Connector
import index_actions, knob_actions
from workload import Workload


def parse_config(file: str, conn: Connector) -> List[ActionGenerator]:
    generators = []
    with open(file, 'r') as f:
        config = yaml.safe_load(f)
        for generator in config['Generators']:
            gen_type = list(generator.keys())[0]
            gen_args = generator[gen_type]
            if gen_type == "DropIndexGenerator":
                # TODO: pass in specified input indexes to drop generator
                # inds = gen_args['Indexes']
                generators.append(index_actions.DropIndexGenerator(conn))
            elif gen_type == "ExhaustiveIndexGenerator":
                maxwidth = int(gen_args['MaxWidth'])
                generators.append(
                    index_actions.ExhaustiveIndexGenerator(conn, maxwidth))
            elif gen_type == "WorkloadIndexGenerator":
                workload = Workload(gen_args['WorkloadFile'], conn)
                maxwidth = int(gen_args['MaxWidth'])
                generators.append(index_actions.WorkloadIndexGenerator(workload, maxwidth))
            elif gen_type == "TypedIndexGenerator":
                pass
            elif gen_type == "NumericalKnobGenerator":
                name = gen_args['Name']
                knob_type = knob_actions.KnobType[gen_args['Type']]
                minVal = float(gen_args['Min'])
                maxVal = float(gen_args['Max'])
                interval = float(gen_args['Interval'])
                generators.append(knob_actions.NumericalKnobGenerator(
                    conn, name, knob_type, minVal, maxVal, interval))
            elif gen_type == "CategoricalKnobGenerator":
                name = gen_args['Name']
                values = gen_args['Values']
                generators.append(knob_actions.CategoricalKnobGenerator(
                    conn, name, values))


def __main__():
    parse_config('temp.yaml')