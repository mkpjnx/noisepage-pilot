from collections import defaultdict
from plumbum import cli
from workload import Workload

import ruleconfig

import index_actions

import constants
import connector


class GenerateActions(cli.Application):
    output_sql = cli.SwitchAttr("--output-sql", str, mandatory=True)
    config_file = cli.SwitchAttr("--config-file", str, mandatory=True)
    workload_csv = cli.SwitchAttr("--workload-csv", str, mandatory=True)

    def main(self):
        self.conn = connector.Connector()
        # workload = Workload(self.workload_csv, conn)
        generators = ruleconfig.parse_config(self.config_file, self.conn)

        with open(self.output_sql, "w") as f:
            for gen in generators:
                for action in gen:
                    print(str(action), file=f)

if __name__ == "__main__":
    GenerateActions.run()
