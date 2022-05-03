from plumbum import cli
import ruleconfig
import connector


def run_generator(conn, config_path, output_path):
    generators = ruleconfig.parse_config(config_path, conn)
    with open(output_path, "w") as f:
        for _, gen in generators.items():
            for action in gen:
                print(str(action), file=f)


class GenerateActions(cli.Application):
    output_sql = cli.SwitchAttr("--output-sql", str, mandatory=True)
    config_file = cli.SwitchAttr("--config-file", str, mandatory=True)

    def main(self):
        self.conn = connector.Connector()
        run_generator(connector.Connector(), self.config_file, self.output_sql,)


if __name__ == "__main__":
    GenerateActions.run()
