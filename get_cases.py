import yaml
from loguru import logger
import rj_script, pr_script
import pandas as pd


@logger.catch()
def collect_state_report(date, uf):

    config = yaml.load(open("config.yml", "r"))

    if uf == "PR":
        df = pr_script.main(date, config, uf)
        path = "outputs/pr/pr_{}.xlsx".format(date[3:] + "_" + date[:2])

    if uf == "RJ":
        df = rj_script.main(date, config, uf)
        path = "outputs/rj/rj_{}.xlsx".format(date[3:] + "_" + date[:2])

    if not isinstance(df, pd.DataFrame):
        logger.error("Output não é um dataframe!")
        return

    df.to_excel(path)
    logger.success("Dados salvos em: {display}", display=path)


if __name__ == "__main__":

    # FROM TERMINAL: $ python get_cases [DD_MM] [UF]
    import sys

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    collect_state_report(sys.argv[1], sys.argv[2])
