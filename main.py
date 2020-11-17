from loguru import logger
from datetime import datetime as dt
import argparse

# UF parsers
from rj_script import main as rj_main
from pr_script import main as pr_main


if __name__ == "__main__":

    import sys

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    parser = argparse.ArgumentParser()
    parser.add_argument("UF", help="Estado para coleta do dado [RJ|PR]")
    parser.add_argument(
        "--DD",
        help="Dia de referência (número, 2 dígitos) - Opcional, por padrão hoje.",
    )
    parser.add_argument(
        "--MM",
        help="Mês de referência (número, 2 dígitos) - Opcional, por padrão hoje.",
    )

    args = parser.parse_args()

    if not args.DD or not args.MM:
        # Data atual
        args.MM = (
            str(dt.today().month)
            if dt.today().month > 9
            else "0" + str(dt.today().month)
        )
        args.DD = (
            str(dt.today().day) if dt.today().day > 9 else "0" + str(dt.today().day)
        )

    if args.UF == "RJ":
        df = rj_main(args.DD, args.MM)

    path = f"outputs/{args.UF.lower()}/{args.UF.lower()}_{args.MM}_{args.DD}.xlsx"

    df.to_excel(path)
    logger.success("Dados salvos em: {display}", display=path)
