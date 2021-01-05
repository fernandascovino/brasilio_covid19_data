from loguru import logger
from datetime import datetime as dt
import argparse

# UF parsers
from rj_script import main as rj_main
from pr_script import main as pr_main

scripts = {"RJ": rj_main, "PR": pr_main}

if __name__ == "__main__":

    import sys

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    parser = argparse.ArgumentParser()
    parser.add_argument("UF", help="Estado para coleta do dado [RJ|PR]")
    parser.add_argument(
        "--DD",
        help="Dia de referência (número, 2 dígitos) - Opcional, por padrão referente a hoje.",
    )
    parser.add_argument(
        "--MM",
        help="Mês de referência (número, 2 dígitos) - Opcional, por padrão referente a hoje.",
    )
    parser.add_argument(
        "--AA", help="Ano de referência - Opcional, por padrão referente a hoje.",
    )

    args = parser.parse_args()

    if not args.DD or not args.MM:
        # Data atual
        args.AA = str(dt.today().year)
        args.MM = (
            str(dt.today().month)
            if dt.today().month > 9
            else "0" + str(dt.today().month)
        )
        args.DD = (
            str(dt.today().day) if dt.today().day > 9 else "0" + str(dt.today().day)
        )

    if args.UF in scripts.keys():
        path = f"outputs/{args.UF.lower()}/{args.UF.lower()}_{args.AA}_{args.MM}_{args.DD}.xlsx"
        scripts[args.UF](args.DD, args.MM, args.AA).to_excel(path)

        logger.success("Dados salvos em: {display}", display=path)
    else:
        logger.error(
            "Script não implementado para a UF! Disponíveis: {display}",
            display=list(scripts.keys()),
        )
