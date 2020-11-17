from urllib.request import urlopen
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from difflib import get_close_matches
import re
from loguru import logger


def treat_city_name(name, list_names):
    """
    Busca nome mais proximo dado lista de nomes e substitui termos para Importados/Indefinidos.
    """

    if name in ["Município em investigação", "Outro estado", "Outros estados"]:
        return "Importados/Indefinidos"

    if name == "VarreSai":
        return "Varre-Sai"

    if name == "Page":
        return "Magé"

    return get_close_matches(name, list_names, cutoff=0.75)[0]


def main(day, month):

    # (1) Busca URL do boletim de hoje
    url = "https://coronavirus.rj.gov.br/boletins/"
    soup = (
        BeautifulSoup(urlopen(url), "html.parser")
        .find("div", {"class": "entry-content"})
        .select(".elementor-post__read-more")
    )

    boletim = [
        i
        for i in soup
        if "coronavirus" + "-" + str(day) + "-" + str(month) in i["href"]
    ]

    # URL errada dia 30/10
    if day == "30" and month == "10":
        boletim = [
            {
                "href": "https://coronavirus.rj.gov.br/boletim/boletim-coronavirus-31-10-20-600-obitos-e-309-977-casos-confirmados-no-rj/"
            }
        ]

    if len(boletim) == 0:
        logger.warning(
            "Boletim não indexado ou ainda não atualizado! Último boletim: {display}",
            display=soup[0]["href"],
        )
    else:
        logger.info("URL Boletim: {display}", display=boletim[0]["href"])
        boletim = BeautifulSoup(urlopen(boletim[0]["href"]), features="lxml").find(
            "div", {"class": "entry-content"}
        )

    # (2) Carrega e trata os dados
    if len(boletim.findAll("p")) > 10:

        # Cria tabela com nomes padrão das cidades
        rj_model = pd.read_excel("models/RJ_modelo.xlsx", index_col=0)
        df = pd.DataFrame(columns=["confirmados", "mortes"], index=rj_model.index)

        # Limpa div (pega somente p que possui texto, não recursivo)
        cleaned_div = [i for i in boletim.find_all("p") if len(i.find_all("p")) == 0]

        # Trata totais
        total = {
            "confirmados": "casos confirmados",
            "mortes": "óbitos por Coronavírus",
        }

        for tipo in total.keys():

            df.loc["TOTAL NO ESTADO", tipo] = int(
                re.search(
                    "\d+(.|)\d+(?=\s+{})".format(total[tipo]),
                    cleaned_div[0].text.replace("\xa0", " "),
                    flags=re.IGNORECASE,
                )
                .group()
                .replace(".", "")
            )

        # Trata confirmados e mortes das cidades
        init_casos = [
            i + 1
            for i, e in enumerate(cleaned_div)
            if "casos confirmados estão distribuídos" in e.text
        ][0]
        init_mortes = [
            i + 1
            for i, e in enumerate(cleaned_div)
            if "vítimas de Covid-19 no estado" in e.text
        ][0]

        content = {
            "confirmados": cleaned_div[init_casos : init_mortes - 1],
            "mortes": cleaned_div[init_mortes:-1],
        }

        for tipo in content.keys():
            for i in content[tipo]:
                try:
                    separator = [sep for sep in ["– ", "- "] if sep in i.text][0]

                    city_name = treat_city_name(
                        i.text.split(separator)[0].strip(), df.index
                    )
                    value = (
                        i.text.split(separator)[1]
                        .replace("–", "")
                        .strip()
                        .replace(".", "")
                    )

                    df.loc[city_name, tipo] = value

                except Exception as e:
                    city_name = i.text.split("–")[0].strip()

                    logger.warning(
                        "Cidade não encontrada: {display}", display=city_name,
                    )

                    logger.info(
                        "\nErro: {display}", display=e,
                    )

    # (3) Finaliza a tabela
    df = (
        df.dropna(subset=["confirmados"])
        .fillna(0)
        .assign(
            confirmados=lambda df: df.confirmados.astype(int),
            mortes=lambda df: df.mortes.astype(int),
        )
    )

    if not "Importados/Indefinidos" in df.index:
        df.loc["Importados/Indefinidos"] = [0, 0]

    # (4) Checa total
    cidades = df[df.index != "TOTAL NO ESTADO"]
    if any(cidades.sum().values != df.loc["TOTAL NO ESTADO"].values):
        logger.info(
            "Soma das cidades diverge do total do estado: \n==> Soma:\n{display1}\n==> Total pela Secretaria:\n{display2}",
            display1=cidades.sum(),
            display2=df.loc["TOTAL NO ESTADO"],
        )

    return df


if __name__ == "__main__":
    pass
