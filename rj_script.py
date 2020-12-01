from urllib.request import urlopen
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from difflib import get_close_matches
import re
from loguru import logger
import os
import ssl

from utils import fix_typos


def get_report_url(day, month):
    """
    Busca URL do boletim da data especificada e retorna conteúdo.

    Args:
        day (str): Dia no formato DD/MM/AAAA
        month (str): Mês no formato DD/MM/AAAA       
    """

    # TODO: melhorar função
    # Busca no site de Covid-19 criado pelo estado
    try:
        if not os.environ.get("PYTHONHTTPSVERIFY", "") and getattr(
            ssl, "_create_unverified_context", None
        ):
            ssl._create_default_https_context = ssl._create_unverified_context

        url = "https://coronavirus.rj.gov.br/boletins/"
        soup = (
            BeautifulSoup(urlopen(url), "html.parser")
            .find("div", {"class": "entry-content"})
            .select(".elementor-post__read-more")
        )

    # Caso falhe, busca direto nas notícias da secretaria de saúde
    except:
        boletim = f"https://www.saude.rj.gov.br/noticias/2020/11/boletim-coronavirus-{day}{month}"

        logger.info(
            "Site de boletins está fora do ar (https://coronavirus.rj.gov.br/boletins/). Dados retirados do site da secretaria de saúde: {display}\n",
            display=boletim,
        )
        return (
            BeautifulSoup(urlopen(boletim), features="lxml")
            .find("div", {"class": "materia"})
            .find("span", {"class": "texto"})
        )

    boletim = [
        i
        for i in soup
        if "coronavirus" + "-" + str(day) + "-" + str(month) in i["href"]
    ][0]["href"]

    if len(boletim) == 0:
        logger.warning(
            "Boletim não indexado ou ainda não atualizado! Último boletim: {display}\n",
            display=soup[0]["href"],
        )
        return

    elif day == "30" and month == "10":
        boletim = "https://coronavirus.rj.gov.br/boletim/boletim-coronavirus-31-10-20-600-obitos-e-309-977-casos-confirmados-no-rj/"

    logger.info("URL Boletim: {display}\n", display=boletim[0]["href"])
    return BeautifulSoup(urlopen(boletim), features="lxml").find(
        "div", {"class": "entry-content"}
    )


def treat_data(boletim):
    """
    Transforma dados de confirmados e mortes  por cidade de texto corrido em dataframe
    
    Args:
        boletim (bs4.element.Tag): Conteúdo do corpo do boletim retornado por `get_report_url`
    
    Returns:
        pd.Dataframe: Tabela de `confirmados` e `mortes` por cidade
    """

    # Limpa div (pega somente p que possui texto, não recursivo)
    cleaned_div = [i for i in boletim.find_all("p") if len(i.find_all("p")) == 0]

    # Trata formatação de confirmados e mortes das cidades
    # TODO: melhorar condicional... diferenciar estrutura de cidade por `p` X numa mesma `p`, separado por `br`
    if len(cleaned_div) == 2:
        cleaned_div = cleaned_div[0].text.split("\r\n") + cleaned_div[1].text.split(
            "\r\n"
        )
    elif len(cleaned_div) == 5:
        aux = cleaned_div.copy()
        cleaned_div = []
        for i in range(len(aux)):
            cleaned_div += aux[i].text.split("\r\n")
    else:
        cleaned_div = [i.text for i in cleaned_div]

    cleaned_div = [i for i in cleaned_div if i not in ["\xa0", "\n", ""]]

    init_casos = [
        i + 1
        for i, e in enumerate(cleaned_div)
        if "casos confirmados estão distribuídos" in e
    ][0]

    init_mortes = [
        i + 1 for i, e in enumerate(cleaned_div) if "vítimas de Covid-19 no estado" in e
    ][0]

    seps = "– |- "

    content = {
        "confirmados": {
            re.split(seps, x)[0].strip(): int(
                re.split(seps, x)[1].strip("\n").replace(".", "")
            )
            for x in cleaned_div[init_casos : init_mortes - 1]
        },
        "mortes": {
            re.split(seps, x)[0].strip(): int(
                re.split(seps, x)[1].strip("\n").replace(".", "")
            )
            for x in cleaned_div[init_mortes:-1]
        },
    }

    # Adiciona total do estado
    total = {
        "confirmados": "casos confirmados",
        "mortes": "óbitos por Coronavírus",
    }

    for tipo in content.keys():
        content[tipo]["TOTAL NO ESTADO"] = int(
            re.search(
                "\d+(.|)\d+(?=\s+{})".format(total[tipo]),
                cleaned_div[0].replace("\xa0", " "),
                flags=re.IGNORECASE,
            )
            .group()
            .replace(".", "")
        )

    # (3) Finaliza a tabela
    df = pd.DataFrame(content).dropna(subset=["confirmados"]).fillna(0).astype(int)
    df.index.name = "municipio"

    return df


def main(day, month):

    # (1) Busca URL do boletim da data especificada e retorna conteúdo.
    boletim = get_report_url(day, month)
    df = treat_data(boletim)

    tricky = {
        "MUNICÍPIO EM INVESTIGAÇÃO": "IMPORTADOS/INDEFINIDOS",
        "OUTRO ESTADO": "IMPORTADOS/INDEFINIDOS",
        "OUTROS ESTADOS": "IMPORTADOS/INDEFINIDOS",
        "VARRESAI": "VARRE-SAI",
        "PAGE": "MAGÉ",
        "ÍTALA": "ITALVA",
    }

    # (2) Conserta nomes de municípios com base no modelo da UF.
    df = fix_typos(df, uf="RJ", tricky=tricky)

    # (3) Completa importados
    if not "Importados/Indefinidos" in df.index:
        df.loc["Importados/Indefinidos"] = [0, 0]

    # (4) Checa total
    if any(
        df[df.index != "TOTAL NO ESTADO"].sum().values
        != df.loc["TOTAL NO ESTADO"].values
    ):
        logger.info(
            "SOMA DIVERGENTE \nTotal de mortes do estado difere da soma dos municípios divulgado pela Secretaria: \n\n* Soma dos municípios: {display1}\n* Total do estado:\n{display2}\n",
            display1=df[df.index != "TOTAL NO ESTADO"].sum().to_dict(),
            display2=df.loc["TOTAL NO ESTADO"].to_dict(),
        )
    else:
        logger.info(
            "Total no estado: \n{display}\n", display=df.loc["TOTAL NO ESTADO"],
        )

    return df


if __name__ == "__main__":
    pass
