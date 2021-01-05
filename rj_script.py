from urllib.request import urlopen
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from difflib import get_close_matches
import re
from loguru import logger
import os
import ssl
import unidecode
import time

from utils import fix_typos


def _search_boletim_page(search):
    """
    Busca URL da data especificada em search em todas as páginas do site
    Boletins Coronavírus RJ: https://coronavirus.rj.gov.br/boletins/

    Args:
        search (str): texto para busca do boletim da data ('coronavirus-DD-MM')
    """
    if not os.environ.get("PYTHONHTTPSVERIFY", "") and getattr(
        ssl, "_create_unverified_context", None
    ):

        # Cria autenticacao para navegar a pagina
        ssl._create_default_https_context = ssl._create_unverified_context

        # Itera nos links da pagina atual e proximas para encontrar url
        # que contenha search
        url = "https://coronavirus.rj.gov.br/boletins/1"
        boletim = None
        page_next = 2
        links = ["qualquer", "coisa"]

        while len(links) > 0:
            # Obtem links dos boletins da pagina atual
            links = [
                i["href"]
                for i in BeautifulSoup(urlopen(url), "html.parser").find_all(
                    name="a", attrs={"class": "elementor-post__thumbnail__link"}
                )
            ]
            for link in links:
                if search in link:
                    boletim = link
                    break
            if not boletim:
                url = url[:-1] + str(page_next)
                page_next += 1
            else:
                break

        return boletim


def get_report_url(day, month, year):
    """
    Busca URL do boletim da data especificada e retorna conteúdo.

    Args:
        day (str): Dia no formato DD
        month (str): Mês no formato MM  
        year (str): Ano no formato AA (ex: 2021 -> 21)     
    """

    # Busca primeiro no site de boletins
    try:
        search = "coronavirus" + "-" + str(day) + "-" + str(month) + "-" + str(year)
        boletim = _search_boletim_page(search)

        if search == "coronavirus-31-10-20":
            boletim = "https://coronavirus.rj.gov.br/boletim/boletim-coronavirus-31-10-20-600-obitos-e-309-977-casos-confirmados-no-rj/"

        if boletim:
            logger.info("URL Boletim: {display}\n", display=boletim)

        time.sleep(2)
        boletim = BeautifulSoup(urlopen(boletim), features="lxml")
        return boletim.find("div", {"class": "entry-content"})

    # Caso falhe, busca direto nas notícias da secretaria de saúde
    except:
        boletim = f"https://www.saude.rj.gov.br/noticias/20{year}/{month}/boletim-coronavirus-{day}{month}"

        try:
            content = (
                BeautifulSoup(urlopen(boletim), features="lxml")
                .find("div", {"class": "materia"})
                .find("span", {"class": "texto"})
            )

            # TODO: remover repeticao de codigo
            logger.info(
                "Site de boletins está fora do ar (https://coronavirus.rj.gov.br/boletins/). Dados retirados do site da secretaria de saúde.\n"
            )
            logger.info("URL Boletim: {display}\n", display=boletim)
            return content

        except:
            logger.info(
                "URL não encontrada nos boletins (https://coronavirus.rj.gov.br/boletins/) nem nas notícias da SES (https://www.saude.rj.gov.br/noticias/2020/)\n",
                display=boletim,
            )
            return


def treat_data(boletim):
    """
    Transforma dados de confirmados e mortes  por cidade de texto corrido em dataframe
    
    Args:
        boletim (bs4.element.Tag): Conteúdo do corpo do boletim retornado por `get_report_url`
    
    Returns:
        pd.Dataframe: Tabela de `confirmados` e `mortes` por cidade
    """

    # (1) Limpa div: pega somente parágrafos que não vazios na camada
    # mais alta do html (i.e. não recursivo)
    cleaned_div = [i for i in boletim.find_all("p") if len(i.find_all("p")) == 0]

    # (2) Conserta estrutura do texto corrido para lista: ao final da iteração,
    # a lista deve ser composta SOMENTE de "Cidade - XXXXX" e parágrafos da notícia.
    aux = cleaned_div.copy()
    cleaned_div = list()
    # Itera na lista de parágrafos: pode ter diversas estruturas, desde
    # todas as cidades numa mesma tag ("p") até uma cidade por tag ("p").
    for i in range(len(aux)):
        text = aux[i].text.split("\r\n")
        # Caso todas as cidades estejam na mesma tag ("p"), cria
        # lista por quebra de linha ("\n" transformado do html "</br>")
        nested = [x.split("\n") for x in text]
        # Remove textos vazios
        null = ["\xa0", "\n", ""]
        flatten = [
            item
            for sublist in nested
            for item in sublist
            if len(item) > 0 and item not in null
        ]
        # Adiciona os itens na lista de casos e mortes por município
        cleaned_div += flatten

    # 22/12: Conserta caso com \n entre cidade e valor
    # x = [
    #     i for i, e in enumerate(cleaned_div) if "São José do Vale do Rio Preto –" in e
    # ][0]
    # cleaned_div[x] += " " + cleaned_div[x + 1]
    # cleaned_div.pop(x + 1)

    # (3) Identifica casos e mortes e cria dicionário de dados dos municípios
    init_casos = [
        i + 1
        for i, e in enumerate(cleaned_div)
        if "casos confirmados estão distribuídos" in e
    ][0]

    init_mortes = [
        i + 1 for i, e in enumerate(cleaned_div) if "vítimas de Covid-19 no estado" in e
    ][0]

    seps = "– |- |– "

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

    # Remove acentos e transforma em uppercase para normalizar nomes
    for tipo in content.keys():
        content[tipo] = {
            unidecode.unidecode(k.upper().replace("  ", " ")): v
            for k, v in content[tipo].items()
        }

    # Adiciona total do estado
    total = {
        "confirmados": "casos confirmados",
        "mortes": "óbitos por Coronavírus",
    }

    tricky = {
        "MUNICÍPIO EM INVESTIGAÇÃO": "IMPORTADOS/INDEFINIDOS",
        "OUTRO ESTADO": "IMPORTADOS/INDEFINIDOS",
        "OUTROS ESTADOS": "IMPORTADOS/INDEFINIDOS",
        "VARRESAI": "VARRE-SAI",
        "PAGE": "MAGE",
        "ÍTALA": "ITALVA",
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

        for old_key, new_key in tricky.items():
            try:
                content[tipo][new_key] = content[tipo].pop(old_key)
            except:
                pass

    # (3) Finaliza a tabela
    df = pd.DataFrame(content).dropna(subset=["confirmados"]).fillna(0).astype(int)
    df.index.name = "municipio"

    return df


def main(day, month, year):

    # (1) Busca URL do boletim da data especificada e retorna conteúdo.
    boletim = get_report_url(day, month, year[2:])
    df = treat_data(boletim)

    # (2) Conserta nomes de municípios com base no modelo da UF.
    df = fix_typos(df, uf="RJ")

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
