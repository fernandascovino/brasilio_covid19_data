from urllib.request import urlopen
from bs4 import BeautifulSoup
import bs4
import numpy as np
import pandas as pd
import re
from loguru import logger

from utils import fix_typos


def get_report_url(date, filename="Geral.csv"):
    """
    Busca URL do boletim da data especificada.

    Args:
        date (str): Data no formato DD/MM/AAAA
        filename (str): Opcional. Tipo de arquivo que se deseja buscar.

            * Agregado por município: 'Geral.csv'
            * Microdados: 'Casos e Óbitos.csv'
            * PDF: 'Informe Completo e Detalhado.pdf'        
    """

    baseurl = "https://www.saude.pr.gov.br/Pagina/Coronavirus-COVID-19"

    section = {
        "Geral.csv": "Boletim - Informe Epidemiológico Coronavírus (COVID-19) - Arquivos CSV",
        "Casos e Óbitos.csv": "Boletim - Informe Epidemiológico Coronavírus (COVID-19) - Arquivos CSV",
        "Informe Completo e Detalhado.pdf": "Boletim - Informe Epidemiológico Coronavírus (COVID-19)",
    }

    tag = (
        BeautifulSoup(urlopen(baseurl), "html.parser")
        .find(
            "div",
            {
                "class": "field field--name-field-texto field--type-text-long field--label-hidden field--item"
            },
        )
        .find(
            lambda tag: tag.name == "p"
            and date in tag.text
            and tag.parent.parent.parent.parent.parent.parent.parent.parent.find(
                text=section[filename]
            )
        )
        .parent.parent.parent.find(text=re.compile(f"^{filename}"))
        .parent.parent
    )

    if tag.find("a"):
        return tag.find("a")["href"]
    elif tag["href"]:
        return tag["href"]


def main(day, month):

    # (1) Busca URL do boletim de hoje
    date = day + "/" + month + "/2020"

    try:
        tables = {
            "geral": get_report_url(date, filename="Geral.csv"),
            "municipios": get_report_url(date, filename="Casos e Óbitos.csv"),
        }

        logger.info("URL Boletim (CSV): {display}", display=tables["municipios"])
    except Exception as e:
        logger.error("Boletim não encontrado! - ERRO: {error}", error=e)

    # TODO: verificar erros na estrutura HTML de algumas datas para o PDF
    try:
        logger.info(
            "URL Boletim (PDF): {display}",
            display=get_report_url(date, filename="Informe Completo e Detalhado.pdf"),
        )
    except:
        pass

    for k in tables.keys():
        try:
            tables[k] = pd.read_csv(tables[k], sep=";", skiprows=0, low_memory=False)
        except:
            tables[k] = pd.read_csv(
                tables[k], sep=";", skiprows=0, encoding="latin-1", low_memory=False
            )

    # (2) Contabiliza importados (tem somente nos microdados)
    importados = {
        "confirmados": len(tables["geral"].query("IBGE_RES_PR == 9999999")),
        "mortes": len(
            tables["geral"].query("IBGE_RES_PR == 9999999 & DATA_OBITO == DATA_OBITO")
        ),
    }

    # (2) Trata dados agregados
    tricky = {
        "CAMBARA": "CAMBARÁ",
        "GUAIRACA": "GUAIRAÇÁ",
        "GUAIRA": "GUAÍRA",
        "PINHAO": "PINHÃO",
    }

    tables["municipios"] = (
        tables["municipios"]
        .rename(
            columns={
                "Casos": "confirmados",
                "Obitos": "mortes",
                "Obito": "mortes",  # typo 6/ago
                "Municipio": "municipio",
            }
        )
        .set_index("municipio")[["confirmados", "mortes"]]
        .pipe(fix_typos, uf="PR", tricky=tricky)
    )

    # (3) Trata microdados
    tables["geral"] = (
        tables["geral"]
        .query("IBGE_RES_PR != 9999999")
        .rename(columns={"DATA_OBITO": "mortes", "MUN_RESIDENCIA": "municipio",})[
            ["municipio", "mortes"]
        ]
        .fillna(0)
    )

    tables["geral"].loc[tables["geral"]["mortes"] != 0, "mortes"] = 1

    tables["geral"] = (
        tables["geral"]
        .groupby("municipio")["mortes"]
        .agg(confirmados="count", mortes=sum)
        .pipe(fix_typos, uf="PR", tricky=tricky)
        .reset_index()
        .groupby("municipio")
        .agg("sum")
    )

    # (5) Adiciona importados e total nas tabelas tratadas
    for k in tables.keys():
        tables[k].loc["Importados/Indefinidos"] = importados
        tables[k].loc["TOTAL NO ESTADO"] = tables[k].sum(axis=0)

    # (6) Compara valores finais e checa total
    try:
        check = (tables["geral"].sort_index() != tables["municipios"].sort_index()).any(
            1
        )
        if len(check) > 0:
            logger.warning(
                "Dados divergentes entre tabelas: \n==> Microdados:\n{display1}\n==> Agregado:\n{display2}",
                display1=tables["geral"].loc[check],
                display2=tables["municipios"].loc[check],
            )
    except:
        diff = set(tables["municipios"].index) - set(tables["geral"].index)
        if len(diff) > 0:
            logger.warning(
                "Cidades nos microdados NÃO presentes no agregado: {display}",
                display=diff,
            )

        diff = set(tables["geral"].index) - set(tables["municipios"].index)
        if len(diff) > 0:
            logger.warning(
                "Cidades no agregado NÃO presentes nos micordados: \n{display}",
                display=diff,
            )

    logger.info(
        "Total no estado (sem importados): \n{display}",
        display=tables["municipios"].loc["TOTAL NO ESTADO"]
        - tables["municipios"].loc["Importados/Indefinidos"],
    )

    logger.info(
        "Importados/Indefinidos: \n{display}",
        display=tables["municipios"].loc["Importados/Indefinidos"],
    )

    # O que retorna? Tabela agregada + Importados dos microdados
    return tables["municipios"]
