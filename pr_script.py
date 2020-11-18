from urllib.request import urlopen
from bs4 import BeautifulSoup
import bs4
import numpy as np
import pandas as pd
from difflib import get_close_matches
import re
from loguru import logger


def get_content(url, date, title):

    section = {
        "Geral.csv": "Boletim - Informe Epidemiológico Coronavírus (COVID-19) - Arquivos CSV",
        "Casos e Óbitos.csv": "Boletim - Informe Epidemiológico Coronavírus (COVID-19) - Arquivos CSV",
        "Informe Completo e Detalhado.pdf": "Boletim - Informe Epidemiológico Coronavírus (COVID-19)",
    }

    return (
        BeautifulSoup(urlopen(url), "html.parser")
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
                text=section[title]
            )
        )
        .parent.parent.parent.find("span", {"title": title})
        .find("a")["href"]
    )


def _fix_typos(data):

    # conserta casos limites
    data.index = data.index.str.upper()

    tricky = {
        "CAMBARA": "CAMBARÁ",
        "GUAIRACA": "GUAIRAÇÁ",
        "GUAIRA": "GUAÍRA",
        "PINHAO": "PINHÃO",
    }

    data = (
        data.reset_index()
        .assign(municipio=lambda df: df.municipio.replace(tricky))
        .set_index("municipio")
    )

    # busca nome + proximo no modelo
    df = pd.read_excel("models/PR_modelo.xlsx", index_col=0)
    df.index = df.index.str.upper()

    errors = [i for i in list(data.index) if i not in list(df.index)]

    rename = dict()
    not_matched = list()
    if len(errors) > 0:
        for city in errors:
            match = get_close_matches(city, df.index.unique(), 1)
            if len(match) > 0:
                rename[city] = match[0]
            else:
                print(city)
                not_matched += [city]

        logger.warning("Typos NÃO identificados: {display}", display=not_matched)

        return data.rename(index=rename)

    return data


def main(day, month):

    # (1) Busca URL do boletim de hoje
    url = "https://www.saude.pr.gov.br/Pagina/Coronavirus-COVID-19"

    # try:
    date = day + "/" + month + "/2020"
    tables = {
        "geral": get_content(url, date, "Geral.csv"),
        "municipios": get_content(url, date, "Casos e Óbitos.csv"),
    }

    logger.info("URL Boletim (CSV): {display}", display=tables["municipios"])

    # TODO: verificar erros na estrutura HTML de algumas datas para o PDF
    try:
        logger.info(
            "URL Boletim (PDF): {display}",
            display=get_content(url, date, "Informe Completo e Detalhado.pdf"),
        )
    except:
        pass

    for k in tables.keys():
        try:
            tables[k] = pd.read_csv(tables[k], sep=";", skiprows=0)
        except:
            tables[k] = pd.read_csv(tables[k], sep=";", skiprows=0, encoding="latin-1")

    # except Exception as e:
    #     logger.error("Boletim não encontrado! - ERRO: {error}", error=e)

    # (2) Contabiliza importados (tem somente nos microdados)
    importados = {
        "confirmados": len(tables["geral"].query("IBGE_RES_PR == 9999999")),
        "mortes": len(
            tables["geral"].query("IBGE_RES_PR == 9999999 & DATA_OBITO == DATA_OBITO")
        ),
    }

    # (2) Trata dados agregados
    tables["municipios"] = (
        tables["municipios"]
        .rename(
            columns={
                "Casos": "confirmados",
                "Obitos": "mortes",
                "Municipio": "municipio",
            }
        )
        .set_index("municipio")[["confirmados", "mortes"]]
        .pipe(_fix_typos)
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
        .pipe(_fix_typos)
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
                display1=tables["geral"][check],
                display2=tables["municipios"][check],
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
        "Total no estado: \n{display}",
        display=tables["municipios"].loc["TOTAL NO ESTADO"],
    )

    # O que retorna? Tabela agregada + Importados dos microdados
    return tables["municipios"]
