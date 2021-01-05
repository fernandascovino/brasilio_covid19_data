from urllib.request import urlopen
from bs4 import BeautifulSoup
import bs4
import numpy as np
import pandas as pd
import re
from loguru import logger
import time

from utils import fix_typos


def _iterable_text_search(content, text):
    """
    Busca por um texto específico em todos os childs de um html, retorna uma lista do(s) child(s) achados.
    
    Args:
        content (bs4.element.Tag or list): Elemento ou lista de elementos html para busca
        text (str): Texto a ser buscado nos elementos
    Returns:
        list
    """
    if type(content) == bs4.element.Tag:
        content = content.contents

    result = []
    for item in content:
        if type(item) == bs4.element.NavigableString:
            pass
        elif item.find(text=re.compile(text + "(\d+|)", flags=re.I)):
            result.append(item)

    return result


def get_report_url(date, filename):
    """
    Busca URL do boletim da data especificada.

    Args:
        date (str): Data no formato DD/MM/AAAA
        filename (str): Opcional. Tipo de arquivo que se deseja buscar.

            * Agregado por município: 'Geral'
            * Microdados: 'Casos e Óbitos'
            * PDF: 'Informe Completo'        
    """

    baseurl = "https://www.saude.pr.gov.br/Pagina/Coronavirus-COVID-19"

    content = BeautifulSoup(urlopen(baseurl), "html.parser").find(
        "div",
        {
            "class": "field field--name-field-texto field--type-text-long field--label-hidden field--item"
        },
    )

    # Busca os arquivos na tabela do site respectivos a data
    files = [
        x.parent.parent
        for x in _iterable_text_search(
            content.find_all("div", {"class": "content"}), date
        )
    ]

    # Captura a URL do arquivo específico referente à data
    for i in _iterable_text_search(files, filename):
        if i.find("span", {"title": re.compile(filename + "(\d+|)", flags=re.I)}):
            return i.find(
                "span", {"title": re.compile(filename + "(\d+|)", flags=re.I)}
            ).a["href"]
        else:
            return


def main(day, month, year):

    # (1) Busca URL do boletim de hoje
    date = day + "/" + month + "/" + year

    try:
        tables = {
            "geral": get_report_url(date, filename="Geral"),
            "municipios": get_report_url(date, filename="Casos e Óbitos"),
        }

        logger.info("URL Boletim (Municípios): {display}", display=tables["municipios"])
        logger.info("URL Boletim (Microdados): {display}", display=tables["geral"])
    except Exception as e:
        logger.error("Boletim não encontrado! - ERRO: {error}", error=e)

    # TODO: verificar erros na estrutura HTML de algumas datas para o PDF
    time.sleep(1)
    try:
        logger.info(
            "URL Boletim (PDF): {display}",
            display=get_report_url(date, filename="Informe Completo"),
        )
    except:
        pass

    # Nomes padrão de colunas
    rename = {
        "geral": {
            "IBGE": "cod_municipio",
            "IBGE_RES_PR": "cod_municipio",
            "DATA_OBITO": "mortes",
            "MUN_RESIDENCIA": "municipio",
        },
        "municipios": {
            "Casos": "confirmados",
            "Obitos": "mortes",
            "Obito": "mortes",  # typo 6/ago
            "Municipio": "municipio",
        },
    }

    for k in tables.keys():
        try:
            tables[k] = pd.read_csv(tables[k], sep=";", skiprows=0)
        except:
            tables[k] = pd.read_csv(tables[k], sep=";", skiprows=0, encoding="latin-1")

        # Padroniza colunas
        tables[k] = tables[k].rename(columns=rename[k])

    # (2) Contabiliza importados (tem somente nos microdados)
    # print(tables["geral"].columns)
    importados = {
        "confirmados": len(tables["geral"].query("cod_municipio == 9999999")),
        "mortes": len(
            tables["geral"].query("cod_municipio == 9999999").dropna(subset=["mortes"])
        ),
    }

    # (2) Trata dados agregados
    tricky = {
        "CAMBARÁ": "CAMBARA",
        "GUAIRAÇÁ": "GUAIRACA",
        "GUAÍRA": "GUAIRA",
        "PINHÃO": "PINHAO",
    }

    tables["municipios"] = (
        tables["municipios"]
        .set_index("municipio")[["confirmados", "mortes"]]
        .pipe(fix_typos, uf="PR", tricky=tricky)
    )

    # (3) Trata microdados
    tables["geral"] = (
        tables["geral"]
        .query("cod_municipio != 9999999")[["municipio", "mortes"]]
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
