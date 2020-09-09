from urllib.request import urlopen
from bs4 import BeautifulSoup
import bs4

import glob
import os

import numpy as np
import pandas as pd

from difflib import get_close_matches
import re

from loguru import logger


def _fix_typos(data, config, uf):

    df = pd.read_excel(config[uf]["model"], index_col=0)
    errors = [
        i for i in list(data.index.str.upper()) if i not in list(df.index.str.upper())
    ]

    rename = dict()
    not_matched = list()
    if len(errors) > 0:
        for city in errors:
            match = get_close_matches(city.upper(), df.index.str.upper().unique(), 1)
            if len(match) > 0:
                rename[city] = match[0]
            else:
                print(city)
                not_matched += [city]

        logger.warning("Typos identificados: {display}", display=rename)
        logger.warning("Typos NÃO identificados: {display}", display=not_matched)

        return data.rename(index=rename)

    return data


def _text_to_dic_element(s):
    idx = [p for p, i in enumerate(s) if i.isdigit()][0]
    return {re.sub(r"[^\w\s]", "", s[:idx]).strip(): int(s[idx:].replace(".", ""))}


def _create_city_dic(numbers, config, city_names):

    dic = dict()

    for i in numbers:

        if type(i) == bs4.element.NavigableString:
            i = i.string

        # ex: <p>São Gonçalo – 9.295</p>
        if type(i) == bs4.element.Tag:
            i = i.text

        if "casos confirmados" in i or "vítimas de Covid-19" in i:
            pass
        elif i.strip() == "":
            pass
        else:
            dic.update(_text_to_dic_element(i))

    # Conserta typos
    for key in dic.keys():
        dic[get_close_matches(key, city_names, 1)[0]] = dic.pop(key)

    # Junta total de importados
    importados = {k: dic[k] for k in dic.keys() if k in config["importados"]}
    dic["Importados/Indefinidos"] = sum(importados.values())

    for k in importados:
        dic.pop(k)

    return dic


# A PARTIR DE 24/07
def _test_microdata_url(date, config, uf="PR"):

    # Procura boletim na página
    url_list = [
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_{}_geral.csv".format(
            date[3:], date
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMILOGICO_{}_CASOS_OBITOS_MUNICIPIOS.csv".format(
            date[3:], date
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMILOGICO_{}_GERAL.csv".format(
            date[3:], date
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMIOLOGICO_{}_GERAL.csv".format(
            date[3:], date
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMIOLOGICO_{}_GERAL.csv".format(
            date[3:], date.replace("_", ".")
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_{}_2020_geral_0.csv".format(
            date[3:], date
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMIOL%C3%93GICO_{}_2020_GERAL.csv".format(
            date[3:], date
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_geral_{}.2020.csv".format(
            date[3:], date.replace("_", ".")
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_{}_2020_geral_atualizado.csv".format(
            date[3:], date
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMIOLOGICO_{}_2020%20.csv".format(
            date[3:], date
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_{}_2020_geral.csv".format(
            date[3:], date
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMIOLOGICO_{}_2020_GERAL.csv".format(
            date[3:], date
        ),
        # "raw/INFORME_EPIDEMIOLOGICO_03_08_2020_GERAL.csv",  # 03/08 -> arquivo baixado
        # "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/arquivo_csv_0.csv".format(
        #     date[3:]
        # ),  # 26/07 -> mortes atualizadas posteriormente
    ]

    replace = {
        "Mun_Resid": "municipio",
        "MUN_RESIDENCIA": "municipio",
        "dt_notificacao": "confirmados",
        "DATA_CONFIRMACAO_DIVULGACAO": "confirmados",
        "Dt_obito": "mortes",
        "DATA_OBITO": "mortes",
    }

    for url in url_list:
        try:
            data = pd.read_csv(url, sep=";", skiprows=0, encoding="latin-1")

            if "IBGE_RES_PR" not in data.columns:
                data = pd.read_csv(url, sep=";", skiprows=0, encoding="utf-8")

            data.columns = data.columns.str.strip()
            data = data.rename(replace, axis=1)

            logger.info("URL Boletim: {display}", display=url)
            return data  # [config[uf]["keep"]]

        except Exception as e:
            logger.error("{display} | {error}", display=url, error=e)
            pass

    return


def main(date, config, uf="PR"):

    data = _test_microdata_url(date, config)
    data["municipio"] = data["municipio"].str.upper()

    # conserta casos limites
    tricky = {
        "CAMBARA": "CAMBARÁ",
        "GUAIRACA": "GUAIRAÇÁ",
        "GUAIRA": "GUAÍRA",
        "PINHAO": "PINHÃO",
    }
    data["municipio"] = data["municipio"].replace(tricky)

    casos_pr = data[data["IBGE_RES_PR"] != 9999999]

    # Erro em 30/08 (linha 92190): Município de residência (SOROCABA) veio com o código do município de atendimento (RIO NEGRO)
    # casos_pr = casos_pr[casos_pr["municipio"] != "SOROCABA"]

    # Gera dict com cod_ibge: nome padrão
    cod_nome_municipio = (
        _fix_typos(
            casos_pr[["IBGE_RES_PR", "municipio"]]
            .drop_duplicates()
            .set_index("municipio"),
            config,
            uf,
        )
        .reset_index()
        .set_index("IBGE_RES_PR")
        .to_dict()["municipio"]
    )

    casos_pr = (
        casos_pr.assign(municipio=lambda df: df["IBGE_RES_PR"].map(cod_nome_municipio))
        .groupby(["municipio"])[["confirmados", "mortes"]]
        .count()
    )

    casos_pr.loc["Importados/Indefinidos"] = data[data["IBGE_RES_PR"] == 9999999][
        ["confirmados", "mortes"]
    ].count()
    casos_pr.loc["TOTAL NO ESTADO"] = casos_pr.sum()

    logger.info(
        "Total PR: \n{display}",
        display=casos_pr.loc["TOTAL NO ESTADO"]
        - casos_pr.loc["Importados/Indefinidos"],
    )
    logger.info(
        "Fora do PR: \n{display}", display=casos_pr.loc["Importados/Indefinidos"]
    )

    if not "LARANJAL" in casos_pr.index:
        casos_pr.loc["LARANJAL"] = [0, 0]

    return casos_pr


# ANTES DE 24/07
def _test_urls(date, config, uf="PR"):

    # Procura boletim na página
    url_list = [
        # "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_municipios_{}_0.csv".format(
        #     date[3:], date.replace("_", ".")
        # ),
        # "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_municipios_{}.csv".format(
        #     date[3:], date.replace("_", ".")
        # ),
        # "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMIOLOGICO_MUNICIPIOS_{}.csv".format(
        #     date[3:], date
        # ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_{}.csv".format(
            date[3:], date
        ),
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMIOLOGICO_{}_2020%20.csv".format(
            date[3:], date
        ),
    ]

    for url in url_list:
        try:
            data = pd.read_csv(
                url, sep=";", skiprows=0, encoding="latin-1", low_memory=False
            )
            data.columns = data.columns.str.strip()
            data = data.rename(config[uf]["rename"], axis=1)

            if "municipio" not in data.columns:
                data = pd.read_csv(
                    url, sep=";", skiprows=0, encoding="utf-8", low_memory=False
                )
                data.columns = data.columns.str.strip()
                data = data.rename(config[uf]["rename"], axis=1)

            if len(data) > 0:
                logger.info("URL Boletim: {display}", display=url)
                return data[config[uf]["keep"]]

        except Exception as e:
            logger.error("{display} | {error}", display=url, error=e)
            pass

    return


def get_pr_cases(date, config, uf="PR"):

    data = _test_urls(date, config)
    # data = _extract_pdf_data(date, config)

    if len(data) == 0:
        return

    # TEST: Checa se as colunas foram identificadas
    if not all(x in data.columns.values for x in config[uf]["keep"]):
        logger.warning("Colunas não identificadas: {display}", display=data.columns)
        return

    # Separa dados de fora da UF
    for i in config[uf]["fora_pr"]:
        if i in data["municipio"].str.upper().values:
            idx_fora_pr = data.loc[
                data["municipio"].str.upper().str.contains(i) == True
            ].index[0]

    # Trata dados do PR
    dados_uf = _fix_typos(
        data.loc[: idx_fora_pr - 1]
        .dropna(subset=["municipio"])
        .fillna(0)
        .query("municipio != 'TOTAL'")
        .set_index("municipio", drop=True)
        .astype(int),
        config,
        uf,
    )

    # Calcula total fora do PR
    # NÃO FUNCIONA EM 08/08
    # dados_uf.loc["Importados/Indefinidos"] = (
    #     data.loc[idx_fora_pr:][data["municipio"] == "Total"][config[uf]["keep"]]
    #     .set_index("municipio", drop=True)
    #     .astype(int)
    #     .fillna(0)
    #     .values[0]
    # )
    dados_uf.loc["Importados/Indefinidos"] = (
        data.loc[[idx_fora_pr]]
        .set_index("municipio", drop=True)
        .astype(int)
        .fillna(0)
        .values[0]
    )

    # Checa cidades que já apareceram
    last_cities = pd.read_excel(
        max(glob.glob(os.getcwd() + "/outputs/pr/pr*"), key=os.path.getctime),
        index_col=0,
    ).index.drop("TOTAL NO ESTADO")

    add = list(set(last_cities) - set(dados_uf.index))
    if add:
        logger.info(
            "ADD: Cidades não presentes no novo boletim - adicionando (0,0): : {display}",
            display=add,
        )

        for city in add:
            dados_uf.loc[city] = [0, 0]

    # Calcula total no estado
    dados_uf.loc["TOTAL NO ESTADO"] = dados_uf.sum()

    logger.warning(
        "TOTAL: CASOS - {display[0]} | MORTES - {display[1]}",
        display=list(dados_uf.sum() / 2),
    )

    return dados_uf
