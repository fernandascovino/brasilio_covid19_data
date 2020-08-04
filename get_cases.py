from urllib.request import urlopen
from bs4 import BeautifulSoup
import bs4
import re
from unidecode import unidecode
import glob
import os
import numpy as np
import pandas as pd
import yaml
from difflib import get_close_matches
from urllib.error import HTTPError
from loguru import logger
import re
import tabula


def _fix_typos(data, config, uf):

    df = pd.read_excel(config[uf]["model"], index_col=0)
    errors = [
        i for i in list(data.index.str.upper()) if i not in list(df.index.str.upper())
    ]

    if len(errors) > 0:
        rename = {
            city: get_close_matches(city.upper(), df.index.str.upper().unique(), 1)[0]
            for city in errors
        }
        logger.warning("Typos identificados: {display}", display=rename)
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

        if (
            "casos confirmados" in i
            or "vítimas de Covid-19" in i
            or type(i) == bs4.element.Tag
        ):
            pass
        elif i.strip() == "":
            pass
        else:
            dic.update(_text_to_dic_element(i.string))

    # Conserta typos
    for key in dic.keys():
        dic[get_close_matches(key, city_names, 1)[0]] = dic.pop(key)

    # Junta total de importados
    importados = {k: dic[k] for k in dic.keys() if k in config["importados"]}
    dic["Importados/Indefinidos"] = sum(importados.values())

    for k in importados:
        dic.pop(k)

    return dic


def _load_content(date, config, city_names):

    # Find date url
    url = "https://coronavirus.rj.gov.br/boletins/"
    soup = (
        BeautifulSoup(urlopen(url), "html.parser")
        .find("div", {"class": "entry-content"})
        .select(".elementor-post__read-more")
    )

    boletim = None
    for page in soup:
        try:
            if (
                re.search(r"(?:boletim-coronavirus-)(\d+.\d+)", page["href"])[
                    1
                ].replace("-", "_")
                == date
            ):
                soup = BeautifulSoup(urlopen(page["href"]), "html.parser")
                logger.info("URL Boletim: {display}", display=page["href"])
                boletim = soup.find("div", {"class": "entry-content"})
                break
        except:
            pass

    if not boletim:
        logger.warning(
            "Boletim ainda não atualizado! Último boletim: {display}",
            display=soup[0]["href"],
        )
        return

    # Get html content
    if len(boletim.findAll("p")) > 10:
        init_mortes = [
            i
            for i, p in enumerate(boletim.find_all("p"))
            if re.search(
                "\d+(.|)\d+(?=\s+{})".format(config["p_city_deaths"]["init"]), p.text
            )
        ][0]

        final_mortes = [
            i
            for i, p in enumerate(boletim.find_all("p"))
            if re.search("{}".format(config["p_city_deaths"]["final"]), p.text)
        ][0]

        content = {
            "total": boletim.findAll("p")[0].text.replace(u"\xa0", u" "),
            "confirmados": boletim.findAll("p")[2:init_mortes],
            "mortes": boletim.findAll("p")[init_mortes + 1 : final_mortes],
        }

    else:
        content = {
            "total": str(boletim.findAll("p")[0]),
            "confirmados": boletim.findAll("p")[2],
            "mortes": boletim.findAll("p")[4],
        }

    # print(content)

    # Treat data
    data = {"confirmados": dict(), "mortes": dict()}

    for k in data.keys():

        data[k] = _create_city_dic(content[k], config, city_names)

        data[k]["TOTAL NO ESTADO"] = int(
            re.search(
                "\d+(.|)\d+(?=\s+{})".format(config["total_regex"][k]),
                content["total"],
                flags=re.IGNORECASE,
            )
            .group()
            .replace(".", "")
        )

    return data


def get_rj_cases(date, config, uf="RJ"):

    df = pd.read_excel(config[uf]["model"], index_col=0)

    # Coleta os casos e mortes do boletim
    cases = pd.DataFrame(_load_content(date, config[uf], df.index.unique()))

    if len(cases) == 0:
        return

    # Verifica erros
    errors = [i for i in list(cases.index) if i not in list(df.index)]

    if len(errors) > 0:

        rename = {
            city: get_close_matches(city, df.index.unique(), 1)[0] for city in errors
        }
        logger.warning("Typos não registrados: {display}", display=rename)
        cases = cases.rename(index=rename)

    df["confirmados"] = cases["confirmados"]
    df["mortes"] = cases["mortes"]
    df["mortes"] = cases.apply(
        lambda row: 0
        if not np.isnan(row["confirmados"]) and np.isnan(row["mortes"])
        else row["mortes"],
        axis=1,
    )

    logger.info("Checando total: {display}", display=list(df.sum() / 2))

    return df


# A PARTIR DE 24/07
def _test_microdata_url(date, config, uf="PR"):

    # Procura boletim na página
    url_list = [
        "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_geral_{}.2020.csv".format(
            date[3:], date.replace("_", ".")
        ),
        # "raw/INFORME_EPIDEMIOLOGICO_03_08_2020_GERAL.csv",  # 03/08 -> arquivo baixado
        # "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_{}_2020_geral_atualizado.csv".format(
        #     date[3:], date
        # ),
        # "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMIOLOGICO_{}_2020%20.csv".format(
        #     date[3:], date
        # ),
        # "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/informe_epidemiologico_{}_2020_geral.csv".format(
        #     date[3:], date
        # ),
        # "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/INFORME_EPIDEMIOLOGICO_{}_2020_GERAL.csv".format(
        #     date[3:], date
        # ),
        # "http://www.saude.pr.gov.br/sites/default/arquivos_restritos/files/documento/2020-{}/arquivo_csv_0.csv".format(
        #     date[3:]
        # ),  # 26/07 -> não atualizado!
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


def get_pr_cases_from_micro_data(date, config, uf="PR"):

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
                print(data)

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
        data.columns = data.columns.str.strip()
        # print(data.columns)
        # data = data.rename(config[uf]["rename"], axis=1)[config[uf]["keep"]]
        # else:
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


@logger.catch()
def collect_state_report(date, uf):

    config = yaml.load(open("config.yml", "r"))

    if uf == "PR":
        # df = get_pr_cases(date, config)
        df = get_pr_cases_from_micro_data(date, config)
        path = "outputs/pr/pr_{}.xlsx".format(date[3:] + "_" + date[:2])

    if uf == "RJ":
        df = get_rj_cases(date, config)
        path = "outputs/rj/rj_{}.xlsx".format(date[3:] + "_" + date[:2])

    df.to_excel(path)
    logger.success("Dados salvos em: {display}", display=path)


if __name__ == "__main__":

    # FROM TERMINAL: $ python get_cases [DD_MM] [UF]
    import sys

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    collect_state_report(sys.argv[1], sys.argv[2])
