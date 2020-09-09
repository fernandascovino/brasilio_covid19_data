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

    # Nova estrutura: div nested
    if len(boletim.findAll("p")) > 10:
        cleaned_div = [i for i in boletim.find_all("p") if len(i.find_all("p")) == 0]
        init_mortes = [
            i
            for i, p in enumerate(cleaned_div)
            if re.search(
                r"\d+(.|)\d+(?=\s+{})".format(config["p_city_deaths"]["init"]), p.text,
            )
        ][0]

        final_mortes = [
            i
            for i, p in enumerate(cleaned_div)
            if re.search("{}".format(config["p_city_deaths"]["final"]), p.text)
        ][0]

        content = {
            "total": cleaned_div[0].text.replace(u"\xa0", u" "),
            "confirmados": cleaned_div[2:init_mortes],
            "mortes": cleaned_div[init_mortes + 1 : final_mortes],
        }

    # Nova estrutura: entry-content + p direto no body
    elif len(boletim.findAll("p")) == 0:
        init_mortes = [
            i
            for i, p in enumerate(soup.find_all("p"))
            if re.search(
                r"\d+(.|)\d+(?=\s+{})".format(config["p_city_deaths"]["init"]), p.text
            )
        ][0]

        final_mortes = [
            i
            for i, p in enumerate(soup.find_all("p"))
            if re.search("{}".format(config["p_city_deaths"]["final"]), p.text)
        ][0]

        content = {
            "total": str(boletim.findAll("span")[0]),
            "confirmados": soup.findAll("p")[2:init_mortes],
            "mortes": soup.findAll("p")[init_mortes + 1 : final_mortes],
        }

        # print(content)

    else:
        content = {
            "total": str(soup.findAll("p")[0]),
            "confirmados": soup.findAll("p")[2],
            "mortes": soup.findAll("p")[3],
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


def main(date, config, uf="RJ"):

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
        cases = cases.rename(index=rename).reset_index().groupby("index").sum()

    df["confirmados"] = cases["confirmados"]
    df["mortes"] = cases.apply(
        lambda row: 0
        if not np.isnan(row["confirmados"]) and np.isnan(row["mortes"])
        else row["mortes"],
        axis=1,
    )

    logger.info("Checando total: {display}", display=list(df.sum() / 2))

    return df


if __name__ == "__main__":
    pass
