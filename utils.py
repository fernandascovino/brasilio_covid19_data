from difflib import get_close_matches
import pandas as pd
from loguru import logger
import unidecode


def fix_typos(data, uf, tricky=None):
    """
    Conserta nomes de municípios com base no modelo da UF.

    Args:
        data (pd.DataFrame): Tabela com dados do boletim por município
        uf (str): Sigla do estado [RJ|PR]
        tricky (dict): Opcional. Dicionário com typos não capturados pela`get_closest_match` (acentuados, etc)
        
    Returns:
        pd.DataFrame: Tabela `data` com nomes dos municípios tratados
    """
    # conserta casos limites
    data.index = data.index.str.upper()

    if tricky:
        data = (
            data.reset_index()
            .assign(municipio=lambda df: df.municipio.replace(tricky))
            .set_index("municipio")
        )

    # busca nome + proximo no modelo
    df = pd.read_excel(f"models/{uf}_modelo.xlsx", index_col=0)
    df.index = df.index.str.upper().map(unidecode.unidecode)

    rename = dict()
    not_matched = list()
    for city in data.index:
        match = get_close_matches(city, df.index.unique(), 1)
        if len(match) > 0:
            rename[city] = match[0]
        else:
            not_matched += [city]

    if len(not_matched) > 0:
        logger.warning("Typos NÃO identificados: {display}", display=not_matched)

    return data.rename(index=rename)
