import pandas as pd
import os
import glob

#uma função de exctract que le e consolida no json

def extrair_dados(pasta:str) -> pd.DataFrame:
    pathjson = glob.glob(os.path.join(pasta, "*.json"))
    df_list = [pd.read_json(arquivo) for arquivo in pathjson]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total


# uma função que transforma
def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

# uma função que da load em csv ou parquet
def carregar_dados(df: pd.DataFrame, formato_saida: list):
    """
    Parametro que vai ser ou "csv" ou "parquet" ou "os dois"
    """
    for formato in formato_saida:
        if formato == "csv":
            df.to_csv("dados.csv", index=False)
        if formato == "parquet":
            df.to_parquet("dados.parquet", index=False)   

def pipeline_calcular_kpi_de_vendas_consolidado(pasta: str, formato_de_saida: list):
    dataframe = extrair_dados(pasta)
    df_calculado = calcular_kpi_de_total_de_vendas(dataframe)
    carregar_dados(df_calculado, formato_de_saida)
