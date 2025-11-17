# -*- coding: utf-8 -*-
import os
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import logging

# ================= CONFIGURAÇÕES =================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PASTAS = {
    "Arbitragem": Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Bonificação\Setembro\1. Arbitragem"),
    "Motorista": Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Bonificação\Setembro\2. Motorista"),
    "SemMovimentacao": Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Bonificação\Setembro\3. Sem Movimentação"),
    "T-0": Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Bonificação\Setembro\4. T-0"),
    "ColetadosExpedidos": Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Bonificação\Setembro\5. Coletados + Expedidos"),
    "SC_TaxaExpedicao": Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Bonificação\Setembro\6. SC - Taxa de Expedição"),
    "SC_Processamento": Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Bonificação\Setembro\7. SC - Processamento SC"),
    "DC_Processamento": Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Bonificação\Setembro\8. DC - Processamento")
}

PASTA_SAIDA = Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Bonificação\Planilha Final")
PASTA_SAIDA.mkdir(exist_ok=True)

data_hoje = datetime.now().strftime("%Y-%m-%d")
ARQUIVO_SAIDA = PASTA_SAIDA / f"Consolidado_Bonificacao_{data_hoje}.xlsx"


# ================= FUNÇÕES AUXILIARES =================
def ler_excel(arquivo):
    try:
        df = pd.read_excel(arquivo)
        logging.info(f"📄 {arquivo.name} -> colunas: {list(df.columns)}")
        return df
    except Exception as e:
        logging.error(f"❌ Erro em {arquivo.name}: {e}")
        return pd.DataFrame()


def ler_arquivos(pasta):
    arquivos = list(pasta.glob("*.xlsx")) + list(pasta.glob("*.xls"))
    if not arquivos:
        logging.warning(f"⚠️ Nenhum arquivo encontrado em {pasta}")
        return []
    with ThreadPoolExecutor() as executor:
        dfs = list(executor.map(ler_excel, arquivos))
    return dfs


# ================= FUNÇÕES DE PROCESSAMENTO =================
def processar_t0(pasta):
    dfs = ler_arquivos(pasta)
    if not dfs:
        return pd.DataFrame()
    df_total = pd.concat(dfs, ignore_index=True)

    colunas = ["Nome da base", "T日签收率-应签收量", "T日签收率-已签收量"]
    if not all(col in df_total.columns for col in colunas):
        return pd.DataFrame()

    agrupado = df_total.groupby("Nome da base").agg({
        "T日签收率-应签收量": "sum",
        "T日签收率-已签收量": "sum"
    }).reset_index()

    agrupado = agrupado.rename(columns={
        "T日签收率-应签收量": "Pedidos Recebidos",
        "T日签收率-已签收量": "Pedidos Entregues"
    })

    agrupado["Taxa de Entrega (%)"] = (
        agrupado["Pedidos Entregues"] / agrupado["Pedidos Recebidos"] * 100
    )
    return agrupado


def processar_sem_mov(pasta):
    dfs = ler_arquivos(pasta)
    if not dfs:
        return pd.DataFrame()
    df_total = pd.concat(dfs, ignore_index=True)

    colunas = ["Unidade responsável责任机构", "Número de pedido JMS 运单号"]
    if not all(col in df_total.columns for col in colunas):
        return pd.DataFrame()

    agrupado = df_total.groupby("Unidade responsável责任机构").agg({
        "Número de pedido JMS 运单号": "count"
    }).reset_index()

    agrupado = agrupado.rename(columns={
        "Número de pedido JMS 运单号": "Qtde Pedidos"
    })
    return agrupado


def processar_arbitragem(pasta):
    dfs = ler_arquivos(pasta)
    if not dfs:
        return pd.DataFrame()
    df_total = pd.concat(dfs, ignore_index=True)

    colunas = ["Base responsável", "Tipo de anomalia primária", "Tipo de anomalia secundária", "Remessa", "Valor a pagar (yuan)"]
    if not all(col in df_total.columns for col in colunas):
        return pd.DataFrame()

    agrupado = df_total.groupby(
        ["Base responsável", "Tipo de anomalia primária", "Tipo de anomalia secundária"]
    ).agg({
        "Remessa": "count",
        "Valor a pagar (yuan)": "sum"
    }).reset_index()

    agrupado = agrupado.rename(columns={
        "Remessa": "Qtde Remessas",
        "Valor a pagar (yuan)": "Valor Total a Pagar (yuan)"
    })

    return agrupado


def processar_motorista(pasta):
    dfs = ler_arquivos(pasta)
    if not dfs:
        return pd.DataFrame()
    df_total = pd.concat(dfs, ignore_index=True)

    colunas = ["Base de entrega", "Responsável pela entrega", "Marca de assinatura"]
    if not all(col in df_total.columns for col in colunas):
        return pd.DataFrame()

    agrupado = df_total.groupby("Base de entrega").agg({
        "Responsável pela entrega": "nunique"
    }).reset_index()

    agrupado = agrupado.rename(columns={
        "Responsável pela entrega": "Motoristas Únicos"
    })
    return agrupado


def processar_coletados_expedidos(pasta):
    dfs = ler_arquivos(pasta)
    if not dfs:
        return pd.DataFrame()
    df_total = pd.concat(dfs, ignore_index=True)

    colunas = ["Nome da base", "Quantidade coletada", "Quantidade com saída para entrega"]
    if not all(col in df_total.columns for col in colunas):
        return pd.DataFrame()

    agrupado = df_total.groupby("Nome da base").agg({
        "Quantidade coletada": "sum",
        "Quantidade com saída para entrega": "sum"
    }).reset_index()

    agrupado = agrupado.rename(columns={
        "Quantidade coletada": "Total Coletado",
        "Quantidade com saída para entrega": "Total Expedidos"
    })

    agrupado["Total Coletado + Expedidos"] = (
        agrupado["Total Coletado"] + agrupado["Total Expedidos"]
    )
    return agrupado


def processar_sc_taxa_expedicao(pasta):
    dfs = ler_arquivos(pasta)
    if not dfs:
        return pd.DataFrame()
    df_total = pd.concat(dfs, ignore_index=True)

    colunas_sc_saida = ["Nome SC", "Bilhetes de operação em tempo hábil", "Qtd pedidos expedidos"]
    colunas_sc_entrada = ["Sorting Center", "Qtd expedidos no prazo", "Qtd processada", "Qtd expedida sem rota secundária cadastrada"]

    if all(col in df_total.columns for col in colunas_sc_saida):
        agrupado = df_total.groupby("Nome SC").agg({
            "Bilhetes de operação em tempo hábil": "sum",
            "Qtd pedidos expedidos": "sum"
        }).reset_index()

        agrupado = agrupado.rename(columns={
            "Nome SC": "Nome da Base",
            "Bilhetes de operação em tempo hábil": "Pedidos Enviados no Prazo",
            "Qtd pedidos expedidos": "Pedidos Recebidos"
        })

        agrupado["Taxa de Expedição (%)"] = (
            agrupado["Pedidos Enviados no Prazo"] / agrupado["Pedidos Recebidos"] * 100
        )
        return agrupado

    elif all(col in df_total.columns for col in colunas_sc_entrada):
        agrupado = df_total.groupby("Sorting Center").agg({
            "Qtd expedidos no prazo": "sum",
            "Qtd processada": "sum",
            "Qtd expedida sem rota secundária cadastrada": "sum"
        }).reset_index()

        agrupado = agrupado.rename(columns={
            "Sorting Center": "Nome da Base",
            "Qtd expedidos no prazo": "Pedidos Expedidos no Prazo",
            "Qtd processada": "Qtd Processada",
            "Qtd expedida sem rota secundária cadastrada": "Qtd Sem Rota Secundária"
        })

        agrupado["Qtd Processada Ajustada"] = (
            agrupado["Qtd Processada"] - agrupado["Qtd Sem Rota Secundária"]
        )

        agrupado["Índice Expedição Ajustado (%)"] = (
            agrupado["Pedidos Expedidos no Prazo"] / agrupado["Qtd Processada Ajustada"] * 100
        )
        return agrupado

    return pd.DataFrame()


def processar_sc_processamento(pasta):
    dfs = ler_arquivos(pasta)
    if not dfs:
        return pd.DataFrame()
    df_total = pd.concat(dfs, ignore_index=True)

    colunas = ["Nome SC", "Volume de transferência de carga"]
    if not all(col in df_total.columns for col in colunas):
        return pd.DataFrame()

    agrupado = df_total.groupby("Nome SC").agg({
        "Volume de transferência de carga": "sum"
    }).reset_index()

    agrupado = agrupado.rename(columns={
        "Nome SC": "Nome da Base",
        "Volume de transferência de carga": "Total Transferido"
    })

    return agrupado


def processar_dc_processamento(pasta):
    dfs = ler_arquivos(pasta)
    if not dfs:
        return pd.DataFrame()
    df_total = pd.concat(dfs, ignore_index=True)

    colunas = ["Nome da base", "Volume recebido (pedidos mãe)", "Volume recebido (pedidos regulares)"]
    if not all(col in df_total.columns for col in colunas):
        return pd.DataFrame()

    agrupado = df_total.groupby("Nome da base").agg({
        "Volume recebido (pedidos mãe)": "sum",
        "Volume recebido (pedidos regulares)": "sum"
    }).reset_index()

    agrupado["Total Recebido"] = (
        agrupado["Volume recebido (pedidos mãe)"] +
        agrupado["Volume recebido (pedidos regulares)"]
    )

    agrupado = agrupado.rename(columns={
        "Nome da base": "Nome da Base",
        "Volume recebido (pedidos mãe)": "Total Pedidos Mãe",
        "Volume recebido (pedidos regulares)": "Total Pedidos Regulares"
    })

    return agrupado


# ================= MAIN =================
if __name__ == "__main__":
    with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as writer:
        for nome, funcao in {
            "T-0": processar_t0,
            "SemMovimentacao": processar_sem_mov,
            "Arbitragem": processar_arbitragem,
            "Motorista": processar_motorista,
            "ColetadosExpedidos": processar_coletados_expedidos,
            "SC_TaxaExpedicao": processar_sc_taxa_expedicao,
            "SC_Processamento": processar_sc_processamento,
            "DC_Processamento": processar_dc_processamento
        }.items():
            df = funcao(PASTAS[nome])
            if not df.empty:
                try:
                    df.to_excel(writer, sheet_name=nome, index=False)
                except Exception as e:
                    logging.error(f"❌ Erro ao salvar aba {nome}: {e}")

    logging.info(f"📊 Consolidado salvo em: {ARQUIVO_SAIDA}")