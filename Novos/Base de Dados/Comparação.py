# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# ===========================================================
# CONFIGURAÇÕES
# ===========================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 📂 Pasta de entrada T-0
PASTA_T0 = Path(
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Local de Teste\T-0"
)
# 📂 Planilha base de coordenadores
ARQUIVO_BASE = Path(
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
)

# Colunas T-0
COL_BASE = "Nome da base"
COL_UF = "UF"
COL_DATA = "Horário de término do prazo de coleta"
COL_RECEBIDO = "T日签收率-应签收量"
COL_ENTREGUE = "T日签收率-已签收量"


# ===========================================================
# FUNÇÕES AUXILIARES
# ===========================================================
def ler_planilhas_t0(pasta: Path):
    arquivos = list(pasta.glob("*.xlsx")) + list(pasta.glob("*.xls"))
    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo encontrado na pasta T-0.")
    dfs = []
    for arq in arquivos:
        if "Resumo_T0_Semanal_" in arq.name:
            continue
        logging.info(f"📄 Lendo T-0: {arq.name}")
        df = pd.read_excel(arq)
        df["_arquivo"] = arq.name
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def calcular_resumo(df: pd.DataFrame, semana: str):
    df_semana = df[df["Semana"] == semana].copy()
    resumo = df_semana.groupby(["Semana", COL_UF]).agg(
        Recebido=(COL_RECEBIDO, "sum"),
        Entregue=(COL_ENTREGUE, "sum")
    ).reset_index()
    resumo["Taxa de Entrega (%)"] = (resumo["Entregue"] / resumo["Recebido"] * 100).round(2)

    # Total BRASIL
    total = pd.DataFrame({
        "Semana": [semana],
        COL_UF: ["TOTAL BRASIL"],
        "Recebido": [resumo["Recebido"].sum()],
        "Entregue": [resumo["Entregue"].sum()]
    })
    total["Taxa de Entrega (%)"] = (total["Entregue"] / total["Recebido"] * 100).round(2)

    return pd.concat([resumo, total], ignore_index=True), df_semana


def calcular_por_dia(df_semana: pd.DataFrame):
    df_semana["Dia"] = df_semana[COL_DATA].dt.date
    resumo_dia = df_semana.groupby([COL_UF, "Dia"]).agg(
        Recebido=(COL_RECEBIDO, "sum"),
        Entregue=(COL_ENTREGUE, "sum")
    ).reset_index()
    resumo_dia["Taxa de Entrega (%)"] = (resumo_dia["Entregue"] / resumo_dia["Recebido"] * 100).round(2)

    # Total BRASIL por dia
    total_dia = resumo_dia.groupby("Dia").agg(
        Recebido=("Recebido", "sum"),
        Entregue=("Entregue", "sum")
    ).reset_index()
    total_dia["Taxa de Entrega (%)"] = (total_dia["Entregue"] / total_dia["Recebido"] * 100).round(2)
    total_dia[COL_UF] = "TOTAL BRASIL"

    return pd.concat([resumo_dia, total_dia], ignore_index=True)


# ===========================================================
# EXECUÇÃO
# ===========================================================
def executar_t0():
    hoje = datetime.now()
    df_dados = ler_planilhas_t0(PASTA_T0)

    logging.info("📂 Lendo planilha base...")
    df_base = pd.read_excel(ARQUIVO_BASE)

    # Debug das colunas da planilha base
    logging.info(f"Colunas encontradas na Base_Atualizada.xlsx: {list(df_base.columns)}")

    # Normaliza nomes da base
    df_dados[COL_BASE] = df_dados[COL_BASE].astype(str).str.strip().str.upper()
    df_base[COL_BASE] = df_base[COL_BASE].astype(str).str.strip().str.upper()

    # Garante que UF exista na base
    if COL_UF not in df_base.columns:
        logging.warning("⚠️ Coluna 'UF' não encontrada na Base_Atualizada.xlsx. Criando coluna 'UF não encontrado'.")
        df_base[COL_UF] = "UF não encontrado"

    # Converte datas
    df_dados[COL_DATA] = pd.to_datetime(df_dados[COL_DATA], errors="coerce")

    # Faz merge para trazer UF
    df_merge = df_dados.merge(df_base[[COL_BASE, COL_UF]], how="left", on=COL_BASE)

    # Garante que a coluna UF exista no merge
    if COL_UF not in df_merge.columns:
        logging.warning("⚠️ Coluna 'UF' não veio no merge. Criando manualmente.")
        df_merge[COL_UF] = "UF não encontrado"
    else:
        df_merge[COL_UF] = df_merge[COL_UF].fillna("UF não encontrado")

    # Log das bases sem UF
    bases_sem_uf = df_merge[df_merge[COL_UF] == "UF não encontrado"][COL_BASE].unique()
    if len(bases_sem_uf) > 0:
        logging.warning("⚠️ Bases sem UF encontrado na base de referência:")
        for b in bases_sem_uf:
            logging.warning(f" - {b}")

    # Cria semana ISO
    df_merge["Semana"] = "W" + df_merge[COL_DATA].dt.isocalendar().week.astype(str)

    # Define semana atual e anterior
    semanas = sorted(df_merge["Semana"].dropna().unique())
    semana_atual = semanas[-1]
    semana_anterior = semanas[-2] if len(semanas) > 1 else None

    logging.info(f"📅 Semana Atual: {semana_atual}")
    logging.info(f"📅 Semana Anterior: {semana_anterior}")

    # Calcula resumos
    resumo_atual, df_semana_atual = calcular_resumo(df_merge, semana_atual)
    resumo_anterior, df_semana_anterior, comparacao = (None, None, None)

    if semana_anterior:
        resumo_anterior, df_semana_anterior = calcular_resumo(df_merge, semana_anterior)
        comparacao = resumo_atual.merge(resumo_anterior, on=[COL_UF], suffixes=("_Atual", "_Anterior"))
        comparacao["Dif_Recebido"] = comparacao["Recebido_Atual"] - comparacao["Recebido_Anterior"]
        comparacao["Dif_Entregue"] = comparacao["Entregue_Atual"] - comparacao["Entregue_Anterior"]
        comparacao["Dif_Taxa (%)"] = comparacao["Taxa de Entrega (%)_Atual"] - comparacao["Taxa de Entrega (%)_Anterior"]

    por_dia = calcular_por_dia(df_semana_atual)

    # Salva Excel
    nome_saida = PASTA_T0 / f"Resumo_T0_Semanal_{hoje.strftime('%Y-%m-%d')}.xlsx"
    with pd.ExcelWriter(nome_saida, engine="openpyxl") as writer:
        resumo_atual.to_excel(writer, index=False, sheet_name=f"{semana_atual}")
        if resumo_anterior is not None:
            resumo_anterior.to_excel(writer, index=False, sheet_name=f"{semana_anterior}")
        if comparacao is not None:
            comparacao.to_excel(writer, index=False, sheet_name="Comparação")
        por_dia.to_excel(writer, index=False, sheet_name="Por Dia")

    logging.info(f"✅ Resumo T-0 gerado: {nome_saida}")


if __name__ == "__main__":
    executar_t0()