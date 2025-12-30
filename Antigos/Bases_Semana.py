# -*- coding: utf-8 -*-
import os
import warnings
import unicodedata
from pathlib import Path
from datetime import datetime

import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askopenfilename, askdirectory

# ==========================================================
# CONFIG
# ==========================================================
NOME_ABA = None  # None = primeira aba automaticamente
COL_NOME_BASE = "Nome da base"
COL_COD_BASE = "Código da base"  # se existir, usamos para auditoria

UFS_PERMITIDAS = ["PA", "RR", "MT", "GO", "AP", "AM", "RO", "MS", "AC", "DF", "TO"]
BASES_EXCLUIR = ["PA DEVOLUÇÃO-GO"]

CRIAR_ABAS_POR_UF = True
NOME_ARQUIVO_SAIDA = "bases_por_uf_unico.xlsx"


# ==========================================================
# FUNÇÕES
# ==========================================================
def normalize_text(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = " ".join(s.split())
    return s


def escolher_coluna_uf(df: pd.DataFrame, ufs_permitidas: list[str]) -> str:
    candidatos = [c for c in df.columns if str(c).strip().lower().startswith("estado")]
    if not candidatos:
        raise ValueError("Não encontrei nenhuma coluna parecida com 'Estado' no arquivo.")

    ufs_set = set(ufs_permitidas)
    melhor_col = candidatos[0]
    melhor_score = -1.0

    for col in candidatos:
        s = (
            df[col]
            .astype(str)
            .str.strip()
            .str.upper()
            .replace({"NAN": None, "NONE": None, "": None})
        )
        validos = s.dropna()
        score = 0.0 if len(validos) == 0 else float(validos.isin(ufs_set).mean())
        if score > melhor_score:
            melhor_score = score
            melhor_col = col

    return melhor_col


def validar_limite_excel(df: pd.DataFrame, nome: str) -> None:
    if len(df) > 1_048_576:
        raise ValueError(
            f"A planilha '{nome}' tem {len(df):,} linhas e excede o limite do Excel (1.048.576)."
        )


def caminho_disponivel(caminho: str) -> str:
    """Se o arquivo existir (ou estiver travado), gera um caminho alternativo com sufixo."""
    if not os.path.exists(caminho):
        return caminho

    base, ext = os.path.splitext(caminho)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    alt = f"{base}_{ts}{ext}"
    return alt


def escrever_excel(saida_xlsx: str,
                  resumo: pd.DataFrame,
                  df_filtrado: pd.DataFrame,
                  col_uf: str,
                  variantes: pd.DataFrame,
                  multi_uf: pd.DataFrame) -> str:
    """Escreve o Excel. Se der PermissionError, escreve com nome alternativo."""
    try:
        with pd.ExcelWriter(saida_xlsx, engine="openpyxl") as writer:
            resumo.to_excel(writer, sheet_name="Resumo", index=False)
            df_filtrado.to_excel(writer, sheet_name="Base_Filtrada", index=False)

            if len(variantes) > 0:
                validar_limite_excel(variantes, "Variantes_NomeBase")
                variantes.to_excel(writer, sheet_name="Variantes_NomeBase", index=False)

            if len(multi_uf) > 0:
                validar_limite_excel(multi_uf, "Bases_em_Mais_de_1_UF")
                multi_uf.to_excel(writer, sheet_name="Bases_em_Mais_de_1_UF", index=False)

            if CRIAR_ABAS_POR_UF:
                for uf, duf in df_filtrado.groupby(col_uf):
                    validar_limite_excel(duf, str(uf))
                    duf.to_excel(writer, sheet_name=str(uf), index=False)

        return saida_xlsx

    except PermissionError:
        alt = caminho_disponivel(saida_xlsx)
        print("\n⚠️ PermissionError ao salvar (arquivo aberto/travado ou OneDrive).")
        print(f"➡️ Vou salvar com outro nome: {alt}\n")

        with pd.ExcelWriter(alt, engine="openpyxl") as writer:
            resumo.to_excel(writer, sheet_name="Resumo", index=False)
            df_filtrado.to_excel(writer, sheet_name="Base_Filtrada", index=False)

            if len(variantes) > 0:
                validar_limite_excel(variantes, "Variantes_NomeBase")
                variantes.to_excel(writer, sheet_name="Variantes_NomeBase", index=False)

            if len(multi_uf) > 0:
                validar_limite_excel(multi_uf, "Bases_em_Mais_de_1_UF")
                multi_uf.to_excel(writer, sheet_name="Bases_em_Mais_de_1_UF", index=False)

            if CRIAR_ABAS_POR_UF:
                for uf, duf in df_filtrado.groupby(col_uf):
                    validar_limite_excel(duf, str(uf))
                    duf.to_excel(writer, sheet_name=str(uf), index=False)

        return alt


# ==========================================================
# MAIN
# ==========================================================
def main():
    warnings.filterwarnings(
        "ignore",
        message="Workbook contains no default style*",
        category=UserWarning
    )

    root = Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    arquivo_excel = askopenfilename(
        title="Selecione a planilha Excel",
        filetypes=[("Excel", "*.xlsx *.xls")]
    )
    if not arquivo_excel:
        print("❌ Nenhum arquivo selecionado. Encerrando.")
        return

    pasta_saida = askdirectory(title="Selecione a pasta de saída")
    if not pasta_saida:
        print("❌ Nenhuma pasta selecionada. Encerrando.")
        return

    Path(pasta_saida).mkdir(parents=True, exist_ok=True)
    saida_xlsx = os.path.join(pasta_saida, NOME_ARQUIVO_SAIDA)

    # Abas
    xls = pd.ExcelFile(arquivo_excel, engine="openpyxl")
    abas = xls.sheet_names
    print(f"📄 Abas encontradas: {abas}")

    aba_escolhida = abas[0] if NOME_ABA is None else NOME_ABA
    if aba_escolhida not in abas:
        raise ValueError(f"Aba '{aba_escolhida}' não existe. Abas disponíveis: {abas}")
    print(f"✅ Aba usada: {aba_escolhida}")

    # Ler
    df = pd.read_excel(arquivo_excel, sheet_name=aba_escolhida, engine="openpyxl")

    # UF correto
    col_uf = escolher_coluna_uf(df, UFS_PERMITIDAS)
    print(f"✅ Coluna UF selecionada: {col_uf}")

    df[col_uf] = df[col_uf].astype(str).str.strip().str.upper()
    df_filtrado = df[df[col_uf].isin(UFS_PERMITIDAS)].copy()

    if COL_NOME_BASE not in df_filtrado.columns:
        raise ValueError(
            f"Coluna '{COL_NOME_BASE}' não encontrada.\n"
            f"Colunas disponíveis: {list(df_filtrado.columns)}"
        )

    df_filtrado["__nome_base_norm"] = df_filtrado[COL_NOME_BASE].map(normalize_text)

    # Remover base específica
    excluir_norm = {normalize_text(x) for x in BASES_EXCLUIR}
    antes = len(df_filtrado)
    df_filtrado = df_filtrado[~df_filtrado["__nome_base_norm"].isin(excluir_norm)].copy()
    print(f"🧹 Removidos por Nome da base: {antes - len(df_filtrado)} (lista: {BASES_EXCLUIR})")

    # Resumo
    agg_dict = {
        "qtd_linhas": (COL_NOME_BASE, "size"),
        "qtd_bases_unicas_norm": ("__nome_base_norm", pd.Series.nunique),
    }
    if COL_COD_BASE in df_filtrado.columns:
        agg_dict["qtd_codigos_unicos"] = (COL_COD_BASE, pd.Series.nunique)

    resumo = (
        df_filtrado
        .groupby(col_uf)
        .agg(**agg_dict)
        .reset_index()
        .rename(columns={col_uf: "UF"})
        .sort_values("UF")
    )

    # Auditorias
    variantes = (
        df_filtrado
        .groupby("__nome_base_norm")
        .agg(
            qtd_variantes_raw=(COL_NOME_BASE, pd.Series.nunique),
            total_linhas=(COL_NOME_BASE, "size"),
            exemplos_raw=(COL_NOME_BASE, lambda s: " | ".join(pd.Series(s.dropna().unique()).astype(str).head(6)))
        )
        .reset_index()
        .sort_values(["qtd_variantes_raw", "total_linhas"], ascending=[False, False])
    )
    variantes = variantes[variantes["qtd_variantes_raw"] > 1].copy()

    multi_uf = (
        df_filtrado
        .groupby("__nome_base_norm")
        .agg(
            qtd_ufs=(col_uf, pd.Series.nunique),
            ufs=(col_uf, lambda s: ", ".join(sorted(pd.Series(s.dropna().unique()).astype(str)))),
            total_linhas=(COL_NOME_BASE, "size"),
            exemplos_raw=(COL_NOME_BASE, lambda s: " | ".join(pd.Series(s.dropna().unique()).astype(str).head(6)))
        )
        .reset_index()
        .sort_values(["qtd_ufs", "total_linhas"], ascending=[False, False])
    )
    multi_uf = multi_uf[multi_uf["qtd_ufs"] > 1].copy()

    # Totais (para você comparar)
    total_unicas_norm_geral = int(df_filtrado["__nome_base_norm"].nunique())
    soma_por_uf_norm = int(resumo["qtd_bases_unicas_norm"].sum())

    print("\n📌 Auditoria de totals:")
    print(f"Total único (geral) por Nome normalizado: {total_unicas_norm_geral}")
    print(f"Soma por UF (Nome normalizado): {soma_por_uf_norm}")
    if COL_COD_BASE in df_filtrado.columns:
        print(f"Total único (geral) por Código da base: {int(df_filtrado[COL_COD_BASE].nunique())}")

    # Gravar (com fallback para PermissionError)
    validar_limite_excel(df_filtrado, "Base_Filtrada")
    validar_limite_excel(resumo, "Resumo")

    saida_final = escrever_excel(
        saida_xlsx=saida_xlsx,
        resumo=resumo,
        df_filtrado=df_filtrado,
        col_uf=col_uf,
        variantes=variantes,
        multi_uf=multi_uf
    )

    print("\n✅ Concluído.")
    print(f"📌 Arquivo gerado: {saida_final}")


if __name__ == "__main__":
    main()
