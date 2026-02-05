# -*- coding: utf-8 -*-
# 🚀 União de Planilhas com Polars (CSV/XLSX/XLS) + múltiplas abas Excel
# - Lê TODAS as abas de cada Excel
# - Adiciona colunas de rastreio: __arquivo e __aba
# - Exporta Excel em partes (1M linhas por aba) sem converter tudo para pandas de uma vez
# - Filtro 1: manter apenas valores específicos em "Aging超时类型"
# - Filtro 2: REMOVER bases específicas em "Unidade responsável责任机构"

import os
import sys
import tkinter as tk
from tkinter import filedialog
from datetime import datetime
import polars as pl
import pandas as pd


# =========================
# UTIL
# =========================
def _sniff_csv_separator(path: str, default=";") -> str:
    """Tenta adivinhar separador olhando a primeira linha."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            line = f.readline()
        candidates = [";", ",", "\t", "|"]
        scores = {c: line.count(c) for c in candidates}
        best = max(scores, key=scores.get)
        return best if scores[best] > 0 else default
    except Exception:
        return default


def _read_csv_polars(path: str) -> pl.DataFrame:
    """Lê CSV tentando encoding e separador."""
    sep = _sniff_csv_separator(path, default=";")
    for enc in ("utf-8", "latin1"):
        try:
            return pl.read_csv(path, separator=sep, encoding=enc, ignore_errors=True)
        except Exception:
            pass
    return pl.read_csv(path, separator=sep, ignore_errors=True)


def _read_excel_all_sheets(path: str) -> list[tuple[str, pl.DataFrame]]:
    """Lê TODAS as abas do Excel e retorna lista (nome_aba, df_polars)."""
    sheets = []
    try:
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
    except Exception:
        sheet_names = [None]

    for sh in sheet_names:
        # 1) tenta Polars
        try:
            if sh is None:
                df = pl.read_excel(path)
                sheets.append(("Sheet1", df))
            else:
                df = pl.read_excel(path, sheet_name=sh)
                sheets.append((str(sh), df))
            continue
        except Exception:
            pass

        # 2) fallback pandas -> polars
        try:
            if sh is None:
                pdf = pd.read_excel(path)
                sheets.append(("Sheet1", pl.from_pandas(pdf)))
            else:
                pdf = pd.read_excel(path, sheet_name=sh)
                sheets.append((str(sh), pl.from_pandas(pdf)))
        except Exception as e:
            raise RuntimeError(f"Falha ao ler Excel (aba={sh}): {e}")

    return sheets


# =========================
# MAIN
# =========================
def juntar_planilhas_na_pasta(diretorio_entrada=None, nome_saida="planilha_unificada"):
    if diretorio_entrada is None:
        print("📂 Selecione a pasta com as planilhas...")
        root = tk.Tk()
        root.withdraw()
        diretorio_entrada = filedialog.askdirectory(title="Selecione a pasta com as planilhas")
        root.destroy()

        if not diretorio_entrada:
            print("❌ Nenhuma pasta selecionada. Operação cancelada.")
            return

    if not os.path.isdir(diretorio_entrada):
        print(f"❌ Diretório inválido: {diretorio_entrada}")
        return

    print(f"🚀 Iniciando união de planilhas em: {diretorio_entrada}")

    arquivos = [
        f for f in os.listdir(diretorio_entrada)
        if os.path.isfile(os.path.join(diretorio_entrada, f))
    ]
    arquivos_planilha = [f for f in arquivos if f.lower().endswith((".csv", ".xlsx", ".xls"))]

    if not arquivos_planilha:
        print("⚠️ Nenhum arquivo CSV/XLSX/XLS encontrado.")
        return

    frames: list[pl.DataFrame] = []

    for arquivo in arquivos_planilha:
        caminho = os.path.join(diretorio_entrada, arquivo)
        ext = os.path.splitext(arquivo)[1].lower()

        try:
            if ext == ".csv":
                df = _read_csv_polars(caminho).with_columns([
                    pl.lit(arquivo).alias("__arquivo"),
                    pl.lit("CSV").alias("__aba"),
                ])
                print(f"📖 {arquivo} (CSV) -> {df.height} linhas, {df.width} colunas")
                frames.append(df)

            elif ext in (".xlsx", ".xls"):
                sheets = _read_excel_all_sheets(caminho)
                for aba, df in sheets:
                    df2 = df.with_columns([
                        pl.lit(arquivo).alias("__arquivo"),
                        pl.lit(aba).alias("__aba"),
                    ])
                    print(f"📖 {arquivo} [{aba}] -> {df2.height} linhas, {df2.width} colunas")
                    frames.append(df2)

        except Exception as e:
            print(f"❌ Erro ao ler {arquivo}: {e}")

    if not frames:
        print("⚠️ Nenhuma planilha foi lida com sucesso.")
        return

    print("🧩 Concatenando planilhas...")
    planilha_final = pl.concat(frames, how="diagonal_relaxed")

    # =========================
    # FILTRO 1 (MANTER) Aging超时类型
    # =========================
    COL_FILTRO_1 = "Aging超时类型"
    VALORES_PERMITIDOS = [
        "Exceed 7 days with no track",
        "Exceed 6 days with no track",
        "Exceed 5 days with no track",
        "Exceed 30 days with no track",
        "Exceed 14 days with no track",
        "Exceed 10 days with no track",
    ]

    if COL_FILTRO_1 in planilha_final.columns:
        antes = planilha_final.height
        planilha_final = planilha_final.filter(
            pl.col(COL_FILTRO_1)
              .cast(pl.Utf8)
              .str.strip_chars()
              .is_in(VALORES_PERMITIDOS)
        )
        depois = planilha_final.height
        print(f"🔎 Filtro (MANTER) '{COL_FILTRO_1}': {antes} -> {depois} linhas.")
    else:
        print(f"⚠️ Coluna '{COL_FILTRO_1}' não encontrada. (Sem filtro 1.)")

    # =========================
    # FILTRO 2 (REMOVER) Unidade responsável责任机构
    # =========================
    COL_FILTRO_2 = "Unidade responsável责任机构"
    BASES_REMOVER = [
        "TO PMW",
        "DC JUI-MT",
        "PA STM",
        "DC STM-PA",
        "DF BSB",
        "GO GYN",
        "DC GYN-GO",
        "DC MAO-AM",
        "AM MAO",
        "RO PVH",
        "DC PVH-RO",
        "MT CGB",
        "DC AGB-MT",
        "MS CGR",
        "DC CGR-MS",
        "PA MRB",
        "PA ANA",
        "DC PMW-TO",
        "DC MRB-PA",
        "DC RBR-AC",
    ]

    if COL_FILTRO_2 in planilha_final.columns:
        antes = planilha_final.height

        col_norm = (
            pl.col(COL_FILTRO_2)
              .cast(pl.Utf8)
              .fill_null("")
              .str.strip_chars()
        )

        # Mantém tudo que NÃO está na lista (e também mantém vazios/null)
        planilha_final = planilha_final.filter(
            (col_norm == "") | (~col_norm.is_in(BASES_REMOVER))
        )

        depois = planilha_final.height
        print(f"🧹 Filtro (REMOVER) '{COL_FILTRO_2}': {antes} -> {depois} linhas.")
    else:
        print(f"⚠️ Coluna '{COL_FILTRO_2}' não encontrada. (Sem filtro 2.)")

    total_linhas, total_colunas = planilha_final.shape
    print(f"✅ Planilha final contém {total_linhas} linhas e {total_colunas} colunas")

    # === Exportar CSV ===
    caminho_csv = os.path.join(diretorio_entrada, f"{nome_saida}.csv")
    try:
        planilha_final.write_csv(caminho_csv, separator=";", null_value="")
        print(f"💾 CSV salvo em: {caminho_csv}")
    except Exception as e:
        print(f"❌ Erro ao salvar CSV: {e}")

    # === Exportar Excel dividido em abas ===
    caminho_xlsx = os.path.join(diretorio_entrada, f"{nome_saida}.xlsx")
    max_linhas = 1_000_000
    print("📘 Exportando para Excel dividido em abas...")

    partes = max(1, (total_linhas + max_linhas - 1) // max_linhas)

    try:
        with pd.ExcelWriter(caminho_xlsx, engine="openpyxl") as writer:
            for i in range(partes):
                inicio = i * max_linhas
                tamanho = min(max_linhas, total_linhas - inicio)

                parte_pd = (
                    planilha_final
                    .slice(inicio, tamanho)
                    .to_pandas(use_pyarrow_extension_array=True)
                )

                sheet_name = f"Parte_{i + 1}"
                parte_pd.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f" -> Aba {sheet_name} com {len(parte_pd)} linhas salva.")

        print(f"✅ Excel salvo com sucesso em: {caminho_xlsx}")
    except Exception as e:
        print(f"❌ Erro ao salvar Excel: {e}")

    print(f"✨ Finalizado em {datetime.now().strftime('%H:%M:%S')} ✨")


if __name__ == "__main__":
    diretorio = sys.argv[1] if len(sys.argv) > 1 else None
    juntar_planilhas_na_pasta(diretorio)
