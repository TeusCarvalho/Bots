# -*- coding: utf-8 -*-
"""
Auditoria de colunas em planilhas (XLSX/XLSM/XLS/XLSB) e CSV com seletor de pasta (Windows).

O que faz:
- Seleciona pasta via janela (tkinter)
- Varre arquivos (com opção recursiva)
- Imprime lista de arquivos encontrados e total (para validar "77 planilhas")
- Lê apenas cabeçalho (nrows=0)
- Identifica padrão de colunas (mais comum)
- Aponta divergências (faltando/sobrando)
- Gera relatório CSV + lista TXT de arquivos encontrados

Requisitos mínimos:
  pip install pandas openpyxl

Para ler .xls:
  pip install xlrd

Para ler .xlsb:
  pip install pyxlsb
"""

# =========================
# BLOCO 1 — IMPORTS
# =========================
import os
import re
import unicodedata
import warnings
from datetime import datetime
from collections import Counter

import pandas as pd

# Seletor de pasta (built-in no Windows Python)
import tkinter as tk
from tkinter import filedialog


# =========================
# BLOCO 2 — CONFIG
# =========================
RECURSIVO = True

# Inclui .xls e .xlsb para contar/varrer — leitura depende de libs extras
EXTENSOES = (".xlsx", ".xlsm", ".csv", ".xls", ".xlsb")

CHECAR_TODAS_ABAS = True
NORMALIZAR_PARA_COMPARAR = True

# Opcional: conferir se existe uma coluna específica em todos os arquivos/abas
COLUNA_OBRIGATORIA = None  # ex: "Número de telefone"

# Se você quiser ignorar abas específicas (ex.: resumos), coloque nomes aqui
IGNORAR_ABAS_EXATAS = set()  # ex: {"Resumo", "Bases_em_Mais_de_1_UF"}

# Mostrar e salvar lista de arquivos encontrados (recomendado para validar "77")
MOSTRAR_LISTA_ARQUIVOS = True
SALVAR_LISTA_ARQUIVOS = True

# Suprimir warning do openpyxl que não atrapalha o processamento
warnings.filterwarnings("ignore", message="Workbook contains no default style*")


# =========================
# BLOCO 3 — FUNÇÕES UTIL
# =========================
def escolher_pasta() -> str:
    """Abre janela para selecionar pasta e retorna o caminho."""
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    pasta = filedialog.askdirectory(title="Selecione a pasta com as planilhas")
    root.destroy()
    return pasta


def is_temp_file(filename: str) -> bool:
    """Ignora arquivos temporários do Excel (ex: ~$arquivo.xlsx)."""
    base = os.path.basename(filename)
    return base.startswith("~$")


def normalize_col(col: str) -> str:
    """Normaliza nomes de colunas para comparação."""
    if col is None:
        return ""
    s = str(col).strip()
    s = re.sub(r"\s+", " ", s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower()


def list_files(base_dir: str, recursivo: bool, extensoes: tuple) -> list:
    files = []
    if recursivo:
        for root, _, fnames in os.walk(base_dir):
            for fn in fnames:
                if is_temp_file(fn):
                    continue
                if fn.lower().endswith(tuple(e.lower() for e in extensoes)):
                    files.append(os.path.join(root, fn))
    else:
        for fn in os.listdir(base_dir):
            full = os.path.join(base_dir, fn)
            if os.path.isfile(full) and (not is_temp_file(fn)) and fn.lower().endswith(tuple(e.lower() for e in extensoes)):
                files.append(full)
    return sorted(files)


def get_excel_engine_by_ext(ext: str):
    """
    Define engine do pandas conforme extensão.
    - .xlsx/.xlsm -> openpyxl
    - .xls -> xlrd (requer xlrd instalado)
    - .xlsb -> pyxlsb (requer pyxlsb instalado)
    """
    ext = ext.lower()
    if ext in (".xlsx", ".xlsm"):
        return "openpyxl"
    if ext == ".xls":
        return "xlrd"
    if ext == ".xlsb":
        return "pyxlsb"
    return None


# =========================
# BLOCO 4 — LEITURA DE CABEÇALHO
# =========================
def read_header_csv(path: str) -> list:
    df = pd.read_csv(path, nrows=0, sep=None, engine="python")
    return list(df.columns)


def read_header_excel_any(path: str, all_sheets: bool) -> list:
    """
    Lê cabeçalho de Excel (xlsx/xlsm/xls/xlsb).
    Retorna lista de dicts: [{"sheet": nome, "cols": [...]}, ...]
    """
    ext = os.path.splitext(path)[1].lower()
    engine = get_excel_engine_by_ext(ext)

    # ExcelFile com engine explícito (importante para xls/xlsb)
    xls = pd.ExcelFile(path, engine=engine)
    sheets = xls.sheet_names

    targets = sheets if all_sheets else sheets[:1]
    out = []

    for sh in targets:
        if sh in IGNORAR_ABAS_EXATAS:
            continue

        df = pd.read_excel(xls, sheet_name=sh, nrows=0)  # lê só colunas
        out.append({"sheet": sh, "cols": list(df.columns)})

    return out


# =========================
# BLOCO 5 — BASELINE (PADRÃO)
# =========================
def compute_baseline(records: list) -> dict:
    """
    Define o padrão como o conjunto de colunas mais comum (ignora ordem).
    """
    if not records:
        return {"baseline_set": frozenset(), "baseline_order": tuple(), "baseline_label": ""}

    set_keys = []
    order_keys = []

    for r in records:
        cols_cmp = r["cols_cmp"]
        set_keys.append(frozenset(cols_cmp))
        order_keys.append(tuple(cols_cmp))

    set_counter = Counter(set_keys)
    baseline_set, _ = set_counter.most_common(1)[0]

    order_counter = Counter([ok for ok, sk in zip(order_keys, set_keys) if sk == baseline_set])
    baseline_order, _ = order_counter.most_common(1)[0]

    return {
        "baseline_set": baseline_set,
        "baseline_order": baseline_order,
        "baseline_label": "Padrão = conjunto de colunas mais comum (ignora ordem)"
    }


# =========================
# BLOCO 6 — MAIN
# =========================
def main():
    base_dir = escolher_pasta()

    if not base_dir:
        print("Nenhuma pasta selecionada. Encerrando.")
        return

    if not os.path.isdir(base_dir):
        raise FileNotFoundError(f"Pasta não encontrada: {base_dir}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    arq_saida_csv = os.path.join(base_dir, f"auditoria_colunas_{timestamp}.csv")
    arq_lista_txt = os.path.join(base_dir, f"arquivos_encontrados_{timestamp}.txt")

    files = list_files(base_dir, RECURSIVO, EXTENSOES)

    # ---------- validação: "77 planilhas?" ----------
    if MOSTRAR_LISTA_ARQUIVOS:
        print("=" * 80)
        print("ARQUIVOS ENCONTRADOS (para validar quantidade)")
        for p in files:
            print(" -", p)
        print("TOTAL DE ARQUIVOS ENCONTRADOS:", len(files))
        print("=" * 80)

    if SALVAR_LISTA_ARQUIVOS:
        with open(arq_lista_txt, "w", encoding="utf-8") as f:
            for p in files:
                f.write(p + "\n")
            f.write(f"\nTOTAL: {len(files)}\n")
        print("Lista de arquivos salva em:", arq_lista_txt)

    if not files:
        print("Nenhum arquivo encontrado com as extensões:", EXTENSOES)
        return

    records = []

    for fpath in files:
        ext = os.path.splitext(fpath)[1].lower()

        try:
            if ext == ".csv":
                cols = read_header_csv(fpath)
                cols_cmp = [normalize_col(c) for c in cols] if NORMALIZAR_PARA_COMPARAR else list(cols)

                records.append({
                    "arquivo": fpath,
                    "aba": "(CSV)",
                    "tipo": "csv",
                    "cols_original": cols,
                    "cols_cmp": cols_cmp,
                    "erro": ""
                })

            else:
                sheets_info = read_header_excel_any(fpath, CHECAR_TODAS_ABAS)
                # Se ignorou todas as abas por nome e ficou vazio, registra isso
                if not sheets_info:
                    records.append({
                        "arquivo": fpath,
                        "aba": "(nenhuma aba processada)",
                        "tipo": ext.replace(".", ""),
                        "cols_original": [],
                        "cols_cmp": [],
                        "erro": "Nenhuma aba foi processada (talvez ignorada por nome)."
                    })
                else:
                    for item in sheets_info:
                        cols = item["cols"]
                        cols_cmp = [normalize_col(c) for c in cols] if NORMALIZAR_PARA_COMPARAR else list(cols)

                        records.append({
                            "arquivo": fpath,
                            "aba": item["sheet"],
                            "tipo": ext.replace(".", ""),
                            "cols_original": cols,
                            "cols_cmp": cols_cmp,
                            "erro": ""
                        })

        except Exception as e:
            # Não trava: registra erro no relatório
            records.append({
                "arquivo": fpath,
                "aba": "(erro ao ler)",
                "tipo": ext.replace(".", ""),
                "cols_original": [],
                "cols_cmp": [],
                "erro": str(e)
            })

    ok_records = [r for r in records if not r["erro"]]
    baseline = compute_baseline(ok_records)

    baseline_set = baseline["baseline_set"]
    baseline_order = baseline["baseline_order"]

    rows = []
    for r in records:
        if r["erro"]:
            status = "ERRO_LEITURA"
            missing = []
            extra = []
            has_required = ""
        else:
            s = set(r["cols_cmp"])
            missing = sorted(list(baseline_set - s))
            extra = sorted(list(s - baseline_set))

            if missing or extra:
                status = "DIFERENTE"
            else:
                status = "OK" if tuple(r["cols_cmp"]) == tuple(baseline_order) else "MESMAS_COLUNAS_ORDEM_DIFERENTE"

            if COLUNA_OBRIGATORIA:
                required_cmp = normalize_col(COLUNA_OBRIGATORIA) if NORMALIZAR_PARA_COMPARAR else COLUNA_OBRIGATORIA
                has_required = "SIM" if required_cmp in s else "NAO"
            else:
                has_required = ""

        rows.append({
            "status": status,
            "arquivo": r["arquivo"],
            "aba": r["aba"],
            "tipo": r["tipo"],
            "faltando_colunas_vs_padrao": ", ".join(missing),
            "sobrando_colunas_vs_padrao": ", ".join(extra),
            "tem_coluna_obrigatoria": has_required,
            "colunas_lidas": " | ".join([str(c) for c in r.get("cols_original", [])]),
            "erro": r["erro"],
        })

    df = pd.DataFrame(rows)

    # ---------- saída / resumo ----------
    print("=" * 80)
    print("AUDITORIA DE COLUNAS")
    print("Pasta:", base_dir)
    print(baseline.get("baseline_label", ""))
    print("Arquivos encontrados:", len(files))
    print("Registros (arquivo/aba):", len(records))
    print("-" * 80)
    print("Status count:")
    print(df["status"].value_counts(dropna=False))
    print("-" * 80)

    diffs = df[df["status"].isin(["DIFERENTE", "ERRO_LEITURA"])]
    if diffs.empty:
        print("✅ Nenhuma diferença encontrada (tudo OK).")
    else:
        print("⚠️ Itens com diferença/erro (primeiros 30):")
        print(
            diffs[["status", "arquivo", "aba", "faltando_colunas_vs_padrao", "sobrando_colunas_vs_padrao", "erro"]]
            .head(30)
            .to_string(index=False)
        )

    df.to_csv(arq_saida_csv, index=False, encoding="utf-8-sig")
    print("-" * 80)
    print("Relatório salvo em:", arq_saida_csv)
    print("=" * 80)


if __name__ == "__main__":
    main()
