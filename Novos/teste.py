# -*- coding: utf-8 -*-
"""
scan_nome_arquivo.py

Varre recursivamente a pasta ROOT_DIR e lista arquivos cujo NOME contém o texto TARGET.
Gera: relatorio_match_nome_arquivo.csv
"""

import os
import pandas as pd

ROOT_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\QUALIDADE_ FILIAL GO - BASE DE DADOS"
TARGET = "订单来源name"

OUT_CSV = "relatorio_match_nome_arquivo.csv"


def main():
    rows = []
    total = 0

    for base, _, files in os.walk(ROOT_DIR):
        for fn in files:
            total += 1
            if TARGET in fn:
                rows.append({
                    "file_name": fn,
                    "full_path": os.path.join(base, fn)
                })

    pd.DataFrame(rows).to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    print("===============================================")
    print("SCAN NOME DE ARQUIVO FINALIZADO")
    print(f"Pasta raiz: {ROOT_DIR}")
    print(f"Alvo: {TARGET}")
    print(f"Arquivos varridos: {total}")
    print(f"Matches no nome: {len(rows)}")
    print(f"Relatório: {OUT_CSV}")
    print("===============================================")


if __name__ == "__main__":
    main()
# -*- coding: utf-8 -*-
"""
scan_coluna_dentro_arquivo.py

Varre recursivamente a pasta ROOT_DIR e identifica em quais arquivos/abas
existe a coluna TARGET_COLUMN (header).

Suporta:
- Excel: .xlsx, .xlsm (por abas, tentando header em várias linhas)
- CSV/TXT: .csv, .txt (lê header)

Gera:
- relatorio_coluna_encontrada.csv
- relatorio_erros_leitura.csv
"""

import os
import csv
import traceback
import unicodedata
from datetime import datetime

import pandas as pd

# =========================
# CONFIG
# =========================
ROOT_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\QUALIDADE_ FILIAL GO - BASE DE DADOS"
TARGET_COLUMN = "订单来源name"

OUTPUT_FOUND = "relatorio_coluna_encontrada.csv"
OUTPUT_ERRORS = "relatorio_erros_leitura.csv"

EXTENSIONS = {".xlsx", ".xlsm", ".csv", ".txt"}

CSV_ENCODINGS_TO_TRY = ["utf-8-sig", "utf-8", "latin1", "cp1252"]

# tenta header em 0..30 (ajuste se precisar)
MAX_HEADER_ROW_GUESS = 30

# remove caracteres invisíveis comuns
ZERO_WIDTH = {
    "\u200b", "\u200c", "\u200d", "\u200e", "\u200f",
    "\u202a", "\u202b", "\u202c", "\u202d", "\u202e",
}


def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace("\ufeff", "").replace("\u00a0", " ")
    for ch in ZERO_WIDTH:
        s = s.replace(ch, "")
    s = s.strip()
    s = unicodedata.normalize("NFC", s)
    return s


def list_files(root_dir: str):
    for base, _, files in os.walk(root_dir):
        for fn in files:
            if fn.startswith("~$"):  # ignora lock do Excel
                continue
            ext = os.path.splitext(fn)[1].lower()
            if ext in EXTENSIONS:
                yield os.path.join(base, fn)


def read_csv_header(path: str):
    for enc in CSV_ENCODINGS_TO_TRY:
        try:
            with open(path, "r", encoding=enc, errors="strict", newline="") as f:
                reader = csv.reader(f)
                header = next(reader, [])
            cols = [normalize_text(x) for x in header]
            return cols, enc
        except Exception:
            continue

    # fallback tolerante
    with open(path, "r", encoding="latin1", errors="replace", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
    cols = [normalize_text(x) for x in header]
    return cols, "latin1(errors=replace)"


def scan_excel(path: str, target_norm: str):
    xf = pd.ExcelFile(path)
    hits = []  # (sheet, header_row)

    for sheet in xf.sheet_names:
        for header_row in range(0, MAX_HEADER_ROW_GUESS + 1):
            try:
                df_head = pd.read_excel(path, sheet_name=sheet, header=header_row, nrows=0)
                cols = [normalize_text(c) for c in list(df_head.columns)]
                if target_norm in cols:
                    hits.append((sheet, header_row))
                    break
            except Exception:
                continue

    return hits


def main():
    start = datetime.now()
    target_norm = normalize_text(TARGET_COLUMN)

    found_rows = []
    error_rows = []

    total = 0
    for path in list_files(ROOT_DIR):
        total += 1
        ext = os.path.splitext(path)[1].lower()

        try:
            if ext in (".csv", ".txt"):
                cols, enc = read_csv_header(path)
                if target_norm in cols:
                    found_rows.append({
                        "file": path,
                        "type": "csv/txt",
                        "sheet": "",
                        "header_row": "",
                        "encoding": enc
                    })

            elif ext in (".xlsx", ".xlsm"):
                hits = scan_excel(path, target_norm)
                for sheet, header_row in hits:
                    found_rows.append({
                        "file": path,
                        "type": "excel",
                        "sheet": sheet,
                        "header_row": header_row,
                        "encoding": ""
                    })

        except Exception as e:
            error_rows.append({
                "file": path,
                "type": ext,
                "error": str(e),
                "traceback": traceback.format_exc()
            })

        if total % 200 == 0:
            print(f"[{total}] arquivos varridos... encontrados={len(found_rows)} erros={len(error_rows)}")

    pd.DataFrame(found_rows).to_csv(OUTPUT_FOUND, index=False, encoding="utf-8-sig")
    pd.DataFrame(error_rows).to_csv(OUTPUT_ERRORS, index=False, encoding="utf-8-sig")

    end = datetime.now()
    print("===============================================")
    print("SCAN (COLUNA NO ARQUIVO) FINALIZADO")
    print(f"Pasta raiz: {ROOT_DIR}")
    print(f"Coluna alvo: {TARGET_COLUMN}")
    print(f"Arquivos analisados: {total}")
    print(f"Ocorrências encontradas: {len(found_rows)}")
    print(f"Erros de leitura: {len(error_rows)}")
    print(f"Relatório (found): {OUTPUT_FOUND}")
    print(f"Relatório (errors): {OUTPUT_ERRORS}")
    print(f"Duração: {end - start}")
    print("===============================================")


if __name__ == "__main__":
    main()
