# -*- coding: utf-8 -*-
"""
KPI - Franquia
- Junta todas as planilhas Excel da pasta "SLA - Franquias"
- Salva consolidado em "KPI - Franquia.xlsx" (divide em abas se passar do limite do Excel)
- Padroniza a coluna de Base em TODAS as abas/arquivos para: "Base de entrega"
- Separa por Base e (opcional) exporta somente Franquias (Base começa com "F")

Requisitos:
    pip install pandas openpyxl
    (se tiver .xls antigo: pip install xlrd)
"""

from __future__ import annotations

import math
import re
import unicodedata
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd


# =============================================================================
# CONFIG (AJUSTADA PRA VOCÊ)
# =============================================================================

INPUT_DIR = Path(
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Política de incentivo e penalidade por KPI\SLA - Franquias"
)

OUTPUT_DIR = Path(
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Política de incentivo e penalidade por KPI"
)

CONSOLIDATED_FILENAME = "KPI - Franquia.xlsx"
CONSOLIDATED_SHEET_PREFIX = "Consolidado"

RECURSIVE = True
READ_ALL_SHEETS = True

EXPORT_BY_BASE = True
OUTPUT_BY_BASE_FORMAT = "csv"  # "xlsx" se preferir
CSV_SEP = ";"

# ✅ Nome PADRÃO que vamos garantir em todas as planilhas
CANONICAL_BASE_COLUMN = "Base de entrega"

# ✅ Se quiser exportar SOMENTE franquias (bases que começam com "F")
EXPORT_ONLY_FRANQUIAS = True
FRANQUIA_PREFIX = "F"  # base começa com "F" (ignorando espaços)

# (Fallback) nomes possíveis para coluna base em arquivos diferentes
BASE_COLUMN_CANDIDATES = [
    "base de entrega",
    "base entrega",
    "base",
    "unidade",
    "unidade responsavel",
    "unidade responsável",
    "cd",
    "hub",
    "filial",
    "station",
]

# Limite do Excel
EXCEL_MAX_ROWS = 1_048_576
EXCEL_DATA_ROWS_PER_SHEET = EXCEL_MAX_ROWS - 1  # reserva 1 linha pro cabeçalho


# =============================================================================
# HELPERS
# =============================================================================

def _norm_text(s: str) -> str:
    s = str(s).replace("\u00A0", " ").strip().lower()  # NBSP -> space
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa nomes das colunas (NBSP, espaços duplicados)."""
    df = df.copy()
    df.columns = [re.sub(r"\s+", " ", str(c).replace("\u00A0", " ").strip()) for c in df.columns]
    return df


def _safe_filename(name: str) -> str:
    name = str(name).replace("\u00A0", " ").strip()
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:120] if len(name) > 120 else name


def _unique_path(path_no_ext: Path, ext: str) -> Path:
    """Evita sobrescrever quando dois nomes viram o mesmo filename."""
    p = path_no_ext.with_suffix(ext)
    if not p.exists():
        return p

    k = 2
    while True:
        p2 = path_no_ext.parent / f"{path_no_ext.name} ({k})"
        p2 = p2.with_suffix(ext)
        if not p2.exists():
            return p2
        k += 1


def _list_excel_files(folder: Path, recursive: bool) -> List[Path]:
    exts = (".xlsx", ".xls")
    if recursive:
        files = [p for p in folder.rglob("*") if p.is_file() and p.suffix.lower() in exts]
    else:
        files = [p for p in folder.glob("*") if p.is_file() and p.suffix.lower() in exts]
    return sorted(files)


def _read_excel_any_sheet(path: Path, read_all_sheets: bool) -> List[Tuple[str, pd.DataFrame]]:
    suffix = path.suffix.lower()
    if suffix == ".xlsx":
        engine = "openpyxl"
    elif suffix == ".xls":
        engine = "xlrd"
    else:
        engine = None

    if read_all_sheets:
        sheets = pd.read_excel(path, sheet_name=None, engine=engine)
        out: List[Tuple[str, pd.DataFrame]] = []
        for sn, df in sheets.items():
            if df is not None and len(df) > 0:
                out.append((sn, df))
        return out
    else:
        df = pd.read_excel(path, sheet_name=0, engine=engine)
        return [("Sheet1", df)]


def _find_column_by_name(columns: List[str], wanted: str) -> Optional[str]:
    """Match exato ou normalizado."""
    if wanted in columns:
        return wanted
    wn = _norm_text(wanted)
    for c in columns:
        if _norm_text(c) == wn:
            return c
    return None


def _find_base_column_in_df(df: pd.DataFrame) -> Optional[str]:
    """Acha a coluna base dentro de um DF individual (fixa + candidatos)."""
    cols = list(df.columns)

    # 1) tenta achar pelo nome canônico
    c = _find_column_by_name(cols, CANONICAL_BASE_COLUMN)
    if c:
        return c

    # 2) tenta pelos candidatos
    cols_norm_map = {_norm_text(x): x for x in cols}
    for cand in BASE_COLUMN_CANDIDATES:
        cn = _norm_text(cand)
        if cn in cols_norm_map:
            return cols_norm_map[cn]

    # 3) match parcial
    for norm_col, orig in cols_norm_map.items():
        for cand in BASE_COLUMN_CANDIDATES:
            cn = _norm_text(cand)
            if cn and cn in norm_col:
                return orig

    return None


def _write_excel_split_sheets(df: pd.DataFrame, out_path: Path, sheet_prefix: str) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    n = len(df)

    if n <= EXCEL_DATA_ROWS_PER_SHEET:
        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_prefix[:31])
        return

    parts = math.ceil(n / EXCEL_DATA_ROWS_PER_SHEET)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for i in range(parts):
            start = i * EXCEL_DATA_ROWS_PER_SHEET
            end = min((i + 1) * EXCEL_DATA_ROWS_PER_SHEET, n)
            sheet_name = f"{sheet_prefix}_{i+1}"[:31]
            df.iloc[start:end].to_excel(writer, index=False, sheet_name=sheet_name)


def _export_df(df: pd.DataFrame, out_path_no_ext: Path, fmt: str) -> None:
    out_path_no_ext.parent.mkdir(parents=True, exist_ok=True)

    if fmt.lower() == "csv":
        outp = _unique_path(out_path_no_ext, ".csv")
        df.to_csv(outp, index=False, sep=CSV_SEP, encoding="utf-8-sig")
    elif fmt.lower() == "xlsx":
        outp = _unique_path(out_path_no_ext, ".xlsx")
        _write_excel_split_sheets(df, outp, sheet_prefix="Base")
    else:
        raise ValueError("OUTPUT_BY_BASE_FORMAT deve ser 'csv' ou 'xlsx'")


def _is_franquia(base_value: str) -> bool:
    s = str(base_value).replace("\u00A0", " ").strip().upper()
    return s.startswith(FRANQUIA_PREFIX.upper())
def main() -> None:
    if not INPUT_DIR.exists():
        raise FileNotFoundError(f"INPUT_DIR não existe: {INPUT_DIR}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    files = _list_excel_files(INPUT_DIR, RECURSIVE)
    if not files:
        print(f"[INFO] Nenhum Excel encontrado em: {INPUT_DIR}")
        return

    print(f"[INFO] Encontrados {len(files)} arquivos Excel em: {INPUT_DIR}")

    frames: List[pd.DataFrame] = []
    failed_files: List[str] = []

    # 1) Ler e juntar (padronizando a coluna Base em cada DF)
    for i, f in enumerate(files, start=1):
        try:
            items = _read_excel_any_sheet(f, READ_ALL_SHEETS)
            for sheet_name, df in items:
                if df is None or df.empty:
                    continue

                df = _clean_columns(df)

                # acha coluna base dentro desse df e renomeia p/ canônica
                base_col_local = _find_base_column_in_df(df)
                if base_col_local and base_col_local != CANONICAL_BASE_COLUMN:
                    df = df.rename(columns={base_col_local: CANONICAL_BASE_COLUMN})

                df["_arquivo_origem"] = f.name
                df["_aba_origem"] = sheet_name
                frames.append(df)

            print(f"[OK] ({i}/{len(files)}) {f.name}")
        except Exception as e:
            failed_files.append(f"{f.name} -> {e}")
            print(f"[ERRO] ({i}/{len(files)}) Falha lendo {f.name}: {e}")

    if not frames:
        print("[INFO] Nenhuma linha foi carregada.")
        if failed_files:
            print("\n[DETALHES] Arquivos que falharam:")
            for x in failed_files:
                print(" -", x)
        return

    df_all = pd.concat(frames, ignore_index=True, sort=False)
    print(f"[INFO] Linhas consolidadas: {len(df_all):,}".replace(",", "."))

    # 2) Salvar consolidado (quebrando em abas se precisar)
    consolidated_path = OUTPUT_DIR / CONSOLIDATED_FILENAME
    try:
        _write_excel_split_sheets(df_all, consolidated_path, sheet_prefix=CONSOLIDATED_SHEET_PREFIX)
        if len(df_all) <= EXCEL_DATA_ROWS_PER_SHEET:
            print(f"[OK] Consolidado salvo (1 aba) em: {consolidated_path}")
        else:
            parts = math.ceil(len(df_all) / EXCEL_DATA_ROWS_PER_SHEET)
            print(f"[OK] Consolidado salvo (dividido em {parts} abas) em: {consolidated_path}")
    except PermissionError:
        print(f"[ERRO] Sem permissão para escrever: {consolidated_path}")
        print("       Feche o Excel (se estiver aberto) e rode novamente.")
        return

    # 3) Separar por Base (agora usando SEMPRE a coluna canônica)
    if not EXPORT_BY_BASE:
        print("[DONE] (sem separação por base)")
        return

    if CANONICAL_BASE_COLUMN not in df_all.columns:
        print("[WARN] A coluna de base não foi encontrada em nenhum arquivo.")
        print("       Mantive apenas o consolidado.")
        return

    df_all[CANONICAL_BASE_COLUMN] = (
        df_all[CANONICAL_BASE_COLUMN]
        .astype(str)
        .str.replace("\u00A0", " ", regex=False)
        .str.strip()
    )

    # stats pra você bater o olho
    total_com_base = (df_all[CANONICAL_BASE_COLUMN].notna() & (df_all[CANONICAL_BASE_COLUMN] != "")).sum()
    print(f"[INFO] Linhas com Base preenchida: {int(total_com_base):,}".replace(",", "."))

    # filtra franquias só na hora de exportar por base (consolidado fica completo)
    df_split = df_all[df_all[CANONICAL_BASE_COLUMN].notna() & (df_all[CANONICAL_BASE_COLUMN] != "")].copy()

    if EXPORT_ONLY_FRANQUIAS:
        mask_f = df_split[CANONICAL_BASE_COLUMN].apply(_is_franquia)
        print(f"[INFO] Linhas franquias (Base começa com '{FRANQUIA_PREFIX}'): {int(mask_f.sum()):,}".replace(",", "."))
        df_split = df_split[mask_f].copy()

    bases = sorted(df_split[CANONICAL_BASE_COLUMN].dropna().unique().tolist())
    print(f"[INFO] Bases para exportar: {len(bases)} (coluna: {CANONICAL_BASE_COLUMN})")

    out_base_dir = OUTPUT_DIR / "KPI - Franquia (por base)"
    out_base_dir.mkdir(parents=True, exist_ok=True)

    exported = 0
    for b in bases:
        df_b = df_split[df_split[CANONICAL_BASE_COLUMN] == b].copy()
        fname = _safe_filename(b)
        out_path = out_base_dir / fname
        _export_df(df_b, out_path, OUTPUT_BY_BASE_FORMAT)
        exported += 1

    print(f"[OK] Arquivos gerados por base: {exported}")
    print(f"[DONE] Saída em: {OUTPUT_DIR}")

    if failed_files:
        print("\n[WARN] Alguns arquivos falharam ao ler. Detalhes:")
        for x in failed_files:
            print(" -", x)


if __name__ == "__main__":
    main()
