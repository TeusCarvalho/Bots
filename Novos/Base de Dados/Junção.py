from __future__ import annotations

import argparse
import fnmatch
import logging
import re
from datetime import datetime
from math import ceil
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import polars as pl


# -----------------------------
# LOG
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("unir_planilhas")


# -----------------------------
# CONSTANTES
# -----------------------------
EXCEL_EXTS = {".xlsx", ".xlsm", ".xls"}
CSV_EXTS = {".csv"}

# Limite real do Excel por aba: 1.048.576 linhas (inclui a linha de cabeçalho)
XLSX_MAX_ROWS_DEFAULT = 1_048_576


# -----------------------------
# UI (Tkinter)
# -----------------------------
def pick_folder_dialog(title: str) -> Optional[Path]:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        log.error("Tkinter não está disponível. Use --input-dir para informar a pasta.")
        return None

    root = tk.Tk()
    root.withdraw()
    folder = filedialog.askdirectory(title=title)
    root.destroy()
    if not folder:
        return None
    return Path(folder)


def pick_save_file_dialog(title: str) -> Optional[Path]:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        log.error("Tkinter não está disponível. Use --output para informar o arquivo de saída.")
        return None

    root = tk.Tk()
    root.withdraw()
    path = filedialog.asksaveasfilename(
        title=title,
        defaultextension=".xlsx",
        filetypes=[
            ("Excel", "*.xlsx"),
            ("Parquet", "*.parquet"),
            ("CSV", "*.csv"),
            ("Todos", "*.*"),
        ],
    )
    root.destroy()
    if not path:
        return None
    return Path(path)


# -----------------------------
# EXTRAIR DATA DO NOME DO ARQUIVO
# -----------------------------
def extract_date_from_filename(file_name: str) -> Tuple[str, str]:
    """
    Retorna (raw, iso). Se não achar, retorna ("", "").
    Pega a ÚLTIMA data encontrada no nome para ser mais robusto.
    Suporta:
      - dd.mm.yyyy  (12.02.2026)
      - dd-mm-yyyy  (12-02-2026)
      - dd_mm_yyyy  (12_02_2026)
      - yyyy-mm-dd  (2026-02-12)
      - yyyymmdd    (20260212)
      - ddmmyyyy    (12022026)
    """
    stem = Path(file_name).stem

    patterns = [
        (r"(\d{2}\.\d{2}\.\d{4})", "%d.%m.%Y"),
        (r"(\d{2}-\d{2}-\d{4})", "%d-%m-%Y"),
        (r"(\d{2}_\d{2}_\d{4})", "%d_%m_%Y"),
        (r"(\d{4}-\d{2}-\d{2})", "%Y-%m-%d"),
    ]

    # primeiro tenta formatos explícitos com separadores
    candidates: List[Tuple[str, str]] = []
    for rx, fmt in patterns:
        matches = re.findall(rx, stem)
        for m in matches:
            candidates.append((m, fmt))

    if candidates:
        raw, fmt = candidates[-1]  # pega o último
        try:
            dt = datetime.strptime(raw, fmt).date()
            return raw, dt.isoformat()
        except Exception:
            return raw, ""

    # tenta números puros (8 dígitos)
    matches8 = re.findall(r"(\d{8})", stem)
    if matches8:
        raw = matches8[-1]
        # tenta YYYYMMDD
        try:
            dt = datetime.strptime(raw, "%Y%m%d").date()
            return raw, dt.isoformat()
        except Exception:
            pass
        # tenta DDMMYYYY
        try:
            dt = datetime.strptime(raw, "%d%m%Y").date()
            return raw, dt.isoformat()
        except Exception:
            return raw, ""

    return "", ""
# =========================
# BLOCO 2/4 — LEITURA / CONCAT
# =========================
def list_files(input_dir: Path, pattern: str, recursive: bool) -> List[Path]:
    files = [p for p in (input_dir.rglob("*") if recursive else input_dir.glob("*")) if p.is_file()]
    files = [p for p in files if fnmatch.fnmatch(p.name, pattern)]
    files = [p for p in files if p.suffix.lower() in (EXCEL_EXTS | CSV_EXTS)]
    files.sort()
    return files


def safe_pl_concat(dfs: List[pl.DataFrame]) -> pl.DataFrame:
    if not dfs:
        return pl.DataFrame()

    # concat mantendo todas as colunas
    for how in ("diagonal_relaxed", "diagonal"):
        try:
            return pl.concat(dfs, how=how)  # type: ignore[arg-type]
        except Exception:
            pass

    # fallback: alinha colunas manualmente
    all_cols = sorted({c for df in dfs for c in df.columns})
    aligned: List[pl.DataFrame] = []
    for df in dfs:
        missing = [c for c in all_cols if c not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(c) for c in missing])
        df = df.select(all_cols)
        aligned.append(df)

    return pl.concat(aligned, how="vertical")


def read_csv_to_polars(fp: Path, encoding: str, separator: str) -> pl.DataFrame:
    raw, iso = extract_date_from_filename(fp.name)

    df = pl.read_csv(
        fp,
        encoding=encoding,
        separator=separator,
        infer_schema_length=2000,
        ignore_errors=True,
        try_parse_dates=True,
    )

    df = df.with_columns([
        pl.lit(fp.name).alias("_arquivo"),
        pl.lit("").alias("_aba"),
        pl.lit(raw).alias("_data_raw_nome"),
        pl.lit(iso).alias("_data_arquivo_nome"),
    ])
    return df


def read_excel_all_sheets_to_polars(fp: Path) -> List[pl.DataFrame]:
    raw, iso = extract_date_from_filename(fp.name)

    out: List[pl.DataFrame] = []
    xls = pd.ExcelFile(fp)  # openpyxl para xlsx/xlsm

    for sheet in xls.sheet_names:
        pdf = pd.read_excel(fp, sheet_name=sheet, dtype="object")
        if pdf is None or pdf.empty:
            continue

        pdf.columns = [str(c).strip() for c in pdf.columns]
        df = pl.from_pandas(pdf, include_index=False)

        df = df.with_columns([
            pl.lit(fp.name).alias("_arquivo"),
            pl.lit(str(sheet)).alias("_aba"),
            pl.lit(raw).alias("_data_raw_nome"),
            pl.lit(iso).alias("_data_arquivo_nome"),
        ])
        out.append(df)

    return out
# =========================
# BLOCO 3/4 — SALVAR SAÍDA (FIX DO LIMITE EXCEL)
# =========================
def _save_xlsx_chunked(df: pl.DataFrame, output: Path, max_rows_excel: int, sheet_prefix: str) -> None:
    """
    Salva em .xlsx.
    FIX: o limite do Excel (1.048.576) inclui o cabeçalho.
    Então o máximo de LINHAS DE DADOS por aba é: max_rows_excel - 1.
    """
    output.parent.mkdir(parents=True, exist_ok=True)

    n_rows = df.height
    if n_rows == 0:
        pdf = df.to_pandas(use_pyarrow_extension_array=True)
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pdf.to_excel(writer, index=False, sheet_name="BASE_UNIFICADA")
        return

    max_data_rows = max(1, int(max_rows_excel) - 1)  # reserva 1 linha para o cabeçalho

    if n_rows <= max_data_rows:
        pdf = df.to_pandas(use_pyarrow_extension_array=True)
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            pdf.to_excel(writer, index=False, sheet_name="BASE_UNIFICADA")
        return

    parts = ceil(n_rows / max_data_rows)
    log.warning(
        f"⚠️ Excel limite por aba: {max_rows_excel:,} (inclui cabeçalho). "
        f"Dados por aba: {max_data_rows:,}. Base tem {n_rows:,} linhas -> dividindo em {parts} abas."
    )

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for i in range(parts):
            start = i * max_data_rows
            length = min(max_data_rows, n_rows - start)
            part = df.slice(start, length)

            sheet_name = f"{sheet_prefix}_{i+1:04d}"[:31]
            log.info(f"Exportando aba {sheet_name} ({start+1:,}..{start+length:,})")

            pdf = part.to_pandas(use_pyarrow_extension_array=True)
            pdf.to_excel(writer, index=False, sheet_name=sheet_name)


def save_output(df: pl.DataFrame, output: Path, xlsx_max_rows: int, xlsx_sheet_prefix: str) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    ext = output.suffix.lower()

    if ext == ".parquet":
        df.write_parquet(output)
        return

    if ext == ".csv":
        df.write_csv(output)
        return

    if ext == ".xlsx":
        _save_xlsx_chunked(df, output, max_rows_excel=xlsx_max_rows, sheet_prefix=xlsx_sheet_prefix)
        return

    raise ValueError(f"Extensão não suportada: {ext} (use .csv, .parquet ou .xlsx)")
# =========================
# BLOCO 4/4 — MAIN
# =========================
def main():
    ap = argparse.ArgumentParser(description="Juntar várias planilhas (Excel/CSV) em uma base única.")
    ap.add_argument("--input-dir", default=None, help="Pasta com as planilhas (se não passar, abre janela)")
    ap.add_argument("--output", default=None, help="Arquivo de saída .csv/.parquet/.xlsx (se não passar, abre janela)")
    ap.add_argument("--pattern", default="*.*", help='Filtro, ex: "*.xlsx" ou "*.csv" (padrão: *.*)')
    ap.add_argument("--recursive", action="store_true", help="Buscar em subpastas também")
    ap.add_argument("--csv-encoding", default="utf-8", help="Encoding dos CSV (ex: latin1)")
    ap.add_argument("--csv-sep", default=",", help="Separador do CSV (ex: ;)")

    ap.add_argument("--xlsx-max-rows", type=int, default=XLSX_MAX_ROWS_DEFAULT,
                    help=f"Máximo de linhas por aba no Excel (inclui cabeçalho). Padrão: {XLSX_MAX_ROWS_DEFAULT}")
    ap.add_argument("--xlsx-sheet-prefix", default="BASE", help="Prefixo das abas quando dividir (padrão: BASE)")

    args = ap.parse_args()

    # escolher pasta
    if not args.input_dir:
        picked = pick_folder_dialog("Selecione a pasta com as planilhas")
        if not picked:
            log.warning("Nenhuma pasta selecionada. Operação cancelada.")
            return
        input_dir = picked
    else:
        input_dir = Path(args.input_dir)

    if not input_dir.exists():
        raise FileNotFoundError(f"Pasta não existe: {input_dir}")

    # escolher saída
    if not args.output:
        picked_out = pick_save_file_dialog("Salvar arquivo unificado como...")
        if not picked_out:
            log.warning("Nenhum arquivo de saída selecionado. Operação cancelada.")
            return
        output = picked_out
    else:
        output = Path(args.output)

    files = list_files(input_dir, args.pattern, args.recursive)
    if not files:
        log.warning("Nenhum arquivo encontrado com esse filtro.")
        return

    log.info(f"Pasta: {input_dir}")
    log.info(f"Arquivos encontrados: {len(files)}")

    dfs: List[pl.DataFrame] = []

    for i, fp in enumerate(files, start=1):
        ext = fp.suffix.lower()
        log.info(f"[{i}/{len(files)}] Lendo: {fp.name}")

        try:
            if ext in CSV_EXTS:
                dfs.append(read_csv_to_polars(fp, encoding=args.csv_encoding, separator=args.csv_sep))

            elif ext in EXCEL_EXTS:
                sheets = read_excel_all_sheets_to_polars(fp)
                if sheets:
                    dfs.extend(sheets)
                else:
                    log.warning(f"Excel sem dados úteis: {fp.name}")

        except Exception as e:
            log.error(f"Falhou ao ler {fp.name}: {e}")

    if not dfs:
        log.warning("Nenhuma tabela foi carregada (tudo falhou ou vazio).")
        return

    log.info("Concatenando (mantendo todas as colunas)...")
    base = safe_pl_concat(dfs)

    log.info(f"Linhas: {base.height:,} | Colunas: {len(base.columns):,}")
    log.info(f"Salvando em: {output}")

    save_output(
        base,
        output,
        xlsx_max_rows=int(args.xlsx_max_rows),
        xlsx_sheet_prefix=str(args.xlsx_sheet_prefix),
    )

    log.info("✅ Concluído.")


if __name__ == "__main__":
    main()