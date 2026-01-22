# -*- coding: utf-8 -*-
"""
v3.1 — Polars: motoristas ENTREGUES (por Status) + assinatura normal (opcional) + export seguro

- Lê Excel/CSV dentro de uma pasta (recursivo)
- Detecta colunas (motorista / assinatura / status / id)
- Recorte A: ENTREGUES (pelo Status) -> lista + contagem por motorista
- Recorte B (opcional): Assinatura normal (+ entregue se status existir) -> lista + contagem por motorista
- Exporta Excel com:
  - Resumo
  - Motoristas entregues (lista + contagem)
  - Motoristas assinatura normal (lista + contagem) (se habilitado)
  - Amostra (assinatura normal) (se existir)

Requisitos:
  pip install polars pandas openpyxl
"""

from __future__ import annotations

import re
import unicodedata
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import polars as pl

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.styles.stylesheet")

# =========================
# CONFIG
# =========================
PASTA = Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Dez")

SAIDA_XLSX = PASTA / "resultado_motoristas_entregues.xlsx"

# (Opcional) Detalhe grande da assinatura normal (recomendado Parquet)
DETALHE_DIR = PASTA / "_assinatura_detalhe_parquet"   # pasta com múltiplos parquet
SALVAR_DETALHE_PARQUET = True

AMOSTRA_EXCEL_ROWS = 200_000

ALVO_ASSINATURA = "Recebimento com assinatura normal"

# Se você quiser DESLIGAR tudo de assinatura normal e ficar só nos ENTREGUES:
HABILITAR_ASSINATURA_NORMAL = False  # <-- coloque True se quiser também assinatura normal


# =========================
# HELPERS
# =========================
def _norm_text(s: str) -> str:
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def _find_col(columns: List[str], candidates_norm: List[str]) -> Optional[str]:
    norm_map = {_norm_text(c): c for c in columns}
    for cand in candidates_norm:
        if cand in norm_map:
            return norm_map[cand]

    # fallback por "contém"
    for col in columns:
        n = _norm_text(col)
        for cand in candidates_norm:
            if cand in n:
                return col
    return None


def _list_files(pasta: Path) -> List[Path]:
    exts = {".xlsx", ".xls", ".csv"}
    files = []
    for p in pasta.rglob("*"):
        if p.is_file() and p.suffix.lower() in exts and not p.name.startswith("~$"):
            files.append(p)
    return sorted(files)


def _clean_str_expr(colname: str) -> pl.Expr:
    c = pl.col(colname).cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    return pl.when(c.is_in(["nan", "None", "NaT"])).then("").otherwise(c)


def _read_excel_all_sheets_as_polars(file: Path) -> List[Tuple[str, pl.DataFrame]]:
    out: List[Tuple[str, pl.DataFrame]] = []
    sheets = pd.read_excel(file, sheet_name=None, engine="openpyxl")
    for sheet_name, df in sheets.items():
        if df is None or df.empty:
            continue
        df = df.copy()
        df["__arquivo"] = file.name
        df["__aba"] = sheet_name
        out.append((sheet_name, pl.from_pandas(df)))
    return out


def _read_csv_as_polars(file: Path) -> pl.DataFrame:
    seps = [",", ";", "\t", "|"]
    encodings = ["utf8", "utf8-lossy", "iso8859-1"]

    for sep in seps:
        for enc in encodings:
            try:
                df = pl.read_csv(
                    file,
                    separator=sep,
                    encoding=enc,
                    ignore_errors=True,
                    infer_schema_length=5000,
                )
                if df.width > 1:
                    return df.with_columns(
                        pl.lit(file.name).alias("__arquivo"),
                        pl.lit("CSV").alias("__aba"),
                    )
            except Exception:
                pass

    # fallback pandas
    try:
        dfp = pd.read_csv(file, sep=None, engine="python", encoding="utf-8")
    except Exception:
        dfp = pd.read_csv(file, sep=None, engine="python", encoding="latin1")

    dfp["__arquivo"] = file.name
    dfp["__aba"] = "CSV"
    return pl.from_pandas(dfp)


def _append_parquet(df: pl.DataFrame, out_dir: Path, part_idx: int) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"part_{part_idx:05d}.parquet"
    df.write_parquet(out_file, compression="zstd")


def _is_entregue_expr(status_col: str) -> pl.Expr:
    """
    Heurística de 'entregue' a partir do texto do status.
    Evita falso positivo em 'não entregue' (que contém 'entreg').
    """
    stc = _clean_str_expr(status_col).str.to_lowercase()

    neg = (
        stc.str.contains("nao entreg") |
        stc.str.contains("não entreg") |
        stc.str.contains("nao entregue") |
        stc.str.contains("não entregue") |
        stc.str.contains("devolv") |
        stc.str.contains("retorn") |
        stc.str.contains("cancel") |
        stc.str.contains("extravi") |
        stc.str.contains("perd")
    )

    pos = (
        stc.str.contains("entreg") |
        stc.str.contains("delivered") |
        stc.str.contains("conclu") |
        stc.str.contains("finaliz") |
        stc.str.contains("pod") |
        stc.str.contains("comprov")
    )

    return pos & (~neg)


def _accum_counts_from_df(df: pl.DataFrame, driver_col: str, out_counts: Dict[str, int]) -> None:
    if df.is_empty():
        return

    tmp = (
        df.select(_clean_str_expr(driver_col).alias("_drv"))
          .filter(pl.col("_drv") != "")
          .group_by("_drv")
          .agg(pl.len().alias("qtd"))
    )

    for row in tmp.iter_rows(named=True):
        k = row["_drv"]
        v = int(row["qtd"])
        out_counts[k] = out_counts.get(k, 0) + v


def _process_one_df(
    df: pl.DataFrame,
    col_driver: Optional[str],
    col_sign: Optional[str],
    col_status: Optional[str],
    motoristas_entregues: set,
    motoristas_assinatura: set,
    counts_entregues: Dict[str, int],
    counts_assinatura: Dict[str, int],
    state: dict,
) -> None:
    if not col_driver:
        return

    # =========================
    # A) ENTREGUES (pelo status)
    # =========================
    if col_status and col_status in df.columns:
        entregue_ok = _is_entregue_expr(col_status)
        df_ent = df.filter(entregue_ok)

        if not df_ent.is_empty():
            drv_ent = (
                df_ent.select(_clean_str_expr(col_driver).alias("_drv"))
                      .get_column("_drv")
                      .to_list()
            )
            for x in drv_ent:
                if x:
                    motoristas_entregues.add(x)

            _accum_counts_from_df(df_ent, col_driver, counts_entregues)
            state["linhas_entregues"] += df_ent.height

    # =========================
    # B) Assinatura normal (opcional)
    # =========================
    if not HABILITAR_ASSINATURA_NORMAL:
        return

    if not col_sign or col_sign not in df.columns:
        return

    sign_ok = _clean_str_expr(col_sign).str.to_lowercase() == ALVO_ASSINATURA.casefold()

    if col_status and col_status in df.columns:
        entregue_ok2 = _is_entregue_expr(col_status)
    else:
        entregue_ok2 = pl.lit(True)

    df_sig = df.filter(sign_ok & entregue_ok2)
    if df_sig.is_empty():
        return

    state["linhas_assinatura"] += df_sig.height

    drv_sig = (
        df_sig.select(_clean_str_expr(col_driver).alias("_drv"))
              .get_column("_drv")
              .to_list()
    )
    for x in drv_sig:
        if x:
            motoristas_assinatura.add(x)

    _accum_counts_from_df(df_sig, col_driver, counts_assinatura)

    if SALVAR_DETALHE_PARQUET:
        _append_parquet(df_sig, DETALHE_DIR, state["part_idx"])
        state["part_idx"] += 1

    if state["amostra_assinatura"] is None:
        state["amostra_assinatura"] = df_sig.head(AMOSTRA_EXCEL_ROWS)
    else:
        if state["amostra_assinatura"].height < AMOSTRA_EXCEL_ROWS:
            faltam = AMOSTRA_EXCEL_ROWS - state["amostra_assinatura"].height
            state["amostra_assinatura"] = pl.concat(
                [state["amostra_assinatura"], df_sig.head(faltam)],
                how="vertical"
            )


def main() -> None:
    if not PASTA.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {PASTA}")

    files = _list_files(PASTA)
    if not files:
        raise FileNotFoundError(f"Nenhum .xlsx/.xls/.csv encontrado em: {PASTA}")

    driver_candidates = list(map(_norm_text, [
        "Responsável pela entrega",
        "Responsavel pela entrega",
        "Entregador",
        "Motorista",
        "Courier",
    ]))
    sign_candidates = list(map(_norm_text, [
        "Marca de assinatura",
        "Marca assinatura",
        "Assinatura",
        "Signature",
    ]))
    status_candidates = list(map(_norm_text, [
        "Status",
        "Situação",
        "Situacao",
        "Ocorrência",
        "Ocorrencia",
        "Delivery status",
        "Status do pedido",
        "Status entrega",
        "Status da entrega",
        "Última ocorrência",
        "Ultima ocorrencia",
        "Evento",
        "Operação",
        "Operacao",
    ]))

    col_driver = col_sign = col_status = None

    motoristas_entregues = set()
    motoristas_assinatura = set()

    counts_entregues: Dict[str, int] = {}
    counts_assinatura: Dict[str, int] = {}

    state = {
        "part_idx": 0,
        "linhas_entregues": 0,
        "linhas_assinatura": 0,
        "amostra_assinatura": None,
    }

    if SALVAR_DETALHE_PARQUET and HABILITAR_ASSINATURA_NORMAL:
        if DETALHE_DIR.exists():
            for p in DETALHE_DIR.glob("part_*.parquet"):
                try:
                    p.unlink()
                except Exception:
                    pass

    for f in files:
        try:
            if f.suffix.lower() in [".xlsx", ".xls"]:
                sheets = _read_excel_all_sheets_as_polars(f)
                for _, df in sheets:
                    if df.is_empty():
                        continue

                    if col_driver is None:
                        cols = df.columns
                        col_driver = _find_col(cols, driver_candidates)
                        col_sign = _find_col(cols, sign_candidates)
                        col_status = _find_col(cols, status_candidates)

                    _process_one_df(
                        df=df,
                        col_driver=col_driver,
                        col_sign=col_sign,
                        col_status=col_status,
                        motoristas_entregues=motoristas_entregues,
                        motoristas_assinatura=motoristas_assinatura,
                        counts_entregues=counts_entregues,
                        counts_assinatura=counts_assinatura,
                        state=state,
                    )
            else:
                df = _read_csv_as_polars(f)
                if df.is_empty():
                    continue

                if col_driver is None:
                    cols = df.columns
                    col_driver = _find_col(cols, driver_candidates)
                    col_sign = _find_col(cols, sign_candidates)
                    col_status = _find_col(cols, status_candidates)

                _process_one_df(
                    df=df,
                    col_driver=col_driver,
                    col_sign=col_sign,
                    col_status=col_status,
                    motoristas_entregues=motoristas_entregues,
                    motoristas_assinatura=motoristas_assinatura,
                    counts_entregues=counts_entregues,
                    counts_assinatura=counts_assinatura,
                    state=state,
                )

        except Exception as e:
            print(f"[ERRO] Falha lendo/processando {f.name}: {e}")

    print("=" * 80)
    print("COLUNAS IDENTIFICADAS")
    print(f"- Motorista: {col_driver}")
    print(f"- Status (para ENTREGUES): {col_status}")
    print(f"- Assinatura (opcional): {col_sign}")
    print("=" * 80)

    print("\nRESULTADOS")
    print(f"1) Motoristas únicos ENTREGUES: {len(motoristas_entregues)}")
    print(f"2) Linhas ENTREGUES (pelo status): {state['linhas_entregues']}")

    if not col_status:
        print("\n[ATENÇÃO] Não encontrei coluna de Status. Não dá para filtrar ENTREGUES.")
        print("Nesse caso, o resultado de ENTREGUES ficará vazio/zero.")

    if HABILITAR_ASSINATURA_NORMAL:
        print(f"3) Motoristas únicos (assinatura normal): {len(motoristas_assinatura)}")
        print(f"4) Linhas (assinatura normal): {state['linhas_assinatura']}")
        if SALVAR_DETALHE_PARQUET:
            print(f"5) Detalhe assinatura (parquet): {DETALHE_DIR}")

    # Export Excel
    resumo_rows = [
        {"Métrica": "Arquivos lidos", "Valor": len(files)},
        {"Métrica": "Coluna motorista", "Valor": col_driver or ""},
        {"Métrica": "Coluna status", "Valor": col_status or ""},
        {"Métrica": "Motoristas únicos ENTREGUES", "Valor": len(motoristas_entregues)},
        {"Métrica": "Linhas ENTREGUES", "Valor": state["linhas_entregues"]},
        {"Métrica": "Assinatura normal habilitada", "Valor": "Sim" if HABILITAR_ASSINATURA_NORMAL else "Não"},
        {"Métrica": "Coluna assinatura", "Valor": col_sign or ""},
        {"Métrica": "Motoristas únicos (assinatura normal)", "Valor": len(motoristas_assinatura) if HABILITAR_ASSINATURA_NORMAL else 0},
        {"Métrica": "Linhas (assinatura normal)", "Valor": state["linhas_assinatura"] if HABILITAR_ASSINATURA_NORMAL else 0},
        {"Métrica": "Detalhe assinatura (parquet)", "Valor": str(DETALHE_DIR) if (HABILITAR_ASSINATURA_NORMAL and SALVAR_DETALHE_PARQUET) else "Não"},
    ]

    df_resumo = pd.DataFrame(resumo_rows)

    df_motoristas_ent = pd.DataFrame({"Motorista": sorted(motoristas_entregues)})
    df_counts_ent = (
        pd.DataFrame([{"Motorista": k, "Linhas": v} for k, v in counts_entregues.items()])
          .sort_values(["Linhas", "Motorista"], ascending=[False, True])
    )

    df_motoristas_sig = pd.DataFrame({"Motorista": sorted(motoristas_assinatura)})
    df_counts_sig = (
        pd.DataFrame([{"Motorista": k, "Linhas": v} for k, v in counts_assinatura.items()])
          .sort_values(["Linhas", "Motorista"], ascending=[False, True])
    )

    try:
        with pd.ExcelWriter(SAIDA_XLSX, engine="openpyxl") as writer:
            df_resumo.to_excel(writer, sheet_name="Resumo", index=False)

            df_motoristas_ent.to_excel(writer, sheet_name="Motoristas_Entregues", index=False)
            df_counts_ent.to_excel(writer, sheet_name="Entregues_Contagem", index=False)

            if HABILITAR_ASSINATURA_NORMAL:
                df_motoristas_sig.to_excel(writer, sheet_name="Motoristas_Assinatura", index=False)
                df_counts_sig.to_excel(writer, sheet_name="Assinatura_Contagem", index=False)

                amostra = state["amostra_assinatura"]
                if amostra is not None and not amostra.is_empty():
                    amostra.to_pandas().to_excel(writer, sheet_name="Assinatura_Amostra", index=False)

        print(f"\nExcel gerado: {SAIDA_XLSX}")

    except Exception as e:
        print(f"\n[ERRO] Não consegui salvar o Excel: {e}")
        print("Os resultados no console continuam válidos.")


if __name__ == "__main__":
    main()
