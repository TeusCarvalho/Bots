# compare_jms_excel.py
# -*- coding: utf-8 -*-

import argparse
import os
from typing import Tuple, Optional

import pandas as pd


# =========================
# UI: seleção de arquivos
# =========================
def pick_excel_file(title: str) -> str:
    """
    Abre uma janela para selecionar um arquivo Excel.
    Retorna o caminho selecionado. Lança erro se cancelar.
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as e:
        raise RuntimeError(
            "Não foi possível carregar tkinter para abrir a janela. "
            "Rode via terminal passando --file-a e --file-b."
        ) from e

    root = tk.Tk()
    root.withdraw()  # oculta janela principal
    root.attributes("-topmost", True)

    file_path = filedialog.askopenfilename(
        title=title,
        filetypes=[
            ("Excel files", "*.xlsx *.xlsm *.xls"),
            ("All files", "*.*"),
        ],
    )

    root.destroy()

    if not file_path:
        raise RuntimeError("Seleção cancelada. Nenhum arquivo foi escolhido.")
    return file_path


def _norm_col_name(s: str) -> str:
    return str(s).strip().casefold()


def _find_column(df: pd.DataFrame, target: str) -> str:
    """
    Encontra a coluna no df por comparação case-insensitive e ignorando espaços.
    """
    target_n = _norm_col_name(target)
    mapping = {_norm_col_name(c): c for c in df.columns}
    if target_n in mapping:
        return mapping[target_n]

    def squeeze_spaces(x: str) -> str:
        return " ".join(str(x).strip().split()).casefold()

    target_s = squeeze_spaces(target)
    mapping2 = {squeeze_spaces(c): c for c in df.columns}
    if target_s in mapping2:
        return mapping2[target_s]

    cols = ", ".join([str(c) for c in df.columns])
    raise KeyError(f'Coluna "{target}" não encontrada. Colunas disponíveis: {cols}')


def _normalize_jms_series(s: pd.Series) -> pd.Series:
    """
    Normaliza valores do JMS para comparação:
    - converte para string
    - remove espaços
    - trata vazios como NA
    - corrige "123.0" -> "123" (caso Excel tenha virado float)
    """
    s2 = s.astype("string").str.strip()
    s2 = s2.replace("", pd.NA)
    s2 = s2.str.replace(r"^(\d+)\.0$", r"\1", regex=True)
    return s2


def read_excel_safe(path: str, sheet: Optional[str]) -> pd.DataFrame:
    if sheet is None:
        return pd.read_excel(path, engine="openpyxl")
    return pd.read_excel(path, sheet_name=sheet, engine="openpyxl")


def compare_excels(
    file_a: str,
    file_b: str,
    col_target: str,
    sheet_a: Optional[str],
    sheet_b: Optional[str],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_a = read_excel_safe(file_a, sheet_a)
    df_b = read_excel_safe(file_b, sheet_b)

    col_a = _find_column(df_a, col_target)
    col_b = _find_column(df_b, col_target)

    jms_a = _normalize_jms_series(df_a[col_a])
    jms_b = _normalize_jms_series(df_b[col_b])

    set_a = set(jms_a.dropna().unique().tolist())
    set_b = set(jms_b.dropna().unique().tolist())

    comuns = sorted(set_a & set_b)
    so_a = sorted(set_a - set_b)
    so_b = sorted(set_b - set_a)

    a_status = jms_a.apply(
        lambda x: "EXISTE_EM_B" if pd.notna(x) and x in set_b else "NAO_EXISTE_EM_B"
    )
    b_status = jms_b.apply(
        lambda x: "EXISTE_EM_A" if pd.notna(x) and x in set_a else "NAO_EXISTE_EM_A"
    )

    df_a_out = df_a.copy()
    df_b_out = df_b.copy()

    df_a_out["JMS_NORMALIZADO"] = jms_a
    df_a_out["STATUS_COMPARACAO"] = a_status

    df_b_out["JMS_NORMALIZADO"] = jms_b
    df_b_out["STATUS_COMPARACAO"] = b_status

    df_comuns = pd.DataFrame({col_target: comuns})
    df_so_a = pd.DataFrame({col_target: so_a})
    df_so_b = pd.DataFrame({col_target: so_b})

    return df_a_out, df_b_out, df_comuns, df_so_a, df_so_b


def main():
    parser = argparse.ArgumentParser(
        description='Compara dois Excels pelo campo "Número de pedido JMS" e gera outputs.'
    )
    parser.add_argument("--file-a", default="", help="Caminho do Excel A")
    parser.add_argument("--file-b", default="", help="Caminho do Excel B")
    parser.add_argument(
        "--col",
        default="Número de pedido JMS",
        help='Nome da coluna alvo (default: "Número de pedido JMS")',
    )
    parser.add_argument("--sheet-a", default=None, help="Aba do Excel A (default: primeira)")
    parser.add_argument("--sheet-b", default=None, help="Aba do Excel B (default: primeira)")
    parser.add_argument("--outdir", default="output_comparacao", help="Pasta de saída")

    args = parser.parse_args()

    file_a = args.file_a.strip()
    file_b = args.file_b.strip()

    # Se não passou os caminhos, abre a janela para escolher
    if not file_a:
        file_a = pick_excel_file("Selecione o Excel A")
    if not file_b:
        file_b = pick_excel_file("Selecione o Excel B")

    os.makedirs(args.outdir, exist_ok=True)

    a_out, b_out, comuns, so_a, so_b = compare_excels(
        file_a=file_a,
        file_b=file_b,
        col_target=args.col,
        sheet_a=args.sheet_a,
        sheet_b=args.sheet_b,
    )

    total_a = a_out["JMS_NORMALIZADO"].dropna().nunique()
    total_b = b_out["JMS_NORMALIZADO"].dropna().nunique()

    print("=== RESUMO ===")
    print(f"A (únicos): {total_a}")
    print(f"B (únicos): {total_b}")
    print(f"Comuns: {len(comuns)}")
    print(f"Só A: {len(so_a)}")
    print(f"Só B: {len(so_b)}")

    path_a = os.path.join(args.outdir, "A_com_status.xlsx")
    path_b = os.path.join(args.outdir, "B_com_status.xlsx")
    path_comuns = os.path.join(args.outdir, "JMS_comuns.xlsx")
    path_so_a = os.path.join(args.outdir, "JMS_so_na_A.xlsx")
    path_so_b = os.path.join(args.outdir, "JMS_so_na_B.xlsx")

    a_out.to_excel(path_a, index=False)
    b_out.to_excel(path_b, index=False)
    comuns.to_excel(path_comuns, index=False)
    so_a.to_excel(path_so_a, index=False)
    so_b.to_excel(path_so_b, index=False)

    print("\nArquivos gerados em:", os.path.abspath(args.outdir))
    print("-", path_a)
    print("-", path_b)
    print("-", path_comuns)
    print("-", path_so_a)
    print("-", path_so_b)


if __name__ == "__main__":
    main()
