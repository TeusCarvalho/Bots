# -*- coding: utf-8 -*-
import os
import re
from pathlib import Path
from typing import Optional, List

import polars as pl


# ======================================================
# CONFIG
# ======================================================
PASTA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Dez"
SAIDA_XLSX = os.path.join(PASTA, "Codigo_Base_Entregador_Dezembro_2025.xlsx")

# Colunas da sua base
COL_BASE = "Base de entrega"
COL_ENTREGADOR = "Responsável pela entrega"
COL_TEMPO = "Tempo de entrega"
COL_CODIGO = "Código entregador"

# Filtro por mês/ano (se quiser pegar tudo, coloque APLICAR_FILTRO_MES = False)
APLICAR_FILTRO_MES = True
ANO_ALVO = 2025
MES_ALVO = 12


# ======================================================
# HELPERS
# ======================================================
def listar_excels(pasta: str) -> List[str]:
    """Lista .xlsx válidos na pasta (ignora temporários ~$ e o arquivo de saída)."""
    pasta_path = Path(pasta)
    arquivos = []
    for p in pasta_path.glob("*.xlsx"):
        nome = p.name.lower()
        if nome.startswith("~$"):
            continue
        if p.name == Path(SAIDA_XLSX).name.lower():
            continue
        if p.name.lower() == Path(SAIDA_XLSX).name.lower():
            continue
        arquivos.append(str(p))
    return arquivos


def detectar_coluna(df: pl.DataFrame, nome_exato: str) -> Optional[str]:
    """Casa o nome exato; se não achar, tenta por normalização (espaços)."""
    if nome_exato in df.columns:
        return nome_exato

    def norm(s: str) -> str:
        s = str(s).strip().lower()
        s = re.sub(r"\s+", " ", s)
        return s

    alvo = norm(nome_exato)
    for c in df.columns:
        if norm(c) == alvo:
            return c
    return None


def ler_excel(path: str) -> pl.DataFrame:
    """Lê Excel com Polars; fallback para pandas."""
    try:
        return pl.read_excel(path)
    except Exception:
        import pandas as pd
        pdf = pd.read_excel(path)
        return pl.from_pandas(pdf)


def parse_datetime_flex(col_expr: pl.Expr) -> pl.Expr:
    """Converte 'YYYY-MM-DD HH:MM:SS' (e variações) para Datetime."""
    dt1 = col_expr.cast(pl.Utf8, strict=False).str.strptime(
        pl.Datetime,
        format="%Y-%m-%d %H:%M:%S",
        strict=False,
    )
    dt2 = col_expr.cast(pl.Utf8, strict=False).str.to_datetime(strict=False)
    return pl.coalesce([dt1, dt2])


def limpar_texto(col_expr: pl.Expr) -> pl.Expr:
    """Apenas limpa espaços e nulos, sem 'normalizar' nomes."""
    return (
        col_expr
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
    )


def limpar_codigo(col_expr: pl.Expr) -> pl.Expr:
    """
    Converte para texto e remove sufixos comuns de float (ex.: '1234.0').
    Observação: se o Excel tiver salvo o código como número, zeros à esquerda já foram perdidos no arquivo.
    """
    s = col_expr.cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()
    # remove ".0" no fim
    s = s.str.replace(r"\.0$", "")
    # remove espaços duplicados
    s = s.str.replace_all(r"\s+", " ")
    return s


# ======================================================
# MAIN
# ======================================================
def main():
    arquivos = listar_excels(PASTA)
    if not arquivos:
        raise FileNotFoundError(f"Nenhum .xlsx encontrado em: {PASTA}")

    print(f"[INFO] Arquivos encontrados: {len(arquivos)}")

    dfs = []
    for arq in arquivos:
        try:
            df = ler_excel(arq)

            col_base = detectar_coluna(df, COL_BASE)
            col_ent = detectar_coluna(df, COL_ENTREGADOR)
            col_tempo = detectar_coluna(df, COL_TEMPO)
            col_cod = detectar_coluna(df, COL_CODIGO)

            faltando = [n for n, c in [
                (COL_CODIGO, col_cod),
                (COL_BASE, col_base),
                (COL_ENTREGADOR, col_ent),
                (COL_TEMPO, col_tempo),
            ] if c is None]

            if faltando:
                print(f"[WARN] Pulando (colunas faltando {faltando}): {Path(arq).name}")
                continue

            temp = df.select([
                limpar_codigo(pl.col(col_cod)).alias("Codigo"),
                limpar_texto(pl.col(col_base)).alias("Base"),
                limpar_texto(pl.col(col_ent)).alias("Entregador"),
                parse_datetime_flex(pl.col(col_tempo)).alias("_tempo_entrega"),
            ])

            if APLICAR_FILTRO_MES:
                temp = (
                    temp
                    .filter(pl.col("_tempo_entrega").is_not_null())
                    .filter(pl.col("_tempo_entrega").dt.year() == ANO_ALVO)
                    .filter(pl.col("_tempo_entrega").dt.month() == MES_ALVO)
                )

            dfs.append(temp)
            print(f"[OK] {Path(arq).name} | linhas: {temp.height}")

        except Exception as e:
            print(f"[ERRO] {Path(arq).name}: {e}")

    if not dfs:
        raise ValueError("Nenhum arquivo válido foi processado (verifique colunas/arquivos).")

    df_all = pl.concat(dfs, how="vertical_relaxed")

    # Remove vazios
    df_all = df_all.filter(
        (pl.col("Codigo") != "") & (pl.col("Base") != "") & (pl.col("Entregador") != "")
    )

    # Remove duplicados (exatamente o que você pediu: sem nomes/códigos repetidos)
    df_out = (
        df_all
        .select(["Codigo", "Base", "Entregador"])
        .unique(subset=["Codigo", "Base", "Entregador"], keep="first")
        .sort(["Codigo", "Base", "Entregador"])
    )

    print(f"[INFO] Linhas finais (sem duplicados): {df_out.height}")
    print("\n=== AMOSTRA (TOP 10) ===")
    print(df_out.head(10))

    out = Path(SAIDA_XLSX)
    out.parent.mkdir(parents=True, exist_ok=True)

    try:
        with pl.ExcelWriter(str(out)) as writer:
            df_out.write_excel(writer, worksheet="Mapa_Codigo_Base_Entregador")
        print(f"\n[OK] Gerado: {out}")
    except Exception:
        # fallback CSV
        csv_out = out.with_suffix(".csv")
        df_out.write_csv(str(csv_out))
        print(f"\n[WARN] Não consegui salvar Excel; gerei CSV: {csv_out}")


if __name__ == "__main__":
    main()
