from __future__ import annotations

import os
from pathlib import Path
import polars as pl


# =========================
# CONFIG
# =========================
pasta_faixa = Path(r"C:\Users\J&T-099\Downloads\BAIRRO")
pasta_dados = Path(r"C:\Users\J&T-099\Downloads\D1")

saida = Path(r"C:\Users\J&T-099\Downloads\D1\DADOS_COM_AREA_AFILIADA.xlsx")

EXTS_EXCEL = {".xlsx", ".xlsm"}  # se precisar, adiciono ".xls" (mas pode falhar no openpyxl)
MAX_LISTAR = 40  # quantos arquivos listar no diagnóstico


# =========================
# HELPERS
# =========================
def listar_excel_validos(pasta: Path) -> list[Path]:
    """
    Busca recursiva por Excel (.xlsx/.xlsm), ignorando temporários do Excel (~$...).
    """
    if not pasta.exists():
        raise FileNotFoundError(f"Pasta não existe: {pasta}")

    # busca recursiva
    todos = [p for p in pasta.rglob("*") if p.is_file()]

    # pega apenas excel
    excels = [p for p in todos if p.suffix.lower() in EXTS_EXCEL]

    # ignora temporários do Excel
    validos = [p for p in excels if not p.name.startswith("~$")]

    # se não achou nada, imprime diagnóstico
    if not validos:
        print("\n[DIAGNÓSTICO] Nenhum Excel válido encontrado.")
        print(f" - Pasta: {pasta}")
        print(f" - Pasta existe? {pasta.exists()}")
        print(f" - Total de arquivos (recursivo): {len(todos)}")
        print(f" - Excel encontrados ({EXTS_EXCEL}): {len(excels)}")
        temp = [p for p in excels if p.name.startswith("~$")]
        print(f" - Excel temporários (~$): {len(temp)}")

        if excels:
            print("\nExemplos de Excel encontrados:")
            for p in excels[:MAX_LISTAR]:
                print(f"  - {p.name}")
        else:
            print("\nExemplos de arquivos encontrados (qualquer tipo):")
            for p in todos[:MAX_LISTAR]:
                print(f"  - {p.name}")

    return sorted(validos, key=lambda x: x.name.lower())


def read_excel_safe(path: Path) -> pl.DataFrame:
    """
    Tenta ler com engine padrão (calamine/fastexcel).
    Se falhar, tenta engine='openpyxl' (mais tolerante, porém mais lento).
    """
    try:
        return pl.read_excel(str(path))
    except Exception:
        return pl.read_excel(str(path), engine="openpyxl")


def to_int_cep(expr: pl.Expr) -> pl.Expr:
    """
    Normaliza CEP para inteiro:
    - converte para string
    - remove tudo que não é dígito
    - converte para Int64 (inválidos viram null)
    """
    return (
        expr.cast(pl.Utf8)
        .str.replace_all(r"\D+", "")
        .cast(pl.Int64, strict=False)
    )


# =========================
# MAIN
# =========================
# 1) Ler faixa (BAIRRO)
arquivos_faixa = listar_excel_validos(pasta_faixa)
if not arquivos_faixa:
    raise Exception("Nenhum arquivo Excel válido encontrado na pasta BAIRRO (ignorando ~$.xlsx).")

df_faixa = pl.concat(
    [read_excel_safe(arq) for arq in arquivos_faixa],
    how="vertical_relaxed",
)

# Garantir CEP como inteiro
df_faixa = df_faixa.with_columns([
    to_int_cep(pl.col("CEP inicial")).alias("CEP inicial"),
    to_int_cep(pl.col("CEP final")).alias("CEP final"),
])

# 2) Ler dados (D1)
arquivos_dados = listar_excel_validos(pasta_dados)
if not arquivos_dados:
    raise Exception("Nenhum arquivo Excel válido encontrado na pasta D1 (ignorando ~$.xlsx).")

df_dados = pl.concat(
    [read_excel_safe(arq) for arq in arquivos_dados],
    how="vertical_relaxed",
)

df_dados = df_dados.with_columns(
    to_int_cep(pl.col("CEP destino")).alias("CEP destino")
)

# 3) Range join mantendo todas as linhas
df_dados_id = df_dados.with_row_count("row_id")

df_matches = df_dados_id.join_where(
    df_faixa,
    (pl.col("CEP destino") >= pl.col("CEP inicial")) &
    (pl.col("CEP destino") <= pl.col("CEP final"))
)

# Se houver mais de uma faixa batendo por CEP, pega a primeira por row_id
df_area_por_linha = (
    df_matches
    .group_by("row_id")
    .agg(pl.first("Área afiliada").alias("Área afiliada"))
)

df_resultado = (
    df_dados_id
    .join(df_area_por_linha, on="row_id", how="left")
    .drop("row_id")
    .with_columns(pl.col("Área afiliada").fill_null("Não encontrado"))
)

# 4) Salvar resultado
df_resultado.write_excel(str(saida))

print("✅ Arquivo gerado com sucesso com Polars!")
print(f"📄 Saída: {saida}")
