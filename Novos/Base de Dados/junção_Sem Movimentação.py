from pathlib import Path
import unicodedata
import polars as pl

# =========================================================
# CONFIGURAÇÃO
# =========================================================
PASTA_EXCEL = Path(
    r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Isso ai é uma pasta, só não abre"
)

ARQUIVO_SAIDA_XLSX = "resultado_filtrado_polars.xlsx"
ARQUIVO_SAIDA_PARQUET = "resultado_filtrado_polars.parquet"

BUSCAR_EM_SUBPASTAS = False
MIN_DIAS_AGING = 5

# Se True, tenta inferir o schema olhando a planilha inteira.
# Mais robusto, porém mais lento.
INFERIR_SCHEMA_COMPLETO = False

COLUNAS_UNIDADE = [
    "Unidade responsável",
    "Unidade responsavel",
    "Base responsável",
    "Base responsavel",
    "Responsável",
    "Responsavel",
    "Unidade responsável责任机构",
]

COLUNAS_AGING = [
    "Aging",
    "aging",
]

COLUNAS_REMESSA = [
    "Remessa",
    "remessa",
    "Shipment",
    "Número da remessa",
    "Numero da remessa",
]

COLUNAS_DATA = [
    "Data",
    "DATE",
    "Date",
    "Data Aging",
    "Data do Aging",
]

REGEX_AGING = r"Exceed\s+(\d+)\s+days?\s+with\s+no\s+track"


# =========================================================
# FUNÇÕES AUXILIARES
# =========================================================
def normalizar_texto(texto: str) -> str:
    if texto is None:
        return ""

    texto = str(texto).strip().lower()
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    return " ".join(texto.split())


def encontrar_coluna(colunas, opcoes):
    mapa = {normalizar_texto(col): col for col in colunas}

    for opcao in opcoes:
        chave = normalizar_texto(opcao)
        if chave in mapa:
            return mapa[chave]

    return None


def listar_arquivos_excel(pasta: Path):
    padroes = ("*.xlsx", "*.xls", "*.xlsm", "*.xlsb")
    arquivos = []

    for padrao in padroes:
        encontrados = pasta.rglob(padrao) if BUSCAR_EM_SUBPASTAS else pasta.glob(padrao)
        arquivos.extend(encontrados)

    excluir_nomes = {
        ARQUIVO_SAIDA_XLSX.lower(),
        ARQUIVO_SAIDA_PARQUET.lower(),
        "resultado_filtrado.xlsx",
        "resultado_filtrado_polars.xlsx",
        "resultado_filtrado_polars.parquet",
    }

    arquivos_filtrados = []
    for arq in sorted(set(arquivos)):
        nome = arq.name.lower()

        # ignora temporários do Excel e arquivos de saída do próprio processo
        if nome.startswith("~$"):
            continue
        if nome in excluir_nomes:
            continue

        arquivos_filtrados.append(arq)

    return arquivos_filtrados


def ler_excel_rapido(caminho_arquivo: Path):
    kwargs = {
        "source": str(caminho_arquivo),
        "sheet_id": 1,
        "engine": "calamine",
        "raise_if_empty": False,
    }

    if INFERIR_SCHEMA_COMPLETO:
        kwargs["infer_schema_length"] = None

    try:
        return pl.read_excel(**kwargs)
    except Exception as erro_calamine:
        print(f"[AVISO] Calamine falhou em {caminho_arquivo.name}: {erro_calamine}")

        try:
            return pl.read_excel(
                source=str(caminho_arquivo),
                sheet_id=1,
                engine="openpyxl",
                raise_if_empty=False,
            )
        except Exception as erro_openpyxl:
            print(f"[ERRO] Falha ao ler {caminho_arquivo.name}")
            print(f"       calamine: {erro_calamine}")
            print(f"       openpyxl: {erro_openpyxl}")
            return None


def processar_arquivo(caminho_arquivo: Path):
    print(f"Processando: {caminho_arquivo.name}")

    df = ler_excel_rapido(caminho_arquivo)

    if df is None or df.is_empty():
        print(f"[INFO] Arquivo vazio ou não lido: {caminho_arquivo.name}")
        return None

    # guarda as colunas originais para padronizar depois
    colunas_originais = list(df.columns)

    col_unidade = encontrar_coluna(df.columns, COLUNAS_UNIDADE)
    col_aging = encontrar_coluna(df.columns, COLUNAS_AGING)
    col_remessa = encontrar_coluna(df.columns, COLUNAS_REMESSA)
    col_data = encontrar_coluna(df.columns, COLUNAS_DATA)

    if not col_unidade:
        print(f"[INFO] Coluna de unidade não encontrada em {caminho_arquivo.name}")
        print(f"       Colunas disponíveis: {df.columns}")
        return None

    if not col_aging:
        print(f"[INFO] Coluna Aging não encontrada em {caminho_arquivo.name}")
        print(f"       Colunas disponíveis: {df.columns}")
        return None

    df = df.with_columns(
        [
            pl.col(col_unidade)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
            .str.to_uppercase()
            .alias("__unidade_norm"),

            pl.col(col_aging)
            .cast(pl.Utf8, strict=False)
            .str.extract(REGEX_AGING, 1)
            .cast(pl.Int64, strict=False)
            .alias("dias_aging"),

            pl.lit(caminho_arquivo.name).alias("arquivo_origem"),
        ]
    )

    # filtros principais
    df = df.filter(
        pl.col("__unidade_norm").str.starts_with("F") &
        (pl.col("dias_aging") > MIN_DIAS_AGING)
    )

    if df.is_empty():
        print(f"[INFO] Nenhum registro após filtros em {caminho_arquivo.name}")
        return None

    # marca remessas duplicadas, mas mantém todas
    if col_remessa:
        remessa_limpa = (
            pl.col(col_remessa)
            .cast(pl.Utf8, strict=False)
            .str.strip_chars()
        )

        df = df.with_columns(
            pl.when(remessa_limpa.is_null() | (remessa_limpa == ""))
            .then(pl.lit(False))
            .otherwise(remessa_limpa.is_duplicated())
            .alias("remessa_duplicada")
        )
    else:
        df = df.with_columns(pl.lit(False).alias("remessa_duplicada"))

    # cria uma coluna padrão para data, mantendo o valor
    if col_data:
        df = df.with_columns(
            pl.col(col_data).cast(pl.Utf8, strict=False).alias("data_original")
        )
    else:
        df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("data_original"))

    # padroniza TODAS as colunas originais como texto para evitar conflito de schema
    expressoes_cast = []
    for col in colunas_originais:
        if col in df.columns:
            expressoes_cast.append(
                pl.col(col).cast(pl.Utf8, strict=False).alias(col)
            )

    if expressoes_cast:
        df = df.with_columns(expressoes_cast)

    return df.drop("__unidade_norm")


# =========================================================
# PRINCIPAL
# =========================================================
def main():
    if not PASTA_EXCEL.exists():
        print("[ERRO] Pasta não encontrada:")
        print(PASTA_EXCEL)
        return

    arquivos_excel = listar_arquivos_excel(PASTA_EXCEL)

    if not arquivos_excel:
        print("[ERRO] Nenhum arquivo Excel encontrado na pasta.")
        return

    print(f"Total de arquivos encontrados: {len(arquivos_excel)}")

    resultados = []

    for arquivo in arquivos_excel:
        df_filtrado = processar_arquivo(arquivo)
        if df_filtrado is not None and not df_filtrado.is_empty():
            resultados.append(df_filtrado)

    if not resultados:
        print("[INFO] Nenhum dado encontrado com os filtros.")
        return

    # diagonal_relaxed = aceita colunas faltando e também ajusta tipos incompatíveis
    df_final = pl.concat(resultados, how="diagonal_relaxed", rechunk=True)

    if "dias_aging" in df_final.columns:
        df_final = df_final.sort("dias_aging", descending=True)

    caminho_saida_xlsx = PASTA_EXCEL / ARQUIVO_SAIDA_XLSX
    caminho_saida_parquet = PASTA_EXCEL / ARQUIVO_SAIDA_PARQUET

    df_final.write_parquet(str(caminho_saida_parquet))
    df_final.write_excel(str(caminho_saida_xlsx), worksheet="Dados")

    total_linhas = df_final.height

    total_duplicadas = 0
    if "remessa_duplicada" in df_final.columns:
        total_duplicadas = int(
            df_final.select(
                pl.col("remessa_duplicada").cast(pl.Int64).sum()
            ).item()
        )

    print("\nProcesso finalizado com sucesso.")
    print(f"Arquivo Excel salvo em:   {caminho_saida_xlsx}")
    print(f"Arquivo Parquet salvo em: {caminho_saida_parquet}")
    print(f"Total de linhas filtradas: {total_linhas}")
    print(f"Total de linhas com Remessa duplicada: {total_duplicadas}")


if __name__ == "__main__":
    main()