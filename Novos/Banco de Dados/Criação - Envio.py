# -*- coding: utf-8 -*-
import os
import unicodedata
import traceback
from typing import List, Optional

import polars as pl
import psycopg2
from psycopg2.extras import execute_values
import logging

# ======================================================
# CONFIG BANCO
# ======================================================
DB = {
    "host": "localhost",
    "database": "analytics",
    "user": "postgres",
    "password": "Jt2025"
}

# ======================================================
# CONFIG ETL
# ======================================================
PASTA_RAIZ = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\QUALIDADE_ FILIAL GO - BASE DE DADOS"
MODO_CARGA = "upsert"

# BATCH
BATCH_SIZE = 10000

# POLARS — usa todos núcleos
os.environ["POLARS_MAX_THREADS"] = "0"

# ======================================================
# LOGGING
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("etl_excel_pg")
def limpar_nome(nome: str) -> str:
    if not nome or not isinstance(nome, str):
        return "col_unk"

    nome = unicodedata.normalize("NFKD", nome)
    nome = "".join(c for c in nome if not unicodedata.combining(c))
    nome = nome.lower()

    for r in [" ", "-", "/", ".", "%", "(", ")", ",", ";", ":", "#", "@", "!", "?"]:
        nome = nome.replace(r, "_")

    while "__" in nome:
        nome = nome.replace("__", "_")

    nome = nome.strip("_")
    if not nome:
        nome = "col_unk"

    if nome[0].isdigit():
        nome = "col_" + nome

    return nome


def conectar():
    return psycopg2.connect(**DB)
def pl_dtype_to_pg(d):
    if d.is_integer():
        return "BIGINT"
    if d.is_float():
        return "DOUBLE PRECISION"
    if d == pl.Boolean:
        return "BOOLEAN"
    if d == pl.Date:
        return "DATE"
    if d.is_temporal():
        return "TIMESTAMP"
    return "TEXT"
def ler_excels_lazy(pasta: str) -> Optional[pl.LazyFrame]:
    arquivos = [f for f in os.listdir(pasta) if f.lower().endswith((".xlsx", ".xls"))]
    if not arquivos:
        return None

    lfs = []
    col_names_global = set()

    for arq in arquivos:
        caminho = os.path.join(pasta, arq)
        try:
            df = pl.read_excel(caminho, engine="calamine").lazy()
        except Exception as e:
            logger.error(f"❌ Erro lendo {caminho}: {e}")
            continue

        # renomear colunas
        cols = df.columns
        norm = [limpar_nome(c) for c in cols]

        df = df.rename(dict(zip(cols, norm)))
        col_names_global.update(norm)

        lfs.append(df)

    if not lfs:
        return None

    col_names_global = sorted(col_names_global)

    lfs_alinhados = []
    for lf in lfs:
        cols_lf = lf.columns
        faltantes = [c for c in col_names_global if c not in cols_lf]

        if faltantes:
            lf = lf.with_columns([pl.lit(None).alias(c) for c in faltantes])

        lf = lf.select(col_names_global)
        lfs_alinhados.append(lf)

    lf_final = pl.concat(lfs_alinhados, how="vertical_relaxed")
    return lf_final
def detectar_pk_por_valores(df: pl.DataFrame) -> List[str]:
    colunas_pk = []

    for col in df.columns:
        serie = df[col].drop_nulls()

        if serie.is_empty():
            continue

        try:
            s = serie.cast(str)
            if s.str.starts_with("888").any() or s.str.startswith("999").any():
                colunas_pk.append(col)
        except:
            continue

    if colunas_pk:
        logger.info(f"🔑 PK detectada automaticamente: {colunas_pk}")
    else:
        logger.warning("⚠ Nenhuma PK encontrada via 888/999")

    return colunas_pk
def criar_ou_ajustar_tabela(tabela: str, df: pl.DataFrame):
    schema = df.schema

    with conectar() as con:
        with con.cursor() as cur:

            # existe?
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=%s
                );
            """, (tabela,))
            existe = cur.fetchone()[0]

            if not existe:
                cols_def = []
                for col, dtype in schema.items():
                    cols_def.append(f'"{col}" {pl_dtype_to_pg(dtype)}')

                ddl = f'CREATE TABLE "{tabela}" (\n    ' + ",\n    ".join(cols_def) + "\n);"
                cur.execute(ddl)
                con.commit()
                logger.info(f"✔ Tabela criada: {tabela}")
                return

            # ajustar colunas
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s;
            """, (tabela,))
            existentes = {r[0] for r in cur.fetchall()}

            novas = [c for c in schema if c not in existentes]

            for col in novas:
                cur.execute(
                    f'ALTER TABLE "{tabela}" ADD COLUMN "{col}" {pl_dtype_to_pg(schema[col])};'
                )

            if novas:
                con.commit()
                logger.info(f"➕ Colunas adicionadas: {novas}")
def inserir_batch(tabela: str, df: pl.DataFrame):
    cols = df.columns
    rows = df.rows()

    if not rows:
        logger.warning(f"⚠ Nada para inserir em {tabela}")
        return

    cols_str = ", ".join([f'"{c}"' for c in cols])

    sql = f"""
        INSERT INTO "{tabela}" ({cols_str})
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    with conectar() as con:
        with con.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                bloco = rows[i:i+BATCH_SIZE]
                execute_values(cur, sql, bloco)
            con.commit()

    logger.info(f"✔ Inseridos {len(rows)} registros (append safe)")
def upsert_batch(tabela: str, df: pl.DataFrame, chaves: List[str]):
    cols = df.columns
    rows = df.rows()

    if not rows:
        return

    cols_str = ", ".join([f'"{c}"' for c in cols])
    keys_str = ", ".join([f'"{c}"' for c in chaves])

    updates = [c for c in cols if c not in chaves]
    set_str = ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in updates])

    sql = f"""
        INSERT INTO "{tabela}" ({cols_str})
        VALUES %s
        ON CONFLICT ({keys_str})
        DO UPDATE SET {set_str};
    """

    with conectar() as con:
        with con.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                bloco = rows[i:i+BATCH_SIZE]
                execute_values(cur, sql, bloco)
            con.commit()

    logger.info(f"✔ UPSERT {len(rows)}")
def processar_pasta(root: str):
    nome = os.path.basename(root)
    tabela = limpar_nome("col_" + nome)

    logger.info(f"\n📁 Pasta: {root}")
    logger.info(f"📌 Tabela: {tabela}")

    lf = ler_excels_lazy(root)
    if lf is None:
        logger.info("⏭ Sem Excel válido.")
        return

    # materializa somente agora
    df = lf.collect()

    criar_ou_ajustar_tabela(tabela, df)

    if MODO_CARGA == "upsert":
        chaves = detectar_pk_por_valores(df)
        if chaves:
            upsert_batch(tabela, df, chaves)
        else:
            inserir_batch(tabela, df)

    elif MODO_CARGA == "append":
        inserir_batch(tabela, df)

    elif MODO_CARGA == "truncate":
        with conectar() as con:
            with con.cursor() as cur:
                cur.execute(f'TRUNCATE TABLE "{tabela}"')
                con.commit()
        inserir_batch(tabela, df)
def main(pasta_raiz: str):
    logger.info("\n🚀 Iniciando ETL Ultra Performance\n")

    for root, dirs, files in os.walk(pasta_raiz):
        if any(f.lower().endswith((".xlsx", ".xls")) for f in files):
            try:
                processar_pasta(root)
            except Exception:
                logger.error(f"❌ Erro em {root}")
                logger.error(traceback.format_exc())

    logger.info("\n🏁 Finalizado.\n")


if __name__ == "__main__":
    main(PASTA_RAIZ)
