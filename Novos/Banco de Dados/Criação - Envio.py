# -*- coding: utf-8 -*-
import os
import unicodedata
import traceback
from typing import List, Optional

import polars as pl
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import Error as PgError
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
MODO_CARGA = "upsert"  # upsert | append | truncate

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


# ======================================================
# FUNÇÕES AUXILIARES
# ======================================================
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


def pl_dtype_to_pg(d: pl.DataType) -> str:
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


# ======================================================
# LEITURA DE EXCELS (LAZY)
# ======================================================
def ler_excels_lazy(pasta: str) -> Optional[pl.LazyFrame]:
    arquivos = [f for f in os.listdir(pasta) if f.lower().endswith((".xlsx", ".xls"))]
    if not arquivos:
        return None

    lfs: List[pl.LazyFrame] = []
    col_names_global = set()

    for arq in arquivos:
        caminho = os.path.join(pasta, arq)
        try:
            lf = pl.read_excel(caminho, engine="calamine").lazy()
        except Exception as e:
            logger.error(f"❌ Erro lendo {caminho}: {e}")
            continue

        # Evita PerformanceWarning: usamos collect_schema().names()
        cols_orig = lf.collect_schema().names()
        cols_norm = [limpar_nome(c) for c in cols_orig]

        rename_map = dict(zip(cols_orig, cols_norm))
        lf = lf.rename(rename_map)
        col_names_global.update(cols_norm)

        lfs.append(lf)

    if not lfs:
        return None

    col_names_global = sorted(col_names_global)

    lfs_alinhados: List[pl.LazyFrame] = []
    for lf in lfs:
        cols_lf = lf.collect_schema().names()
        faltantes = [c for c in col_names_global if c not in cols_lf]

        if faltantes:
            lf = lf.with_columns([pl.lit(None).alias(c) for c in faltantes])

        lf = lf.select(col_names_global)
        lfs_alinhados.append(lf)

    lf_final = pl.concat(lfs_alinhados, how="vertical_relaxed")
    return lf_final


# ======================================================
# DETECÇÃO HEURÍSTICA DE PK
# ======================================================
def detectar_pk_por_valores(df: pl.DataFrame) -> List[str]:
    """
    Heurística: considera PK colunas cuja string começa com 888 ou 999.
    """
    colunas_pk: List[str] = []

    for col in df.columns:
        serie = df[col].drop_nulls()

        if serie.is_empty():
            continue

        try:
            s = serie.cast(str)
            if s.str.starts_with("888").any() or s.str.starts_with("999").any():
                colunas_pk.append(col)
        except Exception:
            continue

    if colunas_pk:
        logger.info(f"🔑 PK detectada automaticamente: {colunas_pk}")
    else:
        logger.warning("⚠ Nenhuma PK encontrada via 888/999")

    return colunas_pk


# ======================================================
# CRIA / AJUSTA TABELA + ÍNDICE ÚNICO (PK)
# ======================================================
def criar_ou_ajustar_tabela(
    tabela: str,
    df: pl.DataFrame,
    chaves: Optional[List[str]] = None
) -> Optional[List[str]]:
    """
    - Cria a tabela se não existir.
    - Adiciona colunas novas se necessário.
    - Se 'chaves' for informado, tenta criar um UNIQUE INDEX nessas colunas.
      Se falhar (duplicidade / coluna inexistente / etc.), loga e devolve None,
      para que o chamador não use UPSERT nessa tabela.
    """
    schema = df.schema
    effective_keys: Optional[List[str]] = None

    with conectar() as con:
        with con.cursor() as cur:
            # Verifica se a tabela existe
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = %s
                );
                """,
                (tabela,),
            )
            existe = cur.fetchone()[0]

            if not existe:
                # Criar tabela
                cols_def = []
                for col, dtype in schema.items():
                    cols_def.append(f'"{col}" {pl_dtype_to_pg(dtype)}')

                ddl = f'CREATE TABLE "{tabela}" (\n    ' + ",\n    ".join(cols_def) + "\n);"
                cur.execute(ddl)
                logger.info(f"✔ Tabela criada: {tabela}")
            else:
                # Ajustar colunas (ADD COLUMN)
                cur.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s;
                    """,
                    (tabela,),
                )
                existentes = {r[0] for r in cur.fetchall()}

                novas = [c for c in schema if c not in existentes]
                for col in novas:
                    cur.execute(
                        f'ALTER TABLE "{tabela}" '
                        f'ADD COLUMN "{col}" {pl_dtype_to_pg(schema[col])};'
                    )

                if novas:
                    logger.info(f"➕ Colunas adicionadas: {novas}")

            # Se temos chaves para UPSERT, tentar criar índice único
            if chaves:
                keys_str = ", ".join([f'"{c}"' for c in chaves])
                index_name = f'idx_{limpar_nome(tabela)}_uniq_{abs(hash(tuple(chaves))) % 1000000}'

                try:
                    cur.execute(
                        f'CREATE UNIQUE INDEX IF NOT EXISTS "{index_name}" '
                        f'ON "{tabela}" ({keys_str});'
                    )
                    effective_keys = chaves
                    logger.info(f"🔐 Índice único garantido para chaves {chaves} em {tabela}")
                except PgError as e:
                    # Algo deu errado: duplicidade, coluna inexistente, sintaxe etc.
                    con.rollback()
                    logger.warning(
                        f"⚠ Falha ao criar índice único em {tabela} "
                        f"para chaves {chaves}. Motivo: {e}"
                    )
                    logger.warning(
                        f"⚠ UPSERT será desabilitado para {tabela}. "
                        f"Será usado apenas INSERT com ON CONFLICT DO NOTHING."
                    )
                    # Após rollback, precisamos reabrir transação para não quebrar
                    with con.cursor() as cur2:
                        # Garante de novo que a tabela exista (se foi criada antes na mesma transação)
                        cur2.execute(
                            """
                            SELECT EXISTS (
                                SELECT 1
                                FROM information_schema.tables
                                WHERE table_schema = 'public'
                                  AND table_name = %s
                            );
                            """,
                            (tabela,),
                        )
                        _ = cur2.fetchone()[0]
                    effective_keys = None

            con.commit()

    return effective_keys


# ======================================================
# INSERÇÃO / UPSERT
# ======================================================
def inserir_batch(tabela: str, df: pl.DataFrame) -> None:
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
                bloco = rows[i:i + BATCH_SIZE]
                execute_values(cur, sql, bloco)
            con.commit()

    logger.info(f"✔ Inseridos {len(rows)} registros (append safe) em {tabela}")


def upsert_batch(tabela: str, df: pl.DataFrame, chaves: List[str]) -> None:
    cols = df.columns
    rows = df.rows()

    if not rows:
        logger.warning(f"⚠ Nada para upsert em {tabela}")
        return

    cols_str = ", ".join([f'"{c}"' for c in cols])
    keys_str = ", ".join([f'"{c}"' for c in chaves])

    updates = [c for c in cols if c not in chaves]
    set_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in updates])

    sql = f"""
        INSERT INTO "{tabela}" ({cols_str})
        VALUES %s
        ON CONFLICT ({keys_str})
        DO UPDATE SET {set_str};
    """

    with conectar() as con:
        with con.cursor() as cur:
            for i in range(0, len(rows), BATCH_SIZE):
                bloco = rows[i:i + BATCH_SIZE]
                execute_values(cur, sql, bloco)
            con.commit()

    logger.info(f"✔ UPSERT {len(rows)} registros em {tabela}")


# ======================================================
# PROCESSAMENTO DE CADA PASTA
# ======================================================
def processar_pasta(root: str) -> None:
    nome = os.path.basename(root)
    tabela = limpar_nome("col_" + nome)

    logger.info(f"\n📁 Pasta: {root}")
    logger.info(f"📌 Tabela: {tabela}")

    lf = ler_excels_lazy(root)
    if lf is None:
        logger.info("⏭ Sem Excel válido.")
        return

    # Materializa somente agora
    df = lf.collect()

    # Detecta PK antes de criar/ajustar tabela
    chaves: Optional[List[str]] = None
    if MODO_CARGA == "upsert":
        chaves = detectar_pk_por_valores(df)

    # Cria/ajusta tabela + tenta criar índice único (pode devolver None se falhar)
    chaves_efetivas = criar_ou_ajustar_tabela(tabela, df, chaves)

    # Estratégia de carga
    if MODO_CARGA == "upsert":
        if chaves_efetivas:
            upsert_batch(tabela, df, chaves_efetivas)
        else:
            logger.warning(
                f"⚠ MODO_CARGA=upsert, mas sem índice único válido em {tabela}. "
                f"Usando INSERT com ON CONFLICT DO NOTHING."
            )
            inserir_batch(tabela, df)

    elif MODO_CARGA == "append":
        inserir_batch(tabela, df)

    elif MODO_CARGA == "truncate":
        with conectar() as con:
            with con.cursor() as cur:
                cur.execute(f'TRUNCATE TABLE "{tabela}"')
                con.commit()
        inserir_batch(tabela, df)


# ======================================================
# MAIN
# ======================================================
def main(pasta_raiz: str) -> None:
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
