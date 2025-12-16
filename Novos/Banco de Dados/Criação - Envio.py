# -*- coding: utf-8 -*-
import os
import io
import unicodedata
import traceback
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import polars as pl
import psycopg2
from psycopg2 import Error as PgError
import logging

# ======================================================
# CONFIG BANCO (recomendado: usar env vars)
# ======================================================
DB = {
    "host": os.getenv("PGHOST", "localhost"),
    "database": os.getenv("PGDATABASE", "analytics"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "Jt2025"),
    "port": int(os.getenv("PGPORT", "5432")),
}

# ======================================================
# CONFIG ETL
# ======================================================
PASTA_RAIZ = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\QUALIDADE_ FILIAL GO - BASE DE DADOS"

# upsert | append | truncate
MODO_CARGA = "upsert"

# Performance
COPY_CHUNK_ROWS = 200_000   # 200k–500k conforme RAM
PROCESSAR_SUBPASTAS = True  # varre tudo com os.walk

# Incremental
USAR_HASH_ARQUIVO = True    # True = sha256 quando mudou; False = só mtime+size (mais rápido, menos seguro)

# POLARS — usa todos núcleos
os.environ["POLARS_MAX_THREADS"] = "0"

# ======================================================
# LOGGING
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("etl_excel_pg_incremental_copy")

# ======================================================
# UTIL
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


def stable_index_name(prefix: str, tabela: str, keys: List[str]) -> str:
    base = f"{tabela}|{'|'.join(keys)}"
    suf = hashlib.md5(base.encode("utf-8")).hexdigest()[:10]
    name = limpar_nome(f"{prefix}_{tabela}_{suf}")
    return name[:63]


def sha256_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def list_excel_files(pasta: str) -> List[str]:
    if not os.path.isdir(pasta):
        return []
    return [
        os.path.join(pasta, f)
        for f in os.listdir(pasta)
        if f.lower().endswith((".xlsx", ".xls")) and not f.startswith("~$")
    ]


def now_utc_naive() -> datetime:
    # Postgres TIMESTAMP (naive)
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ======================================================
# META (controle incremental por arquivo)
# ======================================================
META_TABLE = "etl_ingest_files"


def ensure_meta_table() -> None:
    with conectar() as con:
        with con.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS public."{META_TABLE}" (
                    file_path    TEXT PRIMARY KEY,
                    file_mtime   TIMESTAMP,
                    file_size    BIGINT,
                    file_hash    TEXT,
                    table_name   TEXT,
                    processed_at TIMESTAMP DEFAULT now()
                );
            """)
            cur.execute(f'CREATE INDEX IF NOT EXISTS "idx_{META_TABLE}_table" ON public."{META_TABLE}" (table_name);')
        con.commit()


def get_file_meta(cur, file_path: str) -> Optional[Tuple[Optional[datetime], Optional[int], Optional[str]]]:
    cur.execute(
        f'SELECT file_mtime, file_size, file_hash FROM public."{META_TABLE}" WHERE file_path = %s;',
        (file_path,),
    )
    row = cur.fetchone()
    return row if row else None


def upsert_file_meta(cur, file_path: str, mtime_dt: datetime, size: int, fhash: Optional[str], table_name: str) -> None:
    cur.execute(
        f"""
        INSERT INTO public."{META_TABLE}" (file_path, file_mtime, file_size, file_hash, table_name, processed_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (file_path) DO UPDATE SET
            file_mtime = EXCLUDED.file_mtime,
            file_size  = EXCLUDED.file_size,
            file_hash  = EXCLUDED.file_hash,
            table_name = EXCLUDED.table_name,
            processed_at = now();
        """,
        (file_path, mtime_dt, int(size), fhash, table_name),
    )


def should_process_file(cur, file_path: str) -> Tuple[bool, Optional[str], int, datetime]:
    st = os.stat(file_path)
    size = int(st.st_size)
    mtime_dt = datetime.fromtimestamp(st.st_mtime).replace(tzinfo=None)

    old = get_file_meta(cur, file_path)
    if old is None:
        fhash = sha256_file(file_path) if USAR_HASH_ARQUIVO else None
        return True, fhash, size, mtime_dt

    old_mtime, old_size, old_hash = old

    # Atalho (igual e hash desligado): não processa
    if (old_size == size) and (old_mtime == mtime_dt) and (not USAR_HASH_ARQUIVO or old_hash):
        return False, old_hash, size, mtime_dt

    # Mudou size/mtime: confirma por hash se habilitado
    if USAR_HASH_ARQUIVO:
        fhash = sha256_file(file_path)
        if old_hash and (old_hash == fhash):
            return False, fhash, size, mtime_dt
        return True, fhash, size, mtime_dt

    # Sem hash: considera que mudou
    return True, None, size, mtime_dt
# ======================================================
# Heurísticas de colunas (ajuste se quiser refinar)
# ======================================================
SEM_MOV_PATTERNS = {
    "pk_pedido": ["运单号", "numero_de_pedido", "número_de_pedido", "pedido", "waybill", "tracking"],
    "dias": ["dias_sem_mov", "dias", "断更天数"],
    "qtd": ["qtd", "quantidade", "件量", "pedidos件量", "pedidos"],
    "hora_ult": ["hora", "horario", "最新操作时间", "horario_da_ultima_operacao"],
}


def detect_col_by_patterns(columns: List[str], patterns: List[str]) -> Optional[str]:
    cols_lower = {c.lower(): c for c in columns if isinstance(c, str)}
    for pat in patterns:
        p = pat.lower()
        for name_low, orig in cols_lower.items():
            if p in name_low:
                return orig
    return None


def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    cols_orig = df.columns
    cols_norm = [limpar_nome(c) for c in cols_orig]
    rename_map = dict(zip(cols_orig, cols_norm))
    return df.rename(rename_map)


def parse_numeric_expr(colname: str) -> pl.Expr:
    return (
        pl.col(colname)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(",", ".")
        .str.replace_all(r"[^0-9\.\-]+", "")
        .replace("", None)
        .cast(pl.Float64, strict=False)
    )


def parse_datetime_expr(colname: str) -> pl.Expr:
    c = pl.col(colname)
    return pl.when(c.is_temporal()).then(c.cast(pl.Datetime, strict=False)).otherwise(
        pl.coalesce(
            [
                c.cast(pl.Utf8, strict=False).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
                c.cast(pl.Utf8, strict=False).str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False),
                c.cast(pl.Utf8, strict=False).str.strptime(pl.Datetime, "%d/%m/%Y %H:%M", strict=False),
                c.cast(pl.Utf8, strict=False).str.strptime(pl.Datetime, "%Y-%m-%d", strict=False),
            ]
        )
    )


def add_computed_fields(df: pl.DataFrame) -> pl.DataFrame:
    cols = df.columns

    col_dias = detect_col_by_patterns(cols, SEM_MOV_PATTERNS["dias"])
    col_qtd = detect_col_by_patterns(cols, SEM_MOV_PATTERNS["qtd"])
    col_hora = detect_col_by_patterns(cols, SEM_MOV_PATTERNS["hora_ult"])

    exprs = []

    if col_dias and "dias_num" not in cols:
        exprs.append(parse_numeric_expr(col_dias).cast(pl.Int64, strict=False).alias("dias_num"))

    if col_qtd and "qtd_num" not in cols:
        exprs.append(parse_numeric_expr(col_qtd).alias("qtd_num"))

    if col_hora and "hora_ult_ts" not in cols:
        exprs.append(parse_datetime_expr(col_hora).alias("hora_ult_ts"))

    if "ingested_at" not in cols:
        exprs.append(pl.lit(now_utc_naive()).cast(pl.Datetime).alias("ingested_at"))

    if exprs:
        df = df.with_columns(exprs)

    return df


def add_row_hash(df: pl.DataFrame, hash_cols: List[str]) -> pl.DataFrame:
    # Hash rápido e estável (struct hash)
    return df.with_columns(pl.struct([pl.col(c) for c in hash_cols]).hash(seed=0).cast(pl.Int64).alias("row_hash"))


# ======================================================
# DB helpers
# ======================================================
def get_table_columns(cur, tabela: str) -> List[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position;
        """,
        (tabela,),
    )
    return [r[0] for r in cur.fetchall()]


def table_exists(cur, tabela: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema='public' AND table_name=%s
        );
        """,
        (tabela,),
    )
    return bool(cur.fetchone()[0])


def ensure_table_and_columns(cur, tabela: str, df_cols: List[str]) -> None:
    """
    - colunas originais: TEXT (robusto p/ excel heterogêneo)
    - computadas: tipos fixos (bom p/ filtros/índices)
    """
    computed_types = {
        "dias_num": "BIGINT",
        "qtd_num": "DOUBLE PRECISION",
        "hora_ult_ts": "TIMESTAMP",
        "row_hash": "BIGINT",
        "ingested_at": "TIMESTAMP",
    }

    if not table_exists(cur, tabela):
        cols_def = []
        for c in df_cols:
            pg_type = computed_types.get(c, "TEXT")
            cols_def.append(f'"{c}" {pg_type}')
        ddl = f'CREATE TABLE public."{tabela}" (\n    ' + ",\n    ".join(cols_def) + "\n);"
        cur.execute(ddl)
        logger.info(f"✔ Tabela criada: {tabela}")
        return

    existing = set(get_table_columns(cur, tabela))
    new_cols = [c for c in df_cols if c not in existing]
    for c in new_cols:
        pg_type = computed_types.get(c, "TEXT")
        cur.execute(f'ALTER TABLE public."{tabela}" ADD COLUMN "{c}" {pg_type};')
    if new_cols:
        logger.info(f"➕ {tabela}: colunas adicionadas = {new_cols}")


def ensure_unique_index(cur, tabela: str, keys: List[str]) -> bool:
    if not keys:
        return False
    idx = stable_index_name("uidx", tabela, keys)
    keys_str = ", ".join([f'"{k}"' for k in keys])
    try:
        cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx}" ON public."{tabela}" ({keys_str});')
        logger.info(f"🔐 UNIQUE OK em {tabela}: {keys}")
        return True
    except PgError as e:
        logger.warning(f"⚠ Falha UNIQUE em {tabela} ({keys}): {e}")
        return False


def ensure_btree_index(cur, tabela: str, col: str) -> None:
    idx = limpar_nome(f"idx_{tabela}_{col}")[:63]
    cur.execute(f'CREATE INDEX IF NOT EXISTS "{idx}" ON public."{tabela}" ("{col}");')


def create_temp_staging(cur, target_table: str) -> str:
    stg = f"stg_{target_table}"[:63]
    cur.execute(f'DROP TABLE IF EXISTS "{stg}";')
    cur.execute(f'CREATE TEMP TABLE "{stg}" (LIKE public."{target_table}" INCLUDING DEFAULTS) ON COMMIT PRESERVE ROWS;')
    return stg


def copy_df_to_table(cur, tabela: str, df: pl.DataFrame, cols: List[str]) -> None:
    buf = io.StringIO()
    df.select(cols).write_csv(
        buf,
        include_header=False,
        separator=",",
        null_value="\\N",
    )
    buf.seek(0)

    cols_str = ", ".join([f'"{c}"' for c in cols])
    sql = f"""
        COPY "{tabela}" ({cols_str})
        FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '\\N');
    """
    cur.copy_expert(sql, buf)


def detect_pk(cols: List[str]) -> List[str]:
    # PK por nome (barato e estável)
    c = detect_col_by_patterns(cols, SEM_MOV_PATTERNS["pk_pedido"])
    return [c] if c else []


def merge_from_staging(cur, target: str, stg: str, cols: List[str], keys: Optional[List[str]]) -> None:
    cols_str = ", ".join([f'"{c}"' for c in cols])
    sel_str = ", ".join([f'"{c}"' for c in cols])

    if keys:
        keys_str = ", ".join([f'"{k}"' for k in keys])
        updates = [c for c in cols if c not in keys]

        # update só quando mudou
        if "row_hash" in cols:
            where_change = f'public."{target}"."row_hash" IS DISTINCT FROM EXCLUDED."row_hash"'
        else:
            where_change = "TRUE"

        set_str = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in updates]) if updates else ""

        if updates:
            cur.execute(f"""
                INSERT INTO public."{target}" ({cols_str})
                SELECT {sel_str} FROM "{stg}"
                ON CONFLICT ({keys_str})
                DO UPDATE SET {set_str}
                WHERE {where_change};
            """)
        else:
            cur.execute(f"""
                INSERT INTO public."{target}" ({cols_str})
                SELECT {sel_str} FROM "{stg}"
                ON CONFLICT ({keys_str}) DO NOTHING;
            """)
    else:
        cur.execute(f"""
            INSERT INTO public."{target}" ({cols_str})
            SELECT {sel_str} FROM "{stg}"
            ON CONFLICT DO NOTHING;
        """)
def processar_pasta(cur, root: str) -> None:
    nome = os.path.basename(root)
    tabela = limpar_nome("col_" + nome)

    files = list_excel_files(root)
    if not files:
        return

    # Delta por arquivo
    to_process = []
    for fp in files:
        ok, fhash, size, mtime_dt = should_process_file(cur, fp)
        if ok:
            to_process.append((fp, fhash, size, mtime_dt))

    if not to_process:
        logger.info(f"⏭ {tabela}: nenhum arquivo novo/alterado.")
        return

    logger.info(f"\n📁 Pasta: {root}")
    logger.info(f"📌 Tabela: {tabela}")
    logger.info(f"🆕 Arquivos a processar: {len(to_process)}/{len(files)}")

    # Performance (carga local)
    cur.execute("SET synchronous_commit TO OFF;")

    pk_cols_table: Optional[List[str]] = None
    pk_ready = False

    for (fp, fhash, size, mtime_dt) in to_process:
        try:
            logger.info(f"➡️ Lendo: {os.path.basename(fp)}")

            df = pl.read_excel(fp, engine="calamine")
            df = normalize_columns(df)
            df = add_computed_fields(df)

            # garante colunas de controle (caso não existam por algum motivo)
            if "row_hash" not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Int64).alias("row_hash"))

            # garante tabela e colunas antes de truncate/merge
            ensure_table_and_columns(cur, tabela, df.columns)

            # truncate (1x por tabela) — executa na primeira vez que entrar numa pasta
            if MODO_CARGA == "truncate":
                cur.execute(f'TRUNCATE TABLE public."{tabela}";')
                # depois do truncate, muda para append para não truncar de novo nessa pasta
                # (mantém o comportamento por pasta)
                # Se você quer truncar sempre que rodar o ETL, mantenha assim mesmo.
                logger.info(f"🧹 TRUNCATE: {tabela}")

            # pega colunas atuais da tabela (ordem estável)
            table_cols = get_table_columns(cur, tabela)

            # PK detecta 1x por tabela
            if pk_cols_table is None:
                pk_cols_table = detect_pk(table_cols)
                if (MODO_CARGA == "upsert") and pk_cols_table:
                    pk_ready = ensure_unique_index(cur, tabela, pk_cols_table)
                else:
                    pk_ready = False

            # staging
            stg = create_temp_staging(cur, tabela)

            # alinha DF para exatamente as colunas da tabela
            missing = [c for c in table_cols if c not in df.columns]
            if missing:
                df = df.with_columns([pl.lit(None).alias(c) for c in missing])
            df = df.select(table_cols)

            # row_hash baseado em todas colunas exceto ele próprio
            hash_cols = [c for c in table_cols if c != "row_hash"]
            df = add_row_hash(df, hash_cols)

            n = df.height
            logger.info(f"📦 Linhas no arquivo: {n:,}".replace(",", "."))

            for start in range(0, n, COPY_CHUNK_ROWS):
                chunk = df.slice(start, min(COPY_CHUNK_ROWS, n - start))

                cur.execute(f'TRUNCATE "{stg}";')
                copy_df_to_table(cur, stg, chunk, table_cols)

                if (MODO_CARGA == "upsert") and pk_ready and pk_cols_table:
                    merge_from_staging(cur, tabela, stg, table_cols, pk_cols_table)
                else:
                    merge_from_staging(cur, tabela, stg, table_cols, keys=None)

            # marca meta como processado apenas se terminou OK
            upsert_file_meta(cur, fp, mtime_dt, size, fhash, tabela)
            logger.info(f"✅ Processado e registrado: {os.path.basename(fp)}")

        except Exception:
            logger.error(f"❌ Erro no arquivo: {fp}")
            logger.error(traceback.format_exc())
            # não atualiza meta deste arquivo

    # Índices mínimos úteis (não cria índice para tudo)
    cols_set = set(get_table_columns(cur, tabela))
    if "dias_num" in cols_set:
        ensure_btree_index(cur, tabela, "dias_num")
    if "hora_ult_ts" in cols_set:
        ensure_btree_index(cur, tabela, "hora_ult_ts")

    cur.execute(f'ANALYZE public."{tabela}";')
    logger.info(f"📊 ANALYZE: {tabela}")


def main(pasta_raiz: str) -> None:
    logger.info("\n🚀 Iniciando ETL Incremental (PostgreSQL) — COPY + UPSERT\n")
    ensure_meta_table()

    with conectar() as con:
        with con.cursor() as cur:
            try:
                if PROCESSAR_SUBPASTAS:
                    for root, _, files in os.walk(pasta_raiz):
                        if any(f.lower().endswith((".xlsx", ".xls")) for f in files):
                            processar_pasta(cur, root)
                    con.commit()
                else:
                    processar_pasta(cur, pasta_raiz)
                    con.commit()

            except Exception:
                con.rollback()
                logger.error("❌ Erro geral no ETL")
                logger.error(traceback.format_exc())

    logger.info("\n🏁 Finalizado.\n")


if __name__ == "__main__":
    main(PASTA_RAIZ)
