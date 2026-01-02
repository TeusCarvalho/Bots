# -*- coding: utf-8 -*-
"""
ETL Incremental Excel -> PostgreSQL (Polars + COPY + Staging + UPSERT)

Correções aplicadas:
- remove Expr.is_temporal() (inexistente em algumas versões do Polars)
- row_hash: converte u64 -> i64 de forma segura (sem overflow)
- ANALYZE/índices só se a tabela existir (evita UndefinedTable)
- mantém SAVEPOINT por arquivo
"""

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
COPY_CHUNK_ROWS = 200_000
PROCESSAR_SUBPASTAS = True

# Incremental
USAR_HASH_ARQUIVO = True

# Polars — usa todos núcleos
os.environ["POLARS_MAX_THREADS"] = "0"

# Excel
EXCEL_ENGINE_PREFERIDO = "calamine"

# ======================================================
# LOGGING
# ======================================================
LOG_FILE = os.getenv("ETL_LOG_FILE", "").strip()
_handlers = [logging.StreamHandler()]
if LOG_FILE:
    _handlers.append(logging.FileHandler(LOG_FILE, encoding="utf-8"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=_handlers,
)
logger = logging.getLogger("etl_excel_pg_incremental_copy")
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


def sanitize_ident(nome: str, max_len: int = 63) -> str:
    n = limpar_nome(nome)
    return n[:max_len] if len(n) > max_len else n


def dedupe_names(names: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for n in names:
        base = n or "col_unk"
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out


def conectar():
    return psycopg2.connect(**DB)


def stable_index_name(prefix: str, tabela: str, keys: List[str]) -> str:
    base = f"{tabela}|{'|'.join(keys)}"
    suf = hashlib.md5(base.encode("utf-8")).hexdigest()[:10]
    name = sanitize_ident(f"{prefix}_{tabela}_{suf}", 63)
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
    files = []
    for f in os.listdir(pasta):
        fl = f.lower()
        if fl.endswith((".xlsx", ".xls")) and not f.startswith("~$"):
            files.append(os.path.join(pasta, f))
    return files


def now_utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def read_excel_safe(path: str) -> pl.DataFrame:
    errs = []
    engines = [EXCEL_ENGINE_PREFERIDO, "openpyxl", "calamine"]
    seen = set()
    for eng in engines:
        if eng in seen:
            continue
        seen.add(eng)
        try:
            return pl.read_excel(path, engine=eng)
        except Exception as e:
            errs.append(f"{eng}: {type(e).__name__}: {e}")
    raise RuntimeError("Falha ao ler Excel. Tentativas: " + " | ".join(errs))
# ======================================================
# META (controle incremental por arquivo)
# ======================================================
META_TABLE = "etl_ingest_files"


def ensure_meta_table(con) -> None:
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

    if (old_size == size) and (old_mtime == mtime_dt):
        return False, old_hash, size, mtime_dt

    if USAR_HASH_ARQUIVO:
        fhash = sha256_file(file_path)
        if old_hash and (old_hash == fhash):
            return False, fhash, size, mtime_dt
        return True, fhash, size, mtime_dt

    return True, None, size, mtime_dt


# ======================================================
# Heurísticas de colunas
# ======================================================
SEM_MOV_PATTERNS = {
    "pk_pedido": ["运单号", "numero_de_pedido", "número_de_pedido", "pedido", "waybill", "tracking"],
    "dias": ["dias_sem_mov", "dias", "断更天数"],
    "qtd": ["qtd", "quantidade", "件量", "pedidos件量", "pedidos"],
    "hora_ult": ["hora", "horario", "最新操作时间", "horario_da_ultima_operacao"],
}


def detect_col_by_patterns(columns: List[str], patterns: List[str]) -> Optional[str]:
    cols_norm_map: Dict[str, str] = {}
    for c in columns:
        if isinstance(c, str):
            cols_norm_map[limpar_nome(c)] = c

    pats = [limpar_nome(p) for p in patterns if isinstance(p, str)]
    for p in pats:
        if not p:
            continue
        for cnorm, corig in cols_norm_map.items():
            if p in cnorm:
                return corig
    return None


def normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
    cols_orig = df.columns
    cols_norm = [sanitize_ident(c, 63) for c in cols_orig]
    cols_norm = dedupe_names(cols_norm)
    rename_map = dict(zip(cols_orig, cols_norm))
    return df.rename(rename_map)


def parse_numeric_expr(colname: str) -> pl.Expr:
    s = (
        pl.col(colname)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(r"\s+", "")
    )

    s = pl.when(s.str.contains(",") & s.str.contains(r"\.")).then(
        s.str.replace_all(r"\.", "").str.replace_all(",", ".")
    ).otherwise(
        s.str.replace_all(",", ".")
    )

    return (
        s.str.replace_all(r"[^0-9\.\-]+", "")
        .replace("", None)
        .cast(pl.Float64, strict=False)
    )


def parse_datetime_expr(df: pl.DataFrame, colname: str) -> pl.Expr:
    """
    CORREÇÃO: Não usar Expr.is_temporal().
    Aqui a gente decide pelo dtype via df.schema (Python).
    """
    dtype = df.schema.get(colname)

    # Se já for Date/Datetime, só cast para Datetime
    if isinstance(dtype, (pl.Date, pl.Datetime)):
        return pl.col(colname).cast(pl.Datetime, strict=False)

    # Caso contrário, tenta parse via string
    s = (
        pl.col(colname)
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
    )

    return pl.coalesce(
        [
            s.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
            s.str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False),
            s.str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False),
            s.str.strptime(pl.Datetime, "%d/%m/%Y %H:%M", strict=False),
            s.str.strptime(pl.Datetime, "%Y-%m-%d", strict=False),
            s.str.strptime(pl.Datetime, "%d/%m/%Y", strict=False),
        ]
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
        exprs.append(parse_datetime_expr(df, col_hora).alias("hora_ult_ts"))

    if "ingested_at" not in cols:
        exprs.append(pl.lit(now_utc_naive()).cast(pl.Datetime).alias("ingested_at"))

    if exprs:
        df = df.with_columns(exprs)

    return df


def add_row_hash(df: pl.DataFrame, hash_cols: List[str]) -> pl.DataFrame:
    """
    CORREÇÃO: hash() pode gerar UInt64 > BIGINT.
    Convertendo para signed int64 com mapeamento (2's complement).
    """
    cols = [c for c in hash_cols if c in df.columns]
    if not cols:
        return df.with_columns(pl.lit(None).cast(pl.Int64).alias("row_hash"))

    h = pl.struct([pl.col(c) for c in cols]).hash(seed=0)  # normalmente u64
    h128 = h.cast(pl.Int128)

    max_i64 = 9223372036854775807
    two64 = 18446744073709551616

    signed = (
        pl.when(h128 > pl.lit(max_i64))
        .then(h128 - pl.lit(two64))
        .otherwise(h128)
        .cast(pl.Int64)
    )

    return df.with_columns(signed.alias("row_hash"))
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
    idx = sanitize_ident(f"idx_{tabela}_{col}", 63)
    cur.execute(f'CREATE INDEX IF NOT EXISTS "{idx}" ON public."{tabela}" ("{col}");')


def create_temp_staging(cur, target_table: str) -> str:
    stg = sanitize_ident(f"stg_{target_table}", 63)
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
    c = detect_col_by_patterns(cols, SEM_MOV_PATTERNS["pk_pedido"])
    return [c] if c else []


def merge_from_staging(cur, target: str, stg: str, cols: List[str], keys: Optional[List[str]]) -> None:
    cols_str = ", ".join([f'"{c}"' for c in cols])
    sel_str = ", ".join([f'"{c}"' for c in cols])

    if keys:
        keys_str = ", ".join([f'"{k}"' for k in keys])
        updates = [c for c in cols if c not in keys]

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
def processar_pasta(con, root: str) -> Dict[str, int]:
    stats = {
        "files_total": 0,
        "files_to_process": 0,
        "files_ok": 0,
        "files_skipped": 0,
        "files_error": 0,
    }

    nome = os.path.basename(root)
    tabela = sanitize_ident("col_" + nome, 63)

    files = list_excel_files(root)
    if not files:
        return stats

    stats["files_total"] = len(files)

    with con.cursor() as cur:
        cur.execute("SET LOCAL synchronous_commit TO OFF;")

        if MODO_CARGA == "truncate":
            to_process = []
            for fp in files:
                st = os.stat(fp)
                size = int(st.st_size)
                mtime_dt = datetime.fromtimestamp(st.st_mtime).replace(tzinfo=None)
                fhash = sha256_file(fp) if USAR_HASH_ARQUIVO else None
                to_process.append((fp, fhash, size, mtime_dt))
        else:
            to_process = []
            for fp in files:
                ok, fhash, size, mtime_dt = should_process_file(cur, fp)
                if ok:
                    to_process.append((fp, fhash, size, mtime_dt))

    if not to_process:
        logger.info(f"⏭ {tabela}: nenhum arquivo novo/alterado.")
        stats["files_skipped"] = stats["files_total"]
        return stats

    stats["files_to_process"] = len(to_process)
    stats["files_skipped"] = stats["files_total"] - stats["files_to_process"]

    logger.info(f"\n📁 Pasta: {root}")
    logger.info(f"📌 Tabela: {tabela}")
    logger.info(f"🆕 Arquivos a processar: {len(to_process)}/{len(files)}")

    pk_cols_table: Optional[List[str]] = None
    pk_ready = False
    did_truncate = False
    tabela_existe_no_final = False

    with con.cursor() as cur:
        cur.execute("SET LOCAL synchronous_commit TO OFF;")

        for i, (fp, fhash, size, mtime_dt) in enumerate(to_process, start=1):
            sp = f"sp_file_{i}"
            try:
                cur.execute(f"SAVEPOINT {sp};")

                logger.info(f"➡️ Lendo: {os.path.basename(fp)}")
                df = read_excel_safe(fp)
                df = normalize_columns(df)
                df = add_computed_fields(df)

                ensure_table_and_columns(cur, tabela, df.columns)
                tabela_existe_no_final = True  # pelo menos tentou criar/garantir

                if MODO_CARGA == "truncate" and not did_truncate:
                    cur.execute(f'TRUNCATE TABLE public."{tabela}";')
                    did_truncate = True
                    logger.info(f"🧹 TRUNCATE: {tabela}")

                table_cols = get_table_columns(cur, tabela)

                if pk_cols_table is None:
                    pk_cols_table = detect_pk(table_cols)
                    if (MODO_CARGA == "upsert") and pk_cols_table:
                        pk_ready = ensure_unique_index(cur, tabela, pk_cols_table)
                    else:
                        pk_ready = False

                stg = create_temp_staging(cur, tabela)

                missing = [c for c in table_cols if c not in df.columns]
                if missing:
                    df = df.with_columns([pl.lit(None).alias(c) for c in missing])

                df = df.select(table_cols)

                hash_cols = [c for c in table_cols if c != "row_hash"]
                df = add_row_hash(df, hash_cols)

                n = df.height
                logger.info(f"📦 Linhas no arquivo: {n:,}".replace(",", "."))

                for start in range(0, n, COPY_CHUNK_ROWS):
                    length = min(COPY_CHUNK_ROWS, n - start)
                    chunk = df.slice(start, length)

                    cur.execute(f'TRUNCATE "{stg}";')
                    copy_df_to_table(cur, stg, chunk, table_cols)

                    if (MODO_CARGA == "upsert") and pk_ready and pk_cols_table:
                        merge_from_staging(cur, tabela, stg, table_cols, pk_cols_table)
                    else:
                        merge_from_staging(cur, tabela, stg, table_cols, keys=None)

                upsert_file_meta(cur, fp, mtime_dt, size, fhash, tabela)

                cur.execute(f"RELEASE SAVEPOINT {sp};")
                stats["files_ok"] += 1
                logger.info(f"✅ Processado e registrado: {os.path.basename(fp)}")

            except Exception:
                stats["files_error"] += 1
                logger.error(f"❌ Erro no arquivo: {fp}")
                logger.error(traceback.format_exc())

                try:
                    cur.execute(f"ROLLBACK TO SAVEPOINT {sp};")
                    cur.execute(f"RELEASE SAVEPOINT {sp};")
                except Exception:
                    raise

        # Só cria índices/analyze se a tabela realmente existir
        if table_exists(cur, tabela):
            cols_set = set(get_table_columns(cur, tabela))
            if "dias_num" in cols_set:
                ensure_btree_index(cur, tabela, "dias_num")
            if "hora_ult_ts" in cols_set:
                ensure_btree_index(cur, tabela, "hora_ult_ts")

            cur.execute(f'ANALYZE public."{tabela}";')
            logger.info(f"📊 ANALYZE: {tabela}")
        else:
            logger.warning(f"⚠ Skipping ANALYZE/índices: tabela não existe ({tabela}). Provável: todos arquivos falharam.")

    return stats


def main(pasta_raiz: str) -> None:
    if COPY_CHUNK_ROWS <= 0:
        raise ValueError("COPY_CHUNK_ROWS deve ser > 0")

    logger.info("\n🚀 Iniciando ETL Incremental (PostgreSQL) — COPY + UPSERT\n")

    with conectar() as con:
        try:
            ensure_meta_table(con)
            con.commit()

            total = {"pastas": 0, "files_ok": 0, "files_error": 0, "files_skipped": 0, "files_total": 0}

            if PROCESSAR_SUBPASTAS:
                for root, _, files in os.walk(pasta_raiz):
                    if any(f.lower().endswith((".xlsx", ".xls")) for f in files):
                        st = processar_pasta(con, root)
                        con.commit()
                        total["pastas"] += 1
                        total["files_ok"] += st["files_ok"]
                        total["files_error"] += st["files_error"]
                        total["files_skipped"] += st["files_skipped"]
                        total["files_total"] += st["files_total"]
            else:
                st = processar_pasta(con, pasta_raiz)
                con.commit()
                total["pastas"] = 1
                total["files_ok"] = st["files_ok"]
                total["files_error"] = st["files_error"]
                total["files_skipped"] = st["files_skipped"]
                total["files_total"] = st["files_total"]

            logger.info(
                "\n📌 Resumo:"
                f"\n- Pastas processadas: {total['pastas']}"
                f"\n- Arquivos total: {total['files_total']}"
                f"\n- OK: {total['files_ok']}"
                f"\n- Erro: {total['files_error']}"
                f"\n- Pulados: {total['files_skipped']}"
            )

        except Exception:
            con.rollback()
            logger.error("❌ Erro geral no ETL (rollback total)")
            logger.error(traceback.format_exc())

    logger.info("\n🏁 Finalizado.\n")


if __name__ == "__main__":
    main(PASTA_RAIZ)
