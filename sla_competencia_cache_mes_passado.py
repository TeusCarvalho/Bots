
from __future__ import annotations

import os
import json
import mimetypes
import warnings
import logging
import shutil
import unicodedata
import multiprocessing
import time
import calendar

from io import BytesIO
from pathlib import Path
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

import pandas as pd
import polars as pl
import requests
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ============================================================
# .ENV
# ============================================================
BASE_DIR = Path(__file__).resolve().parent
ENV_CANDIDATOS = [
    BASE_DIR / ".env",
    Path.cwd() / ".env",
]
for _env_path in ENV_CANDIDATOS:
    try:
        if _env_path.exists():
            load_dotenv(_env_path, override=False)
    except Exception:
        pass


def getenv_int(nome: str, default: int) -> int:
    try:
        return int(str(os.getenv(nome, default)).strip())
    except Exception:
        return default


def getenv_float(nome: str, default: float) -> float:
    try:
        return float(str(os.getenv(nome, default)).strip())
    except Exception:
        return default


def getenv_bool(nome: str, default: bool) -> bool:
    raw = str(os.getenv(nome, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "t", "sim", "s", "yes", "y", "on"}


def getenv_json_dict(nome: str) -> Dict[str, str]:
    raw = str(os.getenv(nome, "")).strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


# ============================================================
# CONFIG
# ============================================================
FONTE_DADOS = os.getenv("FONTE_DADOS", "PASTA").strip().upper()
ARQUIVO_RESUMO = os.getenv("ARQUIVO_RESUMO", "").strip()

# COMPETENCIA_MODO:
# - AUTO_MES_ANTERIOR   -> usa o mês anterior ao da execução
# - AUTO_MES_ATUAL      -> usa o mês atual da execução
# - AUTO_ULTIMO_MES_DADOS -> usa automaticamente o último mês encontrado na base
# - MANUAL              -> usa ANO_REF e MES_REF
COMPETENCIA_MODO = os.getenv("COMPETENCIA_MODO", "AUTO_ULTIMO_MES_DADOS").strip().upper()
ARQUIVO_MES_ANTERIOR = os.getenv("ARQUIVO_MES_ANTERIOR", "").strip()
AJUSTAR_COMPETENCIA_SEM_DADOS = getenv_bool("AJUSTAR_COMPETENCIA_SEM_DADOS", True)

PASTA_ENTRADA = os.getenv(
    "PASTA_ENTRADA",
    r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\06-  SLA Entrega Realizada Franquia",
).strip()

CAMINHO_COORDENADOR = os.getenv(
    "CAMINHO_COORDENADOR",
    r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\01 - Coordenador",
).strip()

PASTA_SAIDA = os.getenv(
    "PASTA_SAIDA",
    r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\12 - SLA Mês Passado",
).strip()

# Pasta bruta usada para calcular o SLA do mês anterior.
# O resultado calculado fica salvo em cache para não precisar recalcular todos os dias.
PASTA_MES_ANTERIOR = os.getenv(
    "PASTA_MES_ANTERIOR",
    r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\06.1-  SLA Entrega Realizada Franquia - mês passado",
).strip()

# Se False, o script só calcula o SLA do mês passado uma vez.
# Depois usa o arquivo salvo em cache.
# Se quiser recalcular, coloque True temporariamente no .env.
REPROCESSAR_SLA_MES_ANTERIOR = getenv_bool("REPROCESSAR_SLA_MES_ANTERIOR", False)

PASTA_CACHE_MES_ANTERIOR = os.getenv(
    "PASTA_CACHE_MES_ANTERIOR",
    os.path.join(PASTA_SAIDA, "Cache SLA Mês Anterior"),
).strip()

COL_DATA_BASE = os.getenv("COL_DATA_BASE", "DATA PREVISTA DE ENTREGA").strip().upper()
INDICADOR_NOME = os.getenv("INDICADOR_NOME", "Relatório SLA — Bases por quantidade").strip()
RELATORIO_TITULO = os.getenv("RELATORIO_TITULO", "Relatório SLA — Bases por quantidade").strip()
LINK_PASTA = os.getenv("LINK_PASTA", "").strip()

# ============================================================
# FILTRO DE PEDIDOS REMOVIDOS (não entram no resumo e não são enviados)
# Regra atual:
#   remover toda linha em que a coluna "Turno de linha secundária" for igual a "0Rit".
# ============================================================
COLUNA_0RIT_EXCLUSAO = os.getenv("COLUNA_0RIT_EXCLUSAO", "Turno de linha secundária").strip()
VALOR_0RIT_EXCLUSAO = os.getenv("VALOR_0RIT_EXCLUSAO", "0Rit").strip()

IMG_ROWS_PER_PAGE = getenv_int("IMG_ROWS_PER_PAGE", 28)
CASAS_PERCENTUAL = getenv_int("CASAS_PERCENTUAL", 2)
SLA_META_VERDE = getenv_float("SLA_META_VERDE", 0.97)
SLA_META_AMARELO = getenv_float("SLA_META_AMARELO", 0.95)

# Meta especial para domingos e feriados.
# Regra solicitada: domingo/feriado com SLA >= 70% fica verde.
SLA_META_DOMINGO_FERIADO = getenv_float("SLA_META_DOMINGO_FERIADO", 0.70)

FEISHU_BASE_DOMAIN = "https://open.feishu.cn"
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "").strip()
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "").strip()
# Webhooks definidos diretamente no código para facilitar manutenção.
# Preencha ou altere os links abaixo conforme necessário.
COORDENADOR_WEBHOOKS = {
    "João Melo": "COLE_SEU_WEBHOOK_AQUI",
    "João Melo GO": "COLE_SEU_WEBHOOK_AQUI",
    "Anderson Matheus": "COLE_SEU_WEBHOOK_AQUI",
    "Marcelo Medina": "COLE_SEU_WEBHOOK_AQUI",
    "Rodrigo Castro": "COLE_SEU_WEBHOOK_AQUI",
    "Orlean Nascimento": "COLE_SEU_WEBHOOK_AQUI",
    "Emerson Silva - AM e RR": "COLE_SEU_WEBHOOK_AQUI",
    "Emerson Silva": "COLE_SEU_WEBHOOK_AQUI",
    "Marcos Caique": "COLE_SEU_WEBHOOK_AQUI",
    "Ana Cunha": "COLE_SEU_WEBHOOK_AQUI",
    "Jose Marlon": "COLE_SEU_WEBHOOK_AQUI",
}

EXTS = (".xlsx", ".xls", ".csv")
EXCEL_MAX_ROWS = 1_048_576
_TOKEN_CACHE = {"token": None, "exp": 0}

# ============================================================
# PASTAS AUXILIARES
# ============================================================
PASTA_ARQUIVO = os.path.join(PASTA_SAIDA, "Arquivo Morto")
PASTA_BASE_CONSOLIDADA = os.path.join(PASTA_SAIDA, "Base Consolidada")
PASTA_IMAGENS = os.path.join(PASTA_SAIDA, "Imagens_Coordenadores_SLA")
PASTA_LOG = os.path.join(PASTA_SAIDA, "Logs")

for pasta in [
    PASTA_SAIDA,
    PASTA_ARQUIVO,
    PASTA_BASE_CONSOLIDADA,
    PASTA_IMAGENS,
    PASTA_LOG,
    PASTA_CACHE_MES_ANTERIOR,
]:
    os.makedirs(pasta, exist_ok=True)

DATA_HOJE = datetime.now().strftime("%Y%m%d")
DATA_HORA_BR = datetime.now().strftime("%d/%m/%Y %H:%M")
ARQUIVO_LOG = os.path.join(PASTA_LOG, "sla_competencia_flex.log")
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_SLA_{DATA_HOJE}.xlsx")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(ARQUIVO_LOG, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

# ============================================================
# PALETA
# ============================================================
JT_RED_MAIN = (227, 6, 19)
JT_RED_DARK = (194, 0, 12)
JT_RED_SOFT = (196, 39, 46)
JT_TEXT = (51, 51, 51)
JT_WHITE = (255, 255, 255)
JT_GRAY_GRID = (220, 220, 220)
JT_GRAY_TEXT = (120, 120, 120)   # cor para SLA anterior (tom neutro)
JT_ROW_A = (255, 255, 255)
JT_ROW_B = (248, 248, 248)
JT_GREEN = (22, 163, 74)
JT_AMBER = (180, 120, 0)


# ============================================================
# HTTP
# ============================================================
def _post_multipart_with_retry(
    url: str,
    data: dict,
    file_bytes: bytes,
    file_field: str,
    filename: str,
    headers: dict,
    timeout: int = 90,
    tries: int = 7,
) -> requests.Response:
    if not file_bytes:
        raise RuntimeError(f"Arquivo '{filename}' está vazio antes do upload.")

    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    last = None

    for i in range(1, tries + 1):
        try:
            files = {file_field: (filename, BytesIO(file_bytes), content_type)}
            return requests.post(
                url,
                data=data,
                files=files,
                headers=headers,
                timeout=timeout,
            )
        except Exception as e:
            last = e
            time.sleep(0.7 * i)

    raise RuntimeError(f"Falha UPLOAD {url} após {tries} tentativas. Último erro: {last}")


# ============================================================
# HELPERS
# ============================================================
def normalizar(s) -> str:
    if s is None:
        return ""
    s = str(s).upper().strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    while "  " in s:
        s = s.replace("  ", " ")
    return s


def localizar_coluna_por_alias(df: pl.DataFrame, aliases: List[str]) -> Optional[str]:
    if df is None or df.is_empty():
        return None

    mapa = {normalizar(c): c for c in df.columns}

    for alias in aliases:
        alias_norm = normalizar(alias)
        if alias_norm in mapa:
            return mapa[alias_norm]

    return None


def separar_pedidos_removidos(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Remove os pedidos 0Rit usando uma única coluna da base.

    Regra:
        coluna "Turno de linha secundária" == "0Rit"

    Resultado:
        - filtrado: base sem os pedidos 0Rit
        - removidos: somente os pedidos removidos, para gerar planilha separada
    """
    if df is None or df.is_empty():
        return df, pl.DataFrame()

    col_0rit = localizar_coluna_por_alias(
        df,
        [
            COLUNA_0RIT_EXCLUSAO,
            "Turno de linha secundária",
            "TURNO DE LINHA SECUNDÁRIA",
            "TURNO DE LINHA SECUNDARIA",
            "TURNO LINHA SECUNDÁRIA",
            "TURNO LINHA SECUNDARIA",
            "LINHA SECUNDÁRIA",
            "LINHA SECUNDARIA",
        ],
    )

    if not col_0rit:
        logging.warning(
            "⚠️ Filtro 0Rit não aplicado. "
            f"Coluna esperada não encontrada: {COLUNA_0RIT_EXCLUSAO}. "
            f"Colunas disponíveis: {df.columns}"
        )
        return df, pl.DataFrame()

    valor_norm = normalizar(VALOR_0RIT_EXCLUSAO)

    filtro_remocao = (
        pl.col(col_0rit)
        .cast(pl.Utf8)
        .map_elements(normalizar, return_dtype=pl.Utf8)
        == valor_norm
    )

    removidos = df.filter(filtro_remocao)
    filtrado = df.filter(~filtro_remocao)

    logging.info(
        f"🧹 Pedidos removidos pelo filtro [{col_0rit}={VALOR_0RIT_EXCLUSAO}]: {removidos.height}"
    )

    return filtrado, removidos

def formatar_periodo(inicio: date, fim: date) -> str:
    if inicio == fim:
        return inicio.strftime("%d/%m/%Y")
    return f"{inicio.strftime('%d/%m/%Y')} a {fim.strftime('%d/%m/%Y')}"


def obter_competencia() -> Tuple[date, date]:
    ano_env = str(os.getenv("ANO_REF", "")).strip()
    mes_env = str(os.getenv("MES_REF", "")).strip()
    modo = COMPETENCIA_MODO

    if modo == "MANUAL":
        if not (ano_env and mes_env):
            raise ValueError("COMPETENCIA_MODO=MANUAL exige ANO_REF e MES_REF no .env.")
        ano = int(ano_env)
        mes = int(mes_env)
        logging.info(f"📌 Competência manual: {mes:02d}/{ano}")

    elif ano_env and mes_env:
        ano = int(ano_env)
        mes = int(mes_env)
        logging.info(f"📌 Competência definida no .env: {mes:02d}/{ano}")

    else:
        hoje = datetime.now().date()

        if modo == "AUTO_MES_ATUAL":
            ano = hoje.year
            mes = hoje.month
            logging.info(f"📌 Competência automática (mês atual): {mes:02d}/{ano}")
        elif modo == "AUTO_ULTIMO_MES_DADOS":
            ano = hoje.year
            mes = hoje.month
            logging.info("📌 Competência provisória: será ajustada pelo último mês encontrado na base.")
        else:
            if hoje.month == 1:
                ano = hoje.year - 1
                mes = 12
            else:
                ano = hoje.year
                mes = hoje.month - 1
            logging.info(f"📌 Competência automática (mês anterior): {mes:02d}/{ano}")

    if mes < 1 or mes > 12:
        raise ValueError(f"MES_REF inválido: {mes}")

    ultimo_dia = calendar.monthrange(ano, mes)[1]
    inicio = date(ano, mes, 1)
    fim = date(ano, mes, ultimo_dia)
    return inicio, fim


def obter_competencia_anterior(inicio_atual: date) -> Tuple[date, date]:
    """Retorna o intervalo completo do mês imediatamente anterior à competência atual."""
    if inicio_atual.month == 1:
        ano = inicio_atual.year - 1
        mes = 12
    else:
        ano = inicio_atual.year
        mes = inicio_atual.month - 1

    ultimo_dia = calendar.monthrange(ano, mes)[1]
    inicio = date(ano, mes, 1)
    fim = date(ano, mes, ultimo_dia)
    logging.info(f"📌 Mês anterior para comparação: {mes:02d}/{ano}")
    return inicio, fim


def nome_mes_portugues(mes: int) -> str:
    meses = {
        1: "JANEIRO",
        2: "FEVEREIRO",
        3: "MARCO",
        4: "ABRIL",
        5: "MAIO",
        6: "JUNHO",
        7: "JULHO",
        8: "AGOSTO",
        9: "SETEMBRO",
        10: "OUTUBRO",
        11: "NOVEMBRO",
        12: "DEZEMBRO",
    }
    return meses[int(mes)]


def localizar_arquivo_mes_anterior(pasta: str, inicio_referencia: date) -> str:
    if ARQUIVO_MES_ANTERIOR:
        caminho = os.path.abspath(ARQUIVO_MES_ANTERIOR)
        if not os.path.exists(caminho):
            raise FileNotFoundError(f"ARQUIVO_MES_ANTERIOR não encontrado: {caminho}")
        logging.info(f"📂 Arquivo do mês anterior informado no .env: {caminho}")
        return caminho

    if not pasta or not os.path.isdir(pasta):
        raise FileNotFoundError(f"Pasta do mês anterior não encontrada: {pasta}")

    candidatos = [
        os.path.join(pasta, f)
        for f in os.listdir(pasta)
        if f.lower().endswith(".xlsx")
        and not f.startswith("~$")
        and not f.startswith("Resumo_SLA_")
    ]

    if not candidatos:
        raise FileNotFoundError(f"Nenhum arquivo .xlsx encontrado em: {pasta}")

    mes_nome = nome_mes_portugues(inicio_referencia.month)
    ano_txt = str(inicio_referencia.year)

    def normalizar_nome_arquivo(p: str) -> str:
        return normalizar(os.path.splitext(os.path.basename(p))[0]).replace("Ç", "C")

    candidatos_norm = [(p, normalizar_nome_arquivo(p)) for p in candidatos]

    correspondentes = [
        p for p, nome_norm in candidatos_norm
        if mes_nome in nome_norm and ano_txt in nome_norm
    ]

    if not correspondentes:
        correspondentes = [
            p for p, nome_norm in candidatos_norm
            if mes_nome in nome_norm
        ]

    if correspondentes:
        correspondentes.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        escolhido = correspondentes[0]
        logging.info(f"📂 Arquivo do mês anterior localizado por competência: {escolhido}")
        return escolhido

    candidatos.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    escolhido = candidatos[0]
    logging.warning(
        f"⚠️ Nenhum arquivo com o mês esperado ({mes_nome}/{inicio_referencia.year}) foi encontrado. "
        f"Usando o mais recente da pasta: {escolhido}"
    )
    return escolhido



def _primeiro_dia_mes(dt_ref: date) -> date:
    return date(dt_ref.year, dt_ref.month, 1)


def _ultimo_dia_mes(dt_ref: date) -> date:
    return date(dt_ref.year, dt_ref.month, calendar.monthrange(dt_ref.year, dt_ref.month)[1])


def competencia_do_mes_da_data(dt_ref: date) -> Tuple[date, date]:
    return _primeiro_dia_mes(dt_ref), _ultimo_dia_mes(dt_ref)


def ajustar_competencia_pelos_dados(
    inicio_atual: date,
    fim_atual: date,
    df: pl.DataFrame,
    coluna_data: str,
) -> Tuple[date, date, bool]:
    if coluna_data not in df.columns or df.is_empty():
        return inicio_atual, fim_atual, False

    max_data = df.select(pl.col(coluna_data).max()).item()
    min_data = df.select(pl.col(coluna_data).min()).item()

    if max_data is None:
        return inicio_atual, fim_atual, False

    precisa_ajustar = False

    if COMPETENCIA_MODO == "AUTO_ULTIMO_MES_DADOS":
        precisa_ajustar = True
    elif AJUSTAR_COMPETENCIA_SEM_DADOS:
        tem_linhas = df.filter(
            pl.col(coluna_data).is_not_null()
            & (pl.col(coluna_data).dt.year() == inicio_atual.year)
            & (pl.col(coluna_data).dt.month() == inicio_atual.month)
        ).height > 0
        precisa_ajustar = not tem_linhas

    if not precisa_ajustar:
        return inicio_atual, fim_atual, False

    novo_inicio, novo_fim = competencia_do_mes_da_data(max_data)
    if novo_inicio == inicio_atual and novo_fim == fim_atual:
        return inicio_atual, fim_atual, False

    logging.warning(
        f"⚠️ Competência ajustada automaticamente para {novo_inicio.strftime('%m/%Y')} "
        f"com base nos dados encontrados ({min_data} a {max_data})."
    )
    return novo_inicio, novo_fim, True


def detectar_e_padronizar_colunas_entrada(df: pl.DataFrame, origem: str = "") -> pl.DataFrame:
    if df is None or df.is_empty():
        return pl.DataFrame()

    try:
        df = df.rename({c: str(c).strip().upper() for c in df.columns})
    except Exception:
        return pl.DataFrame()

    colunas = list(df.columns)
    colunas_norm = {normalizar(c): c for c in colunas}

    aliases = {
        "BASE DE ENTREGA": [
            "BASE DE ENTREGA",
            "BASE ENTREGA",
            "NOME DA BASE",
            "BASE",
        ],
        "DATA PREVISTA DE ENTREGA": [
            "DATA PREVISTA DE ENTREGA",
            "DATA PREVISTA",
            "PREVISAO DE ENTREGA",
            "PREVISAO ENTREGA",
        ],
        "ENTREGUE NO PRAZO?": [
            "ENTREGUE NO PRAZO?",
            "ENTREGUE NO PRAZO？",
            "ENTREGUE NO PRAZO",
        ],
    }

    rename_map = {}
    for canonico, possiveis in aliases.items():
        for possivel in possiveis:
            possivel_norm = normalizar(possivel)
            if possivel_norm in colunas_norm:
                original = colunas_norm[possivel_norm]
                if original != canonico:
                    rename_map[original] = canonico
                break

    if rename_map:
        df = df.rename(rename_map)

    obrigatorias = ["BASE DE ENTREGA", "DATA PREVISTA DE ENTREGA", "ENTREGUE NO PRAZO?"]
    faltantes = [c for c in obrigatorias if c not in df.columns]
    if faltantes:
        if origem:
            logging.warning(
                f"⚠️ Arquivo ignorado por não bater com os modelos esperados: {os.path.basename(origem)} | "
                f"faltando: {faltantes}"
            )
        return pl.DataFrame()

    return df

def localizar_arquivo_coordenador(caminho: str) -> str:
    if not caminho or not str(caminho).strip():
        raise ValueError("CAMINHO_COORDENADOR está vazio.")

    caminho = os.path.abspath(caminho)

    if os.path.isfile(caminho):
        if not caminho.lower().endswith((".xlsx", ".xls")):
            raise ValueError(f"O arquivo informado em CAMINHO_COORDENADOR não é Excel: {caminho}")
        logging.info(f"📎 Arquivo de coordenador informado diretamente: {caminho}")
        return caminho

    if not os.path.isdir(caminho):
        raise FileNotFoundError(f"CAMINHO_COORDENADOR não existe: {caminho}")

    arquivos = [
        os.path.join(caminho, f)
        for f in os.listdir(caminho)
        if f.lower().endswith((".xlsx", ".xls")) and not f.startswith("~$")
    ]

    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo Excel encontrado em: {caminho}")

    prioridades = ["BASE_ATUALIZADA", "COORDENADOR", "BASE", "MAPEAMENTO"]

    def prioridade_arquivo(p: str) -> Tuple[int, float, str]:
        nome = normalizar(os.path.basename(p))
        idx = len(prioridades)
        for i, termo in enumerate(prioridades):
            if termo in nome:
                idx = i
                break
        return (idx, -os.path.getmtime(p), os.path.basename(p).lower())

    arquivos.sort(key=prioridade_arquivo)
    escolhido = arquivos[0]
    logging.info(f"📎 Arquivo de coordenador localizado automaticamente: {escolhido}")
    return escolhido


def ler_planilha_rapido(caminho: str) -> pl.DataFrame:
    try:
        if caminho.lower().endswith(".csv"):
            df = pl.read_csv(caminho, ignore_errors=True)
        else:
            df = pl.read_excel(caminho)
        return detectar_e_padronizar_colunas_entrada(df, caminho)
    except Exception as e:
        logging.error(f"Falha ao ler {os.path.basename(caminho)}: {e}")
        return pl.DataFrame()


def consolidar_planilhas(pasta_entrada: str) -> pl.DataFrame:
    arquivos = [
        os.path.join(pasta_entrada, f)
        for f in os.listdir(pasta_entrada)
        if f.lower().endswith(EXTS) and not f.startswith("~$")
    ]

    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo válido encontrado na pasta de entrada.")

    logging.info(f"📂 Arquivos encontrados para consolidar: {len(arquivos)}")

    with ThreadPoolExecutor(max_workers=min(16, len(arquivos))) as ex:
        dfs = list(ex.map(ler_planilha_rapido, arquivos))

    validos: List[pl.DataFrame] = []
    schemas_por_arquivo: List[Tuple[str, int, List[str]]] = []

    for caminho, df in zip(arquivos, dfs):
        if df is None or df.is_empty():
            continue

        # Garante nomes limpos e padronizados antes de consolidar.
        df = df.rename({c: str(c).strip().upper() for c in df.columns})

        validos.append(df)
        schemas_por_arquivo.append((os.path.basename(caminho), len(df.columns), list(df.columns)))

        logging.info(
            f"✅ Arquivo válido: {os.path.basename(caminho)} | "
            f"Linhas: {df.height:,} | Colunas: {len(df.columns)}"
        )

    ignorados = len(dfs) - len(validos)
    if ignorados:
        logging.warning(f"⚠️ Arquivos ignorados por formato incompatível: {ignorados}")

    if not validos:
        raise ValueError(
            "Falha ao ler todos os arquivos da pasta de entrada ou "
            "nenhum arquivo bateu com os modelos esperados."
        )

    # Diagnóstico: mostra quando há layouts diferentes entre os arquivos.
    qtd_colunas_distintas = sorted({qtd for _, qtd, _ in schemas_por_arquivo})
    if len(qtd_colunas_distintas) > 1:
        logging.warning(
            "⚠️ Foram encontrados arquivos com quantidades de colunas diferentes: "
            f"{qtd_colunas_distintas}. A consolidação será feita com diagonal_relaxed."
        )
        for nome, qtd, cols in schemas_por_arquivo:
            logging.warning(f"📌 Schema: {nome} | Colunas: {qtd} | {cols}")

    try:
        # vertical_relaxed falha quando os arquivos têm quantidade de colunas diferente.
        # diagonal_relaxed mantém todas as colunas e preenche com null onde a coluna não existir.
        df_final = pl.concat(validos, how="diagonal_relaxed")
    except Exception as e:
        logging.error("❌ Falha ao consolidar arquivos. Detalhe dos schemas encontrados:")
        for nome, qtd, cols in schemas_por_arquivo:
            logging.error(f"Arquivo: {nome} | Colunas: {qtd} | {cols}")
        raise e

    logging.info(
        f"✅ Consolidação concluída | Linhas: {df_final.height:,} | "
        f"Colunas finais: {len(df_final.columns)}"
    )

    return df_final


def mostrar_amostra_coluna_data(df: pl.DataFrame, coluna: str, limite: int = 5) -> None:
    try:
        amostra = (
            df.select(pl.col(coluna).cast(pl.Utf8).alias(coluna))
            .head(limite)
            .to_series()
            .to_list()
        )
        logging.info(f"🔎 Amostra bruta da coluna {coluna}: {amostra}")
    except Exception as e:
        logging.warning(f"⚠️ Não consegui mostrar amostra da coluna {coluna}: {e}")


def garantir_coluna_data(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    if coluna not in df.columns:
        raise KeyError(f"Coluna '{coluna}' não encontrada.")

    tipo = df[coluna].dtype

    if tipo == pl.Date:
        return df

    if tipo == pl.Datetime:
        return df.with_columns(pl.col(coluna).dt.date().alias(coluna))

    s = (
        pl.col(coluna)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace_all(r"\s+", " ")
    )

    expr = (
        s.str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False)
        .fill_null(s.str.strptime(pl.Datetime, "%d/%m/%Y %H:%M", strict=False))
        .fill_null(s.str.strptime(pl.Datetime, "%d/%m/%Y", strict=False))
        .fill_null(s.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False))
        .fill_null(s.str.strptime(pl.Datetime, "%Y-%m-%d", strict=False))
    )

    return df.with_columns(expr.dt.date().alias(coluna))


def diagnosticar_coluna_data(df: pl.DataFrame, coluna: str) -> None:
    if coluna not in df.columns:
        logging.warning(f"⚠️ Coluna {coluna} não encontrada para diagnóstico.")
        return

    total = df.height
    validos = df.filter(pl.col(coluna).is_not_null()).height
    nulos = total - validos

    logging.info(f"📅 Diagnóstico {coluna} | Total: {total} | Válidos: {validos} | Nulos: {nulos}")

    if validos == 0:
        logging.warning(f"⚠️ A coluna {coluna} ficou totalmente nula após conversão.")
        return

    min_data = df.select(pl.col(coluna).min()).item()
    max_data = df.select(pl.col(coluna).max()).item()

    logging.info(f"📅 Menor data encontrada: {min_data}")
    logging.info(f"📅 Maior data encontrada: {max_data}")


def ler_resumo_pronto(caminho: str) -> pd.DataFrame:
    if not caminho:
        raise ValueError("ARQUIVO_RESUMO não foi informado no .env.")

    caminho = os.path.abspath(caminho)
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Arquivo de resumo não encontrado: {caminho}")

    ext = os.path.splitext(caminho)[1].lower()

    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(caminho)
    elif ext == ".csv":
        try:
            df = pd.read_csv(caminho, sep=None, engine="python")
        except Exception:
            df = pd.read_csv(caminho, sep=";")
    else:
        try:
            df = pd.read_csv(caminho, sep="\t")
        except Exception:
            df = pd.read_csv(caminho, sep=None, engine="python")

    df.columns = [str(c).strip() for c in df.columns]

    col_map = {normalizar(c): c for c in df.columns}

    obrigatorias = ["BASE", "RECEBIDO", "ENTREGUE", "SLA"]
    faltantes = [c for c in obrigatorias if c not in col_map]
    if faltantes:
        raise KeyError(f"Arquivo-resumo sem colunas obrigatórias: {faltantes}. Colunas encontradas: {list(df.columns)}")

    df = df.rename(
        columns={
            col_map["BASE"]: "Base",
            col_map["RECEBIDO"]: "Recebido",
            col_map["ENTREGUE"]: "Entregue",
            col_map["SLA"]: "SLA",
        }
    )

    df = df[["Base", "Recebido", "Entregue", "SLA"]].copy()
    df["Base"] = df["Base"].astype(str).str.strip()
    df["Recebido"] = pd.to_numeric(df["Recebido"], errors="coerce").fillna(0).astype(int)
    df["Entregue"] = pd.to_numeric(df["Entregue"], errors="coerce").fillna(0).astype(int)
    df["SLA"] = pd.to_numeric(df["SLA"], errors="coerce").fillna(0.0)

    logging.info(f"📥 Resumo pronto carregado: {caminho}")
    logging.info(f"📊 Linhas do resumo pronto: {len(df)}")
    return df


# ============================================================
# LER SLA DO MÊS ANTERIOR A PARTIR DA PASTA BRUTA COM CACHE
# ============================================================
def ler_sla_mes_anterior_da_pasta(pasta: str, inicio_referencia: date) -> pd.DataFrame:
    """
    Calcula o SLA do mês anterior usando uma pasta bruta e salva em cache.

    Fluxo:
    1) Se já existir cache do mês anterior, usa o cache.
    2) Se não existir cache, consolida a pasta PASTA_MES_ANTERIOR.
    3) Filtra somente a competência do mês anterior.
    4) Remove pedidos 0Rit.
    5) Calcula SLA por base.
    6) Salva em Excel para não precisar recalcular todos os dias.

    Retorna DataFrame pandas com colunas:
        - Base
        - Recebido_Anterior
        - Entregue_Anterior
        - SLA_Anterior
    """
    os.makedirs(PASTA_CACHE_MES_ANTERIOR, exist_ok=True)

    nome_cache = f"SLA_Mes_Anterior_{inicio_referencia.strftime('%Y%m')}.xlsx"
    caminho_cache = os.path.join(PASTA_CACHE_MES_ANTERIOR, nome_cache)

    if os.path.exists(caminho_cache) and not REPROCESSAR_SLA_MES_ANTERIOR:
        try:
            cache = pd.read_excel(caminho_cache)
            cache.columns = [str(c).strip() for c in cache.columns]

            obrigatorias = ["Base", "SLA_Anterior"]
            faltantes = [c for c in obrigatorias if c not in cache.columns]

            if faltantes:
                logging.warning(
                    f"⚠️ Cache do mês anterior inválido. Faltando colunas: {faltantes}. "
                    f"Será recalculado."
                )
            else:
                cache["Base"] = cache["Base"].astype(str).str.strip()
                cache["SLA_Anterior"] = pd.to_numeric(cache["SLA_Anterior"], errors="coerce")

                logging.info(f"📦 SLA do mês anterior carregado do cache: {caminho_cache}")
                logging.info(f"📊 Bases no cache do mês anterior: {len(cache)}")

                colunas_retorno = ["Base"]
                if "Recebido_Anterior" in cache.columns:
                    colunas_retorno.append("Recebido_Anterior")
                if "Entregue_Anterior" in cache.columns:
                    colunas_retorno.append("Entregue_Anterior")
                colunas_retorno.append("SLA_Anterior")

                return cache[colunas_retorno].copy()

        except Exception as e:
            logging.warning(f"⚠️ Falha ao ler cache do mês anterior. Será recalculado. Erro: {e}")

    if not pasta or not os.path.isdir(pasta):
        logging.warning(f"⚠️ Pasta do mês anterior não encontrada: {pasta}")
        return pd.DataFrame(columns=["Base", "SLA_Anterior"])

    logging.info(f"📂 Calculando SLA do mês anterior pela pasta bruta: {pasta}")

    try:
        df_ant = consolidar_planilhas(pasta)
    except Exception as e:
        logging.error(f"❌ Falha ao consolidar pasta do mês anterior: {e}")
        return pd.DataFrame(columns=["Base", "SLA_Anterior"])

    if df_ant is None or df_ant.is_empty():
        logging.warning("⚠️ Pasta do mês anterior consolidada, mas sem dados válidos.")
        return pd.DataFrame(columns=["Base", "SLA_Anterior"])

    df_ant = df_ant.rename({c: str(c).strip().upper() for c in df_ant.columns})

    try:
        df_ant = garantir_coluna_data(df_ant, COL_DATA_BASE)
    except Exception as e:
        logging.error(f"❌ Falha ao converter data do mês anterior: {e}")
        return pd.DataFrame(columns=["Base", "SLA_Anterior"])

    ano_ref = inicio_referencia.year
    mes_ref = inicio_referencia.month

    df_ant = df_ant.filter(
        pl.col(COL_DATA_BASE).is_not_null()
        & (pl.col(COL_DATA_BASE).dt.year() == ano_ref)
        & (pl.col(COL_DATA_BASE).dt.month() == mes_ref)
    )

    if df_ant.is_empty():
        logging.warning(
            f"⚠️ Nenhum dado encontrado na pasta do mês anterior para "
            f"{inicio_referencia.strftime('%m/%Y')}."
        )
        return pd.DataFrame(columns=["Base", "SLA_Anterior"])

    df_ant, df_removidos_ant = separar_pedidos_removidos(df_ant)

    if df_removidos_ant is not None and not df_removidos_ant.is_empty():
        logging.info(
            f"🧹 Pedidos 0Rit removidos do cálculo do mês anterior: {df_removidos_ant.height}"
        )

    colunas = list(df_ant.columns)
    col_upper = [c.upper() for c in colunas]
    possiveis = ["ENTREGUE NO PRAZO?", "ENTREGUE NO PRAZO？"]

    col_entregue = None
    for nome in possiveis:
        if nome in col_upper:
            col_entregue = colunas[col_upper.index(nome)]
            break

    if not col_entregue:
        logging.warning(
            f"⚠️ Coluna ENTREGUE NO PRAZO não encontrada na pasta do mês anterior. "
            f"Colunas: {df_ant.columns}"
        )
        return pd.DataFrame(columns=["Base", "SLA_Anterior"])

    logging.info(f"📌 Coluna de SLA do mês anterior detectada: {col_entregue}")

    df_ant = df_ant.with_columns(
        pl.when(pl.col(col_entregue).cast(pl.Utf8).str.to_uppercase() == "Y")
        .then(1)
        .otherwise(0)
        .alias("_ENTREGUE_PRAZO")
    )

    resumo_ant = (
        df_ant.group_by("BASE DE ENTREGA")
        .agg(
            [
                pl.len().alias("Recebido_Anterior"),
                pl.col("_ENTREGUE_PRAZO").sum().alias("Entregue_Anterior"),
            ]
        )
        .with_columns(
            pl.when(pl.col("Recebido_Anterior") > 0)
            .then(pl.col("Entregue_Anterior") / pl.col("Recebido_Anterior"))
            .otherwise(0.0)
            .alias("SLA_Anterior")
        )
        .sort("BASE DE ENTREGA")
        .to_pandas()
        .rename(columns={"BASE DE ENTREGA": "Base"})
    )

    resumo_ant = resumo_ant[
        ["Base", "Recebido_Anterior", "Entregue_Anterior", "SLA_Anterior"]
    ].copy()

    resumo_ant["Base"] = resumo_ant["Base"].astype(str).str.strip()
    resumo_ant["Recebido_Anterior"] = pd.to_numeric(
        resumo_ant["Recebido_Anterior"],
        errors="coerce",
    ).fillna(0).astype(int)
    resumo_ant["Entregue_Anterior"] = pd.to_numeric(
        resumo_ant["Entregue_Anterior"],
        errors="coerce",
    ).fillna(0).astype(int)
    resumo_ant["SLA_Anterior"] = pd.to_numeric(
        resumo_ant["SLA_Anterior"],
        errors="coerce",
    ).fillna(0.0)

    try:
        with pd.ExcelWriter(caminho_cache, engine="openpyxl") as w:
            resumo_ant.to_excel(w, index=False, sheet_name="SLA Mes Anterior")

        logging.info(f"✅ SLA do mês anterior salvo em cache: {caminho_cache}")
    except Exception as e:
        logging.warning(f"⚠️ SLA calculado, mas falhou ao salvar cache: {e}")

    logging.info(f"📊 Bases calculadas no SLA do mês anterior: {len(resumo_ant)}")

    return resumo_ant

def anexar_coordenador_no_resumo(resumo: pd.DataFrame, caminho_coordenador: str) -> pd.DataFrame:
    arquivo_coord = localizar_arquivo_coordenador(caminho_coordenador)
    coord_df = pl.read_excel(arquivo_coord)
    logging.info(f"📎 Base de coordenadores carregada: {arquivo_coord}")
    logging.info(f"📥 Registros base coordenador: {coord_df.height}")

    coord_df = coord_df.rename({c: c.strip() for c in coord_df.columns})

    rename_map = {}
    if "Nome da base" in coord_df.columns:
        rename_map["Nome da base"] = "BASE DE ENTREGA"
    if "NOME DA BASE" in coord_df.columns:
        rename_map["NOME DA BASE"] = "BASE DE ENTREGA"
    if "Coordenadores" in coord_df.columns:
        rename_map["Coordenadores"] = "COORDENADOR"
    if "COORDENADORES" in coord_df.columns:
        rename_map["COORDENADORES"] = "COORDENADOR"
    if "Coordenador" in coord_df.columns and "COORDENADOR" not in rename_map.values():
        rename_map["Coordenador"] = "COORDENADOR"

    if rename_map:
        coord_df = coord_df.rename(rename_map)

    cols_norm = {normalizar(c): c for c in coord_df.columns}

    if "BASE DE ENTREGA" not in coord_df.columns:
        if "NOME DA BASE" in cols_norm:
            coord_df = coord_df.rename({cols_norm["NOME DA BASE"]: "BASE DE ENTREGA"})
        elif "BASE DE ENTREGA" in cols_norm:
            coord_df = coord_df.rename({cols_norm["BASE DE ENTREGA"]: "BASE DE ENTREGA"})

    if "COORDENADOR" not in coord_df.columns:
        if "COORDENADORES" in cols_norm:
            coord_df = coord_df.rename({cols_norm["COORDENADORES"]: "COORDENADOR"})
        elif "COORDENADOR" in cols_norm:
            coord_df = coord_df.rename({cols_norm["COORDENADOR"]: "COORDENADOR"})

    if "BASE DE ENTREGA" not in coord_df.columns or "COORDENADOR" not in coord_df.columns:
        raise KeyError(
            f"❌ O arquivo de coordenador precisa ter 'BASE DE ENTREGA' e 'COORDENADOR'. Colunas encontradas: {coord_df.columns}"
        )

    coord_pdf = coord_df.select(["BASE DE ENTREGA", "COORDENADOR"]).to_pandas().copy()
    coord_pdf["BASE_NORM"] = coord_pdf["BASE DE ENTREGA"].astype(str).map(normalizar)
    coord_pdf = coord_pdf.drop_duplicates(subset=["BASE_NORM"], keep="first")

    resumo = resumo.copy()
    resumo["BASE_NORM"] = resumo["Base"].astype(str).map(normalizar)

    resumo = resumo.merge(
        coord_pdf[["BASE_NORM", "COORDENADOR"]],
        on="BASE_NORM",
        how="left",
    )

    resumo["COORD_NORM"] = resumo["COORDENADOR"].fillna("").map(normalizar)
    return resumo


def arquivar_relatorios_antigos(pasta_origem: str, pasta_destino: str, prefixo: str) -> None:
    os.makedirs(pasta_destino, exist_ok=True)
    if not os.path.isdir(pasta_origem):
        return

    for arquivo in os.listdir(pasta_origem):
        if not (arquivo.startswith(prefixo) and arquivo.endswith(".xlsx")):
            continue
        try:
            shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo))
            logging.info(f"📦 Arquivo antigo movido: {arquivo}")
        except Exception as e:
            logging.error(f"Erro ao mover {arquivo}: {e}")


def arquivar_bases_antigas(pasta_origem: str, pasta_destino: str, prefixo: str) -> None:
    os.makedirs(pasta_destino, exist_ok=True)
    if not os.path.isdir(pasta_origem):
        return

    for arquivo in os.listdir(pasta_origem):
        if not arquivo.startswith(prefixo):
            continue
        if not arquivo.lower().endswith((".xlsx", ".csv", ".parquet")):
            continue
        try:
            shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo))
            logging.info(f"📦 Base antiga movida: {arquivo}")
        except Exception as e:
            logging.error(f"Erro ao mover {arquivo}: {e}")


def exportar_base_consolidada(resumo_geral: pd.DataFrame) -> Dict[str, str]:
    os.makedirs(PASTA_BASE_CONSOLIDADA, exist_ok=True)

    prefixo = "Base_Consolidada_SLA_"
    nome_base = f"Base_Consolidada_SLA_{DATA_HOJE}"

    arq_parquet = os.path.join(PASTA_BASE_CONSOLIDADA, f"{nome_base}.parquet")
    arq_csv = os.path.join(PASTA_BASE_CONSOLIDADA, f"{nome_base}.csv")
    arq_xlsx = os.path.join(PASTA_BASE_CONSOLIDADA, f"{nome_base}.xlsx")

    arquivar_bases_antigas(PASTA_BASE_CONSOLIDADA, PASTA_ARQUIVO, prefixo)

    df_pl = pl.from_pandas(resumo_geral)
    df_pl.write_parquet(arq_parquet)
    logging.info(f"✅ Base consolidada (PARQUET) salva em: {arq_parquet}")

    resumo_geral.to_csv(arq_csv, index=False)
    logging.info(f"✅ Base consolidada (CSV) salva em: {arq_csv}")

    if len(resumo_geral) <= (EXCEL_MAX_ROWS - 1):
        with pd.ExcelWriter(arq_xlsx, engine="openpyxl") as w:
            resumo_geral.to_excel(w, index=False, sheet_name="Base Consolidada")
        logging.info(f"✅ Base consolidada (XLSX) salva em: {arq_xlsx}")
    else:
        logging.warning("⚠️ XLSX da base consolidada não gerado (limite do Excel).")

    return {"parquet": arq_parquet, "csv": arq_csv, "xlsx": arq_xlsx}


def gerar_resumo_por_coordenador(resumo_com_coord: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    saida: Dict[str, pd.DataFrame] = {}

    coords = (
        resumo_com_coord.loc[resumo_com_coord["COORDENADOR"].notna(), ["COORDENADOR", "COORD_NORM"]]
        .drop_duplicates()
    )

    for _, row in coords.iterrows():
        coord = row["COORDENADOR"]
        coord_norm = row["COORD_NORM"]

        # inclui SLA_Anterior no Excel se existir
        colunas_export = ["Base", "Recebido", "Entregue", "SLA"]
        if "SLA_Anterior" in resumo_com_coord.columns:
            colunas_export.append("SLA_Anterior")

        sub = resumo_com_coord[resumo_com_coord["COORD_NORM"] == coord_norm].copy()
        sub = sub[colunas_export].sort_values(by=["SLA", "Base"], ascending=[False, True])

        if not sub.empty:
            saida[coord] = sub

    return saida


def exportar_resumo_excel(
    resumo_geral: pd.DataFrame,
    resumo_por_coord: Dict[str, pd.DataFrame],
    arquivo_saida: str,
) -> None:
    os.makedirs(PASTA_SAIDA, exist_ok=True)
    arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, "Resumo_SLA_")

    with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as w:
        resumo_geral.to_excel(w, index=False, sheet_name="Resumo Geral")

        for coord, dfc in resumo_por_coord.items():
            nome_sheet = normalizar(coord).replace("/", "-")[:31]
            dfc.to_excel(w, index=False, sheet_name=nome_sheet)

    logging.info(f"✅ Resumo Excel salvo em: {arquivo_saida}")


def exportar_planilha_pedidos_removidos(df_removidos: pl.DataFrame) -> Optional[str]:
    if df_removidos is None or df_removidos.is_empty():
        return None

    nome_arquivo = os.path.join(PASTA_SAIDA, f"Pedidos_Removidos_0RIT_{DATA_HOJE}.xlsx")
    pdf = df_removidos.to_pandas()

    with pd.ExcelWriter(nome_arquivo, engine="openpyxl") as w:
        pdf.to_excel(w, index=False, sheet_name="Pedidos Removidos")

    logging.info(f"📄 Planilha de pedidos removidos salva em: {nome_arquivo}")
    return nome_arquivo


def montar_arquivos_gerados_md(arquivo_resumo: str, paths_base: Dict[str, str]) -> str:
    """
    Mantido apenas por compatibilidade com o restante do script.

    Antes esta função montava no Feishu o bloco de arquivos gerados.
    Agora retorna vazio para não exibir essa parte no card.
    Os arquivos continuam sendo gerados e salvos normalmente nas pastas de saída.
    """
    return ""



# ============================================================
# IMAGEM
# ============================================================
MESES_PT_ABREV = {
    1: "Jan.",
    2: "Fev.",
    3: "Mar.",
    4: "Abr.",
    5: "Mai.",
    6: "Jun.",
    7: "Jul.",
    8: "Ago.",
    9: "Set.",
    10: "Out.",
    11: "Nov.",
    12: "Dez.",
}


def mes_abrev_pt(mes: int) -> str:
    return MESES_PT_ABREV.get(int(mes), f"{int(mes):02d}")


def calcular_pascoa(ano: int) -> date:
    """Calcula a data da Páscoa para feriados móveis."""
    a = ano % 19
    b = ano // 100
    c = ano % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes = (h + l - 7 * m + 114) // 31
    dia = ((h + l - 7 * m + 114) % 31) + 1
    return date(ano, mes, dia)


def feriados_brasil(ano: int) -> set[date]:
    """
    Feriados usados para a regra especial de SLA.

    Inclui feriados nacionais fixos e principais feriados móveis:
    Carnaval, Sexta-feira Santa e Corpus Christi.
    Ajuste aqui caso precise incluir feriados locais/estaduais da operação.
    """
    pascoa = calcular_pascoa(ano)

    return {
        date(ano, 1, 1),     # Confraternização Universal
        date(ano, 4, 21),    # Tiradentes
        date(ano, 5, 1),     # Dia do Trabalho
        date(ano, 9, 7),     # Independência do Brasil
        date(ano, 10, 12),   # Nossa Senhora Aparecida
        date(ano, 11, 2),    # Finados
        date(ano, 11, 15),   # Proclamação da República
        date(ano, 11, 20),   # Consciência Negra
        date(ano, 12, 25),   # Natal
        pascoa - timedelta(days=48),  # Segunda-feira de Carnaval
        pascoa - timedelta(days=47),  # Terça-feira de Carnaval
        pascoa - timedelta(days=2),   # Sexta-feira Santa
        pascoa + timedelta(days=60),  # Corpus Christi
    }


def eh_domingo_ou_feriado(dt_ref) -> bool:
    """Retorna True quando a data for domingo ou feriado mapeado."""
    if pd.isna(dt_ref):
        return False

    if isinstance(dt_ref, pd.Timestamp):
        dt_ref = dt_ref.date()

    if hasattr(dt_ref, "date") and not isinstance(dt_ref, date):
        dt_ref = dt_ref.date()

    if not isinstance(dt_ref, date):
        return False

    if dt_ref.weekday() == 6:
        return True

    return dt_ref in feriados_brasil(dt_ref.year)


def construir_grade_mensal_simples(
    resumo_pd: pd.DataFrame,
    inicio: date,
    inicio_ant: date,
) -> Tuple[pd.DataFrame, List[str], str]:
    prev_label = f"SLA {mes_abrev_pt(inicio_ant.month)}"
    curr_label = f"SLA {mes_abrev_pt(inicio.month)}"

    grade = resumo_pd.copy()
    if "SLA_Anterior" not in grade.columns:
        grade["SLA_Anterior"] = pd.NA

    grade = grade[["Base", "SLA_Anterior", "SLA"]].copy()
    grade = grade.rename(
        columns={
            "SLA_Anterior": prev_label,
            "SLA": curr_label,
        }
    )

    grade = grade.sort_values(by=[curr_label, "Base"], ascending=[True, True], na_position="last").reset_index(drop=True)

    subtitulo = f"SLA Entrega Realizada — %SLA por Base (pior → melhor) | Competência {inicio.strftime('%m/%Y')}"
    return grade, [prev_label], subtitulo


def preparar_analitico_competencia(
    df_periodo: pl.DataFrame,
    col_data_base: str,
    ultimos_dias: int = 7,
) -> Dict[str, object]:
    if df_periodo.is_empty():
        return {
            "semanal": pd.DataFrame(),
            "diario": pd.DataFrame(),
            "week_labels": [],
            "day_labels": [],
            "ultimas_datas": [],
            "ultimo_dia": None,
            "sunday_labels": [],
            "holiday_labels": [],
            "special_day_labels": [],
        }

    semanas = (
        df_periodo
        .select(pl.col(col_data_base).drop_nulls().dt.week().unique().sort())
        .to_series()
        .to_list()
    )
    week_labels = [f"W{int(w)}" for w in semanas if w is not None]

    datas_distintas = (
        df_periodo
        .select(pl.col(col_data_base).drop_nulls().unique().sort())
        .to_series()
        .to_list()
    )
    datas_distintas = [d for d in datas_distintas if d is not None]
    ultimas_datas = datas_distintas[-ultimos_dias:]
    day_labels = [f"{d.day}/{d.month}" for d in ultimas_datas]
    sunday_labels = [
        f"{d.day}/{d.month}"
        for d in ultimas_datas
        if hasattr(d, "weekday") and d.weekday() == 6
    ]
    holiday_labels = [
        f"{d.day}/{d.month}"
        for d in ultimas_datas
        if eh_domingo_ou_feriado(d)
    ]
    special_day_labels = list(dict.fromkeys(sunday_labels + holiday_labels))
    ultimo_dia = ultimas_datas[-1] if ultimas_datas else None

    semanal = (
        df_periodo
        .with_columns(pl.col(col_data_base).dt.week().alias("_WEEK"))
        .group_by(["COORD_NORM", "BASE DE ENTREGA", "_WEEK"])
        .agg(
            [
                pl.len().alias("Recebido"),
                pl.col("_ENTREGUE_PRAZO").sum().alias("Entregue"),
            ]
        )
        .with_columns(
            pl.when(pl.col("Recebido") > 0)
            .then(pl.col("Entregue") / pl.col("Recebido"))
            .otherwise(None)
            .alias("SLA")
        )
        .select(["COORD_NORM", "BASE DE ENTREGA", "_WEEK", "SLA"])
        .to_pandas()
    )

    if not semanal.empty:
        semanal["_WEEK_LABEL"] = semanal["_WEEK"].apply(lambda x: f"W{int(x)}" if pd.notna(x) else None)

    if ultimas_datas:
        diario = (
            df_periodo
            .filter(pl.col(col_data_base).is_in(ultimas_datas))
            .group_by(["COORD_NORM", "BASE DE ENTREGA", col_data_base])
            .agg(
                [
                    pl.len().alias("Recebido"),
                    pl.col("_ENTREGUE_PRAZO").sum().alias("Entregue"),
                ]
            )
            .with_columns(
                pl.when(pl.col("Recebido") > 0)
                .then(pl.col("Entregue") / pl.col("Recebido"))
                .otherwise(None)
                .alias("SLA")
            )
            .select(["COORD_NORM", "BASE DE ENTREGA", col_data_base, "SLA"])
            .to_pandas()
        )
    else:
        diario = pd.DataFrame(columns=["COORD_NORM", "BASE DE ENTREGA", col_data_base, "SLA"])

    if not diario.empty:
        diario["DIA_LABEL"] = diario[col_data_base].apply(
            lambda d: f"{d.day}/{d.month}" if pd.notna(d) else None
        )

    return {
        "semanal": semanal,
        "diario": diario,
        "week_labels": week_labels,
        "day_labels": day_labels,
        "ultimas_datas": ultimas_datas,
        "ultimo_dia": ultimo_dia,
        "sunday_labels": sunday_labels,
        "holiday_labels": holiday_labels,
        "special_day_labels": special_day_labels,
    }


def construir_grade_analitica_coord(
    coord: str,
    resumo_com_coord: pd.DataFrame,
    analitico: Dict[str, object],
    inicio: date,
    inicio_ant: date,
) -> Tuple[pd.DataFrame, List[str], str]:
    coord_norm = normalizar(coord)
    sub = resumo_com_coord[resumo_com_coord["COORD_NORM"] == coord_norm].copy()

    if sub.empty:
        return pd.DataFrame(), [], ""

    prev_label = f"SLA {mes_abrev_pt(inicio_ant.month)}"
    curr_label = f"SLA {mes_abrev_pt(inicio.month)}"

    grade = sub.copy()
    if "SLA_Anterior" not in grade.columns:
        grade["SLA_Anterior"] = pd.NA

    grade = grade[["Base", "SLA_Anterior", "SLA"]].copy()
    grade = grade.rename(
        columns={
            "SLA_Anterior": prev_label,
            "SLA": curr_label,
        }
    )

    semanal = analitico.get("semanal")
    if isinstance(semanal, pd.DataFrame) and not semanal.empty:
        semanal_coord = semanal[semanal["COORD_NORM"] == coord_norm].copy()
        if not semanal_coord.empty:
            piv_w = (
                semanal_coord
                .pivot_table(
                    index="BASE DE ENTREGA",
                    columns="_WEEK_LABEL",
                    values="SLA",
                    aggfunc="mean",
                )
                .reset_index()
                .rename(columns={"BASE DE ENTREGA": "Base"})
            )
            grade = grade.merge(piv_w, on="Base", how="left")

    diario = analitico.get("diario")
    if isinstance(diario, pd.DataFrame) and not diario.empty:
        diario_coord = diario[diario["COORD_NORM"] == coord_norm].copy()
        if not diario_coord.empty:
            piv_d = (
                diario_coord
                .pivot_table(
                    index="BASE DE ENTREGA",
                    columns="DIA_LABEL",
                    values="SLA",
                    aggfunc="mean",
                )
                .reset_index()
                .rename(columns={"BASE DE ENTREGA": "Base"})
            )
            grade = grade.merge(piv_d, on="Base", how="left")

    week_labels = list(analitico.get("week_labels", []))
    day_labels = list(analitico.get("day_labels", []))

    for col in week_labels + day_labels:
        if col not in grade.columns:
            grade[col] = pd.NA

    ordered_cols = ["Base", prev_label, curr_label] + week_labels + day_labels
    grade = grade[ordered_cols].copy()
    grade = grade.sort_values(by=[curr_label, "Base"], ascending=[True, True], na_position="last").reset_index(drop=True)

    ultimo_dia = analitico.get("ultimo_dia")
    if ultimo_dia is not None and day_labels:
        subtitulo = (
            f"SLA Entrega Realizada — %SLA por Base (pior → melhor) | "
            f"Semanas do mês + últimos {len(day_labels)} dias até {ultimo_dia.strftime('%d/%m/%Y')}"
        )
    else:
        subtitulo = f"SLA Entrega Realizada — %SLA por Base (pior → melhor) | Competência {inicio.strftime('%m/%Y')}"

    return grade, [prev_label], subtitulo


def gerar_imagens_grade_analitica(
    coord: str,
    grade_pd: pd.DataFrame,
    subtitulo_base: str,
    out_dir: str,
    rows_per_page: int = 22,
    colunas_neutras: Optional[List[str]] = None,
    colunas_domingo: Optional[List[str]] = None,
) -> List[str]:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        raise RuntimeError("Falta Pillow. Instale: pip install pillow")

    os.makedirs(out_dir, exist_ok=True)

    if grade_pd is None or grade_pd.empty:
        return []

    colunas_neutras = colunas_neutras or []
    colunas_domingo = set(colunas_domingo or [])

    df = grade_pd.copy()

    def load_font(size: int, bold: bool = False):
        candidates = [
            ("segoeuib.ttf" if bold else "segoeui.ttf"),
            ("arialbd.ttf" if bold else "arial.ttf"),
            ("calibrib.ttf" if bold else "calibri.ttf"),
            ("calibri.ttf"),
        ]
        for name in candidates:
            try:
                return ImageFont.truetype(name, size)
            except Exception:
                continue
        return ImageFont.load_default()

    def measure(draw, text, font):
        text = "" if text is None else str(text)
        try:
            box = draw.textbbox((0, 0), text, font=font)
            return box[2] - box[0], box[3] - box[1]
        except Exception:
            return len(text) * 8, 16

    def ellipsize(draw, text, font, max_w):
        text = "" if text is None else str(text)
        if measure(draw, text, font)[0] <= max_w:
            return text
        base = text
        while base and measure(draw, base + "...", font)[0] > max_w:
            base = base[:-1]
        return (base.rstrip() + "...") if base else "..."

    def fmt_pct(v):
        if pd.isna(v):
            return "-"
        return f"{float(v) * 100:.{CASAS_PERCENTUAL}f}%".replace(".", ",")

    def cor_valor(v, neutral: bool = False, domingo_feriado: bool = False):
        if pd.isna(v):
            return JT_GRAY_TEXT

        if neutral:
            return JT_GRAY_TEXT

        pct = float(v)

        # Regra especial: domingos e feriados ficam verdes a partir de 70%.
        if domingo_feriado:
            if pct >= SLA_META_DOMINGO_FERIADO:
                return JT_GREEN
            return JT_RED_SOFT

        # Regra normal para os demais dias.
        if pct >= SLA_META_VERDE:
            return JT_GREEN
        if pct >= SLA_META_AMARELO:
            return JT_AMBER
        return JT_RED_SOFT

    headers = list(df.columns)

    widths: List[int] = []
    for col in headers:
        if col == "Base":
            widths.append(220)
        elif col.startswith("SLA "):
            widths.append(126)
        elif col.startswith("W"):
            widths.append(104)
        elif "/" in col:
            widths.append(104)
        else:
            widths.append(110)

    pages = [
        df.iloc[i:i + rows_per_page].copy()
        for i in range(0, len(df), rows_per_page)
    ]

    font_title = load_font(24, bold=True)
    font_sub = load_font(13, bold=False)
    font_head = load_font(15, bold=True)
    font_cell = load_font(14, bold=False)
    font_cell_bold = load_font(14, bold=True)

    left = 16
    right = 16
    top_title = 54
    top_sub = 26
    table_top = 70
    header_h = 42
    row_h = 36
    bottom = 18

    total_w = left + sum(widths) + right

    BG_PAGE = (239, 239, 239)
    BG_ROW_A = (239, 239, 239)
    BG_ROW_B = (245, 245, 245)
    GRID = (190, 190, 190)

    HEADER_SUNDAY = (186, 12, 24)
    BG_SUNDAY_A = (252, 235, 236)
    BG_SUNDAY_B = (247, 228, 230)
    GRID_SUNDAY = (214, 167, 171)

    out_paths: List[str] = []

    for page_idx, page_df in enumerate(pages, start=1):
        total_h = table_top + header_h + (len(page_df) * row_h) + bottom
        img = Image.new("RGB", (total_w, total_h), BG_PAGE)
        draw = ImageDraw.Draw(img)

        titulo = f"{coord} — SLA por Base"
        draw.text((left, 10), titulo, fill=JT_RED_MAIN, font=font_title)

        subtitulo = f"{subtitulo_base} | Página {page_idx}/{len(pages)}"
        draw.text((left, 40), subtitulo, fill=JT_TEXT, font=font_sub)

        x = left
        y = table_top

        for col, w in zip(headers, widths):
            header_fill = HEADER_SUNDAY if col in colunas_domingo else JT_RED_MAIN
            draw.rectangle((x, y, x + w, y + header_h), fill=header_fill, outline=JT_WHITE, width=1)
            txt = ellipsize(draw, col, font_head, w - 10)
            tw, th = measure(draw, txt, font_head)
            draw.text((x + (w - tw) / 2, y + (header_h - th) / 2 - 1), txt, fill=JT_WHITE, font=font_head)
            x += w

        start_y = y + header_h

        for ridx, (_, row) in enumerate(page_df.iterrows()):
            y1 = start_y + (ridx * row_h)
            fill_row = BG_ROW_A if ridx % 2 == 0 else BG_ROW_B
            x = left

            for col, w in zip(headers, widths):
                cell_fill = fill_row
                cell_outline = GRID
                if col in colunas_domingo:
                    cell_fill = BG_SUNDAY_A if ridx % 2 == 0 else BG_SUNDAY_B
                    cell_outline = GRID_SUNDAY

                draw.rectangle((x, y1, x + w, y1 + row_h), fill=cell_fill, outline=cell_outline, width=1)
                val = row.get(col)

                if col == "Base":
                    txt = ellipsize(draw, "" if pd.isna(val) else str(val), font_cell, w - 10)
                    _, th = measure(draw, txt, font_cell)
                    draw.text((x + 6, y1 + (row_h - th) / 2 - 1), txt, fill=JT_TEXT, font=font_cell)
                else:
                    txt = fmt_pct(val)
                    cor = cor_valor(
                        val,
                        neutral=(col in colunas_neutras),
                        domingo_feriado=(col in colunas_domingo),
                    )
                    tw, th = measure(draw, txt, font_cell_bold)
                    draw.text((x + (w - tw) / 2, y1 + (row_h - th) / 2 - 1), txt, fill=cor, font=font_cell_bold)

                x += w

        safe_coord = normalizar(coord).replace(" ", "_")
        filename = f"SLA_ANALITICO_{safe_coord}_{DATA_HOJE}_p{page_idx:02d}.png"
        out_path = os.path.join(out_dir, filename)
        img.save(out_path, format="PNG")
        out_paths.append(out_path)

        logging.info(f"🖼️ Imagem gerada: {out_path}")

    return out_paths


# ============================================================
# FEISHU
# ============================================================
def _feishu_enabled() -> bool:
    return bool(FEISHU_APP_ID and FEISHU_APP_SECRET)


def feishu_get_token() -> str:
    if not _feishu_enabled():
        raise RuntimeError("Defina FEISHU_APP_ID e FEISHU_APP_SECRET nas variáveis de ambiente.")

    now = int(time.time())
    if _TOKEN_CACHE["token"] and now < int(_TOKEN_CACHE["exp"]):
        return _TOKEN_CACHE["token"]

    url = f"{FEISHU_BASE_DOMAIN}/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    r = requests.post(url, json=payload, timeout=25)
    data = r.json() if r.content else {}

    if data.get("code") != 0:
        raise RuntimeError(f"Token Feishu falhou: {data}")

    token = data.get("tenant_access_token")
    exp   = int(data.get("expire", 0))
    if not token:
        raise RuntimeError(f"Resposta sem tenant_access_token: {data}")

    _TOKEN_CACHE["token"] = token
    _TOKEN_CACHE["exp"]   = now + max(0, exp - 60)
    return token


def feishu_upload_image_get_key(image_path: str) -> str:
    token = feishu_get_token()
    url   = f"{FEISHU_BASE_DOMAIN}/open-apis/im/v1/images"
    headers = {"Authorization": f"Bearer {token}"}

    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Imagem não encontrada para upload: {image_path}")

    with open(image_path, "rb") as f:
        file_bytes = f.read()

    r = _post_multipart_with_retry(
        url=url,
        data={"image_type": "message"},
        file_bytes=file_bytes,
        file_field="image",
        filename=os.path.basename(image_path),
        headers=headers,
        timeout=90,
    )

    data = r.json() if r.content else {}

    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} no upload da imagem: {data}")

    if data.get("code") != 0:
        raise RuntimeError(f"Upload imagem falhou: {data}")

    image_key = (data.get("data") or {}).get("image_key")
    if not image_key:
        raise RuntimeError(f"Upload OK mas sem image_key: {data}")

    return image_key


def enviar_card_feishu(
    webhook: str,
    coord: str,
    periodo_txt: str,
    sla: float,
    bases: int,
    recebido: int,
    entregue: int,
    arquivos_gerados_md: str,
    image_key: Optional[str] = None,
    page_label: Optional[str] = None,
    sla_anterior: Optional[float] = None,
    periodo_anterior_txt: str = "",
) -> bool:
    try:
        if not webhook or webhook == "COLE_SEU_WEBHOOK_AQUI":
            logging.warning(f"⚠️ Webhook vazio/inválido para {coord}. Pulei.")
            return False

        body = (
            f"📌 **Indicador:** {INDICADOR_NOME}\n"
            f"📅 **Período:** {periodo_txt}\n"
            f"📥 **Recebido:** {recebido}\n"
            f"✅ **Entregue:** {entregue}\n"
            f"📈 **SLA:** {sla:.2%}\n"
        )

        if sla_anterior is not None:
            body += f"📊 **SLA mês anterior ({periodo_anterior_txt}):** {sla_anterior:.2%}\n"

        body += f"🏢 **Bases:** {bases}\n"

        if page_label:
            body += f"🖼️ **Imagem:** {page_label}\n"

        # Não exibir a lista de arquivos gerados no envio do Feishu.
        # Caso futuramente queira voltar a exibir, basta fazer
        # montar_arquivos_gerados_md() retornar texto novamente.
        if arquivos_gerados_md and str(arquivos_gerados_md).strip():
            body += "\n" + str(arquivos_gerados_md).strip()

        elements = []
        if image_key:
            elements.append(
                {
                    "tag": "img",
                    "img_key": image_key,
                    "alt": {"tag": "plain_text", "content": "SLA por Base"},
                    "mode": "fit_horizontal",
                    "preview": True,
                }
            )
            elements.append({"tag": "hr"})

        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": body}})

        if LINK_PASTA:
            elements.append({"tag": "hr"})
            elements.append(
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "📂 Abrir Pasta"},
                            "url": LINK_PASTA,
                            "type": "primary",
                        }
                    ],
                }
            )

        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {"template": "red", "title": {"tag": "plain_text", "content": coord}},
                "elements": elements,
            },
        }

        r = requests.post(webhook, json=payload, timeout=25)
        if r.status_code != 200:
            logging.error(f"❌ ERRO ao enviar card para {coord}. Status: {r.status_code}. Resp: {r.text}")
            return False

        logging.info(f"📨 Card enviado para {coord}")
        return True

    except Exception as e:
        logging.error(f"❌ Falha envio card {coord}: {e}")
        return False


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA por competência...")

    try:
        inicio, fim = obter_competencia()
        periodo_txt = formatar_periodo(inicio, fim)

        inicio_ant, fim_ant = obter_competencia_anterior(inicio)
        periodo_anterior_txt = formatar_periodo(inicio_ant, fim_ant)

        logging.info(f"📅 Período atual: {periodo_txt}")
        logging.info(f"📅 Período anterior: {periodo_anterior_txt}")
        logging.info(f"📁 Pasta de saída: {PASTA_SAIDA}")
        logging.info(f"📌 Fonte de dados: {FONTE_DADOS}")

        # =====================================================
        # MODO 1: RESUMO PRONTO
        # =====================================================
        if FONTE_DADOS == "RESUMO_ARQUIVO":
            resumo_geral = ler_resumo_pronto(ARQUIVO_RESUMO)
            resumo_com_coord = anexar_coordenador_no_resumo(resumo_geral, CAMINHO_COORDENADOR)

            sem_coord = resumo_com_coord["COORDENADOR"].isna().sum()
            logging.info(f"🧩 Bases sem coordenador após join: {int(sem_coord)}")

            resumo_por_coord = gerar_resumo_por_coordenador(resumo_com_coord)
            paths_base = exportar_base_consolidada(resumo_geral)
            exportar_resumo_excel(resumo_geral, resumo_por_coord, ARQUIVO_SAIDA)
            arquivos_md = montar_arquivos_gerados_md(ARQUIVO_SAIDA, paths_base)

            for coord, webhook in COORDENADOR_WEBHOOKS.items():
                coord_norm = normalizar(coord)
                sub = resumo_com_coord[resumo_com_coord["COORD_NORM"] == coord_norm].copy()

                if sub.empty:
                    logging.warning(f"⚠️ Nenhum dado encontrado para {coord} na competência.")
                    continue

                resumo_coord = sub[["Base", "Recebido", "Entregue", "SLA"]].copy()
                if "SLA_Anterior" in sub.columns:
                    resumo_coord["SLA_Anterior"] = sub["SLA_Anterior"]
                resumo_coord = resumo_coord.sort_values(by=["SLA", "Base"], ascending=[True, True], na_position="last")

                bases    = int(resumo_coord["Base"].nunique())
                recebido = int(pd.to_numeric(resumo_coord["Recebido"], errors="coerce").fillna(0).sum())
                entregue = int(pd.to_numeric(resumo_coord["Entregue"], errors="coerce").fillna(0).sum())
                sla      = (entregue / recebido) if recebido > 0 else 0.0

                grade_coord, colunas_neutras, subtitulo_base = construir_grade_mensal_simples(
                    resumo_pd=resumo_coord,
                    inicio=inicio,
                    inicio_ant=inicio_ant,
                )

                img_paths = gerar_imagens_grade_analitica(
                    coord=coord,
                    grade_pd=grade_coord,
                    subtitulo_base=subtitulo_base,
                    out_dir=PASTA_IMAGENS,
                    rows_per_page=IMG_ROWS_PER_PAGE,
                    colunas_neutras=colunas_neutras,
                    colunas_domingo=[],
                )

                if img_paths and _feishu_enabled():
                    for i, p in enumerate(img_paths, start=1):
                        try:
                            img_key = feishu_upload_image_get_key(p)
                            enviar_card_feishu(
                                webhook=webhook,
                                coord=coord,
                                periodo_txt=periodo_txt,
                                sla=sla,
                                bases=bases,
                                recebido=recebido,
                                entregue=entregue,
                                arquivos_gerados_md=arquivos_md,
                                image_key=img_key,
                                page_label=f"{i}/{len(img_paths)}",
                            )
                        except Exception as e:
                            logging.error(f"⚠️ Falha no upload/envio da imagem para {coord}: {e}")
                            enviar_card_feishu(
                                webhook=webhook,
                                coord=coord,
                                periodo_txt=periodo_txt,
                                sla=sla,
                                bases=bases,
                                recebido=recebido,
                                entregue=entregue,
                                arquivos_gerados_md=arquivos_md,
                            )
                        time.sleep(0.35)

            logging.info("🏁 Processamento concluído com sucesso.")
            raise SystemExit(0)

        # =====================================================
        # MODO 2: PASTA BRUTA
        # =====================================================
        df = consolidar_planilhas(PASTA_ENTRADA)
        logging.info(f"📥 Registros carregados: {df.height}")

        df = df.rename({c: c.strip().upper() for c in df.columns})

        mostrar_amostra_coluna_data(df, COL_DATA_BASE)
        df = garantir_coluna_data(df, COL_DATA_BASE)
        diagnosticar_coluna_data(df, COL_DATA_BASE)

        min_data = df.select(pl.col(COL_DATA_BASE).min()).item()
        max_data = df.select(pl.col(COL_DATA_BASE).max()).item()

        inicio, fim, competencia_ajustada = ajustar_competencia_pelos_dados(inicio, fim, df, COL_DATA_BASE)
        if competencia_ajustada:
            periodo_txt = formatar_periodo(inicio, fim)
            inicio_ant, fim_ant = obter_competencia_anterior(inicio)
            periodo_anterior_txt = formatar_periodo(inicio_ant, fim_ant)
            logging.info(f"📅 Período atual ajustado: {periodo_txt}")
            logging.info(f"📅 Período anterior ajustado: {periodo_anterior_txt}")

        colunas  = list(df.columns)
        col_upper = [c.upper() for c in colunas]
        possiveis = ["ENTREGUE NO PRAZO?", "ENTREGUE NO PRAZO？"]

        col_entregue = None
        for nome in possiveis:
            if nome in col_upper:
                col_entregue = colunas[col_upper.index(nome)]
                break

        if not col_entregue:
            raise KeyError(f"❌ Coluna ENTREGUE NO PRAZO não encontrada.\nColunas: {df.columns}")

        logging.info(f"📌 Coluna detectada: {col_entregue}")

        df = df.with_columns(
            pl.when(pl.col(col_entregue).cast(pl.Utf8).str.to_uppercase() == "Y")
            .then(1)
            .otherwise(0)
            .alias("_ENTREGUE_PRAZO")
        )

        # ── coordenador ──────────────────────────────────────────────────────
        arquivo_coord = localizar_arquivo_coordenador(CAMINHO_COORDENADOR)
        coord_df = pl.read_excel(arquivo_coord)

        logging.info(f"📎 Base de coordenadores carregada: {arquivo_coord}")
        logging.info(f"📥 Registros base coordenador: {coord_df.height}")

        coord_df = coord_df.rename({c: c.strip() for c in coord_df.columns})

        rename_map = {}
        if "Nome da base" in coord_df.columns:
            rename_map["Nome da base"] = "BASE DE ENTREGA"
        if "NOME DA BASE" in coord_df.columns:
            rename_map["NOME DA BASE"] = "BASE DE ENTREGA"
        if "Coordenadores" in coord_df.columns:
            rename_map["Coordenadores"] = "COORDENADOR"
        if "COORDENADORES" in coord_df.columns:
            rename_map["COORDENADORES"] = "COORDENADOR"
        if "Coordenador" in coord_df.columns and "COORDENADOR" not in rename_map.values():
            rename_map["Coordenador"] = "COORDENADOR"

        if rename_map:
            coord_df = coord_df.rename(rename_map)

        cols_norm = {normalizar(c): c for c in coord_df.columns}

        if "BASE DE ENTREGA" not in coord_df.columns:
            if "NOME DA BASE" in cols_norm:
                coord_df = coord_df.rename({cols_norm["NOME DA BASE"]: "BASE DE ENTREGA"})
            elif "BASE DE ENTREGA" in cols_norm:
                coord_df = coord_df.rename({cols_norm["BASE DE ENTREGA"]: "BASE DE ENTREGA"})

        if "COORDENADOR" not in coord_df.columns:
            if "COORDENADORES" in cols_norm:
                coord_df = coord_df.rename({cols_norm["COORDENADORES"]: "COORDENADOR"})
            elif "COORDENADOR" in cols_norm:
                coord_df = coord_df.rename({cols_norm["COORDENADOR"]: "COORDENADOR"})

        if "BASE DE ENTREGA" not in coord_df.columns or "COORDENADOR" not in coord_df.columns:
            raise KeyError(
                f"❌ O arquivo de coordenador precisa ter 'BASE DE ENTREGA' e 'COORDENADOR'. "
                f"Colunas encontradas: {coord_df.columns}"
            )

        df = df.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar, return_dtype=pl.Utf8).alias("BASE_NORM")
        )
        coord_df = coord_df.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar, return_dtype=pl.Utf8).alias("BASE_NORM")
        )

        coord_df = coord_df.unique(subset=["BASE_NORM"], keep="first")

        df_com_coord = (
            df.join(
                coord_df.select(["BASE_NORM", "COORDENADOR"]),
                on="BASE_NORM",
                how="left",
            )
            .with_columns(
                pl.when(pl.col("COORDENADOR").is_not_null())
                .then(pl.col("COORDENADOR").map_elements(normalizar, return_dtype=pl.Utf8))
                .otherwise(None)
                .alias("COORD_NORM")
            )
        )

        # ── SLA do mês anterior (lido da pasta, não dos dados brutos) ─────────
        sla_anterior_df = ler_sla_mes_anterior_da_pasta(PASTA_MES_ANTERIOR, inicio_ant)

        # ── filtro competência atual ──────────────────────────────────────────
        ano_ref = inicio.year
        mes_ref = inicio.month

        df_periodo = df_com_coord.filter(
            pl.col(COL_DATA_BASE).is_not_null()
            & (pl.col(COL_DATA_BASE).dt.year() == ano_ref)
            & (pl.col(COL_DATA_BASE).dt.month() == mes_ref)
        )

        df_periodo, df_removidos = separar_pedidos_removidos(df_periodo)
        caminho_planilha_removidos = exportar_planilha_pedidos_removidos(df_removidos)

        logging.info(f"📊 Registros da competência após filtro: {df_periodo.height}")

        if caminho_planilha_removidos:
            logging.info(f"📎 Planilha separada dos pedidos removidos: {caminho_planilha_removidos}")

        if df_periodo.is_empty():
            raise ValueError(
                f"Não há dados para a competência selecionada ({periodo_txt}). "
                f"Faixa encontrada na coluna {COL_DATA_BASE}: {min_data} a {max_data}."
            )

        resumo_geral = (
            df_periodo.group_by("BASE DE ENTREGA")
            .agg(
                [
                    pl.len().alias("Recebido"),
                    pl.col("_ENTREGUE_PRAZO").sum().alias("Entregue"),
                ]
            )
            .with_columns(
                pl.when(pl.col("Recebido") > 0)
                .then(pl.col("Entregue") / pl.col("Recebido"))
                .otherwise(0.0)
                .alias("SLA")
            )
            .sort("BASE DE ENTREGA")
            .to_pandas()
            .rename(columns={"BASE DE ENTREGA": "Base"})
        )

        resumo_geral = resumo_geral[["Base", "Recebido", "Entregue", "SLA"]]

        # ── join com SLA anterior (do arquivo da pasta) ───────────────────────
        if not sla_anterior_df.empty:
            sla_anterior_df["BASE_NORM_ANT"] = sla_anterior_df["Base"].astype(str).map(normalizar)
            resumo_geral["BASE_NORM_ANT"]    = resumo_geral["Base"].astype(str).map(normalizar)
            resumo_geral = resumo_geral.merge(
                sla_anterior_df[["BASE_NORM_ANT", "SLA_Anterior"]],
                on="BASE_NORM_ANT",
                how="left",
            ).drop(columns=["BASE_NORM_ANT"])
            logging.info("✅ SLA do mês anterior vinculado ao resumo geral.")
        else:
            resumo_geral["SLA_Anterior"] = float("nan")

        resumo_com_coord = anexar_coordenador_no_resumo(resumo_geral, CAMINHO_COORDENADOR)

        sem_coord = resumo_com_coord["COORDENADOR"].isna().sum()
        logging.info(f"🧩 Bases sem coordenador após join: {int(sem_coord)}")

        resumo_por_coord = gerar_resumo_por_coordenador(resumo_com_coord)
        paths_base       = exportar_base_consolidada(resumo_geral)
        exportar_resumo_excel(resumo_geral, resumo_por_coord, ARQUIVO_SAIDA)
        arquivos_md      = montar_arquivos_gerados_md(ARQUIVO_SAIDA, paths_base)

        analitico_comp = preparar_analitico_competencia(
            df_periodo=df_periodo,
            col_data_base=COL_DATA_BASE,
            ultimos_dias=7,
        )

        for coord, webhook in COORDENADOR_WEBHOOKS.items():
            coord_norm = normalizar(coord)
            sub = resumo_com_coord[resumo_com_coord["COORD_NORM"] == coord_norm].copy()

            if sub.empty:
                logging.warning(f"⚠️ Nenhum dado encontrado para {coord} na competência.")
                continue

            colunas_coord = ["Base", "Recebido", "Entregue", "SLA"]
            if "SLA_Anterior" in sub.columns:
                colunas_coord.append("SLA_Anterior")

            resumo_coord = sub[colunas_coord].copy()
            resumo_coord = resumo_coord.sort_values(by=["SLA", "Base"], ascending=[True, True], na_position="last")

            bases    = int(resumo_coord["Base"].nunique())
            recebido = int(pd.to_numeric(resumo_coord["Recebido"], errors="coerce").fillna(0).sum())
            entregue = int(pd.to_numeric(resumo_coord["Entregue"], errors="coerce").fillna(0).sum())
            sla      = (entregue / recebido) if recebido > 0 else 0.0

            # SLA anterior consolidado do coordenador (média simples das bases)
            sla_ant_coord: Optional[float] = None
            if "SLA_Anterior" in resumo_coord.columns:
                vals_ant = pd.to_numeric(resumo_coord["SLA_Anterior"], errors="coerce").dropna()
                if not vals_ant.empty:
                    sla_ant_coord = float(vals_ant.mean())

            grade_coord, colunas_neutras, subtitulo_base = construir_grade_analitica_coord(
                coord=coord,
                resumo_com_coord=resumo_com_coord,
                analitico=analitico_comp,
                inicio=inicio,
                inicio_ant=inicio_ant,
            )

            if grade_coord.empty:
                grade_coord, colunas_neutras, subtitulo_base = construir_grade_mensal_simples(
                    resumo_pd=resumo_coord,
                    inicio=inicio,
                    inicio_ant=inicio_ant,
                )

            img_paths = gerar_imagens_grade_analitica(
                coord=coord,
                grade_pd=grade_coord,
                subtitulo_base=subtitulo_base,
                out_dir=PASTA_IMAGENS,
                rows_per_page=IMG_ROWS_PER_PAGE,
                colunas_neutras=colunas_neutras,
                colunas_domingo=list(
                    analitico_comp.get(
                        "special_day_labels",
                        analitico_comp.get("sunday_labels", []),
                    )
                ),
            )

            if img_paths and _feishu_enabled():
                for i, p in enumerate(img_paths, start=1):
                    try:
                        img_key = feishu_upload_image_get_key(p)
                        enviar_card_feishu(
                            webhook=webhook,
                            coord=coord,
                            periodo_txt=periodo_txt,
                            sla=sla,
                            bases=bases,
                            recebido=recebido,
                            entregue=entregue,
                            arquivos_gerados_md=arquivos_md,
                            image_key=img_key,
                            page_label=f"{i}/{len(img_paths)}",
                            sla_anterior=sla_ant_coord,
                            periodo_anterior_txt=periodo_anterior_txt,
                        )
                    except Exception as e:
                        logging.error(f"⚠️ Falha no upload/envio da imagem para {coord}: {e}")
                        enviar_card_feishu(
                            webhook=webhook,
                            coord=coord,
                            periodo_txt=periodo_txt,
                            sla=sla,
                            bases=bases,
                            recebido=recebido,
                            entregue=entregue,
                            arquivos_gerados_md=arquivos_md,
                            sla_anterior=sla_ant_coord,
                            periodo_anterior_txt=periodo_anterior_txt,
                        )
                    time.sleep(0.35)

        logging.info("🏁 Processamento concluído com sucesso.")

    except SystemExit:
        raise
    except Exception as e:
        logging.critical(f"❌ ERRO FATAL: {e}", exc_info=True)
        raise
