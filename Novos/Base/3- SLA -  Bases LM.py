# -*- coding: utf-8 -*-

# =========================
# BLOCO 1/4 — IMPORTS / CONFIG
# =========================

import os
import requests
import warnings
import polars as pl
import pandas as pd
import multiprocessing
import logging
import shutil
import unicodedata
import time
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple, Dict, Set, Any

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("../sla_processor.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

# ============================================================
# Caminhos
# ============================================================
PASTA_ENTRADA = r"C:\Users\mathe_70oz1qs\OneDrive\Desktop\Testes\03 - SLA - Entrega Realizada LM"

# ✅ Pode ser PASTA ou ARQUIVO
# Exemplo pasta:
CAMINHO_COORDENADOR = r"C:\Users\mathe_70oz1qs\OneDrive\Desktop\Testes\01 - Coordenador"

# Exemplo arquivo direto:
# CAMINHO_COORDENADOR = r"C:\Users\mathe_70oz1qs\OneDrive\Desktop\Testes\01 - Coordenador\Base_Atualizada.xlsx"

PASTA_SAIDA = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\SLA - Coordenadores LM"

# Arquivo morto (para relatórios e bases antigas)
PASTA_ARQUIVO = os.path.join(PASTA_SAIDA, "Arquivo Morto")

# pasta específica para base consolidada (original + alterações)
PASTA_BASE_CONSOLIDADA = os.path.join(PASTA_SAIDA, "Base Consolidada")

# pasta para imagens por coordenador
PASTA_IMAGENS = os.path.join(PASTA_SAIDA, "Imagens_Coordenadores_SLA")

DATA_HOJE = datetime.now().strftime("%Y%m%d")

# Resumo principal (Seg–Sáb)
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_{DATA_HOJE}.xlsx")

# Resumo Domingo (se existir)
ARQUIVO_SAIDA_DOMINGO = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_Domingo_{DATA_HOJE}.xlsx")

# Limite de linhas do Excel
EXCEL_MAX_ROWS = 1_048_576

LINK_PASTA = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/IgCBcizkJxWcTYzIEL35TMQtAap5Mm22qqaWrAoK6sSlijs?e=5aj7RG"
)

# ============================================================
# 🏷️ NOME DO INDICADOR (VAI APARECER NA IMAGEM E NO CARD)
# ============================================================
INDICADOR_NOME = "SLA Entrega Realizada — %SLA por Base (pior → melhor)"

# ✅ COLE AQUI SEUS WEBHOOKS ATUAIS
COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/3663dd30-722c-45d6-9e3c-1d4e2838f112",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/0b907801-c73e-4de8-9f84-682d7b54f6fd",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/48c4db73-b5a4-4007-96af-f5d28301f0c1",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/b749fd36-d287-460e-b1e2-c78bfb4c1946",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/606ed22b-dc49-451d-9bfe-0a8829dbe76e",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/840f79b0-1eff-42fe-aae0-433c9edbad80",
    "Fabio Souza": "https://open.feishu.cn/open-apis/bot/v2/hook/ca2c260c-f69c-472d-9757-279db52a79b8",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/63751a67-efe8-40e4-b841-b290a4819836",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/3ddc5962-2d32-4b2d-92d9-a4bc95ac3393",
    "Ana Cunha": "https://open.feishu.cn/open-apis/bot/v2/hook/b2ec868f-3149-4808-af53-9e0c6d2cd94e",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/a53ad30e-17dd-4330-93db-15138b20d8f2", }


EXTS = (".xlsx", ".xls", ".csv")
COL_DATA_BASE = "DATA PREVISTA DE ENTREGA"

# ============================================================
# ✅ Controle de feriados nacionais
# ============================================================
PULAR_FERIADOS_NACIONAIS = True
PULAR_FERIADOS_EM_FDS = False
_CACHE_FERIADOS: Dict[int, Set[date]] = {}

# ============================================================
# ✅ FEISHU (UPLOAD DE IMAGEM)
# ============================================================
FEISHU_BASE_DOMAIN = "https://open.feishu.cn"
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "cli_a906d2d682f8dbd8").strip()
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "Fzh1cr6K55a3oQUBV9wCZd6AWiZH5ONw").strip()

# quantas linhas por página na imagem
IMG_ROWS_PER_PAGE = int(os.getenv("IMG_ROWS_PER_PAGE", "22"))

# cache token
_TOKEN_CACHE = {"token": None, "exp": 0}

# ============================================================
# 🎨 PALETA J&T (RGB)
# ============================================================
JT_RED_MAIN = (227, 6, 19)      # #E30613
JT_RED_SOFT = (196, 39, 46)     # #C4272E
JT_BG_GRAY  = (242, 242, 242)   # #F2F2F2
JT_TEXT     = (51, 51, 51)      # #333333
JT_WHITE    = (255, 255, 255)   # #FFFFFF

JT_STROKE   = (220, 220, 220)
JT_MUTED    = (110, 110, 110)
JT_ROW_ALT  = (248, 248, 248)

# ============================================================
# HTTP (retry simples)
# ============================================================
def _post_with_retry(url: str, json_payload: dict, timeout: int = 25, tries: int = 7) -> requests.Response:
    last = None
    for i in range(1, tries + 1):
        try:
            return requests.post(url, json=json_payload, timeout=timeout)
        except Exception as e:
            last = e
            time.sleep(0.7 * i)
    raise RuntimeError(f"Falha POST {url} após {tries} tentativas. Último erro: {last}")


def _post_multipart_with_retry(url: str, data: dict, files: dict, headers: dict, timeout: int = 90, tries: int = 7) -> requests.Response:
    last = None
    for i in range(1, tries + 1):
        try:
            return requests.post(url, data=data, files=files, headers=headers, timeout=timeout)
        except Exception as e:
            last = e
            time.sleep(0.7 * i)
    raise RuntimeError(f"Falha UPLOAD {url} após {tries} tentativas. Último erro: {last}")
# =========================
# BLOCO 2/4 — FUNÇÕES (FERIADOS / PERÍODO / LEITURA / EXPORT / RESUMO)
# =========================

def normalizar(s) -> str:
    if s is None:
        return ""
    s = str(s).upper().strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    while "  " in s:
        s = s.replace("  ", " ")
    return s


def localizar_arquivo_coordenador(caminho: str) -> str:
    """
    Aceita:
    - caminho direto para arquivo .xlsx/.xls
    - caminho para pasta contendo o arquivo de coordenador

    Prioriza nomes como:
    - Base_Atualizada
    - Coordenador
    - Base
    - Mapeamento

    Se houver vários, pega o mais prioritário e mais recente.
    """
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
        # mais recente primeiro
        return (idx, -os.path.getmtime(p), os.path.basename(p).lower())

    arquivos.sort(key=prioridade_arquivo)
    escolhido = arquivos[0]

    logging.info(f"📎 Arquivo de coordenador localizado automaticamente: {escolhido}")
    return escolhido


def pascoa_gregoriana(ano: int) -> date:
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


def feriados_nacionais_br(ano: int) -> Set[date]:
    fer = {
        date(ano, 1, 1),
        date(ano, 4, 21),
        date(ano, 5, 1),
        date(ano, 9, 7),
        date(ano, 10, 12),
        date(ano, 11, 2),
        date(ano, 11, 15),
        date(ano, 11, 20),
        date(ano, 12, 25),
    }
    pascoa = pascoa_gregoriana(ano)
    fer.add(pascoa - timedelta(days=2))  # Sexta-feira Santa
    return fer


def is_feriado_nacional(d: date) -> bool:
    if not PULAR_FERIADOS_NACIONAIS:
        return False
    if (not PULAR_FERIADOS_EM_FDS) and (d.weekday() in (5, 6)):
        return False
    ano = d.year
    if ano not in _CACHE_FERIADOS:
        _CACHE_FERIADOS[ano] = feriados_nacionais_br(ano)
    return d in _CACHE_FERIADOS[ano]


def formatar_periodo(inicio: date, fim: date) -> str:
    if inicio == fim:
        return inicio.strftime("%d/%m/%Y")
    return f"{inicio.strftime('%d/%m/%Y')} a {fim.strftime('%d/%m/%Y')}"


def formatar_lista_dias(datas: List[date]) -> str:
    if not datas:
        return "-"
    dias_pt = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    return ", ".join([f"{dias_pt[d.weekday()]} {d.strftime('%d/%m')}" for d in datas])


def periodo_txt_de_datas(datas: List[date]) -> str:
    if not datas:
        return "-"
    return formatar_periodo(min(datas), max(datas))


def separar_seg_sab_e_domingo(datas: List[date]) -> Tuple[List[date], List[date]]:
    datas_dom = [d for d in datas if d.weekday() == 6]
    datas_seg_sab = [d for d in datas if d.weekday() != 6]
    return datas_seg_sab, datas_dom


def calcular_periodo_base() -> Optional[Tuple[date, date, List[date]]]:
    hoje = datetime.now().date()
    dia = hoje.weekday()  # 0=Seg ... 6=Dom

    if dia in (5, 6):
        logging.warning("⛔ Hoje é sábado ou domingo. Execução cancelada.")
        return None

    span = 3 if dia == 0 else 1
    fim = hoje - timedelta(days=1)

    tentativas = 0
    while True:
        inicio = fim - timedelta(days=span - 1)
        datas = [inicio + timedelta(days=i) for i in range((fim - inicio).days + 1)]

        if PULAR_FERIADOS_NACIONAIS:
            feriados_removidos = [d for d in datas if is_feriado_nacional(d)]
            datas_ok = [d for d in datas if not is_feriado_nacional(d)]
            if feriados_removidos:
                logging.info(
                    "🗓️ Feriados nacionais ignorados: "
                    + ", ".join([d.strftime("%Y-%m-%d") for d in feriados_removidos])
                )
        else:
            datas_ok = datas

        if datas_ok:
            return min(datas_ok), max(datas_ok), datas_ok

        tentativas += 1
        if tentativas >= 15:
            logging.warning("⚠️ Não foi possível encontrar datas válidas após recuar 15 dias. Cancelando.")
            return None

        logging.warning(f"⚠️ Período ({formatar_periodo(inicio, fim)}) vazio após remover feriados. Recuando 1 dia...")
        fim = fim - timedelta(days=1)


def arquivar_relatorios_antigos(pasta_origem: str, pasta_destino: str, prefixo: str, excluir_contains: Optional[str] = None) -> None:
    os.makedirs(pasta_destino, exist_ok=True)
    if not os.path.isdir(pasta_origem):
        return
    for arquivo in os.listdir(pasta_origem):
        if not (arquivo.startswith(prefixo) and arquivo.endswith(".xlsx")):
            continue
        if excluir_contains and (excluir_contains.lower() in arquivo.lower()):
            continue
        try:
            shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo))
            logging.info(f"📦 Arquivo antigo movido: {arquivo}")
        except Exception as e:
            logging.error(f"Erro ao mover {arquivo}: {e}")


def arquivar_bases_antigas(pasta_origem: str, pasta_destino: str, prefixo: str, excluir_contains: Optional[str] = None) -> None:
    os.makedirs(pasta_destino, exist_ok=True)
    if not os.path.isdir(pasta_origem):
        return

    for arquivo in os.listdir(pasta_origem):
        if not arquivo.startswith(prefixo):
            continue
        if excluir_contains and (excluir_contains.lower() in arquivo.lower()):
            continue
        if not arquivo.lower().endswith((".xlsx", ".csv", ".parquet")):
            continue
        try:
            shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo))
            logging.info(f"📦 Base antiga movida: {arquivo}")
        except Exception as e:
            logging.error(f"Erro ao mover {arquivo}: {e}")


def ler_planilha_rapido(caminho: str) -> pl.DataFrame:
    try:
        if caminho.lower().endswith(".csv"):
            return pl.read_csv(caminho, ignore_errors=True)
        return pl.read_excel(caminho)
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
        raise FileNotFoundError("Nenhum arquivo válido encontrado.")

    with ThreadPoolExecutor(max_workers=min(16, len(arquivos))) as ex:
        dfs = list(ex.map(ler_planilha_rapido, arquivos))

    validos = [df for df in dfs if not df.is_empty()]
    if not validos:
        raise ValueError("Falha ao ler todos os arquivos.")
    return pl.concat(validos, how="vertical_relaxed")


def garantir_coluna_data(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    if coluna not in df.columns:
        raise KeyError(f"Coluna '{coluna}' não encontrada.")

    tipo = df[coluna].dtype
    if tipo == pl.Date:
        return df
    if tipo == pl.Datetime:
        return df.with_columns(pl.col(coluna).dt.date().alias(coluna))

    if tipo == pl.Utf8:
        s = pl.col(coluna).cast(pl.Utf8).str.strip_chars().str.replace_all(r"\s+", " ")
        formatos = [
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
        ]
        expr = None
        for f in formatos:
            tentativa = s.str.strptime(pl.Datetime, f, strict=False)
            expr = tentativa if expr is None else expr.fill_null(tentativa)
        return df.with_columns(expr.dt.date().alias(coluna))

    raise TypeError(f"Tipo inválido para coluna '{coluna}': {tipo}")


def ajustar_periodo_por_dados(
    df: pl.DataFrame, coluna_data: str, inicio: date, fim: date, datas: List[date]
) -> Tuple[date, date, List[date]]:
    if df.is_empty() or coluna_data not in df.columns:
        return inicio, fim, datas

    try:
        qtd = df.filter(pl.col(coluna_data).is_in(datas)).height
        if qtd > 0:
            return inicio, fim, datas
    except Exception:
        pass

    max_le = None
    try:
        max_le = (
            df.filter(pl.col(coluna_data).is_not_null() & (pl.col(coluna_data) <= fim))
            .select(pl.col(coluna_data).max())
            .item()
        )
    except Exception:
        max_le = None

    if max_le is None:
        try:
            max_le = df.filter(pl.col(coluna_data).is_not_null()).select(pl.col(coluna_data).max()).item()
        except Exception:
            max_le = None

    if max_le is None:
        return inicio, fim, datas

    if isinstance(max_le, datetime):
        max_le = max_le.date()

    span = (fim - inicio).days
    novo_fim = max_le
    novo_inicio = novo_fim - timedelta(days=span)
    if novo_inicio > novo_fim:
        novo_inicio = novo_fim

    novo_datas = [novo_inicio + timedelta(days=i) for i in range((novo_fim - novo_inicio).days + 1)]

    logging.warning(
        f"⚠️ Nenhum registro para o período calculado ({formatar_periodo(inicio, fim)}). "
        f"Fallback para última data disponível: {formatar_periodo(novo_inicio, novo_fim)}."
    )
    return novo_inicio, novo_fim, novo_datas


def exportar_base_consolidada(df_periodo: pl.DataFrame, tag: str = "") -> Dict[str, str]:
    os.makedirs(PASTA_BASE_CONSOLIDADA, exist_ok=True)

    if tag == "_Domingo":
        prefixo = "Base_Consolidada_Domingo_"
        nome_base = f"Base_Consolidada_Domingo_{DATA_HOJE}"
        excluir_contains = None
    else:
        prefixo = "Base_Consolidada_"
        nome_base = f"Base_Consolidada_{DATA_HOJE}"
        excluir_contains = "Domingo"

    arq_parquet = os.path.join(PASTA_BASE_CONSOLIDADA, f"{nome_base}.parquet")
    arq_csv = os.path.join(PASTA_BASE_CONSOLIDADA, f"{nome_base}.csv")
    arq_xlsx = os.path.join(PASTA_BASE_CONSOLIDADA, f"{nome_base}.xlsx")

    arquivar_bases_antigas(PASTA_BASE_CONSOLIDADA, PASTA_ARQUIVO, prefixo, excluir_contains=excluir_contains)

    df_periodo.write_parquet(arq_parquet)
    logging.info(f"✅ Base consolidada (PARQUET) salva em: {arq_parquet}")

    df_periodo.write_csv(arq_csv)
    logging.info(f"✅ Base consolidada (CSV) salva em: {arq_csv}")

    if df_periodo.height <= (EXCEL_MAX_ROWS - 1):
        df_pd = df_periodo.to_pandas()
        with pd.ExcelWriter(arq_xlsx, engine="openpyxl") as w:
            df_pd.to_excel(w, index=False, sheet_name="Base Consolidada")
        logging.info(f"✅ Base consolidada (XLSX) salva em: {arq_xlsx}")
    else:
        logging.warning("⚠️ XLSX não gerado (limite do Excel). Use PARQUET/CSV.")

    return {"parquet": arq_parquet, "csv": arq_csv, "xlsx": arq_xlsx}


def exportar_resumo_excel(resumo_pd: pd.DataFrame, arquivo_saida: str, prefixo: str, excluir_contains: Optional[str] = None) -> None:
    os.makedirs(PASTA_SAIDA, exist_ok=True)
    arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, prefixo, excluir_contains=excluir_contains)
    with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as w:
        resumo_pd.to_excel(w, index=False, sheet_name="Resumo SLA")
    logging.info(f"✅ Resumo Excel salvo em: {arquivo_saida}")


def montar_arquivos_gerados_md(arquivo_resumo: str, paths_base: Dict[str, str]) -> str:
    base_xlsx_txt = (
        f"- Base (XLSX): `{os.path.basename(paths_base['xlsx'])}`\n"
        if os.path.exists(paths_base["xlsx"])
        else "- Base (XLSX): *(não gerado — limite do Excel)*\n"
    )
    return (
        "📄 **Arquivos gerados:**\n"
        f"- Resumo: `{os.path.basename(arquivo_resumo)}`\n"
        f"- Base (PARQUET): `{os.path.basename(paths_base['parquet'])}`\n"
        f"- Base (CSV): `{os.path.basename(paths_base['csv'])}`\n"
        + base_xlsx_txt
    )


def gerar_resumo_por_base(df_periodo: pl.DataFrame) -> pd.DataFrame:
    if df_periodo.is_empty():
        return pd.DataFrame(
            columns=["Base De Entrega", "COORDENADOR", "Total", "Entregues no Prazo", "Fora do Prazo", "% SLA Cumprido"]
        )

    resumo = (
        df_periodo.group_by(["BASE DE ENTREGA", "COORDENADOR"])
        .agg(
            [
                pl.len().alias("Total"),
                pl.col("_ENTREGUE_PRAZO").sum().alias("Entregues no Prazo"),
                (pl.len() - pl.col("_ENTREGUE_PRAZO").sum()).alias("Fora do Prazo"),
                (pl.col("_ENTREGUE_PRAZO").sum() / pl.len()).alias("% SLA Cumprido"),
            ]
        )
        .sort("% SLA Cumprido", descending=True)
    )
    return resumo.to_pandas().rename(columns={"BASE DE ENTREGA": "Base De Entrega"})
# =========================
# BLOCO 3/4 — FEISHU + IMAGEM (PIL) + CARD
# =========================

def _feishu_enabled() -> bool:
    return bool(FEISHU_APP_ID and FEISHU_APP_SECRET)


def feishu_get_token() -> str:
    if not _feishu_enabled():
        raise RuntimeError("Defina FEISHU_APP_ID e FEISHU_APP_SECRET (env) para enviar imagens.")

    now = int(time.time())
    if _TOKEN_CACHE["token"] and now < int(_TOKEN_CACHE["exp"]):
        return _TOKEN_CACHE["token"]

    url = f"{FEISHU_BASE_DOMAIN}/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
    r = _post_with_retry(url, payload, timeout=25)
    data = r.json() if r.content else {}

    if data.get("code") != 0:
        raise RuntimeError(f"Token Feishu falhou: {data}")

    token = data.get("tenant_access_token")
    exp = int(data.get("expire", 0))
    if not token:
        raise RuntimeError(f"Resposta sem tenant_access_token: {data}")

    _TOKEN_CACHE["token"] = token
    _TOKEN_CACHE["exp"] = now + max(0, exp - 60)
    return token


def feishu_upload_image_get_key(image_path: str) -> str:
    token = feishu_get_token()
    url = f"{FEISHU_BASE_DOMAIN}/open-apis/im/v1/images"
    headers = {"Authorization": f"Bearer {token}"}

    with open(image_path, "rb") as f:
        r = _post_multipart_with_retry(
            url,
            data={"image_type": "message"},
            files={"image": (os.path.basename(image_path), f)},
            headers=headers,
            timeout=90,
        )

    data = r.json() if r.content else {}
    if data.get("code") != 0:
        if data.get("code") == 234007:
            raise RuntimeError(
                "Upload falhou (234007): seu APP não está com BOT habilitado.\n"
                "Feishu Dev Console: Add Features > Bot (Add) e publique a versão (Test).\n"
                "Permissão típica: im:resource (upload image)."
            )
        raise RuntimeError(f"Upload imagem falhou: {data}")

    image_key = (data.get("data") or {}).get("image_key")
    if not image_key:
        raise RuntimeError(f"Upload OK mas sem image_key: {data}")
    return image_key


def _chunk(items: List[Any], n: int) -> List[List[Any]]:
    return [items[i:i + n] for i in range(0, len(items), n)]


def gerar_imagens_sla_tabela(
    coord: str,
    indicador_nome: str,
    titulo_suffix: str,
    periodo_txt: str,
    dias_txt: str,
    sub: pd.DataFrame,
    sla_total: float,
    out_dir: str,
    rows_per_page: int = 22,
    file_suffix: str = "",
) -> List[str]:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        raise RuntimeError("Falta Pillow. Instale: pip install pillow")

    os.makedirs(out_dir, exist_ok=True)

    sub2 = sub.copy()
    sub2 = sub2.sort_values("% SLA Cumprido", ascending=True)

    rows = []
    for _, r in sub2.iterrows():
        base = str(r.get("Base De Entrega", "")).strip()
        tot = int(float(r.get("Total", 0) or 0))
        ent = int(float(r.get("Entregues no Prazo", 0) or 0))
        fora = int(float(r.get("Fora do Prazo", 0) or 0))
        sla = float(r.get("% SLA Cumprido", 0) or 0)
        rows.append((base, tot, ent, fora, sla))

    pages = _chunk(rows, rows_per_page)
    if not pages:
        return []

    def load_font(size: int, bold: bool = False):
        candidates = [
            ("segoeuib.ttf" if bold else "segoeui.ttf"),
            ("arialbd.ttf" if bold else "arial.ttf"),
            ("calibrib.ttf" if bold else "calibri.ttf"),
        ]
        for name in candidates:
            try:
                return ImageFont.truetype(name, size)
            except Exception:
                continue
        return ImageFont.load_default()

    def rr(draw: ImageDraw.ImageDraw, xy, r, fill, outline=None, width=1):
        try:
            draw.rounded_rectangle(xy, radius=r, fill=fill, outline=outline, width=width)
        except Exception:
            draw.rectangle(xy, fill=fill, outline=outline, width=width)

    def _measure(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> Tuple[int, int]:
        text = text or ""
        try:
            b = draw.textbbox((0, 0), text, font=font)
            return int(b[2] - b[0]), int(b[3] - b[1])
        except Exception:
            try:
                w, h = draw.textsize(text, font=font)  # type: ignore[attr-defined]
                return int(w), int(h)
            except Exception:
                return int(len(text) * 8), 18

    def _ellipsize(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_w: int) -> str:
        text = text or ""
        w, _ = _measure(draw, text, font)
        if w <= max_w:
            return text
        ell = "…"
        lo, hi = 0, len(text)
        best = ell
        while lo <= hi:
            mid = (lo + hi) // 2
            cand = (text[:mid].rstrip() + ell)
            if _measure(draw, cand, font)[0] <= max_w:
                best = cand
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    def _fit_font(draw: ImageDraw.ImageDraw, text: str, start_size: int, min_size: int, bold: bool, max_w: int):
        size = start_size
        while size >= min_size:
            f = load_font(size, bold=bold)
            if _measure(draw, text, f)[0] <= max_w:
                return f
            size -= 1
        return load_font(min_size, bold=bold)

    def _wrap_lines(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_w: int, max_lines: int = 2) -> List[str]:
        text = (text or "").strip()
        if not text:
            return [""]

        words = text.split()
        lines: List[str] = []
        cur = ""

        for w in words:
            cand = (cur + " " + w).strip() if cur else w
            if _measure(draw, cand, font)[0] <= max_w:
                cur = cand
            else:
                if cur:
                    lines.append(cur)
                cur = w
                if len(lines) >= max_lines - 1:
                    break

        if cur:
            lines.append(cur)

        if len(lines) > max_lines:
            lines = lines[:max_lines]

        if lines:
            lines[-1] = _ellipsize(draw, lines[-1], font, max_w)

        return lines

    BG = JT_BG_GRAY
    CARD = JT_WHITE
    STROKE = JT_STROKE
    TXT = JT_TEXT
    MUTED = JT_MUTED
    ROW1 = JT_WHITE
    ROW2 = JT_ROW_ALT

    W = 1800
    pad = 34

    header_h = 205
    row_h = 52
    gap = 18

    f_head = load_font(19, bold=True)
    f_row = load_font(19, bold=False)

    out_paths: List[str] = []
    total_pages = len(pages)
    data_humana = datetime.now().strftime("%d/%m/%Y %H:%M")

    indicador_nome = (indicador_nome or "").strip() or "SLA Entrega Realizada"

    for page_idx, page_rows in enumerate(pages, start=1):
        table_h = 130 + (len(page_rows) * row_h) + 40
        H = pad * 2 + header_h + gap + table_h

        img = Image.new("RGB", (W, H), BG)
        draw = ImageDraw.Draw(img)

        rr(draw, (pad, pad, W - pad, H - pad), 26, CARD, outline=STROKE, width=2)

        hx1, hy1 = pad + 18, pad + 18
        hx2, hy2 = W - pad - 18, pad + header_h

        for i in range(hy2 - hy1):
            t = i / max(1, (hy2 - hy1))
            c = (
                int(JT_RED_SOFT[0] + (JT_RED_MAIN[0] - JT_RED_SOFT[0]) * t),
                int(JT_RED_SOFT[1] + (JT_RED_MAIN[1] - JT_RED_SOFT[1]) * t),
                int(JT_RED_SOFT[2] + (JT_RED_MAIN[2] - JT_RED_SOFT[2]) * t),
            )
            draw.line([(hx1, hy1 + i), (hx2, hy1 + i)], fill=c)

        left = hx1 + 22
        inner_w = (hx2 - hx1) - 44
        y = hy1 + 12

        title = f"{coord}{titulo_suffix}".strip()
        f_title_fit = _fit_font(draw, title, start_size=34, min_size=20, bold=True, max_w=inner_w)
        title = _ellipsize(draw, title, f_title_fit, inner_w)
        draw.text((left, y), title, fill=JT_WHITE, font=f_title_fit)
        y += _measure(draw, title, f_title_fit)[1] + 8

        indicador_full = f"Indicador: {indicador_nome}".strip()
        f_ind_fit = _fit_font(draw, indicador_full, start_size=19, min_size=14, bold=True, max_w=inner_w)
        ind_lines = _wrap_lines(draw, indicador_full, f_ind_fit, inner_w, max_lines=2)
        for line in ind_lines:
            draw.text((left, y), line, fill=JT_WHITE, font=f_ind_fit)
            y += _measure(draw, line, f_ind_fit)[1] + 2
        y += 4

        line_atual = f"Atualizado: {data_humana}   •   Página {page_idx}/{total_pages}   •   SLA total: {sla_total:.2%}"
        f_line_fit = _fit_font(draw, line_atual, start_size=19, min_size=13, bold=False, max_w=inner_w)
        line_atual = _ellipsize(draw, line_atual, f_line_fit, inner_w)
        draw.text((left, y), line_atual, fill=JT_WHITE, font=f_line_fit)
        y += _measure(draw, line_atual, f_line_fit)[1] + 4

        line_periodo = f"Período: {periodo_txt}   •   Dias: {dias_txt}"
        f_per_fit = _fit_font(draw, line_periodo, start_size=19, min_size=13, bold=False, max_w=inner_w)
        line_periodo = _ellipsize(draw, line_periodo, f_per_fit, inner_w)
        draw.text((left, y), line_periodo, fill=JT_WHITE, font=f_per_fit)

        tx1 = pad + 18
        ty1 = hy2 + gap
        tx2 = W - pad - 18
        rr(draw, (tx1, ty1, tx2, H - pad - 18), 20, JT_WHITE, outline=STROKE, width=2)

        draw.text((tx1 + 18, ty1 + 14), "Todas as bases — %SLA (pior → melhor)", fill=TXT, font=f_head)
        draw.line((tx1 + 12, ty1 + 52, tx2 - 12, ty1 + 52), fill=STROKE, width=2)

        col_rank = tx1 + 18
        col_base = tx1 + 90
        col_total = tx2 - 560
        col_ent = tx2 - 420
        col_fora = tx2 - 290
        col_sla_right = tx2 - 22

        draw.text((col_rank, ty1 + 64), "#", fill=MUTED, font=f_head)
        draw.text((col_base, ty1 + 64), "Base", fill=MUTED, font=f_head)
        draw.text((col_total, ty1 + 64), "Total", fill=MUTED, font=f_head)
        draw.text((col_ent, ty1 + 64), "No Prazo", fill=MUTED, font=f_head)
        draw.text((col_fora, ty1 + 64), "Fora", fill=MUTED, font=f_head)

        sla_head = "%SLA"
        bbox_h = draw.textbbox((0, 0), sla_head, font=f_head)
        draw.text((col_sla_right - (bbox_h[2] - bbox_h[0]), ty1 + 64), sla_head, fill=MUTED, font=f_head)

        ytbl = ty1 + 102
        start_rank = (page_idx - 1) * rows_per_page

        for i, (base, tot, ent, fora, sla) in enumerate(page_rows, start=1):
            bg_row = ROW1 if (i % 2 == 1) else ROW2
            rr(draw, (tx1 + 12, ytbl - 8, tx2 - 12, ytbl + row_h - 10), 14, bg_row, outline=None)

            rank = start_rank + i
            base_txt = (base or "")[:78]
            sla_txt = f"{sla:.2%}"

            draw.text((col_rank, ytbl), f"{rank:02d}", fill=TXT, font=f_row)
            draw.text((col_base, ytbl), base_txt, fill=TXT, font=f_row)
            draw.text((col_total, ytbl), str(tot), fill=TXT, font=f_row)
            draw.text((col_ent, ytbl), str(ent), fill=TXT, font=f_row)
            draw.text((col_fora, ytbl), str(fora), fill=TXT, font=f_row)

            bbox = draw.textbbox((0, 0), sla_txt, font=f_row)
            draw.text((col_sla_right - (bbox[2] - bbox[0]), ytbl), sla_txt, fill=JT_RED_SOFT, font=f_row)

            ytbl += row_h

        safe_coord = normalizar(coord).replace(" ", "_")
        fs = (file_suffix or "").strip()
        filename = f"SLA_{safe_coord}{fs}_{DATA_HOJE}_p{page_idx:02d}.png"
        out_path = os.path.join(out_dir, filename)
        img.save(out_path, "PNG")
        out_paths.append(out_path)

    return out_paths


def enviar_card_feishu(
    webhook: str,
    coord: str,
    indicador_nome: str,
    periodo_txt: str,
    dias_txt: str,
    sla: float,
    bases: int,
    arquivos_gerados_md: str,
    image_key: Optional[str] = None,
    page_label: Optional[str] = None,
    titulo_suffix: str = "",
) -> bool:
    try:
        if not webhook:
            logging.warning(f"⚠️ Webhook vazio para {coord}. Pulei.")
            return False

        titulo = f"{coord}{titulo_suffix}"
        indicador_nome = (indicador_nome or "").strip() or "SLA Entrega Realizada"

        body = (
            f"📌 **Indicador:** {indicador_nome}\n"
            f"📅 **Período:** {periodo_txt}\n"
            f"🗓️ **Dias:** {dias_txt}\n"
            f"📈 **SLA:** {sla:.2%}\n"
            f"🏢 **Bases:** {bases}\n"
        )
        if page_label:
            body += f"🖼️ **Imagem:** {page_label}\n"
        body += "\n" + arquivos_gerados_md

        elements = []
        if image_key:
            elements.append(
                {
                    "tag": "img",
                    "img_key": image_key,
                    "alt": {"tag": "plain_text", "content": "Tabela SLA por Base"},
                    "mode": "fit_horizontal",
                    "preview": True,
                }
            )
            elements.append({"tag": "hr"})

        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": body}})
        elements.append({"tag": "hr"})
        elements.append(
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "📂 Abrir Pasta (Resumo/Base)"},
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
                "header": {"template": "red", "title": {"tag": "plain_text", "content": titulo}},
                "elements": elements,
            },
        }

        r = _post_with_retry(webhook, payload, timeout=25)
        if r.status_code != 200:
            logging.error(f"❌ ERRO ao enviar card para {coord}. Status: {r.status_code}. Resp: {r.text}")
            return False

        logging.info(f"📨 Card enviado para {coord}{titulo_suffix}")
        return True

    except Exception as e:
        logging.error(f"❌ Falha envio card {coord}{titulo_suffix}: {e}")
        return False


def processar_parte_e_enviar(
    parte_nome: str,
    df_parte: pl.DataFrame,
    datas_parte: List[date],
    arquivo_resumo: str,
    prefixo_resumo: str,
    tag_base: str,
    titulo_suffix: str,
    file_suffix_imagem: str,
) -> None:
    if not datas_parte:
        logging.warning(f"⚠️ {parte_nome}: lista de datas vazia. Pulei.")
        return

    if df_parte is None or df_parte.is_empty():
        logging.warning(f"⚠️ {parte_nome}: dataframe vazio. Pulei export/envio.")
        return

    periodo_txt_parte = periodo_txt_de_datas(datas_parte)
    dias_txt_parte = formatar_lista_dias(datas_parte)

    logging.info(f"🧾 {parte_nome}: Período: {periodo_txt_parte} | Dias: {dias_txt_parte} | Registros: {df_parte.height}")

    paths_base = exportar_base_consolidada(df_parte, tag=tag_base)

    resumo_pd = gerar_resumo_por_base(df_parte)

    excluir_contains = "Domingo" if tag_base != "_Domingo" else None
    exportar_resumo_excel(resumo_pd, arquivo_resumo, prefixo=prefixo_resumo, excluir_contains=excluir_contains)

    arquivos_md = montar_arquivos_gerados_md(arquivo_resumo, paths_base)

    for coord, webhook in COORDENADOR_WEBHOOKS.items():
        if resumo_pd.empty:
            continue

        sub = resumo_pd[resumo_pd["COORDENADOR"].apply(normalizar) == normalizar(coord)]
        if sub.empty:
            logging.warning(f"⚠️ Nenhuma base encontrada para {coord} ({parte_nome})")
            continue

        bases = sub["Base De Entrega"].nunique()
        total = float(sub["Total"].sum()) if "Total" in sub.columns else 0.0
        ent = float(sub["Entregues no Prazo"].sum()) if "Entregues no Prazo" in sub.columns else 0.0
        sla = (ent / total) if total > 0 else 0.0

        img_paths = gerar_imagens_sla_tabela(
            coord=coord,
            indicador_nome=INDICADOR_NOME,
            titulo_suffix=titulo_suffix,
            periodo_txt=periodo_txt_parte,
            dias_txt=dias_txt_parte,
            sub=sub,
            sla_total=sla,
            out_dir=PASTA_IMAGENS,
            rows_per_page=IMG_ROWS_PER_PAGE,
            file_suffix=file_suffix_imagem,
        )

        if img_paths and _feishu_enabled():
            for i, p in enumerate(img_paths, start=1):
                img_key = feishu_upload_image_get_key(p)
                enviar_card_feishu(
                    webhook=webhook,
                    coord=coord,
                    indicador_nome=INDICADOR_NOME,
                    periodo_txt=periodo_txt_parte,
                    dias_txt=dias_txt_parte,
                    sla=sla,
                    bases=bases,
                    arquivos_gerados_md=arquivos_md,
                    image_key=img_key,
                    page_label=f"{i}/{len(img_paths)}",
                    titulo_suffix=titulo_suffix,
                )
                time.sleep(0.35)
        else:
            enviar_card_feishu(
                webhook=webhook,
                coord=coord,
                indicador_nome=INDICADOR_NOME,
                periodo_txt=periodo_txt_parte,
                dias_txt=dias_txt_parte,
                sla=sla,
                bases=bases,
                arquivos_gerados_md=arquivos_md,
                image_key=None,
                page_label=None,
                titulo_suffix=titulo_suffix,
            )
# =========================
# BLOCO 4/4 — MAIN
# =========================

if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA (v2.18 — leitura robusta do arquivo de coordenador)...")

    try:
        os.makedirs(PASTA_SAIDA, exist_ok=True)
        os.makedirs(PASTA_ARQUIVO, exist_ok=True)
        os.makedirs(PASTA_BASE_CONSOLIDADA, exist_ok=True)
        os.makedirs(PASTA_IMAGENS, exist_ok=True)

        periodo = calcular_periodo_base()
        if periodo is None:
            raise SystemExit(0)

        inicio, fim, datas = periodo

        datas_seg_sab_pre, datas_domingo_pre = separar_seg_sab_e_domingo(datas)
        if not datas_domingo_pre:
            logging.info("ℹ️ Período calculado não contém domingo — vai gerar somente Seg–Sáb (ou dias úteis do período).")

        logging.info(f"📅 Período (após feriados) usado para SLA: {formatar_periodo(inicio, fim)}")
        logging.info(f"🗓️ Dias considerados: {formatar_lista_dias(datas)}")
        logging.info(f"📌 Datas (ISO): {', '.join([d.strftime('%Y-%m-%d') for d in datas])}")

        df = consolidar_planilhas(PASTA_ENTRADA)
        logging.info(f"📥 Registros carregados: {df.height}")

        df = df.rename({c: c.strip().upper() for c in df.columns})
        df = garantir_coluna_data(df, COL_DATA_BASE)

        inicio, fim, datas = ajustar_periodo_por_dados(df, COL_DATA_BASE, inicio, fim, datas)

        logging.info(f"📅 Período FINAL usado para cálculo SLA: {formatar_periodo(inicio, fim)}")
        logging.info(f"🗓️ Dias considerados (FINAL): {formatar_lista_dias(datas)}")

        datas_seg_sab, datas_domingo = separar_seg_sab_e_domingo(datas)

        if datas_domingo:
            logging.info("🧩 Domingo presente no período (vai gerar separado).")
        else:
            logging.info("🧩 Sem domingo no período (vai gerar apenas Seg–Sáb).")

        # Detectar coluna ENTREGUE NO PRAZO
        colunas = list(df.columns)
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

        df_periodo_all = df.filter(pl.col(COL_DATA_BASE).is_in(datas))
        logging.info(f"📊 Registros para o período total: {df_periodo_all.height}")

        # ============================================================
        # Coordenadores
        # ============================================================
        arquivo_coord = localizar_arquivo_coordenador(CAMINHO_COORDENADOR)
        coord_df = pl.read_excel(arquivo_coord)

        logging.info(f"📎 Base de coordenadores carregada: {arquivo_coord}")
        logging.info(f"📥 Registros base coordenador: {coord_df.height}")

        # limpar nomes de colunas
        coord_df = coord_df.rename({c: c.strip() for c in coord_df.columns})

        # tentativa de rename mais tolerante
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

        # se ainda não encontrou, tenta por normalização
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

        # normalizar base para join
        df_periodo_all = df_periodo_all.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar, return_dtype=pl.Utf8).alias("BASE_NORM")
        )
        coord_df = coord_df.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar, return_dtype=pl.Utf8).alias("BASE_NORM")
        )

        # evitar duplicação no join (many-to-many)
        coord_df = coord_df.unique(subset=["BASE_NORM"], keep="first")

        df_periodo_all = df_periodo_all.join(
            coord_df.select(["BASE_NORM", "COORDENADOR"]),
            on="BASE_NORM",
            how="left",
        )

        sem_coord = df_periodo_all.filter(pl.col("COORDENADOR").is_null()).height
        logging.info(f"🧩 Registros sem coordenador após join (período total): {sem_coord}")

        # separar seg-sab / domingo
        df_seg_sab = df_periodo_all.filter(pl.col(COL_DATA_BASE).is_in(datas_seg_sab)) if datas_seg_sab else pl.DataFrame()
        df_domingo = df_periodo_all.filter(pl.col(COL_DATA_BASE).is_in(datas_domingo)) if datas_domingo else pl.DataFrame()

        logging.info(f"📦 Registros Seg–Sáb: {df_seg_sab.height if hasattr(df_seg_sab, 'height') else 0}")
        logging.info(f"📦 Registros Domingo: {df_domingo.height if hasattr(df_domingo, 'height') else 0}")

        # ✅ GERA SEMPRE SEG–SÁB
        if datas_seg_sab and not df_seg_sab.is_empty():
            processar_parte_e_enviar(
                parte_nome="Seg–Sáb",
                df_parte=df_seg_sab,
                datas_parte=datas_seg_sab,
                arquivo_resumo=ARQUIVO_SAIDA,
                prefixo_resumo="Resumo_Consolidado_",
                tag_base="",
                titulo_suffix="",
                file_suffix_imagem="",
            )
        else:
            logging.warning("⚠️ Sem dados para Seg–Sáb no período. Nada a exportar/enviar (Seg–Sáb).")

        # ✅ DOMINGO SÓ SE EXISTIR
        if datas_domingo and not df_domingo.is_empty():
            processar_parte_e_enviar(
                parte_nome="Domingo",
                df_parte=df_domingo,
                datas_parte=datas_domingo,
                arquivo_resumo=ARQUIVO_SAIDA_DOMINGO,
                prefixo_resumo="Resumo_Consolidado_Domingo_",
                tag_base="_Domingo",
                titulo_suffix=" — Domingo",
                file_suffix_imagem="_Domingo",
            )
        else:
            logging.info("ℹ️ Domingo não presente (ou sem dados) — não gerou a parte de Domingo.")

        logging.info("🏁 Processamento concluído.")

    except SystemExit:
        raise
    except Exception as e:
        logging.critical(f"❌ ERRO FATAL: {e}", exc_info=True)
        raise