# -*- coding: utf-8 -*-

# =========================
# BLOCO 1/4 — IMPORTS / CONFIG
# =========================

import os
import mimetypes
import requests
import warnings
import polars as pl
import pandas as pd
import multiprocessing
import logging
import shutil
import unicodedata
import time

from io import BytesIO
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple, Dict, Set, Any

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("../Demais/sla_processor.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

# ============================================================
# Caminhos
# ============================================================
PASTA_ENTRADA = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\06-  SLA Entrega Realizada Franquia"

CAMINHO_COORDENADOR = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Área de Trabalho\Testes\01 - Coordenador"

PASTA_SAIDA = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\SLA - Coordenadores LM"

PASTA_ARQUIVO = os.path.join(PASTA_SAIDA, "Arquivo Morto")
PASTA_BASE_CONSOLIDADA = os.path.join(PASTA_SAIDA, "Base Consolidada")
PASTA_IMAGENS = os.path.join(PASTA_SAIDA, "Imagens_Coordenadores_SLA")

DATA_HOJE = datetime.now().strftime("%Y%m%d")

ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_{DATA_HOJE}.xlsx")
ARQUIVO_SAIDA_DOMINGO = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_Domingo_{DATA_HOJE}.xlsx")

EXCEL_MAX_ROWS = 1_048_576

LINK_PASTA = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/IgCBcizkJxWcTYzIEL35TMQtAap5Mm22qqaWrAoK6sSlijs?e=5aj7RG"
)

INDICADOR_NOME = "SLA Entrega Realizada — %SLA por Base (pior → melhor)"

COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
}

EXTS = (".xlsx", ".xls", ".csv")
COL_DATA_BASE = "DATA PREVISTA DE ENTREGA"

PULAR_FERIADOS_NACIONAIS = True
PULAR_FERIADOS_EM_FDS = False
_CACHE_FERIADOS: Dict[int, Set[date]] = {}

FEISHU_BASE_DOMAIN = "https://open.feishu.cn"
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "cli_a906d2d682f8dbd8").strip()
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "Fzh1cr6K55a3oQUBV9wCZd6AWiZH5ONw    ").strip()

IMG_ROWS_PER_PAGE = int(os.getenv("IMG_ROWS_PER_PAGE", "26"))

_TOKEN_CACHE = {"token": None, "exp": 0}

JT_RED_MAIN = (227, 6, 19)
JT_RED_SOFT = (196, 39, 46)
JT_BG_GRAY = (242, 242, 242)
JT_TEXT = (51, 51, 51)
JT_WHITE = (255, 255, 255)

JT_STROKE = (220, 220, 220)
JT_MUTED = (110, 110, 110)
JT_ROW_ALT = (248, 248, 248)


def _post_with_retry(url: str, json_payload: dict, timeout: int = 25, tries: int = 7) -> requests.Response:
    last = None
    for i in range(1, tries + 1):
        try:
            return requests.post(url, json=json_payload, timeout=timeout)
        except Exception as e:
            last = e
            time.sleep(0.7 * i)
    raise RuntimeError(f"Falha POST {url} após {tries} tentativas. Último erro: {last}")


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
            files = {
                file_field: (
                    filename,
                    BytesIO(file_bytes),
                    content_type,
                )
            }
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


# =========================
# BLOCO 2/4 — FUNÇÕES BASE
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
    fer.add(pascoa - timedelta(days=2))
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
    dia = hoje.weekday()

    if dia in (5, 6):
        logging.warning("⛔ Hoje é sábado ou domingo. Execução cancelada.")
        return None

    ref = hoje - timedelta(days=1)
    inicio = ref - timedelta(days=ref.weekday())
    fim = ref

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

    if not datas_ok:
        logging.warning("⚠️ Nenhuma data válida encontrada na semana após remover feriados.")
        return None

    return min(datas_ok), max(datas_ok), datas_ok


def arquivar_relatorios_antigos(
    pasta_origem: str,
    pasta_destino: str,
    prefixo: str,
    excluir_contains: Optional[str] = None
) -> None:
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


def arquivar_bases_antigas(
    pasta_origem: str,
    pasta_destino: str,
    prefixo: str,
    excluir_contains: Optional[str] = None
) -> None:
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
    df: pl.DataFrame,
    coluna_data: str,
    inicio: date,
    fim: date,
    datas: List[date]
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


def exportar_resumo_excel(
    resumo_pd: pd.DataFrame,
    arquivo_saida: str,
    prefixo: str,
    excluir_contains: Optional[str] = None
) -> None:
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


def nome_mes_pt_abrev(mes: int) -> str:
    meses = {
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
    return meses.get(int(mes), str(mes))


def montar_matriz_mensal_por_base(
    df_coord: pl.DataFrame,
    ref_date: date,
    col_data: str = COL_DATA_BASE,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if df_coord is None or df_coord.is_empty():
        return pd.DataFrame(), {}

    pdf = df_coord.select(["BASE DE ENTREGA", col_data]).to_pandas()
    pdf[col_data] = pd.to_datetime(pdf[col_data], errors="coerce").dt.date
    pdf = pdf.dropna(subset=[col_data])

    if pdf.empty:
        return pd.DataFrame(), {}

    ref_date = pd.to_datetime(ref_date).date()

    mes_atual_ini = ref_date.replace(day=1)
    mes_atual_fim = ref_date

    mes_ant_fim = mes_atual_ini - timedelta(days=1)
    mes_ant_ini = mes_ant_fim.replace(day=1)

    pdf_atual = pdf[(pdf[col_data] >= mes_atual_ini) & (pdf[col_data] <= mes_atual_fim)].copy()
    pdf_ant = pdf[(pdf[col_data] >= mes_ant_ini) & (pdf[col_data] <= mes_ant_fim)].copy()

    if pdf_atual.empty and pdf_ant.empty:
        return pd.DataFrame(), {}

    datas_mes_atual = [
        mes_atual_ini + timedelta(days=i)
        for i in range((mes_atual_fim - mes_atual_ini).days + 1)
    ]
    day_cols = [f"{d.day}/{d.month}" for d in datas_mes_atual]

    week_nums = sorted({int(d.isocalendar()[1]) for d in datas_mes_atual})
    week_cols = [f"W{w}" for w in week_nums]

    qtd_atual = pdf_atual.groupby("BASE DE ENTREGA").size()
    qtd_ant = pdf_ant.groupby("BASE DE ENTREGA").size()

    if not pdf_atual.empty:
        pdf_atual["dia_label"] = pdf_atual[col_data].apply(lambda d: f"{d.day}/{d.month}")
        diario = (
            pdf_atual.assign(_QTD=1)
            .pivot_table(
                index="BASE DE ENTREGA",
                columns="dia_label",
                values="_QTD",
                aggfunc="sum",
                fill_value=0,
            )
        )
    else:
        diario = pd.DataFrame()

    if not pdf_atual.empty:
        pdf_atual["week_num"] = pd.to_datetime(pdf_atual[col_data]).apply(lambda x: int(x.isocalendar().week))
        semanal = (
            pdf_atual.assign(_QTD=1)
            .pivot_table(
                index="BASE DE ENTREGA",
                columns="week_num",
                values="_QTD",
                aggfunc="sum",
                fill_value=0,
            )
        )
        semanal.columns = [f"W{int(c)}" for c in semanal.columns]
    else:
        semanal = pd.DataFrame()

    bases = sorted(
        set(qtd_atual.index.tolist())
        | set(qtd_ant.index.tolist())
        | set(diario.index.tolist() if not diario.empty else [])
        | set(semanal.index.tolist() if not semanal.empty else [])
    )

    if not bases:
        return pd.DataFrame(), {}

    matriz = pd.DataFrame(index=bases)

    col_qtd_ant = f"Qtd. {nome_mes_pt_abrev(mes_ant_ini.month)}"
    col_qtd_atual = f"Qtd. {nome_mes_pt_abrev(ref_date.month)}"

    matriz[col_qtd_ant] = qtd_ant.reindex(bases, fill_value=0).astype(int)
    matriz[col_qtd_atual] = qtd_atual.reindex(bases, fill_value=0).astype(int)

    for w in week_cols:
        if not semanal.empty and w in semanal.columns:
            matriz[w] = semanal[w].reindex(bases, fill_value=0).astype(int)
        else:
            matriz[w] = 0

    for dcol in day_cols:
        if not diario.empty and dcol in diario.columns:
            matriz[dcol] = diario[dcol].reindex(bases, fill_value=0).astype(int)
        else:
            matriz[dcol] = 0

    matriz = (
        matriz.reset_index()
        .rename(columns={"index": "Base"})
        .sort_values(by=[col_qtd_atual, "Base"], ascending=[False, True])
        .reset_index(drop=True)
    )

    info = {
        "ref_date": ref_date,
        "col_qtd_ant": col_qtd_ant,
        "col_qtd_atual": col_qtd_atual,
        "week_cols": week_cols,
        "day_cols": day_cols,
        "recent_day_cols": day_cols[-10:],
    }

    return matriz, info
# =========================
# BLOCO 3/4 — FEISHU + IMAGEM
# =========================

def _feishu_enabled() -> bool:
    return bool(FEISHU_APP_ID and FEISHU_APP_SECRET)


def feishu_get_token() -> str:
    if not _feishu_enabled():
        raise RuntimeError("Defina FEISHU_APP_ID e FEISHU_APP_SECRET para enviar imagens.")

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

    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Imagem não encontrada para upload: {image_path}")

    file_size = os.path.getsize(image_path)
    if file_size <= 0:
        raise RuntimeError(f"Imagem gerada com 0 bytes: {image_path}")

    limite_10mb = 10 * 1024 * 1024
    if file_size > limite_10mb:
        raise RuntimeError(
            f"Imagem maior que 10 MB e será recusada pelo Feishu: "
            f"{os.path.basename(image_path)} ({file_size:,} bytes)"
        )

    with open(image_path, "rb") as f:
        file_bytes = f.read()

    if not file_bytes:
        raise RuntimeError(f"Falha ao ler bytes da imagem: {image_path}")

    r = _post_multipart_with_retry(
        url=url,
        data={"image_type": "message"},
        file_bytes=file_bytes,
        file_field="image",
        filename=os.path.basename(image_path),
        headers=headers,
        timeout=90,
    )

    try:
        data = r.json() if r.content else {}
    except Exception:
        data = {"raw_text": r.text}

    if r.status_code != 200:
        raise RuntimeError(
            f"HTTP {r.status_code} no upload da imagem '{os.path.basename(image_path)}': {data}"
        )

    if data.get("code") != 0:
        raise RuntimeError(f"Upload imagem falhou: {data}")

    image_key = (data.get("data") or {}).get("image_key")
    if not image_key:
        raise RuntimeError(f"Upload OK mas sem image_key: {data}")

    logging.info(f"🖼️ Upload Feishu OK: {os.path.basename(image_path)} ({file_size} bytes)")
    return image_key


def gerar_imagens_sla_matriz_mensal(
    coord: str,
    indicador_nome: str,
    titulo_suffix: str,
    matriz_pd: pd.DataFrame,
    info: Dict[str, Any],
    out_dir: str,
    rows_per_page: int = 26,
    file_suffix: str = "",
) -> List[str]:
    try:
        from PIL import Image, ImageDraw, ImageFont
    except Exception:
        raise RuntimeError("Falta Pillow. Instale: pip install pillow")

    os.makedirs(out_dir, exist_ok=True)

    if matriz_pd is None or matriz_pd.empty:
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

    headers = list(matriz_pd.columns)
    week_cols = set(info.get("week_cols", []))
    day_cols = set(info.get("day_cols", []))
    recent_day_cols = set(info.get("recent_day_cols", []))
    ref_date = info.get("ref_date")

    def col_width(col: str) -> int:
        if col == "Base":
            return 190
        if col.startswith("Qtd."):
            return 96
        if col in week_cols:
            return 70
        if col in day_cols:
            return 56
        return 80

    widths = [col_width(c) for c in headers]

    pages = [
        matriz_pd.iloc[i:i + rows_per_page].copy()
        for i in range(0, len(matriz_pd), rows_per_page)
    ]

    RED = JT_RED_MAIN
    RED_SOFT = JT_RED_SOFT
    WHITE = JT_WHITE
    BLACK = JT_TEXT
    GRID = (210, 210, 210)
    ROW_A = (255, 255, 255)
    ROW_B = (248, 248, 248)
    RECENT_BG = (255, 244, 244)

    font_title = load_font(24, bold=True)
    font_sub = load_font(16, bold=False)
    font_head = load_font(16, bold=True)
    font_cell = load_font(15, bold=False)
    font_cell_bold = load_font(15, bold=True)

    out_paths: List[str] = []

    left = 18
    right = 18
    top_title = 60
    header_h = 40
    row_h = 30
    bottom = 18
    total_w = left + sum(widths) + right

    for page_idx, page_df in enumerate(pages, start=1):
        total_h = top_title + header_h + (len(page_df) * row_h) + bottom
        img = Image.new("RGB", (total_w, total_h), WHITE)
        draw = ImageDraw.Draw(img)

        titulo = f"{coord}{titulo_suffix} — Matriz diária por base"
        subtitulo = (
            f"{indicador_nome} | Referência até {ref_date.strftime('%d/%m/%Y')} | "
            f"Página {page_idx}/{len(pages)}"
        )

        draw.text((left, 10), titulo, fill=RED, font=font_title)
        draw.text((left, 36), subtitulo, fill=BLACK, font=font_sub)

        x = left
        y = top_title

        for col, w in zip(headers, widths):
            draw.rectangle((x, y, x + w, y + header_h), fill=RED, outline=WHITE, width=1)
            txt = ellipsize(draw, col, font_head, w - 8)
            tw, th = measure(draw, txt, font_head)
            draw.text((x + (w - tw) / 2, y + (header_h - th) / 2 - 1), txt, fill=WHITE, font=font_head)
            x += w

        start_y = y + header_h
        for ridx, (_, row) in enumerate(page_df.iterrows()):
            y1 = start_y + (ridx * row_h)
            fill_row = ROW_A if ridx % 2 == 0 else ROW_B

            x = left
            for col, w in zip(headers, widths):
                cell_fill = fill_row
                if col in recent_day_cols:
                    cell_fill = RECENT_BG if ridx % 2 == 0 else (255, 239, 239)

                draw.rectangle((x, y1, x + w, y1 + row_h), fill=cell_fill, outline=GRID, width=1)

                val = row[col]
                if pd.isna(val):
                    txt = ""
                elif col == "Base":
                    txt = str(val)
                else:
                    try:
                        txt = f"{int(val)}"
                    except Exception:
                        txt = str(val)

                if col == "Base":
                    txt = ellipsize(draw, txt, font_cell, w - 10)
                    _, th = measure(draw, txt, font_cell)
                    draw.text((x + 6, y1 + (row_h - th) / 2 - 1), txt, fill=BLACK, font=font_cell)
                else:
                    fill_txt = BLACK
                    font_use = font_cell

                    try:
                        n = int(val)
                    except Exception:
                        n = 0

                    if col in recent_day_cols and n > 0:
                        fill_txt = RED
                        font_use = font_cell_bold
                    elif col in week_cols and n >= 10:
                        fill_txt = RED_SOFT
                        font_use = font_cell_bold

                    tw, th = measure(draw, txt, font_use)
                    draw.text((x + (w - tw) / 2, y1 + (row_h - th) / 2 - 1), txt, fill=fill_txt, font=font_use)

                x += w

        safe_coord = normalizar(coord).replace(" ", "_")
        fs = (file_suffix or "").strip()
        filename = f"MATRIZ_{safe_coord}{fs}_{DATA_HOJE}_p{page_idx:02d}.png"
        out_path = os.path.join(out_dir, filename)
        img.save(out_path, format="PNG")
        out_paths.append(out_path)
        logging.info(f"🖼️ Imagem matriz gerada: {out_path}")

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
        if not webhook or webhook == "COLE_SEU_WEBHOOK_AQUI":
            logging.warning(f"⚠️ Webhook vazio/inválido para {coord}. Pulei.")
            return False

        titulo = f"{coord}{titulo_suffix}"
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
                    "alt": {"tag": "plain_text", "content": "Matriz diária por base"},
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
    df_imagem_full: pl.DataFrame,
) -> None:
    if not datas_parte:
        logging.warning(f"⚠️ {parte_nome}: lista de datas vazia. Pulei.")
        return

    if df_parte is None or df_parte.is_empty():
        logging.warning(f"⚠️ {parte_nome}: dataframe vazio. Pulei export/envio.")
        return

    periodo_txt_parte = periodo_txt_de_datas(datas_parte)
    dias_txt_parte = formatar_lista_dias(datas_parte)

    logging.info(
        f"🧾 {parte_nome}: Período: {periodo_txt_parte} | Dias: {dias_txt_parte} | Registros: {df_parte.height}"
    )

    paths_base = exportar_base_consolidada(df_parte, tag=tag_base)
    resumo_pd = gerar_resumo_por_base(df_parte)

    excluir_contains = "Domingo" if tag_base != "_Domingo" else None
    exportar_resumo_excel(resumo_pd, arquivo_saida=arquivo_resumo, prefixo=prefixo_resumo, excluir_contains=excluir_contains)

    arquivos_md = montar_arquivos_gerados_md(arquivo_resumo, paths_base)
    ref_date_img = max(datas_parte)

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

        df_coord_img = df_imagem_full.filter(pl.col("COORD_NORM") == normalizar(coord))

        matriz_pd, info = montar_matriz_mensal_por_base(
            df_coord=df_coord_img,
            ref_date=ref_date_img,
            col_data=COL_DATA_BASE,
        )

        img_paths: List[str] = []
        if matriz_pd is not None and not matriz_pd.empty:
            img_paths = gerar_imagens_sla_matriz_mensal(
                coord=coord,
                indicador_nome=INDICADOR_NOME,
                titulo_suffix=titulo_suffix,
                matriz_pd=matriz_pd,
                info=info,
                out_dir=PASTA_IMAGENS,
                rows_per_page=IMG_ROWS_PER_PAGE,
                file_suffix=file_suffix_imagem,
            )

        if img_paths and _feishu_enabled():
            for i, p in enumerate(img_paths, start=1):
                try:
                    size_img = os.path.getsize(p) if os.path.exists(p) else -1
                    logging.info(
                        f"📤 Enviando imagem para Feishu | Coord: {coord}{titulo_suffix} | "
                        f"Arquivo: {os.path.basename(p)} | Tamanho: {size_img} bytes"
                    )

                    if size_img <= 0:
                        raise RuntimeError(f"Imagem inválida para upload: {p}")

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

                except Exception as e:
                    logging.error(f"⚠️ Falha no upload/envio da imagem para {coord}{titulo_suffix}: {e}")
                    logging.info(f"↪️ Vou enviar o card sem imagem para {coord}{titulo_suffix}.")

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
# MAIN
# =========================

if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA (v2.30 — matriz mensal por base no estilo solicitado)...")

    try:
        os.makedirs(PASTA_SAIDA, exist_ok=True)
        os.makedirs(PASTA_ARQUIVO, exist_ok=True)
        os.makedirs(PASTA_BASE_CONSOLIDADA, exist_ok=True)
        os.makedirs(PASTA_IMAGENS, exist_ok=True)

        periodo = calcular_periodo_base()
        if periodo is None:
            raise SystemExit(0)

        inicio, fim, datas = periodo

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
                f"❌ O arquivo de coordenador precisa ter 'BASE DE ENTREGA' e 'COORDENADOR'. Colunas encontradas: {coord_df.columns}"
            )

        df = df.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar, return_dtype=pl.Utf8).alias("BASE_NORM")
        )
        coord_df = coord_df.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar, return_dtype=pl.Utf8).alias("BASE_NORM")
        )

        coord_df = coord_df.unique(subset=["BASE_NORM"], keep="first")

        df_com_coord_full = (
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

        df_periodo_all = df_com_coord_full.filter(pl.col(COL_DATA_BASE).is_in(datas))
        logging.info(f"📊 Registros para o período total: {df_periodo_all.height}")

        sem_coord = df_periodo_all.filter(pl.col("COORDENADOR").is_null()).height
        logging.info(f"🧩 Registros sem coordenador após join (período total): {sem_coord}")

        if sem_coord > 0:
            try:
                arq_sem_coord = os.path.join(PASTA_SAIDA, f"Bases_Sem_Coordenador_{DATA_HOJE}.xlsx")

                df_sem_coord = (
                    df_periodo_all
                    .filter(pl.col("COORDENADOR").is_null())
                    .select(["BASE DE ENTREGA"])
                    .unique()
                    .sort("BASE DE ENTREGA")
                    .to_pandas()
                )

                if not df_sem_coord.empty:
                    with pd.ExcelWriter(arq_sem_coord, engine="openpyxl") as w:
                        df_sem_coord.to_excel(w, index=False, sheet_name="Bases Sem Coordenador")
                    logging.info(f"📝 Lista de bases sem coordenador salva em: {arq_sem_coord}")
            except Exception as e:
                logging.warning(f"⚠️ Não consegui salvar a lista de bases sem coordenador: {e}")

        df_seg_sab = df_periodo_all.filter(pl.col(COL_DATA_BASE).is_in(datas_seg_sab)) if datas_seg_sab else pl.DataFrame()
        df_domingo = df_periodo_all.filter(pl.col(COL_DATA_BASE).is_in(datas_domingo)) if datas_domingo else pl.DataFrame()

        logging.info(f"📦 Registros Seg–Sáb: {df_seg_sab.height if hasattr(df_seg_sab, 'height') else 0}")
        logging.info(f"📦 Registros Domingo: {df_domingo.height if hasattr(df_domingo, 'height') else 0}")

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
                df_imagem_full=df_com_coord_full,
            )
        else:
            logging.warning("⚠️ Sem dados para Seg–Sáb no período. Nada a exportar/enviar (Seg–Sáb).")

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
                df_imagem_full=df_com_coord_full,
            )
        else:
            logging.info("ℹ️ Domingo não presente (ou sem dados) — não gerou a parte de Domingo.")

        logging.info("🏁 Processamento concluído.")

    except SystemExit:
        raise
    except Exception as e:
        logging.critical(f"❌ ERRO FATAL: {e}", exc_info=True)
        raise