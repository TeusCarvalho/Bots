# =========================
# BLOCO 1/4 — IMPORTS / CONFIG
# =========================
# -*- coding: utf-8 -*-

import os
import requests
import warnings
import polars as pl
import pandas as pd
import multiprocessing
import logging
import shutil
import unicodedata
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Tuple, Dict, Set

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("sla_processor.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

# ============================================================
# Caminhos
# ============================================================
PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\SLA - Entrega Realizada"
PASTA_COORDENADOR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
PASTA_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\SLA - Entrega Realizada"

# Arquivo morto (para relatórios e bases antigas)
PASTA_ARQUIVO = os.path.join(PASTA_SAIDA, "Arquivo Morto")

# ✅ pasta específica para base consolidada (original + alterações)
PASTA_BASE_CONSOLIDADA = os.path.join(PASTA_SAIDA, "Base Consolidada")

DATA_HOJE = datetime.now().strftime("%Y%m%d")

# ✅ Resumo principal (Seg–Sáb)
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_{DATA_HOJE}.xlsx")

# ✅ Resumo Domingo (se existir)
ARQUIVO_SAIDA_DOMINGO = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_Domingo_{DATA_HOJE}.xlsx")

# Limite de linhas do Excel
EXCEL_MAX_ROWS = 1_048_576

LINK_PASTA = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/IgCkMQtn4udmRZAFJTit7pkaAVAudAyWYHic-zXIKMlQz1Q?e=d3eOd5"
)

COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/3663dd30-722c-45d6-9e3c-1d4e2838f112",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/0b907801-c73e-4de8-9f84-682d7b54f6fd",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/261cefd4-5528-4760-b18e-49a0249718c7",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/b749fd36-d287-460e-b1e2-c78bfb4c1946",
    "Odária Fereira": "https://open.feishu.cn/open-apis/bot/v2/hook/48c4db73-b5a4-4007-96af-f5d28301f0c1",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/606ed22b-dc49-451d-9bfe-0a8829dbe76e",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/840f79b0-1eff-42fe-aae0-433c9edbad80",
    "Fabio Souza": "https://open.feishu.cn/open-apis/bot/v2/hook/ca2c260c-f69c-472d-9757-279db52a79b8",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/63751a67-efe8-40e4-b841-b290a4819836",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/3ddc5962-2d32-4b2d-92d9-a4bc95ac3393",
    "Ana Cunha": "https://open.feishu.cn/open-apis/bot/v2/hook/b2ec868f-3149-4808-af53-9e0c6d2cd94e",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/a53ad30e-17dd-4330-93db-15138b20d8f2"},


EXTS = (".xlsx", ".xls", ".csv")
COL_DATA_BASE = "DATA PREVISTA DE ENTREGA"

# ============================================================
# ✅ Controle de feriados nacionais
# ============================================================
PULAR_FERIADOS_NACIONAIS = True
PULAR_FERIADOS_EM_FDS = False

_CACHE_FERIADOS: Dict[int, Set[date]] = {}
# =========================
# BLOCO 2/4 — FUNÇÕES (FERIADOS / PERÍODO / LEITURA / DATA / FALLBACK / EXPORT / SEPARAÇÃO DOMINGO)
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
        return ""
    dias_pt = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    partes = [f"{dias_pt[d.weekday()]} {d.strftime('%d/%m')}" for d in datas]
    return ", ".join(partes)


def periodo_txt_de_datas(datas: List[date]) -> str:
    if not datas:
        return "-"
    return formatar_periodo(min(datas), max(datas))


def cor_percentual(p: float) -> str:
    if p < 0.95:
        return "🔴"
    elif p < 0.97:
        return "🟡"
    return "🟢"


def separar_seg_sab_e_domingo(datas: List[date]) -> Tuple[List[date], List[date]]:
    """
    ✅ NOVO:
    Retorna (datas_seg_sab, datas_domingo) a partir da lista calculada.
    - Seg–Sáb: weekday != 6
    - Domingo: weekday == 6
    """
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

        logging.warning(
            f"⚠️ Período ({formatar_periodo(inicio, fim)}) ficou vazio após remover feriados. Recuando 1 dia..."
        )
        fim = fim - timedelta(days=1)


def arquivar_relatorios_antigos(pasta_origem: str, pasta_destino: str, prefixo: str) -> None:
    os.makedirs(pasta_destino, exist_ok=True)
    if not os.path.isdir(pasta_origem):
        return
    for arquivo in os.listdir(pasta_origem):
        if arquivo.startswith(prefixo) and arquivo.endswith(".xlsx"):
            try:
                shutil.move(
                    os.path.join(pasta_origem, arquivo),
                    os.path.join(pasta_destino, arquivo),
                )
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
        if not (arquivo.lower().endswith(".xlsx") or arquivo.lower().endswith(".csv") or arquivo.lower().endswith(".parquet")):
            continue
        try:
            shutil.move(
                os.path.join(pasta_origem, arquivo),
                os.path.join(pasta_destino, arquivo),
            )
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
        s = (
            pl.col(coluna)
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.replace_all(r"\s+", " ")
        )

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
    datas: List[date],
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
            max_le = (
                df.filter(pl.col(coluna_data).is_not_null())
                .select(pl.col(coluna_data).max())
                .item()
            )
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

    novo_datas = [
        novo_inicio + timedelta(days=i)
        for i in range((novo_fim - novo_inicio).days + 1)
    ]

    logging.warning(
        f"⚠️ Nenhum registro para o período calculado ({formatar_periodo(inicio, fim)}). "
        f"Fallback para última data disponível: {formatar_periodo(novo_inicio, novo_fim)}."
    )

    return novo_inicio, novo_fim, novo_datas


def exportar_base_consolidada(df_periodo: pl.DataFrame, tag: str = "") -> Dict[str, str]:
    """
    ✅ NOVO:
    Exporta base consolidada por "tag":
      tag=""         -> Base_Consolidada_YYYYMMDD.*
      tag="_Domingo" -> Base_Consolidada_Domingo_YYYYMMDD.*

    Retorna dict com caminhos gerados.
    """
    os.makedirs(PASTA_BASE_CONSOLIDADA, exist_ok=True)

    if tag == "_Domingo":
        prefixo = "Base_Consolidada_Domingo_"
        nome_base = f"Base_Consolidada_Domingo_{DATA_HOJE}"
    else:
        prefixo = "Base_Consolidada_"
        nome_base = f"Base_Consolidada_{DATA_HOJE}"

    arq_parquet = os.path.join(PASTA_BASE_CONSOLIDADA, f"{nome_base}.parquet")
    arq_csv = os.path.join(PASTA_BASE_CONSOLIDADA, f"{nome_base}.csv")
    arq_xlsx = os.path.join(PASTA_BASE_CONSOLIDADA, f"{nome_base}.xlsx")

    # Arquiva bases antigas do mesmo tipo/tag
    arquivar_bases_antigas(PASTA_BASE_CONSOLIDADA, PASTA_ARQUIVO, prefixo)

    # Sempre salva parquet
    try:
        df_periodo.write_parquet(arq_parquet)
        logging.info(f"✅ Base consolidada (PARQUET) salva em: {arq_parquet}")
    except Exception as e:
        logging.error(f"❌ Falha ao salvar PARQUET ({nome_base}): {e}")

    # Sempre salva CSV
    try:
        df_periodo.write_csv(arq_csv)
        logging.info(f"✅ Base consolidada (CSV) salva em: {arq_csv}")
    except Exception as e:
        logging.error(f"❌ Falha ao salvar CSV ({nome_base}): {e}")

    # XLSX só se couber no Excel
    try:
        if df_periodo.height <= (EXCEL_MAX_ROWS - 1):
            df_pd = df_periodo.to_pandas()
            with pd.ExcelWriter(arq_xlsx, engine="openpyxl") as w:
                df_pd.to_excel(w, index=False, sheet_name="Base Consolidada")
            logging.info(f"✅ Base consolidada (XLSX) salva em: {arq_xlsx}")
        else:
            logging.warning(
                f"⚠️ {nome_base} tem {df_periodo.height:,} linhas. Excel suporta até {EXCEL_MAX_ROWS:,}. "
                "XLSX NÃO gerado (use PARQUET/CSV)."
            )
    except Exception as e:
        logging.error(f"❌ Falha ao salvar XLSX ({nome_base}): {e}")

    return {"parquet": arq_parquet, "csv": arq_csv, "xlsx": arq_xlsx}


def exportar_resumo_excel(resumo_pd: pd.DataFrame, arquivo_saida: str, prefixo: str) -> None:
    os.makedirs(PASTA_SAIDA, exist_ok=True)
    arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, prefixo)
    with pd.ExcelWriter(arquivo_saida, engine="openpyxl") as w:
        resumo_pd.to_excel(w, index=False, sheet_name="Resumo SLA")
    logging.info(f"✅ Resumo Excel salvo em: {arquivo_saida}")


def montar_arquivos_gerados_md(arquivo_resumo: str, paths_base: Dict[str, str]) -> str:
    base_xlsx_txt = (
        f"- Base (XLSX): `{os.path.basename(paths_base['xlsx'])}`\n"
        if os.path.exists(paths_base["xlsx"])
        else "- Base (XLSX): *(não gerado — limite do Excel)*\n"
    )

    txt = (
        "📄 **Arquivos gerados:**\n"
        f"- Resumo: `{os.path.basename(arquivo_resumo)}`\n"
        f"- Base (PARQUET): `{os.path.basename(paths_base['parquet'])}`\n"
        f"- Base (CSV): `{os.path.basename(paths_base['csv'])}`\n"
        + base_xlsx_txt
    )
    return txt


def gerar_resumo_por_base(df_periodo: pl.DataFrame) -> pd.DataFrame:
    if df_periodo.is_empty():
        return pd.DataFrame(
            columns=[
                "Base De Entrega",
                "COORDENADOR",
                "Total",
                "Entregues no Prazo",
                "Fora do Prazo",
                "% SLA Cumprido",
            ]
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
# BLOCO 3/4 — FEISHU
# =========================

def enviar_card_feishu(
    resumo: pd.DataFrame,
    webhook: str,
    coord: str,
    sla: float,
    periodo_txt: str,
    dias_txt: str,
    arquivos_gerados_md: str,
    titulo_suffix: str = "",  # ✅ NOVO: para marcar "Domingo"
) -> bool:
    try:
        if resumo.empty:
            logging.warning(f"⚠️ Nenhuma base para {coord}{titulo_suffix}")
            return False

        bases = resumo["Base De Entrega"].nunique()

        piores = resumo.sort_values("% SLA Cumprido").head(3)
        melhores = resumo.sort_values("% SLA Cumprido", ascending=False).head(3)

        linhas_piores = [
            f"{i}. {cor_percentual(float(l['% SLA Cumprido']))} **{l['Base De Entrega']}** — {float(l['% SLA Cumprido']):.2%}"
            for i, l in enumerate(piores.to_dict("records"), 1)
        ]

        medalhas = ["🥇", "🥈", "🥉"]
        linhas_melhores = [
            f"{medalhas[i-1]} {cor_percentual(float(l['% SLA Cumprido']))} **{l['Base De Entrega']}** — {float(l['% SLA Cumprido']):.2%}"
            for i, l in enumerate(melhores.to_dict("records"), 1)
        ]

        conteudo = (
            f"👤 **Coordenador:** {coord}\n"
            f"📅 **Período:** {periodo_txt}\n"
            f"🗓️ **Dias considerados:** {dias_txt}\n"
            f"📈 **SLA (Período):** {sla:.2%}\n"
            f"🏢 **Bases analisadas:** {bases}\n\n"
            + arquivos_gerados_md
            + "\n"
            f"🔻 **3 Piores:**\n" + "\n".join(linhas_piores) +
            "\n\n🏆 **3 Melhores:**\n" + "\n".join(linhas_melhores)
        )

        titulo = f"SLA - Entrega no Prazo — {coord}{titulo_suffix}"

        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "template": "blue",
                    "title": {"tag": "plain_text", "content": titulo},
                },
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content": conteudo}},
                    {"tag": "hr"},
                    {
                        "tag": "action",
                        "actions": [
                            {
                                "tag": "button",
                                "text": {"tag": "plain_text", "content": "📂 Abrir Pasta (Resumo/Base)"},
                                "url": LINK_PASTA,
                                "type": "default",
                            }
                        ],
                    },
                ],
            },
        }

        r = requests.post(webhook, json=payload, timeout=15)

        if r.status_code != 200:
            logging.error(
                f"❌ ERRO ao enviar card para {coord}{titulo_suffix}. Status: {r.status_code}. Resposta: {r.text}"
            )
            return False

        logging.info(f"📨 Card enviado para {coord}{titulo_suffix}")
        return True

    except Exception as e:
        logging.error(f"❌ Falha no envio para {coord}{titulo_suffix}. Erro: {e}. Webhook: {webhook}")
        return False
# =========================
# BLOCO 4/4 — MAIN (v2.15 — separa Domingo)
# =========================

if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA (v2.15 — separa Domingo)...")

    try:
        # ✅ Garantir pastas
        os.makedirs(PASTA_SAIDA, exist_ok=True)
        os.makedirs(PASTA_ARQUIVO, exist_ok=True)
        os.makedirs(PASTA_BASE_CONSOLIDADA, exist_ok=True)

        # 0) Período-base (ignora feriados nacionais)
        periodo = calcular_periodo_base()
        if periodo is None:
            raise SystemExit(0)

        inicio, fim, datas = periodo
        periodo_txt = formatar_periodo(inicio, fim)
        dias_txt = formatar_lista_dias(datas)

        logging.info(f"📅 Período (após feriados) usado para SLA: {periodo_txt}")
        logging.info(f"🗓️ Dias considerados: {dias_txt}")
        logging.info(f"📌 Datas (ISO): {', '.join([d.strftime('%Y-%m-%d') for d in datas])}")

        # 1) Ler planilhas
        df = consolidar_planilhas(PASTA_ENTRADA)
        logging.info(f"📥 Registros carregados: {df.height}")

        # 2) Padronizar nomes colunas
        df = df.rename({c: c.strip().upper() for c in df.columns})

        # 3) Garantir conversão correta da data
        df = garantir_coluna_data(df, COL_DATA_BASE)

        # 4) Fallback por dados (se vier 0 no período calculado)
        inicio, fim, datas = ajustar_periodo_por_dados(df, COL_DATA_BASE, inicio, fim, datas)
        periodo_txt = formatar_periodo(inicio, fim)
        dias_txt = formatar_lista_dias(datas)

        logging.info(f"📅 Período FINAL usado para cálculo SLA: {periodo_txt}")
        logging.info(f"🗓️ Dias considerados (FINAL): {dias_txt}")
        logging.info(f"📌 Datas (ISO): {', '.join([d.strftime('%Y-%m-%d') for d in datas])}")

        # ✅ 4.1) Separar datas Seg–Sáb vs Domingo
        datas_seg_sab, datas_domingo = separar_seg_sab_e_domingo(datas)

        periodo_txt_seg_sab = periodo_txt_de_datas(datas_seg_sab)
        dias_txt_seg_sab = formatar_lista_dias(datas_seg_sab)

        periodo_txt_domingo = periodo_txt_de_datas(datas_domingo)
        dias_txt_domingo = formatar_lista_dias(datas_domingo)

        if datas_domingo:
            logging.info(f"🧩 Separação ativa: Seg–Sáb = {dias_txt_seg_sab} | Domingo = {dias_txt_domingo}")
        else:
            logging.info("🧩 Não há domingo no período. Vai gerar apenas Seg–Sáb.")

        # 5) Detectar coluna ENTREGUE NO PRAZO
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

        # 6) Converter Y/N → 1/0
        df = df.with_columns(
            pl.when(pl.col(col_entregue).cast(pl.Utf8).str.to_uppercase() == "Y")
            .then(1)
            .otherwise(0)
            .alias("_ENTREGUE_PRAZO")
        )

        # 7) Filtrar registros do período-base (tudo do período, depois separa)
        df_periodo_all = df.filter(pl.col(COL_DATA_BASE).is_in(datas))
        logging.info(f"📊 Registros para {periodo_txt}: {df_periodo_all.height}")

        # 8) Carregar Excel dos coordenadores
        coord_df = pl.read_excel(PASTA_COORDENADOR).rename(
            {"Nome da base": "BASE DE ENTREGA", "Coordenadores": "COORDENADOR"}
        )

        # 9) Normalizar nomes de base (para join)
        df_periodo_all = df_periodo_all.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar, return_dtype=pl.Utf8).alias("BASE_NORM")
        )
        coord_df = coord_df.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar, return_dtype=pl.Utf8).alias("BASE_NORM")
        )

        # 10) JOIN
        df_periodo_all = df_periodo_all.join(coord_df, on="BASE_NORM", how="left")
        sem_coord = df_periodo_all.filter(pl.col("COORDENADOR").is_null()).height
        logging.info(f"🧩 Registros sem coordenador após join (período total): {sem_coord}")

        # ✅ 10.1) Separar DF Seg–Sáb e DF Domingo (por DATA)
        df_seg_sab = df_periodo_all.filter(pl.col(COL_DATA_BASE).is_in(datas_seg_sab)) if datas_seg_sab else pl.DataFrame()
        df_domingo = df_periodo_all.filter(pl.col(COL_DATA_BASE).is_in(datas_domingo)) if datas_domingo else pl.DataFrame()

        logging.info(f"📦 Registros Seg–Sáb: {df_seg_sab.height if hasattr(df_seg_sab, 'height') else 0}")
        logging.info(f"📦 Registros Domingo: {df_domingo.height if hasattr(df_domingo, 'height') else 0}")

        # =========================
        # ✅ PARTE A) SEG–SÁB (principal)
        # =========================
        paths_base_seg_sab = exportar_base_consolidada(df_seg_sab, tag="")  # Base_Consolidada_YYYYMMDD.*
        arquivos_md_seg_sab = montar_arquivos_gerados_md(ARQUIVO_SAIDA, paths_base_seg_sab)

        resumo_seg_sab = gerar_resumo_por_base(df_seg_sab)
        exportar_resumo_excel(resumo_seg_sab, ARQUIVO_SAIDA, prefixo="Resumo_Consolidado_")

        # =========================
        # ✅ PARTE B) DOMINGO (se existir)
        # =========================
        domingo_existe = (not df_domingo.is_empty()) if hasattr(df_domingo, "is_empty") else False

        if domingo_existe:
            paths_base_domingo = exportar_base_consolidada(df_domingo, tag="_Domingo")  # Base_Consolidada_Domingo_YYYYMMDD.*
            arquivos_md_domingo = montar_arquivos_gerados_md(ARQUIVO_SAIDA_DOMINGO, paths_base_domingo)

            resumo_domingo = gerar_resumo_por_base(df_domingo)
            exportar_resumo_excel(resumo_domingo, ARQUIVO_SAIDA_DOMINGO, prefixo="Resumo_Consolidado_Domingo_")
        else:
            resumo_domingo = pd.DataFrame()
            arquivos_md_domingo = ""

        # =========================
        # 13) Enviar cards (SEG–SÁB)
        # =========================
        for coord, webhook in COORDENADOR_WEBHOOKS.items():
            sub = resumo_seg_sab[resumo_seg_sab["COORDENADOR"] == coord] if not resumo_seg_sab.empty else pd.DataFrame()

            if sub.empty:
                logging.warning(f"⚠️ Nenhuma base encontrada para {coord} (Seg–Sáb)")
                continue

            total = float(sub["Total"].sum()) if "Total" in sub.columns else 0.0
            ent = float(sub["Entregues no Prazo"].sum()) if "Entregues no Prazo" in sub.columns else 0.0
            sla = (ent / total) if total > 0 else 0.0

            enviar_card_feishu(
                sub,
                webhook,
                coord,
                sla,
                periodo_txt_seg_sab,
                dias_txt_seg_sab,
                arquivos_md_seg_sab,
                titulo_suffix="",  # principal
            )

        # =========================
        # 14) Enviar cards (DOMINGO separado)
        # =========================
        if domingo_existe:
            for coord, webhook in COORDENADOR_WEBHOOKS.items():
                sub = resumo_domingo[resumo_domingo["COORDENADOR"] == coord] if not resumo_domingo.empty else pd.DataFrame()

                if sub.empty:
                    logging.warning(f"⚠️ Nenhuma base encontrada para {coord} (Domingo)")
                    continue

                total = float(sub["Total"].sum()) if "Total" in sub.columns else 0.0
                ent = float(sub["Entregues no Prazo"].sum()) if "Entregues no Prazo" in sub.columns else 0.0
                sla = (ent / total) if total > 0 else 0.0

                enviar_card_feishu(
                    sub,
                    webhook,
                    coord,
                    sla,
                    periodo_txt_domingo,
                    dias_txt_domingo,
                    arquivos_md_domingo,
                    titulo_suffix=" — Domingo",
                )

        logging.info("🏁 Processamento concluído (v2.15 — separa Domingo)")

    except Exception as e:
        logging.critical(f"❌ ERRO FATAL: {e}", exc_info=True)
