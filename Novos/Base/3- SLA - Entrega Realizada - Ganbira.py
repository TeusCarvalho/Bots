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
from typing import List, Optional, Tuple

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
PASTA_ARQUIVO = os.path.join(PASTA_SAIDA, "Arquivo Morto")

DATA_HOJE = datetime.now().strftime("%Y%m%d")
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_{DATA_HOJE}.xlsx")

LINK_PASTA = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/"
    "matheus_carvalho_jtexpressdf_onmicrosoft_com/"
    "EvIP3oIiLJRAqcB1SZ_1nmYBXLIYSJkIns5Pf_Xz2OqY_w?e=OEXsJN"
)

COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/3663dd30-722c-45d6-9e3c-1d4e2838f112",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/0b907801-c73e-4de8-9f84-682d7b54f6fd",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/261cefd4-5528-4760-b18e-49a0249718c7",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/b749fd36-d287-460e-b1e2-c78bfb4c1946",
    "Odária Fereira": "https://open.feishu.cn/open-apis/bot/v2/hook/48c4db73-b5a4-4007-96af-f5d28301f0c1",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/606ed22b-dc49-451d-9bfe-0a8829dbe76e",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/840f79b0-1eff-42fe-aae0-433c9edbad80",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/95c8e4d2-27aa-4811-b6bf-ebf99cdfd42d",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/63751a67-efe8-40e4-b841-b290a4819836",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/3ddc5962-2d32-4b2d-92d9-a4bc95ac3393",
    "Ana Cunha": "https://open.feishu.cn/open-apis/bot/v2/hook/b2ec868f-3149-4808-af53-9e0c6d2cd94e",
}

EXTS = (".xlsx", ".xls", ".csv")
COL_DATA_BASE = "DATA PREVISTA DE ENTREGA"
# =========================
# BLOCO 2/4 — FUNÇÕES (PERÍODO / LEITURA / DATA / FALLBACK)
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


# ============================================================
# 🗓️ Período inteligente
# - Seg: considera Sex+Sáb+Dom
# - Ter–Sex: considera Ontem
# - Sáb/Dom: não roda
# ============================================================
def calcular_periodo_base() -> Optional[Tuple[date, date, List[date]]]:
    hoje = datetime.now().date()
    dia = hoje.weekday()  # 0=Seg, 1=Ter, ..., 5=Sáb, 6=Dom

    if dia in (5, 6):
        logging.warning("⛔ Hoje é sábado ou domingo. Execução cancelada.")
        return None

    fim = hoje - timedelta(days=1)  # fecha sempre em "ontem"

    if dia == 0:  # segunda
        inicio = hoje - timedelta(days=3)  # sexta
    else:
        inicio = fim  # ontem

    dias = (fim - inicio).days
    datas = [inicio + timedelta(days=i) for i in range(dias + 1)]
    return inicio, fim, datas


def formatar_periodo(inicio: date, fim: date) -> str:
    if inicio == fim:
        return inicio.strftime("%d/%m/%Y")
    return f"{inicio.strftime('%d/%m/%Y')} a {fim.strftime('%d/%m/%Y')}"


def formatar_lista_dias(datas: List[date]) -> str:
    dias_pt = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    partes = [f"{dias_pt[d.weekday()]} {d.strftime('%d/%m')}" for d in datas]
    return ", ".join(partes)


def cor_percentual(p: float) -> str:
    if p < 0.95:
        return "🔴"
    elif p < 0.97:
        return "🟡"
    return "🟢"


def arquivar_relatorios_antigos(pasta_origem: str, pasta_destino: str, prefixo: str) -> None:
    os.makedirs(pasta_destino, exist_ok=True)
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


# ============================================================
# ✅ Converte "26/12/2025  23:59:59" (espaço duplo) -> Date
# ============================================================
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


# ============================================================
# ✅ NOVO: Fallback automático de período
# Se o período calculado não tiver registros, recua para a última data disponível (<= fim).
# Resolve: feriados (ex.: 01/01) ou dia sem operação.
# ============================================================
def ajustar_periodo_por_dados(
    df: pl.DataFrame,
    coluna_data: str,
    inicio: date,
    fim: date,
    datas: List[date],
) -> Tuple[date, date, List[date]]:
    if df.is_empty() or coluna_data not in df.columns:
        return inicio, fim, datas

    # 1) Se já tem registros, mantém
    try:
        qtd = df.filter(pl.col(coluna_data).is_in(datas)).height
        if qtd > 0:
            return inicio, fim, datas
    except Exception:
        # se algo falhar aqui, tenta fallback do mesmo jeito
        pass

    # 2) Busca a maior data <= fim
    max_le = None
    try:
        max_le = (
            df.filter(pl.col(coluna_data).is_not_null() & (pl.col(coluna_data) <= fim))
            .select(pl.col(coluna_data).max())
            .item()
        )
    except Exception:
        max_le = None

    # 3) Se não existir <= fim, pega a maior data geral
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

    # Mantém o mesmo "span" do período original (ex.: segunda -> 3 dias)
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
        f"Aplicando fallback para última data disponível: {formatar_periodo(novo_inicio, novo_fim)}."
    )

    return novo_inicio, novo_fim, novo_datas
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
) -> bool:
    try:
        if resumo.empty:
            logging.warning(f"⚠️ Nenhuma base para {coord}")
            return False

        bases = resumo["Base De Entrega"].nunique()

        piores = resumo.sort_values("% SLA Cumprido").head(3)
        melhores = resumo.sort_values("% SLA Cumprido", ascending=False).head(3)

        linhas_piores = [
            f"{i}. {cor_percentual(l['% SLA Cumprido'])} **{l['Base De Entrega']}** — {l['% SLA Cumprido']:.2%}"
            for i, l in enumerate(piores.to_dict("records"), 1)
        ]

        medalhas = ["🥇", "🥈", "🥉"]
        linhas_melhores = [
            f"{medalhas[i-1]} {cor_percentual(l['% SLA Cumprido'])} **{l['Base De Entrega']}** — {l['% SLA Cumprido']:.2%}"
            for i, l in enumerate(melhores.to_dict("records"), 1)
        ]

        conteudo = (
            f"👤 **Coordenador:** {coord}\n"
            f"📅 **Período:** {periodo_txt}\n"
            f"🗓️ **Dias considerados:** {dias_txt}\n"
            f"📈 **SLA (Período):** {sla:.2%}\n"
            f"🏢 **Bases analisadas:** {bases}\n\n"
            f"🔻 **3 Piores:**\n" + "\n".join(linhas_piores) +
            "\n\n🏆 **3 Melhores:**\n" + "\n".join(linhas_melhores)
        )

        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "template": "blue",
                    "title": {"tag": "plain_text", "content": f"SLA - Entrega no Prazo — {coord}"},
                },
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content": conteudo}},
                    {"tag": "hr"},
                    {
                        "tag": "action",
                        "actions": [
                            {
                                "tag": "button",
                                "text": {"tag": "plain_text", "content": "📂 Abrir Pasta"},
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
                f"❌ ERRO ao enviar card para {coord}. Status: {r.status_code}. Resposta: {r.text}"
            )
            return False

        logging.info(f"📨 Card enviado para {coord}")
        return True

    except Exception as e:
        logging.error(f"❌ Falha no envio para {coord}. Erro: {e}. Webhook: {webhook}")
        return False
# =========================
# BLOCO 4/4 — MAIN (v2.12)
# =========================

if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA (v2.12)...")

    try:
        # 0) Período-base (calendário)
        periodo = calcular_periodo_base()
        if periodo is None:
            raise SystemExit(0)

        inicio, fim, datas = periodo
        logging.info(f"📅 Período (calendário) inicial: {formatar_periodo(inicio, fim)}")
        logging.info(f"🗓️ Dias (calendário) iniciais: {formatar_lista_dias(datas)}")

        # 1) Ler planilhas
        df = consolidar_planilhas(PASTA_ENTRADA)
        logging.info(f"📥 Registros carregados: {df.height}")

        # 2) Padronizar nomes colunas
        df = df.rename({c: c.strip().upper() for c in df.columns})

        # 3) Garantir conversão correta da data
        df = garantir_coluna_data(df, COL_DATA_BASE)

        # 4) Fallback: se o período calculado vier vazio, ajusta para última data disponível (<= fim)
        inicio, fim, datas = ajustar_periodo_por_dados(df, COL_DATA_BASE, inicio, fim, datas)
        periodo_txt = formatar_periodo(inicio, fim)
        dias_txt = formatar_lista_dias(datas)

        logging.info(f"📅 Período FINAL usado para cálculo SLA: {periodo_txt}")
        logging.info(f"🗓️ Dias considerados (FINAL): {dias_txt}")
        logging.info(f"📌 Datas (ISO): {', '.join([d.strftime('%Y-%m-%d') for d in datas])}")

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

        # 7) Filtrar registros do período-base usando DATA PREVISTA DE ENTREGA (Date)
        df_periodo = df.filter(pl.col(COL_DATA_BASE).is_in(datas))
        logging.info(f"📊 Registros para {periodo_txt}: {df_periodo.height}")

        # 8) Carregar Excel dos coordenadores
        coord_df = pl.read_excel(PASTA_COORDENADOR).rename(
            {"Nome da base": "BASE DE ENTREGA", "Coordenadores": "COORDENADOR"}
        )

        # 9) Normalizar nomes de base
        df_periodo = df_periodo.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar).alias("BASE_NORM")
        )
        coord_df = coord_df.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar).alias("BASE_NORM")
        )

        # 10) JOIN
        df_periodo = df_periodo.join(coord_df, on="BASE_NORM", how="left")
        sem_coord = df_periodo.filter(pl.col("COORDENADOR").is_null()).height
        logging.info(f"🧩 Registros sem coordenador após join: {sem_coord}")

        # 11) Resumo por base + coordenador (no período)
        if df_periodo.is_empty():
            resumo_pd = pd.DataFrame(
                columns=[
                    "Base De Entrega",
                    "COORDENADOR",
                    "Total",
                    "Entregues no Prazo",
                    "Fora do Prazo",
                    "% SLA Cumprido",
                ]
            )
        else:
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

            resumo_pd = resumo.to_pandas().rename(columns={"BASE DE ENTREGA": "Base De Entrega"})

        # 12) Exportar Excel
        arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, "Resumo_Consolidado_")
        with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as w:
            resumo_pd.to_excel(w, index=False, sheet_name="Resumo SLA")

        # 13) Enviar cards (SLA ponderado por volume)
        for coord, webhook in COORDENADOR_WEBHOOKS.items():
            sub = resumo_pd[resumo_pd["COORDENADOR"] == coord]

            if sub.empty:
                logging.warning(f"⚠️ Nenhuma base encontrada para {coord}")
                continue

            total = float(sub["Total"].sum()) if "Total" in sub.columns else 0.0
            ent = float(sub["Entregues no Prazo"].sum()) if "Entregues no Prazo" in sub.columns else 0.0
            sla = (ent / total) if total > 0 else 0.0

            enviar_card_feishu(sub, webhook, coord, sla, periodo_txt, dias_txt)

        logging.info("🏁 Processamento concluído (v2.12)")

    except Exception as e:
        logging.critical(f"❌ ERRO FATAL: {e}", exc_info=True)
