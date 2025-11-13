# -*- coding: utf-8 -*-

import os
import requests
import warnings
import polars as pl
import pandas as pd
import multiprocessing
import logging
import time
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ==========================================================
# 📝 CONFIGURAÇÃO DE LOGGING
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sla_franquias.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ==========================================================
# ⚙️ CONFIGURAÇÕES GERAIS
# ==========================================================
os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\SLA - Entrega Realizada"
PASTA_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Franquias\Entrega Realizada"
PASTA_ARQUIVO = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Franquias\Entrega Realizada\Arquivo"

DATA_HOJE = datetime.now().strftime("%Y%m%d")
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_{DATA_HOJE}.xlsx")

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/92a82aea-9b5c-4e3d-9169-8d4753ecef38"

LINK_PASTA = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/"
    "matheus_carvalho_jtexpressdf_onmicrosoft_com/"
    "EvIP3oIiLJRAqcB1SZ_1nmYBXLIYSJkIns5Pf_Xz2OqY_w?e=OEXsJN"
)
BASES_VALIDAS = [
    "F AGL-GO", "F ALV-AM", "F ALX-AM", "F AMB-MS", "F ANP-GO", "F APG - GO",
    "F ARQ - RO", "F BAO-PA", "F BSB - DF", "F BSB-DF", "F BSL-AC", "F CDN-AM",
    "F CEI-DF", "F CGR - MS", "F CGR 02-MS", "F CHR-AM", "F CMV-MT", "F CNC-PA",
    "F CNF-MT", "F DOM -PA", "F DOU-MS", "F ELD-PA", "F FMA-GO", "F GAI-TO",
    "F GRP-TO", "F GYN - GO", "F GYN 02-GO", "F GYN 03-GO", "F IGA-PA", "F ITI -PA",
    "F ITI-PA", "F JCD-PA", "F MCP 02-AP", "F MCP-AP", "F OCD - GO", "F OCD-GO",
    "F ORL-PA", "F PCA-PA", "F PDR-GO", "F PGM-PA", "F PLN-DF", "F PON-GO",
    "F POS-GO", "F PVH 02-RO", "F PVH-RO", "F PVL-MT", "F RDC -PA", "F RVD - GO",
    "F SEN-GO", "F SFX-PA", "F TGA-MT", "F TGT-DF", "F TLA-PA", "F TRD-GO",
    "F TUR-PA", "F VHL-RO", "F VLP-GO", "F XIG-PA", "F TRM-AM", "F STM-PA",
    "F JPN 02-RO", "F CAC-RO"
]
def cor_percentual(pct: float) -> str:
    if pct < 0.95:
        return "🔴"
    elif pct < 0.97:
        return "🟡"
    return "🟢"
def arquivar_relatorios_antigos(pasta_origem, pasta_destino, prefixo):
    os.makedirs(pasta_destino, exist_ok=True)
    for arquivo in os.listdir(pasta_origem):
        if arquivo.startswith(prefixo) and arquivo.endswith('.xlsx'):
            try:
                shutil.move(
                    os.path.join(pasta_origem, arquivo),
                    os.path.join(pasta_destino, arquivo)
                )
                logging.info(f"📦 Arquivo movido: {arquivo}")
            except Exception as e:
                logging.error(f"Erro ao mover {arquivo}: {e}")
def ler_planilha_rapido(caminho):
    try:
        if caminho.endswith(".csv"):
            return pl.read_csv(caminho)
        return pl.read_excel(caminho)
    except:
        return pl.DataFrame()


def consolidar_planilhas(pasta):
    arquivos = [
        os.path.join(pasta, f)
        for f in os.listdir(pasta)
        if f.endswith((".xlsx", ".xls", ".csv")) and not f.startswith("~$")
    ]
    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo encontrado.")

    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as ex:
        dfs = list(ex.map(ler_planilha_rapido, arquivos))

    dfs = [df for df in dfs if not df.is_empty()]
    return pl.concat(dfs, how="vertical_relaxed")
def calcular_sla(df: pl.DataFrame) -> pd.DataFrame:
    colunas = [c.upper() for c in df.columns]
    possiveis = ["ENTREGUE NO PRAZO?", "ENTREGUE NO PRAZO？"]

    col_prazo = None
    for nome in possiveis:
        if nome in colunas:
            col_prazo = df.columns[colunas.index(nome)]
            break

    if not col_prazo:
        raise KeyError(f"Coluna ENTREGUE NO PRAZO não encontrada.\nColunas: {df.columns}")

    df = df.with_columns(
        pl.when(pl.col(col_prazo).cast(pl.Utf8).str.to_uppercase() == "Y")
        .then(1)
        .otherwise(0)
        .alias("_ENTREGUE_PRAZO")
    )

    resumo = (
        df.group_by("BASE DE ENTREGA")
        .agg([
            pl.len().alias("Total"),
            pl.col("_ENTREGUE_PRAZO").sum().alias("Entregues no Prazo"),
            (pl.len() - pl.col("_ENTREGUE_PRAZO").sum()).alias("Fora do Prazo"),
            (pl.col("_ENTREGUE_PRAZO").sum() / pl.len()).alias("% SLA Cumprido")
        ])
        .sort("% SLA Cumprido")
    )

    resumo_pd = resumo.to_pandas()
    resumo_pd.rename(columns={"BASE DE ENTREGA": "Base De Entrega"}, inplace=True)

    return resumo_pd
def enviar_card_feishu(resumo_df: pd.DataFrame):
    ontem = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")
    total_bases = resumo_df["Base De Entrega"].nunique()
    media_geral = resumo_df["% SLA Cumprido"].mean()

    piores = resumo_df.sort_values("% SLA Cumprido").head(7)
    melhores = resumo_df.sort_values("% SLA Cumprido", ascending=False).head(3)

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
        f"📅 **Atualizado em:** {ontem}\n"
        f"🏢 **Bases Avaliadas:** {total_bases}\n\n"
        f"🔻 **7 Piores:**\n" + "\n".join(linhas_piores) +
        "\n\n🏆 **Top 3 Melhores:**\n" + "\n".join(linhas_melhores) +
        f"\n\n📊 **Média Geral:** {media_geral:.2%}"
    )

    payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "blue",
                "title": {"tag": "plain_text", "content": "📊 SLA Franquias (Ontem)"}
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": conteudo}},
                {"tag": "hr"},
                {"tag": "action", "actions": [
                    {"tag": "button", "text": {"tag": "plain_text", "content": "📂 Abrir Pasta"},
                     "url": LINK_PASTA, "type": "default"}
                ]}
            ]
        }
    }

    r = requests.post(WEBHOOK_URL, json=payload, timeout=15)
    if r.status_code != 200:
        logging.error(f"Erro ao enviar card: {r.text}")
    else:
        logging.info("📨 Card enviado com sucesso!")
def salvar_excel(df_dados, df_resumo):
    df_pd = df_dados.to_pandas()

    with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as w:
        df_resumo.to_excel(w, index=False, sheet_name="Resumo SLA")
        df_pd.to_excel(w, index=False, sheet_name="Dados Completos")

    logging.info(f"💾 Relatório salvo em: {ARQUIVO_SAIDA}")
if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA Franquias v2.6...")

    try:
        df = consolidar_planilhas(PASTA_ENTRADA)
        df = df.rename({c: c.strip().upper() for c in df.columns})

        # Filtrar por bases válidas
        df = df.filter(pl.col("BASE DE ENTREGA").is_in([b.upper() for b in BASES_VALIDAS]))

        # Calcular SLA
        resumo_pd = calcular_sla(df)

        # Arquivar relatórios antigos
        arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, "Resumo_Consolidado_")

        # Salvar Excel
        salvar_excel(df, resumo_pd)

        # Enviar card
        enviar_card_feishu(resumo_pd)

        logging.info("🏁 Processo finalizado com sucesso (v2.6 Franquias).")

    except Exception as e:
        logging.critical(f"❌ Erro fatal: {e}", exc_info=True)
