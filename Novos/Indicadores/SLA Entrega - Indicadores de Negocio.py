# -*- coding: utf-8 -*-

import os
import requests
import warnings
import polars as pl
import pandas as pd
import multiprocessing
import logging
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("sla_franquias.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# CONFIG
os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

# PASTAS
PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Indicadores de Negocio\SLA Entrega"
PASTA_SAIDA   = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Indicadores de Negocio\SLA Entrega"
PASTA_ARQUIVO = os.path.join(PASTA_SAIDA, "Arquivo")

DATA_HOJE = datetime.now().strftime("%Y%m%d")
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_{DATA_HOJE}.xlsx")

# CARD
LINK_PASTA = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/"
    "matheus_carvalho_jtexpressdf_onmicrosoft_com/"
    "IgCARpXmNw6wSpHnywwNSSl8AVmzz434V7O5cFKYxUNHVpI?e=pue9Kk"
)

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/77c4243e-6876-4e1f-ab96-59003f733dce"

# COORDENADORES
ARQ_COORDENADORES = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
def cor_percentual(pct: float) -> str:
    if pct < 0.95:
        return "🔴"
    elif pct < 0.97:
        return "🟡"
    return "🟢"


def arquivar_relatorios_antigos(pasta_origem, pasta_destino, prefixo):
    os.makedirs(pasta_destino, exist_ok=True)
    for arquivo in os.listdir(pasta_origem):
        if arquivo.startswith(prefixo) and arquivo.endswith(".xlsx"):
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
        raise FileNotFoundError("Nenhum arquivo encontrado na pasta de entrada.")

    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as ex:
        dfs = list(ex.map(ler_planilha_rapido, arquivos))

    dfs = [df for df in dfs if not df.is_empty()]

    return pl.concat(dfs, how="vertical_relaxed")
def calcular_sla_completo(df: pl.DataFrame) -> pd.DataFrame:

    colunas = [c.upper() for c in df.columns]
    possiveis = ["ENTREGUE NO PRAZO?", "ENTREGUE NO PRAZO？"]

    col_prazo = None
    for nome in possiveis:
        if nome in colunas:
            col_prazo = df.columns[colunas.index(nome)]
            break

    if not col_prazo:
        raise KeyError("Coluna ENTREGUE NO PRAZO não encontrada.")

    df = df.with_columns(
        pl.when(pl.col(col_prazo).cast(pl.Utf8).str.to_uppercase() == "Y")
        .then(1)
        .otherwise(0)
        .alias("_ENTREGUE_PRAZO")
    )

    resumo = (
        df.group_by("BASE DE ENTREGA")
        .agg([
            pl.len().alias("Qtd Entrega"),
            pl.col("_ENTREGUE_PRAZO").sum().alias("Qtd Entregue"),
            (pl.col("_ENTREGUE_PRAZO").sum() / pl.len()).alias("SLA")
        ])
    )

    base_coord = pd.read_excel(ARQ_COORDENADORES)
    base_coord.rename(columns={
        "Nome da base": "BASE DE ENTREGA",
        "Coordenadores": "Coordenador"
    }, inplace=True)

    resumo_pd = resumo.to_pandas()

    resumo_pd = resumo_pd.merge(
        base_coord,
        on="BASE DE ENTREGA",
        how="left"
    )

    resumo_pd = resumo_pd.sort_values("SLA", ascending=True)
    resumo_pd.insert(0, "Ranking", range(1, len(resumo_pd) + 1))

    resumo_pd.rename(columns={"BASE DE ENTREGA": "Base"}, inplace=True)

    return resumo_pd
def enviar_card_feishu(resumo_df: pd.DataFrame):

    ontem = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")
    total_bases = resumo_df["Base"].nunique()
    media_geral = resumo_df["SLA"].mean()

    piores = resumo_df.sort_values("SLA").head(7)
    melhores = resumo_df.sort_values("SLA", ascending=False).head(3)

    linhas_piores = [
        f"{i}. {cor_percentual(l['SLA'])} **{l['Base']}** — {l['SLA']:.2%}"
        for i, l in enumerate(piores.to_dict("records"), 1)
    ]

    medalhas = ["🥇", "🥈", "🥉"]
    linhas_melhores = [
        f"{medalhas[i-1]} {cor_percentual(l['SLA'])} **{l['Base']}** — {l['SLA']:.2%}"
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
                "title": {"tag": "plain_text", "content": "📊 SLA Bases (Ontem)"}
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": conteudo}},
                {"tag": "hr"},
                {"tag": "action", "actions": [
                    {"tag": "button",
                     "text": {"tag": "plain_text", "content": "📂 Abrir Pasta"},
                     "url": LINK_PASTA,
                     "type": "default"}
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

        ws = w.sheets["Resumo SLA"]

        for idx, col in enumerate(df_resumo.columns, start=1):
            if col.strip().upper() == "SLA":
                col_letra = ws.cell(row=1, column=idx).column_letter
                break

        for cell in ws[f"{col_letra}"][1:]:
            cell.number_format = "0.00%"

    logging.info(f"💾 Relatório salvo em: {ARQUIVO_SAIDA}")
if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA Franquias v3.0...")

    try:
        df = consolidar_planilhas(PASTA_ENTRADA)

        df = df.rename({c: c.strip().upper() for c in df.columns})

        # REMOVER COLUNAS INDESEJADAS
        colunas_remover = ["UNNAMED: 0", "UF", "FILIAL"]
        df = df.drop([c for c in colunas_remover if c in df.columns])

        resumo_pd = calcular_sla_completo(df)

        arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, "Resumo_Consolidado_")

        salvar_excel(df, resumo_pd)

        enviar_card_feishu(resumo_pd)

        logging.info("🏁 Processo finalizado com sucesso (v3.0 Franquias).")

    except Exception as e:
        logging.critical(f"❌ Erro fatal: {e}", exc_info=True)
