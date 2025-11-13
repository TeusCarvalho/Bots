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
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sla_processor.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

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
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
    # adicione outros quando desejar
}

EXTS = (".xlsx", ".xls", ".csv")

def cor_percentual(p: float) -> str:
    if p < 0.95:
        return "🔴"
    elif p < 0.97:
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


def consolidar_planilhas(pasta_entrada):
    arquivos = [
        os.path.join(pasta_entrada, f)
        for f in os.listdir(pasta_entrada)
        if f.endswith(EXTS) and not f.startswith("~$")
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
        formatos = [
            "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y",
            "%d/%m/%Y %H:%M:%S"
        ]
        expr = None
        for f in formatos:
            tentativa = pl.col(coluna).str.strptime(pl.Datetime, f, strict=False)
            expr = tentativa if expr is None else expr.fill_null(tentativa)
        return df.with_columns(expr.dt.date().alias(coluna))

    raise TypeError(f"Tipo inválido para coluna '{coluna}': {tipo}")
def enviar_card_feishu(resumo: pd.DataFrame, webhook: str, coord: str, sla: float):
    try:
        if resumo.empty:
            logging.warning(f"⚠️ Nenhuma base para {coord}")
            return False

        data_card = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

        bases = resumo["Base De Entrega"].nunique()

        piores = resumo.sort_values("% SLA Cumprido").head(3)
        melhores = resumo.sort_values("% SLA Cumprido", ascending=False).head(3)

        linhas_piores = [
            f"{i}. {cor_percentual(l['% SLA Cumprido'])} **{l['Base De Entrega']}** — {l['% SLA Cumprido']:.2%}"
            for i, l in enumerate(piores.to_dict('records'), 1)
        ]

        medalhas = ["🥇", "🥈", "🥉"]
        linhas_melhores = [
            f"{medalhas[i-1]} {cor_percentual(l['% SLA Cumprido'])} **{l['Base De Entrega']}** — {l['% SLA Cumprido']:.2%}"
            for i, l in enumerate(melhores.to_dict('records'), 1)
        ]

        conteudo = (
            f"👤 **Coordenador:** {coord}\n"
            f"📅 **Atualizado em:** {data_card}\n"
            f"📈 **SLA (Ontem):** {sla:.2%}\n"
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
                    "title": {"tag": "plain_text",
                              "content": f"SLA - Entrega no Prazo (Ontem) — {coord}"}
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

        r = requests.post(webhook, json=payload, timeout=15)

        if r.status_code != 200:
            logging.error(
                f"❌ ERRO ao enviar card para {coord}. "
                f"Status: {r.status_code}. Resposta: {r.text}"
            )
            return False

        logging.info(f"📨 Card enviado para {coord}")
        return True

    except Exception as e:
        logging.error(
            f"❌ Falha no envio para {coord}. Erro: {e}. Webhook: {webhook}"
        )
        return False
if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA (v2.5)...")

    try:
        # 1) Ler planilhas
        df = consolidar_planilhas(PASTA_ENTRADA)
        logging.info(f"📥 Registros carregados: {df.height}")

        # 2) Padronizar nomes
        df = df.rename({c: c.strip().upper() for c in df.columns})

        # 3) Garantir data prevista
        df = garantir_coluna_data(df, "DATA PREVISTA DE ENTREGA")

        # 4) Detectar coluna ENTREGUE NO PRAZO
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

        # 5) Normalizar Y
        df = df.with_columns(
            pl.when(pl.col(col_entregue).cast(pl.Utf8).str.to_uppercase() == "Y")
            .then(1).otherwise(0).alias("_ENTREGUE_PRAZO")
        )

        hoje = datetime.now().date()
        ontem = hoje - timedelta(days=1)

        df_ontem = df.filter(pl.col("DATA PREVISTA DE ENTREGA") == ontem)

        # 6) Coordenadores
        coord_df = pl.read_excel(PASTA_COORDENADOR).rename({
            "Nome da base": "BASE DE ENTREGA",
            "Coordenadores": "COORDENADOR"
        })

        df_ontem = df_ontem.join(coord_df, on="BASE DE ENTREGA", how="left")

        # 7) Resumo
        if df_ontem.is_empty():
            resumo_pd = pd.DataFrame(columns=[
                "Base De Entrega", "COORDENADOR",
                "Total", "Entregues no Prazo", "Fora do Prazo", "% SLA Cumprido"
            ])
        else:
            resumo = (
                df_ontem.group_by(["BASE DE ENTREGA", "COORDENADOR"])
                .agg([
                    pl.len().alias("Total"),
                    pl.col("_ENTREGUE_PRAZO").sum().alias("Entregues no Prazo"),
                    (pl.len() - pl.col("_ENTREGUE_PRAZO").sum()).alias("Fora do Prazo"),
                    (pl.col("_ENTREGUE_PRAZO").sum() / pl.len()).alias("% SLA Cumprido")
                ])
                .sort("% SLA Cumprido", descending=True)
            )
            resumo_pd = resumo.to_pandas().rename(columns={
                "BASE DE ENTREGA": "Base De Entrega"
            })

        # 8) Exportar Excel
        arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, "Resumo_Consolidado_")

        with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as w:
            resumo_pd.to_excel(w, index=False, sheet_name="Resumo SLA")

        # 9) Enviar card
        for coord, webhook in COORDENADOR_WEBHOOKS.items():
            sub = resumo_pd[resumo_pd["COORDENADOR"] == coord]

            if sub.empty:
                logging.warning(f"⚠️ Nenhuma base encontrada para {coord}")
                continue

            sla = sub["% SLA Cumprido"].mean()
            enviar_card_feishu(sub, webhook, coord, sla)

        logging.info("🏁 Processamento concluído (v2.5)")

    except Exception as e:
        logging.critical(f"❌ ERRO FATAL: {e}", exc_info=True)
