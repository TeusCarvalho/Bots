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

# ==========================================================
# 📝 CONFIGURAÇÃO DE LOGGING
# ==========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sla_processor.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ==========================================================
# ⚙️ CONFIGURAÇÕES GERAIS
# ==========================================================
os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\SLA - Entrega Realizada"
PASTA_COORDENADOR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
PASTA_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\SLA - Entrega Realizada"
PASTA_ARQUIVO = os.path.join(PASTA_SAIDA, "Arquivo Morto")

DATA_HOJE = datetime.now().strftime("%Y%m%d")
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_{DATA_HOJE}.xlsx")

LINK_PASTA = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EvIP3oIiLJRAqcB1SZ_1nmYBXLIYSJkIns5Pf_Xz2OqY_w?e=OEXsJN"

# ==========================================================
# 🧭 WEBHOOKS POR COORDENADOR (teste)
# ==========================================================
#WEBHOOK_TESTE = "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b"

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
}

# ==========================================================
# ⚡️ FUNÇÕES AUXILIARES
# ==========================================================
def cor_percentual(pct: float) -> str:
    if pct < 0.95:
        return "🔴"
    elif pct < 0.97:
        return "🟡"
    else:
        return "🟢"


def arquivar_relatorios_antigos(pasta_origem: str, pasta_destino: str, prefixo_arquivo: str):
    os.makedirs(pasta_destino, exist_ok=True)
    arquivos = [f for f in os.listdir(pasta_origem) if f.startswith(prefixo_arquivo) and f.endswith('.xlsx')]
    for arquivo in arquivos:
        try:
            shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo))
            logging.info(f"📦 Relatório antigo movido: {arquivo}")
        except Exception as e:
            logging.error(f"Erro ao mover o arquivo '{arquivo}': {e}")


def ler_planilha_rapido(caminho):
    try:
        if caminho.endswith(".csv"):
            return pl.read_csv(caminho)
        return pl.read_excel(caminho)
    except Exception as e:
        logging.error(f"Falha ao ler {os.path.basename(caminho)}: {e}")
        return pl.DataFrame()


def consolidar_planilhas(pasta_entrada: str) -> pl.DataFrame:
    arquivos = [os.path.join(pasta_entrada, f)
                for f in os.listdir(pasta_entrada)
                if f.endswith((".xlsx", ".xls", ".csv")) and not f.startswith("~$")]
    if not arquivos:
        raise FileNotFoundError("❌ Nenhum arquivo Excel/CSV encontrado na pasta de entrada.")

    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        dfs = list(executor.map(ler_planilha_rapido, arquivos))

    dfs_validos = [df for df in dfs if not df.is_empty()]
    if not dfs_validos:
        raise ValueError("Nenhum arquivo pôde ser lido com sucesso.")

    return pl.concat(dfs_validos, how="vertical_relaxed")


def detectar_coluna_entregue(df: pl.DataFrame) -> str:
    for col in df.columns:
        if "entregue" in col.lower():
            return col
    raise KeyError("❌ Coluna de status de entrega não encontrada.")


def calcular_sla(df: pl.DataFrame, col_entregue: str) -> float:
    if df.is_empty():
        return 0.0
    total = df.height
    entregues = (df[col_entregue] == "Y").sum()
    return entregues / total if total > 0 else 0.0


def salvar_relatorio_completo(df_dados: pl.DataFrame, df_resumo: pd.DataFrame, caminho_arquivo: str):
    try:
        os.makedirs(os.path.dirname(caminho_arquivo), exist_ok=True)
        with pd.ExcelWriter(caminho_arquivo, engine='openpyxl') as writer:
            df_resumo.to_excel(writer, index=False, sheet_name='Resumo SLA')
            df_dados.to_pandas().to_excel(writer, index=False, sheet_name='Dados Completos')
        logging.info(f"📄 Relatório salvo em: {caminho_arquivo}")
    except Exception as e:
        logging.error(f"⚠️ Falha ao salvar o relatório: {e}")


def enviar_card_feishu(resumo_df: pd.DataFrame, webhook: str, coordenador: str, sla_atual: float, sla_anterior: float):
    try:
        data_geracao = datetime.now().strftime("%d/%m/%Y %H:%M")
        bases_coord = resumo_df["Base De Entrega"].nunique()

        piores = resumo_df.sort_values(by="% Entregues").head(3)
        melhores = resumo_df.sort_values(by="% Entregues", ascending=False).head(3)

        linhas_piores = [
            f"{i}. {cor_percentual(row['% Entregues'])} **{row['Base De Entrega']}** — {row['% Entregues']:.2%}"
            for i, row in enumerate(piores.to_dict('records'), 1)
        ]
        medalhas = ["🥇", "🥈", "🥉"]
        linhas_melhores = [
            f"{medalhas[i - 1]} {cor_percentual(row['% Entregues'])} **{row['Base De Entrega']}** — {row['% Entregues']:.2%}"
            for i, row in enumerate(melhores.to_dict('records'), 1)
        ]

        conteudo = (
            f"👤 **Coordenador:** {coordenador}\n"
            f"📅 **Atualizado em:** {data_geracao}\n"
            f"🏢 **Bases Avaliadas:** {bases_coord}\n"
            f"📈 **SLA do dia atual:** {sla_atual:.2%}\n"
            f"📉 **SLA do dia anterior:** {sla_anterior:.2%}\n\n"
            f"🔻 **3 Piores SLAs:**\n" + "\n".join(linhas_piores) +
            "\n\n🏆 **Top 3 Melhores:**\n" + "\n".join(linhas_melhores)
        )

        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "template": "blue",
                    "title": {"tag": "plain_text", "content": f"📊 SLA - Entrega Realizada ({coordenador})"}
                },
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content": conteudo}},
                    {"tag": "hr"},
                    {"tag": "action", "actions": [
                        {"tag": "button",
                         "text": {"tag": "plain_text", "content": "📂 Abrir Pasta no OneDrive"},
                         "url": LINK_PASTA,
                         "type": "default"}
                    ]}
                ]
            }
        }

        resp = requests.post(webhook, json=payload, timeout=10)
        resp.raise_for_status()
        logging.info(f"✅ Card enviado para {coordenador}")
    except Exception as e:
        logging.error(f"🚨 Falha ao enviar card para {coordenador}: {e}")

# ==========================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ==========================================================
if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA por coordenador...")

    try:
        df = consolidar_planilhas(PASTA_ENTRADA)
        logging.info(f"Total de {df.height} registros lidos.")

        df = df.rename({c: c.strip().upper() for c in df.columns})
        col_entregue = detectar_coluna_entregue(df)

        # === Conversão de data ===
        if "DATA PREVISTA DE ENTREGA" not in df.columns:
            raise KeyError("❌ Coluna 'DATA PREVISTA DE ENTREGA' não encontrada.")

        tipo = df["DATA PREVISTA DE ENTREGA"].dtype
        if tipo == pl.Utf8:
            df = df.with_columns(pl.col("DATA PREVISTA DE ENTREGA").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
        elif tipo == pl.Datetime:
            df = df.with_columns(pl.col("DATA PREVISTA DE ENTREGA").dt.date().alias("DATA PREVISTA DE ENTREGA"))

        hoje = datetime.now().date()
        ontem = hoje - timedelta(days=1)

        df_hoje = df.filter(pl.col("DATA PREVISTA DE ENTREGA") == hoje)
        df_ontem = df.filter(pl.col("DATA PREVISTA DE ENTREGA") == ontem)

        sla_geral_hoje = calcular_sla(df_hoje, col_entregue)
        sla_geral_ontem = calcular_sla(df_ontem, col_entregue)

        logging.info(f"SLA geral hoje: {sla_geral_hoje:.2%} | ontem: {sla_geral_ontem:.2%}")

        # === JOIN com Coordenadores ===
        coord_df = pl.read_excel(PASTA_COORDENADOR).rename({
            "Nome da base": "BASE DE ENTREGA",
            "Coordenadores": "COORDENADOR"
        })
        df = df.join(coord_df, on="BASE DE ENTREGA", how="left")

        resumo = (
            df_hoje.join(coord_df, on="BASE DE ENTREGA", how="left")
            .group_by(["BASE DE ENTREGA", "COORDENADOR"])
            .agg([
                pl.count().alias("Total"),
                (pl.col(col_entregue) == "Y").sum().alias("Entregues"),
                ((pl.col(col_entregue) != "Y") | pl.col(col_entregue).is_null()).sum().alias("Nao_Entregues"),
            ])
            .with_columns((pl.col("Entregues") / pl.col("Total")).alias("% Entregues"))
            .sort("% Entregues")
        )

        resumo_pd = resumo.to_pandas().rename(columns={"BASE DE ENTREGA": "Base De Entrega"})

        arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, "Resumo_Consolidado_")
        salvar_relatorio_completo(df, resumo_pd, ARQUIVO_SAIDA)

        for coordenador, webhook in COORDENADOR_WEBHOOKS.items():
            sub_df = resumo_pd[resumo_pd["COORDENADOR"] == coordenador]
            if not sub_df.empty:
                enviar_card_feishu(sub_df, webhook, coordenador, sla_geral_hoje, sla_geral_ontem)

        logging.info("🏁 Processamento concluído com sucesso!")

    except Exception as e:
        logging.critical(f"Erro fatal no processamento: {e}", exc_info=True)
