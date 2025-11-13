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

os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

# ==========================================================
# ⚙️ CONFIGURAÇÕES GERAIS
# ==========================================================
PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\SLA - Entrega Realizada"
PASTA_COORDENADOR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
PASTA_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\SLA - Entrega Realizada"
PASTA_ARQUIVO = os.path.join(PASTA_SAIDA, "Arquivo Morto")

DATA_HOJE = datetime.now().strftime("%Y%m%d")
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_{DATA_HOJE}.xlsx")

LINK_PASTA = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EvIP3oIiLJRAqcB1SZ_1nmYBXLIYSJkIns5Pf_Xz2OqY_w?e=OEXsJN"

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

EXTS = (".xlsx", ".xls", ".csv")


def cor_percentual(pct: float) -> str:
    if pct < 0.95:
        return "🔴"
    elif pct < 0.97:
        return "🟡"
    return "🟢"
def arquivar_relatorios_antigos(pasta_origem: str, pasta_destino: str, prefixo_arquivo: str):
    os.makedirs(pasta_destino, exist_ok=True)
    for arquivo in os.listdir(pasta_origem):
        if arquivo.startswith(prefixo_arquivo) and arquivo.endswith(".xlsx"):
            try:
                shutil.move(
                    os.path.join(pasta_origem, arquivo),
                    os.path.join(pasta_destino, arquivo)
                )
                logging.info(f"📦 Relatório antigo movido: {arquivo}")
            except Exception as e:
                logging.error(f"Erro ao mover '{arquivo}': {e}")


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
        if f.endswith(EXTS) and not f.startswith("~$")
    ]

    if not arquivos:
        raise FileNotFoundError("❌ Nenhum arquivo Excel/CSV encontrado.")

    with ThreadPoolExecutor(max_workers=min(16, len(arquivos))) as executor:
        dfs = list(executor.map(ler_planilha_rapido, arquivos))

    dfs_validos = [df for df in dfs if not df.is_empty()]
    if not dfs_validos:
        raise ValueError("Nenhum arquivo pôde ser lido com sucesso.")

    return pl.concat(dfs_validos, how="vertical_relaxed")
def garantir_coluna_data(df: pl.DataFrame, nome_coluna: str) -> pl.DataFrame:
    if nome_coluna not in df.columns:
        raise KeyError(f"Coluna '{nome_coluna}' não encontrada.")

    tipo = df[nome_coluna].dtype

    if tipo == pl.Date:
        return df

    if tipo == pl.Datetime:
        return df.with_columns(
            pl.col(nome_coluna).dt.date().alias(nome_coluna)
        )

    if tipo == pl.Utf8:
        formatos = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%d/%m/%Y %H:%M:%S"
        ]
        expr = None
        for f in formatos:
            tentativa = pl.col(nome_coluna).str.strptime(pl.Datetime, f, strict=False)
            expr = tentativa if expr is None else expr.fill_null(tentativa)
        return df.with_columns(expr.dt.date().alias(nome_coluna))

    raise TypeError(f"⚠️ Tipo inesperado para '{nome_coluna}': {tipo}")
def enviar_card_feishu(resumo_df: pd.DataFrame, webhook: str, coordenador: str, sla_ontem: float) -> bool:
    try:
        if resumo_df.empty:
            return False

        data_geracao = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

        bases_coord = resumo_df["Base De Entrega"].nunique()

        piores = resumo_df.sort_values(by="% SLA Cumprido").head(3)
        melhores = resumo_df.sort_values(by="% SLA Cumprido", ascending=False).head(3)

        linhas_piores = [
            f"{i}. {cor_percentual(r['% SLA Cumprido'])} **{r['Base De Entrega']}** — {r['% SLA Cumprido']:.2%}"
            for i, r in enumerate(piores.to_dict('records'), 1)
        ]

        medalhas = ["🥇", "🥈", "🥉"]
        linhas_melhores = [
            f"{medalhas[i-1]} {cor_percentual(r['% SLA Cumprido'])} **{r['Base De Entrega']}** — {r['% SLA Cumprido']:.2%}"
            for i, r in enumerate(melhores.to_dict('records'), 1)
        ]

        conteudo = (
            f"👤 **Coordenador:** {coordenador}\n"
            f"📅 **Atualizado em:** {data_geracao}\n"
            f"🏢 **Bases Avaliadas:** {bases_coord}\n"
            f"📈 **SLA (Ontem):** {sla_ontem:.2%}\n\n"
            f"🔻 **3 Piores Bases:**\n" + "\n".join(linhas_piores) +
            "\n\n🏆 **Top 3 Melhores:**\n" + "\n".join(linhas_melhores)
        )

        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "template": "blue",
                    "title": {
                        "tag": "plain_text",
                        "content": f"📊 SLA - Entrega no Prazo (Ontem) — {coordenador}"
                    }
                },
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content": conteudo}},
                    {"tag": "hr"},
                    {
                        "tag": "action",
                        "actions": [
                            {
                                "tag": "button",
                                "text": {"tag": "plain_text", "content": "📂 Abrir Pasta no OneDrive"},
                                "url": LINK_PASTA,
                                "type": "default"
                            }
                        ]
                    }
                ]
            }
        }

        r = requests.post(webhook, json=payload, timeout=12)
        return r.status_code == 200

    except Exception as e:
        logging.error(f"❌ Erro ao enviar card para {coordenador}: {e}")
        return False
if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento de SLA (v2.4)...")

    try:
        # ==========================================================
        # 1) LER E PADRONIZAR COLUNAS
        # ==========================================================
        df = consolidar_planilhas(PASTA_ENTRADA)
        logging.info(f"📥 Total de {df.height} registros lidos.")

        # padroniza nomes: tira espaços e deixa UPPER
        df = df.rename({c: c.strip().upper() for c in df.columns})

        # garante data prevista
        df = garantir_coluna_data(df, "DATA PREVISTA DE ENTREGA")

        # ==========================================================
        # 2) DETECTAR COLUNA "ENTREGUE NO PRAZO?"
        # ==========================================================
        colunas = list(df.columns)
        colunas_upper = [c.upper() for c in colunas]

        possiveis_nomes_upper = [
            "ENTREGUE NO PRAZO?",
            "ENTREGUE NO PRAZO？",  # fullwidth ?
        ]

        col_entregue_prazo = None
        for nome in possiveis_nomes_upper:
            if nome in colunas_upper:
                idx = colunas_upper.index(nome)
                col_entregue_prazo = colunas[idx]
                break

        if col_entregue_prazo is None:
            raise KeyError(
                f"❌ Nenhuma coluna equivalente a 'ENTREGUE NO PRAZO?' encontrada.\n"
                f"Colunas disponíveis: {df.columns}"
            )

        logging.info(f"✅ Coluna de SLA encontrada: {col_entregue_prazo}")

        # ==========================================================
        # 3) NORMALIZAR ENTREGUE NO PRAZO (Y = 1, resto = 0)
        # ==========================================================
        df = df.with_columns(
            pl.when(
                pl.col(col_entregue_prazo)
                .cast(pl.Utf8, strict=False)
                .str.to_uppercase() == "Y"
            )
            .then(1)
            .otherwise(0)
            .alias("_ENTREGUE_PRAZO")
        )

        # define ontem
        hoje = datetime.now().date()
        ontem = hoje - timedelta(days=1)

        # filtra só pedidos com DATA PREVISTA DE ENTREGA = ontem
        df_ontem = df.filter(pl.col("DATA PREVISTA DE ENTREGA") == ontem)

        # ==========================================================
        # 4) CARREGAR COORDENADORES E JUNTAR
        # ==========================================================
        coord_df = pl.read_excel(PASTA_COORDENADOR).rename({
            "Nome da base": "BASE DE ENTREGA",
            "Coordenadores": "COORDENADOR"
        })

        df_ontem = df_ontem.join(coord_df, on="BASE DE ENTREGA", how="left")

        # ==========================================================
        # 5) RESUMO DE SLA POR BASE/COORDENADOR
        # ==========================================================
        if df_ontem.height == 0:
            logging.warning("⚠️ Nenhum pedido com data prevista de ontem.")
            resumo_ontem_pd = pd.DataFrame(
                columns=["Base De Entrega", "COORDENADOR", "Total",
                         "Entregues no Prazo", "Fora do Prazo", "% SLA Cumprido"]
            )
        else:
            resumo_ontem = (
                df_ontem
                .group_by(["BASE DE ENTREGA", "COORDENADOR"])
                .agg([
                    pl.len().alias("Total"),
                    pl.col("_ENTREGUE_PRAZO").sum().alias("Entregues no Prazo"),
                    (pl.len() - pl.col("_ENTREGUE_PRAZO").sum()).alias("Fora do Prazo"),
                    (pl.col("_ENTREGUE_PRAZO").sum() / pl.len()).alias("% SLA Cumprido")
                ])
                .sort("% SLA Cumprido", descending=True)
            )

            resumo_ontem_pd = resumo_ontem.to_pandas().rename(
                columns={"BASE DE ENTREGA": "Base De Entrega"}
            )

        # ==========================================================
        # 6) LOG DETALHADO NO TERMINAL
        # ==========================================================
        logging.info("📌 DETALHAMENTO DAS BASES (ONTEM):")

        total_geral = 0
        entregues_geral = 0

        for _, row in resumo_ontem_pd.iterrows():
            base = row["Base De Entrega"]
            coord = row.get("COORDENADOR", "")
            total = int(row["Total"])
            ent_prazo = int(row["Entregues no Prazo"])
            fora_prazo = int(row["Fora do Prazo"])
            sla = float(row["% SLA Cumprido"])

            total_geral += total
            entregues_geral += ent_prazo

            logging.info(
                f"   • {base} ({coord}): "
                f"Total={total} | No Prazo={ent_prazo} | Fora do Prazo={fora_prazo} | SLA={sla:.2%}"
            )

        logging.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logging.info(f"📦 TOTAL DE PEDIDOS ONTEM: {total_geral}")
        logging.info(f"📬 ENTREGUE NO PRAZO: {entregues_geral}")
        logging.info(f"⏳ FORA DO PRAZO: {total_geral - entregues_geral}")
        logging.info(
            f"📈 SLA GERAL: {(entregues_geral / total_geral) if total_geral else 0:.2%}"
        )
        logging.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

        # ==========================================================
        # 7) EXPORTAR EXCEL
        # ==========================================================
        arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, "Resumo_Consolidado_")

        with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as w:
            resumo_ontem_pd.to_excel(w, index=False, sheet_name="Resumo SLA")

            df_pd = df.to_pandas()
            chunk_size = 1_000_000
            total_chunks = (len(df_pd) // chunk_size) + 1

            for i in range(total_chunks):
                inicio = i * chunk_size
                fim = min((i + 1) * chunk_size, len(df_pd))
                chunk = df_pd.iloc[inicio:fim]
                sheet_name = "Dados" if i == 0 else f"Dados_{i + 1}"
                chunk.to_excel(w, index=False, sheet_name=sheet_name)

        # ==========================================================
        # 8) ENVIAR CARDS POR COORDENADOR
        # ==========================================================
        for coordenador, webhook in COORDENADOR_WEBHOOKS.items():
            sub = resumo_ontem_pd[resumo_ontem_pd["COORDENADOR"] == coordenador]

            if not sub.empty:
                sla_ontem = sub["% SLA Cumprido"].mean()
                enviar_card_feishu(sub, webhook, coordenador, sla_ontem)
            else:
                logging.warning(f"⏩ Nenhuma base para {coordenador}")

        logging.info("🏁 Processamento concluído (v2.4)!")

    except Exception as e:
        logging.critical(f"❌ Erro fatal: {e}", exc_info=True)
