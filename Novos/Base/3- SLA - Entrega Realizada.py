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
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b"
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
                shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo))
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
    arquivos = [os.path.join(pasta_entrada, f) for f in os.listdir(pasta_entrada) if f.endswith(EXTS)]
    if not arquivos:
        raise FileNotFoundError("❌ Nenhum arquivo Excel/CSV encontrado.")
    with ThreadPoolExecutor(max_workers=min(16, len(arquivos))) as executor:
        dfs = list(executor.map(ler_planilha_rapido, arquivos))
    dfs_validos = [df for df in dfs if not df.is_empty()]
    if not dfs_validos:
        raise ValueError("Nenhum arquivo pôde ser lido com sucesso.")
    return pl.concat(dfs_validos, how="vertical_relaxed").rename({c: c.strip().upper() for c in dfs_validos[0].columns})
def normalizar_entregue(df: pl.DataFrame, col_entregue: str, novo_nome="_ENTREGUE_BOOL") -> pl.DataFrame:
    """Corrige tipos mistos e cria coluna booleana sem erros."""
    txt = pl.col(col_entregue).cast(pl.Utf8, strict=False).str.to_lowercase().str.strip_chars()
    num = pl.col(col_entregue).cast(pl.Int64, strict=False).fill_null(0)
    valores_true = {"y", "yes", "sim", "s", "true", "1", "entregue", "ok", "done"}
    expr = txt.is_in(list(valores_true)) | (num == 1) | (txt == "t") | (txt == "verdadeiro") | (txt == "entrega realizada")
    return df.with_columns(expr.alias(novo_nome))


def garantir_coluna_data(df: pl.DataFrame, nome_coluna: str) -> pl.DataFrame:
    """Converte a coluna para Date, evitando erro de tipo."""
    if nome_coluna not in df.columns:
        raise KeyError(f"Coluna '{nome_coluna}' não encontrada.")

    tipo = df[nome_coluna].dtype

    # ✅ compatível com Polars novo
    if tipo == pl.Date:
        return df
    if tipo == pl.Datetime:
        return df.with_columns(pl.col(nome_coluna).dt.date().alias(nome_coluna))
    if tipo == pl.Utf8:
        formatos = ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"]
        expr = None
        for f in formatos:
            tentativa = pl.col(nome_coluna).str.strptime(pl.Date, f, strict=False)
            expr = tentativa if expr is None else expr.fill_null(tentativa)
        return df.with_columns(expr.alias(nome_coluna))
    if tipo in (pl.Int64, pl.Float64):
        base = datetime(1899, 12, 30)
        return df.with_columns(
            (pl.col(nome_coluna).cast(pl.Int64) * 86400)
            .dt.datetime("s", "UTC")
            .dt.date()
            .alias(nome_coluna)
        )

    raise TypeError(f"⚠️ Tipo inesperado para '{nome_coluna}': {tipo}")

def enviar_card_feishu(resumo_df: pd.DataFrame, webhook: str, coordenador: str, sla_atual: float, sla_anterior: float) -> bool:
    try:
        if resumo_df.empty:
            logging.warning(f"⚠️ Nenhum dado encontrado para {coordenador}.")
            return False
        data_geracao = datetime.now().strftime("%d/%m/%Y %H:%M")
        bases_coord = resumo_df["Base De Entrega"].nunique()
        piores = resumo_df.sort_values(by="% Entregues").head(3)
        melhores = resumo_df.sort_values(by="% Entregues", ascending=False).head(3)
        linhas_piores = [f"{i}. {cor_percentual(r['% Entregues'])} **{r['Base De Entrega']}** — {r['% Entregues']:.2%}" for i, r in enumerate(piores.to_dict('records'), 1)]
        medalhas = ["🥇", "🥈", "🥉"]
        linhas_melhores = [f"{medalhas[i - 1]} {cor_percentual(r['% Entregues'])} **{r['Base De Entrega']}** — {r['% Entregues']:.2%}" for i, r in enumerate(melhores.to_dict('records'), 1)]
        conteudo = (f"👤 **Coordenador:** {coordenador}\n📅 **Atualizado em:** {data_geracao}\n🏢 **Bases Avaliadas:** {bases_coord}\n"
                    f"📈 **SLA Hoje:** {sla_atual:.2%}\n📉 **SLA Ontem:** {sla_anterior:.2%}\n\n🔻 **3 Piores Bases:**\n" + "\n".join(linhas_piores) +
                    "\n\n🏆 **Top 3 Melhores:**\n" + "\n".join(linhas_melhores))
        payload = {"msg_type": "interactive", "card": {"config": {"wide_screen_mode": True},
                   "header": {"template": "blue", "title": {"tag": "plain_text", "content": f"📊 SLA - Entrega Realizada ({coordenador})"}},
                   "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": conteudo}},
                                {"tag": "hr"},
                                {"tag": "action", "actions": [{"tag": "button", "text": {"tag": "plain_text", "content": "📂 Abrir Pasta no OneDrive"},
                                                               "url": LINK_PASTA, "type": "default"}]}]}}
        r = requests.post(webhook, json=payload, timeout=12)
        if r.status_code == 200:
            logging.info(f"✅ Card enviado com sucesso para {coordenador}")
            return True
        logging.error(f"🚨 Falha ao enviar card para {coordenador} (status {r.status_code})")
        return False
    except Exception as e:
        logging.error(f"❌ Erro no envio do card para {coordenador}: {e}")
        return False


# ==========================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ==========================================================
if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA por coordenador...")

    try:
        df = consolidar_planilhas(PASTA_ENTRADA)
        logging.info(f"📥 Total de {df.height} registros lidos.")
        df = df.rename({c: c.strip().upper() for c in df.columns})

        col_entregue = [c for c in df.columns if "ENTREGUE" in c.upper()][0]
        df = normalizar_entregue(df, col_entregue)
        df = garantir_coluna_data(df, "DATA PREVISTA DE ENTREGA")

        hoje, ontem = datetime.now().date(), datetime.now().date() - timedelta(days=1)
        df_hoje, df_ontem = df.filter(pl.col("DATA PREVISTA DE ENTREGA") == hoje), df.filter(pl.col("DATA PREVISTA DE ENTREGA") == ontem)

        coord_df = pl.read_excel(PASTA_COORDENADOR).rename({"Nome da base": "BASE DE ENTREGA", "Coordenadores": "COORDENADOR"})
        df = df.join(coord_df, on="BASE DE ENTREGA", how="left")

        resumo = (df_hoje.join(coord_df, on="BASE DE ENTREGA", how="left")
                  .group_by(["BASE DE ENTREGA", "COORDENADOR"])
                  .agg([pl.count().alias("Total"),
                        pl.col("_ENTREGUE_BOOL").cast(pl.Int64).sum().alias("Entregues")])
                  .with_columns((pl.col("Entregues") / pl.col("Total")).alias("% Entregues"))
                  .sort("% Entregues"))
        resumo_pd = resumo.to_pandas().rename(columns={"BASE DE ENTREGA": "Base De Entrega"})

        arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, "Resumo_Consolidado_")
        with pd.ExcelWriter(ARQUIVO_SAIDA, engine='openpyxl') as w:
            resumo_pd.to_excel(w, index=False, sheet_name='Resumo SLA')
            df.to_pandas().to_excel(w, index=False, sheet_name='Dados Completos')

        total_sucesso = total_falha = total_sem_base = 0
        for coordenador, webhook in COORDENADOR_WEBHOOKS.items():
            sub_df = resumo_pd[resumo_pd["COORDENADOR"] == coordenador]
            if not sub_df.empty:
                bases = sub_df["Base De Entrega"].dropna().unique().tolist()
                sla_hoje = (df_hoje.filter(pl.col("BASE DE ENTREGA").is_in(bases))["_ENTREGUE_BOOL"].cast(pl.Int64).sum() / df_hoje.filter(pl.col("BASE DE ENTREGA").is_in(bases)).height) if df_hoje.height else 0
                sla_ontem = (df_ontem.filter(pl.col("BASE DE ENTREGA").is_in(bases))["_ENTREGUE_BOOL"].cast(pl.Int64).sum() / df_ontem.filter(pl.col("BASE DE ENTREGA").is_in(bases)).height) if df_ontem.height else 0
                logging.info(f"📊 {coordenador}: SLA Hoje = {sla_hoje:.2%} | Ontem = {sla_ontem:.2%}")
                if enviar_card_feishu(sub_df, webhook, coordenador, sla_hoje, sla_ontem):
                    total_sucesso += 1
                else:
                    total_falha += 1
            else:
                logging.warning(f"⏩ Nenhuma base vinculada a {coordenador}, card não enviado.")
                total_sem_base += 1

        logging.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logging.info(f"📬 Envio concluído: {total_sucesso} ✅ | {total_falha} ❌ | {total_sem_base} ⏩ sem base.")
        logging.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logging.info("🏁 Processamento concluído com sucesso!")

    except Exception as e:
        logging.critical(f"Erro fatal no processamento: {e}", exc_info=True)
