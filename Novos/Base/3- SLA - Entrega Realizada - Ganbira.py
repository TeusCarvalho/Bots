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
}

EXTS = (".xlsx", ".xls", ".csv")

# ============================================================
# 🔧 Normalização para garantir JOIN
# ============================================================
def normalizar(s):
    if s is None:
        return ""
    s = str(s).upper().strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    while "  " in s:
        s = s.replace("  ", " ")
    return s


# ============================================================
# 🗓️ Cálculo de data inteligente (v2.8)
# ============================================================
def calcular_data_base():
    hoje = datetime.now().date()
    dia = hoje.weekday()  # 0=Seg, 1=Ter, ..., 5=Sáb, 6=Dom

    # Sábado e Domingo → NÃO RODAR
    if dia in (5, 6):
        logging.warning("⛔ Hoje é sábado ou domingo. Execução cancelada.")
        return None

    # Segunda → pegar sexta-feira
    if dia == 0:
        return hoje - timedelta(days=3)

    # Terça a Sexta → ontem
    return hoje - timedelta(days=1)


# ============================================================
# Funções auxiliares
# ============================================================
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


# ============================================================
# Envio do card via Feishu
# ============================================================
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
                              "content": f"SLA - Entrega no Prazo — {coord}"}
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
                f"❌ ERRO ao enviar card para {coord}. Status: {r.status_code}. Resposta: {r.text}"
            )
            return False

        logging.info(f"📨 Card enviado para {coord}")
        return True

    except Exception as e:
        logging.error(
            f"❌ Falha no envio para {coord}. Erro: {e}. Webhook: {webhook}"
        )
        return False


# ============================================================
# 🚀 Execução principal v2.8
# ============================================================
if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento SLA (v2.8)...")

    try:
        # 0) Calcular data-base
        data_base = calcular_data_base()
        if data_base is None:
            exit()

        logging.info(f"📅 Data usada para cálculo SLA: {data_base}")

        # 1) Ler planilhas
        df = consolidar_planilhas(PASTA_ENTRADA)
        logging.info(f"📥 Registros carregados: {df.height}")

        # 2) Padronizar nomes colunas
        df = df.rename({c: c.strip().upper() for c in df.columns})

        # 3) Garantir conversão correta da data
        df = garantir_coluna_data(df, "DATA PREVISTA DE ENTREGA")

        # 4) Detectar a coluna ENTREGUE NO PRAZO
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

        # 5) Converter Y/N → 1/0
        df = df.with_columns(
            pl.when(pl.col(col_entregue).cast(pl.Utf8).str.to_uppercase() == "Y")
            .then(1).otherwise(0).alias("_ENTREGUE_PRAZO")
        )

        # 6) Filtrar somente registros da data-base
        df_ontem = df.filter(pl.col("DATA PREVISTA DE ENTREGA") == data_base)
        logging.info(f"📊 Registros para {data_base}: {df_ontem.height}")

        # 7) Carregar Excel dos coordenadores
        coord_df = pl.read_excel(PASTA_COORDENADOR).rename({
            "Nome da base": "BASE DE ENTREGA",
            "Coordenadores": "COORDENADOR"
        })

        # 8) Normalizar nomes de base em ambos
        df_ontem = df_ontem.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar).alias("BASE_NORM")
        )

        coord_df = coord_df.with_columns(
            pl.col("BASE DE ENTREGA").map_elements(normalizar).alias("BASE_NORM")
        )

        # 9) JOIN corrigido
        df_ontem = df_ontem.join(coord_df, on="BASE_NORM", how="left")

        # Diagnóstico do JOIN
        sem_coord = df_ontem.filter(pl.col("COORDENADOR").is_null()).height
        logging.info(f"🧩 Registros sem coordenador após join: {sem_coord}")

        # 10) Resumo por coordenador
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

        # 11) Exportar Excel
        arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, "Resumo_Consolidado_")

        with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as w:
            resumo_pd.to_excel(w, index=False, sheet_name="Resumo SLA")

        # 12) Enviar cards
        for coord, webhook in COORDENADOR_WEBHOOKS.items():
            sub = resumo_pd[resumo_pd["COORDENADOR"] == coord]

            if sub.empty:
                logging.warning(f"⚠️ Nenhuma base encontrada para {coord}")
                continue

            sla = sub["% SLA Cumprido"].mean()
            enviar_card_feishu(sub, webhook, coord, sla)

        logging.info("🏁 Processamento concluído (v2.8)")

    except Exception as e:
        logging.critical(f"❌ ERRO FATAL: {e}", exc_info=True)
