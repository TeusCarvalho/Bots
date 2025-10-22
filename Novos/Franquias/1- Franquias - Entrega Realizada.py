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
from datetime import datetime
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

PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Local de Teste\Entrega Realizada - Dia"
PASTA_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Franquias\Entrega Realizada"
PASTA_ARQUIVO = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Franquias\Entrega Realizada\Nova pasta"

DATA_HOJE = datetime.now().strftime("%Y%m%d")
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_{DATA_HOJE}.xlsx")

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/92a82aea-9b5c-4e3d-9169-8d4753ecef38"
LINK_PASTA = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EvIP3oIiLJRAqcB1SZ_1nmYBXLIYSJkIns5Pf_Xz2OqY_w?e=OEXsJN"

# ==========================================================
# 🏢 BASES VÁLIDAS
# ==========================================================
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
    "F TUR-PA", "F VHL-RO", "F VLP-GO", "F XIG-PA","F TRM-AM", "F STM-PA",
    "F JPN 02-RO", "F CAC-RO"
]


# ==========================================================
# ⚡️ FUNÇÕES AUXILIARES
# ==========================================================
def cor_percentual(pct: float) -> str:
    """Retorna o emoji correspondente ao SLA."""
    if pct < 0.95:
        return "🔴"
    elif pct < 0.97:
        return "🟡"
    else:
        return "🟢"


def arquivar_relatorios_antigos(pasta_origem: str, pasta_destino: str, prefixo_arquivo: str):
    """
    Move relatórios antigos para uma pasta de arquivo.
    """
    logging.info(f"Verificando relatórios antigos em '{pasta_origem}' para arquivar...")

    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)
        logging.info(f"Pasta de arquivo criada: '{pasta_destino}'")

    try:
        arquivos = os.listdir(pasta_origem)

        relatorios_para_mover = [
            f for f in arquivos
            if f.startswith(prefixo_arquivo) and f.endswith('.xlsx')
        ]

        if not relatorios_para_mover:
            logging.info("Nenhum relatório antigo encontrado para arquivar.")
            return

        for arquivo in relatorios_para_mover:
            caminho_origem = os.path.join(pasta_origem, arquivo)
            caminho_destino = os.path.join(pasta_destino, arquivo)

            try:
                shutil.move(caminho_origem, caminho_destino)
                logging.info(f"Relatório antigo movido: '{arquivo}' -> '{pasta_destino}'")
            except Exception as e:
                logging.error(f"Erro ao mover o arquivo '{arquivo}': {e}")

    except FileNotFoundError:
        logging.error(f"A pasta de origem '{pasta_origem}' não foi encontrada.")
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado ao arquivar relatórios: {e}")


def ler_planilha_rapido(caminho):
    """Lê planilha com Polars (detecta formato automaticamente)."""
    try:
        if caminho.endswith(".csv"):
            return pl.read_csv(caminho)
        return pl.read_excel(caminho)
    except Exception as e:
        logging.error(f"Falha ao ler o arquivo {os.path.basename(caminho)}: {e}")
        return pl.DataFrame()


def consolidar_planilhas(pasta_entrada: str) -> pl.DataFrame:
    """Une todas as planilhas Excel/CSV da pasta."""
    arquivos = [os.path.join(pasta_entrada, f)
                for f in os.listdir(pasta_entrada)
                if f.endswith((".xlsx", ".xls", ".csv"))]
    if not arquivos:
        raise FileNotFoundError("❌ Nenhum arquivo Excel/CSV encontrado na pasta de entrada.")

    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        dfs = list(executor.map(ler_planilha_rapido, arquivos))

    dfs_validos = [df for df in dfs if not df.is_empty()]
    if not dfs_validos:
        raise ValueError("Nenhum arquivo pôde ser lido com sucesso. Verifique o log para detalhes.")

    return pl.concat(dfs_validos, how="vertical_relaxed")


def detectar_coluna_entregue(df: pl.DataFrame) -> str:
    """Detecta automaticamente a coluna que indica se foi entregue."""
    for col in df.columns:
        if "entregue" in col.lower():
            return col
    raise KeyError("❌ Coluna de status de entrega não encontrada.")


def salvar_relatorio_completo(df_dados: pl.DataFrame, df_resumo: pd.DataFrame, caminho_arquivo: str):
    """
    Salva o relatório completo em um único arquivo Excel com duas abas:
    1. 'Resumo SLA' para o DataFrame de resumo.
    2. 'Dados Completos' para o DataFrame de dados brutos filtrados.
    """
    try:
        os.makedirs(os.path.dirname(caminho_arquivo), exist_ok=True)

        # Converte o DataFrame de dados do Polars para Pandas para salvar
        df_dados_pd = df_dados.to_pandas()

        with pd.ExcelWriter(caminho_arquivo, engine='openpyxl') as writer:
            df_resumo.to_excel(writer, index=False, sheet_name='Resumo SLA')
            df_dados_pd.to_excel(writer, index=False, sheet_name='Dados Completos')

        logging.info(f"📄 Relatório completo salvo em: {caminho_arquivo}")
        logging.info(f"  -> Aba 'Resumo SLA' com {len(df_resumo)} linhas.")
        logging.info(f"  -> Aba 'Dados Completos' com {len(df_dados_pd)} linhas.")

    except Exception as e:
        logging.error(f"⚠️ Falha ao salvar o relatório completo: {e}")


def enviar_card_feishu(resumo_df: pd.DataFrame, max_retries=3):
    """Monta e envia o card consolidado para o Feishu com mecanismo de retry."""
    data_geracao = datetime.now().strftime("%d/%m/%Y %H:%M")
    total_bases = resumo_df["Base De Entrega"].nunique()
    media_geral = resumo_df["% Entregues"].mean()

    piores = resumo_df.sort_values(by="% Entregues", ascending=True).head(7)
    melhores = resumo_df.sort_values(by="% Entregues", ascending=False).head(3)

    linhas_piores = [
        f"{i}. {cor_percentual(row['% Entregues'])} **{row['Base De Entrega']}** — {row['% Entregues']:.2%} "
        f"({row['Nao_Entregues']} não entregues de {row['Total']})"
        for i, row in enumerate(piores.to_dict('records'), 1)
    ]
    medalhas = ["🥇", "🥈", "🥉"]
    linhas_melhores = [
        f"{medalhas[i - 1]} {cor_percentual(row['% Entregues'])} **{row['Base De Entrega']}** — {row['% Entregues']:.2%} "
        f"({row['Nao_Entregues']} não entregues de {row['Total']})"
        for i, row in enumerate(melhores.to_dict('records'), 1)
    ]

    conteudo = (
            f"📅 **Data de Geração:** {data_geracao}\n"
            f"🏢 **Bases Avaliadas:** {total_bases}\n\n"
            f"🔻 **7 Piores SLAs:**\n" + "\n".join(linhas_piores) +
            "\n\n🏆 **Top 3 Melhores SLAs:**\n" + "\n".join(linhas_melhores) +
            f"\n\n📊 **Média Geral:** {media_geral:.2%}"
    )

    card_payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"template": "turquoise",
                       "title": {"tag": "plain_text", "content": "📊 Relatório Consolidado de SLA"}},
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

    for attempt in range(max_retries):
        try:
            resp = requests.post(WEBHOOK_URL, json=card_payload, timeout=10)
            resp.raise_for_status()
            logging.info("✅ Card enviado com sucesso ao Feishu!")
            return
        except requests.RequestException as e:
            logging.warning(f"Tentativa {attempt + 1}/{max_retries} de enviar o card falhou: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                logging.error("🚨 Falha ao enviar card ao Feishu após várias tentativas.")


# ==========================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ==========================================================
if __name__ == "__main__":
    logging.info("🚀 Iniciando processamento turbo...")
    try:
        df = consolidar_planilhas(PASTA_ENTRADA)
        logging.info(f"Total de {df.height} registros lidos e consolidados.")

        if df.height == 0:
            logging.warning("Nenhum dado encontrado nos arquivos. Encerrando o processo.")
            exit()

        df = df.rename({c: c.strip().upper() for c in df.columns})
        col_entregue = detectar_coluna_entregue(df)
        logging.info(f"Coluna de entrega detectada: '{col_entregue}'")

        # === FILTROS ===
        if "DATA PREVISTA DE ENTREGA" in df.columns:
            logging.info("Filtrando pela data prevista de entrega (hoje)...")
            tipo_coluna = df["DATA PREVISTA DE ENTREGA"].dtype
            if tipo_coluna == pl.Utf8:
                df = df.with_columns(pl.col("DATA PREVISTA DE ENTREGA").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
            elif tipo_coluna == pl.Datetime:
                df = df.with_columns(pl.col("DATA PREVISTA DE ENTREGA").dt.date().alias("DATA PREVISTA DE ENTREGA"))

            df_hoje = df.filter(pl.col("DATA PREVISTA DE ENTREGA") == datetime.now().date())
            logging.info(f"{df_hoje.height} registros restantes após o filtro de data.")
        else:
            df_hoje = df
            logging.warning("Coluna 'DATA PREVISTA DE ENTREGA' não encontrada. Pulando filtro de data.")

        if "BASE DE ENTREGA" in df_hoje.columns:
            df_filtrado = df_hoje.filter(pl.col("BASE DE ENTREGA").is_in([b.upper() for b in BASES_VALIDAS]))
            logging.info(f"{df_filtrado.height} registros restantes após o filtro de bases válidas.")
        else:
            df_filtrado = df_hoje
            logging.warning("Coluna 'BASE DE ENTREGA' não encontrada. Pulando filtro de bases.")

        # === GERA RESUMO DE SLA ===
        logging.info("Gerando resumo de SLA por base...")
        resumo = (
            df_filtrado.group_by("BASE DE ENTREGA")
            .agg([
                pl.count().alias("Total"),
                (pl.col(col_entregue) == "Y").sum().alias("Entregues"),
                ((pl.col(col_entregue) != "Y") | pl.col(col_entregue).is_null()).sum().alias("Nao_Entregues"),
            ])
            .with_columns((pl.col("Entregues") / pl.col("Total").cast(pl.Float64)).alias("% Entregues"))
            .sort("% Entregues", descending=False)
        )

        resumo_pd = resumo.to_pandas()
        resumo_pd.rename(columns={"BASE DE ENTREGA": "Base De Entrega"}, inplace=True)

        # === ARQUIVAR RELATÓRIOS ANTIGOS ===
        arquivar_relatorios_antigos(PASTA_SAIDA, PASTA_ARQUIVO, "Resumo_Consolidado_")

        # 🔒 Salvar relatório completo + card final
        salvar_relatorio_completo(df_filtrado, resumo_pd, ARQUIVO_SAIDA)
        enviar_card_feishu(resumo_pd)

        logging.info("\n🏁 Processo concluído com sucesso!")

    except FileNotFoundError as e:
        logging.critical(f"Erro de arquivo ou pasta não encontrada: {e}", exc_info=True)
    except KeyError as e:
        logging.critical(f"Erro de coluna não encontrada: {e}. Verifique o nome das colunas nos arquivos de entrada.",
                         exc_info=True)
    except ValueError as e:
        logging.critical(f"Erro de valor ou processamento: {e}", exc_info=True)
    except Exception as e:
        logging.critical(f"Ocorreu um erro inesperado e fatal: {e}", exc_info=True)