# -*- coding: utf-8 -*-

# ==========================================================
# SCRIPT DE ALERTA DE SLA - FRANQUIAS (VERSÃO FINAL)
# ==========================================================
# Autor: [Seu Nome/Equipe]
# Versão: 12.3 (Produção - Relatório Simplificado)
# Descrição: Este script consolida dados de entrega, calcula o SLA de
#              franquias para o mês inteiro, remove duplicatas, exibe o
#              relatório simplificado no terminal e envia um alerta para o
#              Feishu, destacando as 4 piores bases do mês e as 4 piores
#              bases dos domingos do mês.

import os
import requests
import warnings
import polars as pl
import pandas as pd
import multiprocessing
import logging
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ==========================================================
# CONFIGURAÇÃO DE LOGGING
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
# VARIÁVEIS DE CONFIGURAÇÃO
# ==========================================================
os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())

PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\14-  SLA Entrega Realizada Franquia"
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b"
LINK_PASTA = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EvIP3oIiLJRAqcB1SZ_1nmYBXLIYSJkIns5Pf_Xz2OqY_w?e=OEXsJN"

COL_DATA_ORIGINAL = "Data prevista de entrega"
COL_DATA_UPPER = "DATA PREVISTA DE ENTREGA"
COL_DATA_REF = "DATA_REF"

# ==========================================================
# LISTA DE BASES VÁLIDAS
# ==========================================================
BASES_VALIDAS = [
    'MAO FLUVIAL -AM', 'F CHR-AM', 'F CAC-RO', 'F PDR-GO', 'CZS -AC', 'F PVH-RO', 'GNT -MT', 'F ARQ - RO',
    'F AGB-MT', 'F GYN 03-GO', 'SRS -MT', 'SNP -MT', 'MAO -AM', 'RBR 02-AC', 'F RBR-AC', 'IPR -GO',
    'STM FLUVIAL -PA', 'AUX -TO', 'F GYN - GO', 'PTD -MT', 'JPN -RO', 'F VHL-RO', 'F PON-GO', 'F ANP-GO',
    'F GYN 02-GO', 'MDT -MT', 'F CDN-AM', 'F AGL-GO', 'PRG -GO', 'F APG - GO', 'F RVD - GO', 'F PDT-TO',
    'F PLN-DF', 'CGB 03-MT', 'CKS -PA', 'NVT -MT', 'F SEN-GO', 'RFI -DF', 'ATF -MT', 'SMB -GO',
    'F PVL-MT', 'F TRD-GO', 'F CEI-DF', 'F CNF-MT', 'F FMA-GO', 'MCP FLUVIAL -AP', 'RBR -AC', 'RRP -RR',
    'BVB INT-RR', 'F ALV-AM', 'ITT -PA', 'F POS-GO', 'TAR -AC', 'ANA FLUVIAL - PA', 'URC -GO', 'BGA -MT',
    'GNA -GO', 'SMA -GO', 'LRV -MT', 'F PPA-MS', 'BRV -PA', 'F MAC-AP', 'SJA -GO', 'TLL -MS', 'F GAI-TO',
    'F CRX-GO', 'F DOM -PA', 'F CCR-MT', 'F GRP-TO', 'F PVL 02-MT', 'PNA -TO', 'CTL -GO', 'F AMB-MS',
    'F BVB-RR', 'NDI -MS', 'ARI -MT', 'F SVC-RR', 'ALX -AM', 'DNP -TO', 'F MCP-AP', 'JUI -MT',
    'VGR 02-MT', 'F JPN 02-RO', 'F MCP 02-AP', 'ATM -PA', 'AGB -MT', 'URA -PA', 'F BSL-AC', 'SGO -MS',
    'CDT -TO', 'CHS -MS', 'CGB 05-MT', 'AUG -TO', 'PMW 003-TO', 'F PVH 02-RO', 'F JPN-RO', 'F CMV-MT',
    'VSU -PA', 'F DOU-MS', 'EMA -DF', 'F PGM-PA', 'F RDC -PA', 'CPP -PA', 'AQD -MS', 'F XIG-PA',
    'CTN -GO', 'SBN -DF', 'F TGT-DF', 'CGB 04-MT', 'CGB 02-MT', 'F CGR - MS', 'F VLP-GO', 'F CGR 02-MS',
    'F PLA-GO', 'F TGA-MT', 'NRE -PA', 'ROO -MT', 'VGR-MT', 'F RFI-DF', 'F ORL-PA', 'F ITI-PA',
    'CXM -MS', 'JRD -MS', 'PRB -MS', 'PMW 002-TO', 'F PCA-PA', 'CRB -MS', 'BRC -PA', 'SDA -PA',
    'SMD -AC', 'ICR -PA', 'F CNC-PA', 'BVD -PA', 'CPN -PA', 'IGM -PA', 'F SJA-GO', 'F IGA-PA',
    'CNA -PA', 'F PAZ-AM', 'ABT -PA', 'COQ -PA', 'ANA -PA', 'CST -PA', 'PDR -PA', 'BEL -PA', 'SLP -PA',
    'F TUR-PA', 'MRM -PA', 'F JCD-PA', 'F TLA-PA', 'VGA -PA', 'F ELD-PA', 'F BSB-DF', 'F OCD-GO',
    'F EMA-DF', 'F GUA-DF', 'NMB -PA', 'AMP -PA', 'MJU -PA', 'F STM-PA', 'F SBN-DF',
]


# ==========================================================
# FUNÇÕES DE PROCESSAMENTO DE DADOS
# ==========================================================
def ler_planilha_rapido(caminho: str) -> pl.DataFrame:
    """Lê um arquivo (Excel ou CSV) de forma rápida e segura."""
    try:
        if caminho.lower().endswith(".csv"):
            return pl.read_csv(caminho)
        return pl.read_excel(caminho)
    except Exception as e:
        logging.error(f"Erro ao ler {caminho}: {e}")
        return pl.DataFrame()


def consolidar_planilhas(pasta: str) -> pl.DataFrame:
    """Lê e consolida arquivos de forma sequencial para evitar estouro de memória."""
    arquivos = [
        os.path.join(pasta, f)
        for f in os.listdir(pasta)
        if f.lower().endswith((".xlsx", ".xls", ".csv")) and not f.startswith("~$")
    ]

    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo encontrado na pasta de entrada.")

    # --- MUDANÇA: Leitura sequencial em vez de paralela ---
    dfs = []
    logging.info(f"📂 Encontrados {len(arquivos)} arquivos. Iniciando leitura sequencial...")

    for i, arquivo in enumerate(arquivos):
        logging.info(f"Lendo arquivo {i + 1}/{len(arquivos)}: {os.path.basename(arquivo)}...")
        df = ler_planilha_rapido(arquivo)
        if not df.is_empty():
            dfs.append(df)

    if not dfs:
        raise ValueError("Nenhum DataFrame válido foi lido dos arquivos.")

    logging.info("🔄 Todos os arquivos lidos. Iniciando concatenação...")
    df_final = pl.concat(dfs, how="vertical_relaxed")
    logging.info(f"📂 Base consolidada com {df_final.height} linhas.")
    return df_final


def preparar_coluna_data(df: pl.DataFrame) -> pl.DataFrame:
    """Padroniza e converte a coluna de data para o tipo Date."""
    df = df.rename({c: c.strip().upper() for c in df.columns})
    if COL_DATA_UPPER not in df.columns:
        raise KeyError(f"Coluna '{COL_DATA_ORIGINAL}' não encontrada no DataFrame.")
    temp_col = "temp_data_str"
    df = df.with_columns(
        pl.col(COL_DATA_UPPER)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.replace(r" .*$", "")
        .alias(temp_col)
    )
    formatos_data = ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%Y%m%d"]
    tentativas_parse = [pl.col(temp_col).str.strptime(pl.Date, fmt, strict=False) for fmt in formatos_data]
    df = df.with_columns(pl.coalesce(tentativas_parse).alias(COL_DATA_REF)).drop(temp_col)
    df = df.filter(pl.col(COL_DATA_REF).is_not_null())
    logging.info(f"📅 Datas convertidas e validadas. Restam {df.height} linhas.")
    return df


def calcular_sla(df: pl.DataFrame) -> pd.DataFrame | None:
    """
    Calcula o SLA por base de entrega.
    Retorna um DataFrame com o resumo ou None se a coluna de prazo não for encontrada.
    """
    possiveis_nomes_coluna = ["ENTREGUE NO PRAZO?", "ENTREGUE NO PRAZO？"]
    col_upper = [c.upper() for c in df.columns]

    # Encontra a coluna de prazo de forma mais robusta
    col_prazo = next((df.columns[i] for i, nome in enumerate(col_upper) if nome in possiveis_nomes_coluna), None)

    if col_prazo is None:
        logging.warning(
            "Coluna 'ENTREGUE NO PRAZO?' não encontrada. Não será possível calcular SLA para este DataFrame.")
        return None

    df = df.with_columns(
        pl.when(pl.col(col_prazo).cast(pl.Utf8).str.to_uppercase() == "Y")
        .then(1).otherwise(0)
        .alias("_ENTREGUE")
    )

    resumo = df.group_by("BASE DE ENTREGA").agg([
        pl.len().alias("Total"),
        pl.col("_ENTREGUE").sum().alias("No Prazo"),
        (pl.len() - pl.col("_ENTREGUE").sum()).alias("Fora"),
        (pl.col("_ENTREGUE").sum() / pl.len()).alias("SLA")
    ]).sort("SLA")

    r = resumo.to_pandas()
    r.rename(columns={"BASE DE ENTREGA": "Base De Entrega"}, inplace=True)
    return r


# ==========================================================
# FUNÇÃO PRINCIPAL (COM RELATÓRIO SIMPLIFICADO)
# ==========================================================
def exibir_e_enviar_card(resumo_mes: pd.DataFrame, resumo_domingos: pd.DataFrame | None, primeiro_dia: date,
                         ultimo_dia: date):
    """Exibe o relatório simplificado no terminal e o envia para o Feishu."""
    try:
        logging.info("📤 Preparando relatório simplificado...")
        piores_df_mes = resumo_mes.sort_values(by="SLA", ascending=True).head(4)
        periodo_str = f"{primeiro_dia.strftime('%d/%m')} a {ultimo_dia.strftime('%d/%m')}"
        data_atual_str = datetime.now().strftime("%d/%m/%Y")

        # --- Monta o conteúdo das 4 piores do mês (SIMPLIFICADO) ---
        conteudo_piores = (
            f"🚨 **Alerta de SLA — Franquias**\n"
            f"**Atualizado em:** {data_atual_str}\n"
            f"**📉 4 Piores Bases — {periodo_str}**\n\n"
        )
        for _, row in piores_df_mes.iterrows():
            sla_percent = row['SLA'] * 100
            # --- MUDANÇA AQUI: Exibe apenas a Base e o SLA ---
            conteudo_piores += f"{row['Base De Entrega']} | SLA: {sla_percent:.2f}%\n"

        # --- Monta o conteúdo das 4 piores dos domingos (SIMPLIFICADO) ---
        if resumo_domingos is None or resumo_domingos.empty:
            conteudo_domingos = f"\n**📊 Domingos do mês — Nenhuma base registrada.**"
        else:
            piores_df_domingos = resumo_domingos.sort_values(by="SLA", ascending=True).head(4)
            conteudo_domingos = (
                f"\n**📉 4 Piores Bases — Domingos do mês ({primeiro_dia.strftime('%m/%Y')})**\n\n"
            )
            for _, row in piores_df_domingos.iterrows():
                sla_percent = row['SLA'] * 100
                # --- MUDANÇA AQUI: Exibe apenas a Base e o SLA ---
                conteudo_domingos += f"{row['Base De Entrega']} | SLA: {sla_percent:.2f}%\n"

        conteudo_final = conteudo_piores + conteudo_domingos

        # --- Exibe o relatório no terminal ---
        print("\n" + "=" * 80)
        print("📊 RELATÓRIO DE SLA - VISUALIZAÇÃO LOCAL")
        print("=" * 80)
        print(conteudo_final)
        print("=" * 80)
        print("Enviando este relatório para o Feishu...")
        print("=" * 80 + "\n")

        # --- Envia o card para o Feishu ---
        msg = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "template": "red",
                    "title": {"tag": "plain_text", "content": f"SLA Franquias — {periodo_str}"}
                },
                "elements": [
                    {"tag": "markdown", "content": conteudo_final},
                    {
                        "tag": "action",
                        "actions": [
                            {
                                "tag": "button",
                                "text": {"tag": "plain_text", "content": "📁 Abrir Pasta dos Relatórios"},
                                "type": "primary",
                                "url": LINK_PASTA
                            }
                        ]
                    }
                ]
            }
        }

        response = requests.post(WEBHOOK_URL, json=msg, timeout=15)
        response_data = response.json()

        if response.status_code == 200 and response_data.get("code") == 0:
            logging.info("✅ Card enviado com sucesso para o Feishu!")
        else:
            logging.error(
                f"❌ Erro ao enviar card para o Feishu. Status: {response.status_code}, Resposta: {response.text}")

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Erro de conexão com o Feishu: {e}")
    except Exception as e:
        logging.error(f"❌ Erro inesperado ao enviar card: {e}", exc_info=True)


# --- BLOCO PRINCIPAL DE EXECUÇÃO (LÓGICA MENSAL COM REMOÇÃO DE DUPLICATAS) ---
if __name__ == "__main__":
    try:
        logging.info("🚀 Iniciando script de SLA v12.3 (Versão Final Simplificada)...")

        # 1. Processar os dados
        df_consolidado = consolidar_planilhas(PASTA_ENTRADA)
        df_preparado = preparar_coluna_data(df_consolidado)

        df_filtrado = df_preparado.with_columns(
            pl.col("BASE DE ENTREGA").cast(pl.Utf8).str.to_uppercase().str.strip_chars()
        ).filter(
            pl.col("BASE DE ENTREGA").is_in([b.upper() for b in BASES_VALIDAS])
        )

        # --- NOVA ETAPA: REMOVER DUPLICATAS ---
        linhas_antes = df_filtrado.height
        df_filtrado = df_filtrado.unique(keep="first")
        linhas_depois = df_filtrado.height
        duplicatas_removidas = linhas_antes - linhas_depois

        if duplicatas_removidas > 0:
            logging.info(f"🧹 Foram encontradas e removidas {duplicatas_removidas} linhas duplicadas.")
        else:
            logging.info("✅ Nenhuma linha duplicada encontrada nos dados.")

        if df_filtrado.is_empty():
            raise ValueError("Nenhuma linha restante após o filtro de bases válidas e remoção de duplicatas.")

        # 2. Definir o período de análise (MÊS INTEIRO)
        data_ref = df_filtrado.select(pl.col(COL_DATA_REF)).max().item()
        primeiro_dia = data_ref.replace(day=1)

        # Calcular o último dia do mês de forma robusta
        if data_ref.month == 12:
            proximo_mes = date(data_ref.year + 1, 1, 1)
        else:
            proximo_mes = date(data_ref.year, data_ref.month + 1, 1)
        ultimo_dia = proximo_mes - timedelta(days=1)

        logging.info(
            f"📆 Período de análise: Mês de {primeiro_dia.strftime('%m/%Y')} ({primeiro_dia.strftime('%d/%m/%Y')} a {ultimo_dia.strftime('%d/%m/%Y')})")

        # 3. Calcular SLA para o mês inteiro
        df_mes = df_filtrado.filter(pl.col(COL_DATA_REF).is_between(primeiro_dia, ultimo_dia))
        if df_mes.is_empty():
            raise ValueError("Sem dados para o mês atual.")

        resumo_mes = calcular_sla(df_mes)
        if resumo_mes is None:
            raise ValueError("Não foi possível calcular o SLA para o mês.")

        # 4. Calcular SLA para os domingos do mês
        df_domingos = df_mes.filter(pl.col(COL_DATA_REF).dt.weekday() == 6)  # 6 = Domingo
        resumo_domingos = calcular_sla(df_domingos) if not df_domingos.is_empty() else None

        # 5. Exibir e Enviar o card
        exibir_e_enviar_card(resumo_mes, resumo_domingos, primeiro_dia, ultimo_dia)

        logging.info("🏁 Processo finalizado com sucesso.")

    except Exception as e:
        logging.critical(f"❌ ERRO FATAL: {e}", exc_info=True)