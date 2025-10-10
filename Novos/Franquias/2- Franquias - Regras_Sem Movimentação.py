# -*- coding: utf-8 -*-
"""
Script de Processamento - Sem Movimentação
- Aplica regras de status e trânsito
- Filtra apenas registros de Regional "GP" e Bases válidas
- Gera relatórios em Excel (todos os pacotes válidos)
- Monta card no Feishu apenas com pacotes 5+ dias (mostrando só 5 piores)
"""

import pandas as pd
import os
import numpy as np
from datetime import datetime
import logging
import requests
from typing import List, Dict, Any, Optional

# ==============================================================================
# --- CONFIGURAÇÃO GERAL ---
# ==============================================================================

BASE_PATH = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Sem Movimentação'
OUTPUT_BASE_PATH = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Franquias\Sem Movimentação'
COORDENADOR_BASE_PATH = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador'

PATH_INPUT_MAIN = os.path.join(BASE_PATH, 'Sem_Movimentação')
ARQUIVO_MAPEAMENTO_COORDENADORES = os.path.join(COORDENADOR_BASE_PATH, 'Base_Atualizada.xlsx')

PATH_OUTPUT_REPORTS = OUTPUT_BASE_PATH
PATH_OUTPUT_ARQUIVO_MORTO = os.path.join(OUTPUT_BASE_PATH, "Arquivo Morto")

FILENAME_START_MAIN = 'Monitoramento de movimentação em tempo real'

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/18eed487-c172-4b86-95cf-bfbe1cd21df1"

# Colunas principais
COL_REMESSA = 'Remessa'
COL_DIAS_PARADO = 'Dias Parado'
COL_ULTIMA_OPERACAO = 'Tipo da última operação'
COL_REGIONAL = 'Regional responsável'
COL_NOME_PROBLEMATICO = 'Nome de pacote problemático'
COL_HORA_OPERACAO = 'Horário da última operação'
COL_DEVOLUCAO = 'Devolução'
COL_STATUS = 'Status'
COL_BASE_RECENTE = 'Nome da base mais recente'
COL_TRANSITO = 'Trânsito'

# Bases válidas
BASES_VALIDAS = [
    "F AGL-GO","F ALV-AM","F ALX-AM","F AMB-MS","F ANP-GO","F APG - GO","F ARQ - RO",
    "F BAO-PA","F BSL-AC","F CDN-AM","F CGR - MS","F CGR 02-MS","F CHR-AM","F CMV-MT",
    "F CNC-PA","F CNF-MT","F DOM -PA","F DOU-MS","F ELD-PA","F FMA-GO","F GAI-TO",
    "F GRP-TO","F GYN - GO","F GYN 02-GO","F GYN 03-GO","F IGA-PA","F ITI-PA",
    "F JCD-PA","F MCP 02-AP","F MCP-AP","F OCD-GO","F ORL-PA","F PCA-PA","F PDR-GO",
    "F PGM-PA","F PON-GO","F PVH-RO","F PVL-MT","F RDC -PA","F RVD - GO","F SEN-GO",
    "F SFX-PA","F TGT-DF","F TLA-PA","F TRD-GO","F TUR-PA","F VHL-RO","F VLP-GO","F XIG-PA"
]

# Configuração logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ==============================================================================
# --- FUNÇÕES DE REGRAS DE NEGÓCIO ---
# ==============================================================================

def aplicar_regras_transito(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Aplicando regras de trânsito...")
    if COL_BASE_RECENTE not in df.columns:
        df[COL_TRANSITO] = "COLUNA DE BASE RECENTE NÃO ENCONTRADA"
        return df

    cond_em_transito = df[COL_ULTIMA_OPERACAO] == "发件扫描/Bipe de expedição"
    origem_sc_bre = df[COL_BASE_RECENTE] == 'SC BRE'
    destino_pvh = df[COL_REGIONAL].str.contains('PVH-RO', na=False, case=False)
    prazo_5_dias_estourado = df[COL_DIAS_PARADO] >= 5
    prazo_3_dias_estourado = df[COL_DIAS_PARADO] >= 3

    conditions = [
        cond_em_transito & origem_sc_bre & prazo_5_dias_estourado,
        cond_em_transito & origem_sc_bre & ~prazo_5_dias_estourado,
        cond_em_transito & ~origem_sc_bre & destino_pvh & prazo_5_dias_estourado,
        cond_em_transito & ~origem_sc_bre & destino_pvh & ~prazo_5_dias_estourado,
        cond_em_transito & ~origem_sc_bre & ~destino_pvh & prazo_3_dias_estourado,
        cond_em_transito & ~origem_sc_bre & ~destino_pvh & ~prazo_3_dias_estourado,
    ]
    choices = [
        "FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)",
        "EM TRÂNSITO PARA A BASE",
        "FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)",
        "EM TRÂNSITO PARA A BASE",
        "FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)",
        "EM TRÂNSITO PARA A BASE",
    ]

    df[COL_TRANSITO] = np.select(conditions, choices, default='')
    return df


def aplicar_regras_status(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Aplicando regras de status...")
    is_problematico = df[COL_ULTIMA_OPERACAO] == "问题件扫描/Bipe de pacote problemático"

    regras: List[Dict[str, Any]] = [
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Extravio.interno.内部遗失"),
         "status": "PEDIDO EXTRAVIADO"},
    ]

    conditions = [r["condicao"] for r in regras]
    choices = [r["status"] for r in regras]
    df[COL_STATUS] = np.select(conditions, choices, default=df[COL_ULTIMA_OPERACAO].str.upper())
    return df

# ==============================================================================
# --- FUNÇÕES DE APOIO ---
# ==============================================================================

def carregar_relatorio_anterior(pasta_arquivo_morto: str) -> Optional[pd.DataFrame]:
    arquivos = [f for f in os.listdir(pasta_arquivo_morto) if f.endswith(".xlsx")]
    if not arquivos:
        logging.warning("Nenhum relatório encontrado no Arquivo Morto.")
        return None
    arquivo_mais_recente = max([os.path.join(pasta_arquivo_morto, f) for f in arquivos], key=os.path.getctime)
    logging.info(f"📂 Comparando com relatório anterior: {arquivo_mais_recente}")
    return pd.read_excel(arquivo_mais_recente)


def comparar_relatorios(df_atual: pd.DataFrame, df_anterior: Optional[pd.DataFrame]):
    atual_grouped = df_atual.groupby(COL_BASE_RECENTE).size().reset_index(name="QtdAtual")
    if df_anterior is not None:
        anterior_grouped = df_anterior.groupby(COL_BASE_RECENTE).size().reset_index(name="QtdAnterior")
        df_comp = pd.merge(atual_grouped, anterior_grouped, on=COL_BASE_RECENTE, how="outer").fillna(0)
    else:
        df_comp = atual_grouped.copy()
        df_comp["QtdAnterior"] = 0
    df_comp["Diferenca"] = df_comp["QtdAtual"] - df_comp["QtdAnterior"]

    qtd_total = df_comp["QtdAtual"].sum()
    variacao_total = df_comp["Diferenca"].sum()

    # 🔴 Agora pega só os 5 piores
    piores = df_comp.sort_values("QtdAtual", ascending=False).head(5)
    piores_list = [(row[COL_BASE_RECENTE], int(row["QtdAtual"])) for _, row in piores.iterrows()]

    return qtd_total, variacao_total, piores_list


def montar_card_franquias(data, qtd_total, variacao_pacotes, piores, link_relatorio):
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content":
                    f"**📊 Relatório Sem Movimentação (5+ dias)**\n"
                    f"**Data:** {data}\n"
                    f"**Total Pacotes:** {qtd_total}\n"
                    f"**Variação:** {variacao_pacotes}\n"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": "**🔴 5 Piores Franquias (Mais Pacotes)**"}},
                {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join([f"- {b}: {q}" for b, q in piores])}},
                {"tag": "hr"},
                {"tag": "action", "actions": [
                    {"tag": "button", "text": {"tag": "plain_text", "content": "📂 Abrir Relatório"},
                     "url": link_relatorio, "type": "default"}]}
            ],
            "header": {"title": {"tag": "plain_text", "content": "📦 Sem Movimentação - Franquias (5+ dias)"}}
        }
    }


def enviar_card(payload: Dict[str, Any], webhook_url: str):
    response = requests.post(webhook_url, json=payload)
    if response.status_code == 200:
        logging.info("✅ Card enviado com sucesso ao Feishu!")
    else:
        logging.error(f"❌ Erro ao enviar card: {response.status_code} - {response.text}")


# ==============================================================================
# --- MAIN ---
# ==============================================================================

def main():
    logging.info("Iniciando processamento...")

    arquivos = [f for f in os.listdir(PATH_INPUT_MAIN) if f.startswith(FILENAME_START_MAIN) and f.endswith('.xlsx')]
    if not arquivos:
        logging.error("Nenhum arquivo encontrado.")
        return

    arquivo_mais_recente = max([os.path.join(PATH_INPUT_MAIN, f) for f in arquivos], key=os.path.getctime)
    logging.info(f"Lendo arquivo principal: {arquivo_mais_recente}")

    df_main = pd.read_excel(arquivo_mais_recente)

    # 🔹 Filtrar apenas bases válidas
    df_main = df_main[df_main[COL_BASE_RECENTE].isin(BASES_VALIDAS)]

    # Dias parado
    df_main[COL_HORA_OPERACAO] = pd.to_datetime(df_main[COL_HORA_OPERACAO], errors='coerce')
    df_main[COL_DIAS_PARADO] = (datetime.now() - df_main[COL_HORA_OPERACAO]).dt.days.fillna(0).astype(int)

    # Relatório completo
    df_final = aplicar_regras_status(df_main)
    df_final = aplicar_regras_transito(df_final)

    # Salvar relatórios Excel
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    df_final.to_excel(os.path.join(PATH_OUTPUT_REPORTS, f"Relatório_SemMovimentação_Completo_{data_hoje}.xlsx"), index=False)

    # 🔹 Apenas 5+ dias para card
    df_card = df_final[df_final[COL_DIAS_PARADO] >= 5]

    # Comparar com relatório anterior
    df_anterior = carregar_relatorio_anterior(PATH_OUTPUT_ARQUIVO_MORTO)
    qtd_total, variacao_total, piores = comparar_relatorios(df_card, df_anterior)

    data = datetime.now().strftime("%d/%m/%Y %H:%M")
    variacao_pacotes = (
        f"⬇️ Diminuiu {abs(variacao_total)} pacotes" if variacao_total < 0 else
        f"⬆️ Aumentou {variacao_total} pacotes" if variacao_total > 0 else
        "➖ Sem variação"
    )

    card_payload = montar_card_franquias(data, qtd_total, variacao_pacotes, piores, "https://link-relatorio")
    enviar_card(card_payload, WEBHOOK_URL)

    logging.info("✅ Processo concluído com sucesso.")


if __name__ == "__main__":
    main()