# -*- coding: utf-8 -*-

import os
import shutil
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any

import pandas as pd
import numpy as np
from tqdm import tqdm

# ==============================================================================
# --- CONFIGURAÇÃO GERAL ---
# ==============================================================================

# --- 1. Caminhos Principais ---
BASE_PATH = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Sem Movimentação'
OUTPUT_BASE_PATH = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Jt - Relatórios'
COORDENADOR_BASE_PATH = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador'

# --- 2. Pastas e Arquivos de Entrada ---
PATH_INPUT_MAIN = os.path.join(BASE_PATH, 'Sem_Movimentação')
PATH_INPUT_PROBLEMATICOS = os.path.join(BASE_PATH, 'Pacotes Problematicos')
PATH_INPUT_DEVOLUCAO = os.path.join(BASE_PATH, 'Devolução')
ARQUIVO_MAPEAMENTO_COORDENADORES = os.path.join(COORDENADOR_BASE_PATH, 'Base_Atualizada.xlsx')

# --- 3. Pastas de Saída ---
PATH_OUTPUT_REPORTS = OUTPUT_BASE_PATH
PATH_OUTPUT_ARQUIVO_MORTO = os.path.join(OUTPUT_BASE_PATH, "Arquivo Morto")

# --- 4. Nomes de Arquivos e Colunas ---
FILENAME_START_MAIN = 'Monitoramento de movimentação em tempo real'

COL_REMESSA = 'Remessa'
COL_DIAS_PARADO = 'Dias Parado'
COL_ULTIMA_OPERACAO = 'Tipo da última operação'
COL_REGIONAL = 'Regional responsável'
COL_NOME_PROBLEMATICO = 'Nome de pacote problemático'
COL_HORA_OPERACAO = 'Horário da última operação'
COL_DEVOLUCAO = 'Devolução'
COL_STATUS = 'Status'
COL_MULTA = 'Multa (R$)'
COL_BASE_RECENTE = 'Nome da base mais recente'
COL_TRANSITO = 'Trânsito'

# Mapeamento coordenadores
COLUNA_CHAVE_PRINCIPAL = 'Unidade responsável'
COLUNA_CHAVE_MAPEAMENTO = 'Nome da base'
COLUNA_INFO_COORDENADOR = 'Coordenadores'
COLUNA_INFO_FILIAL = 'Filial'
NOVA_COLUNA_COORDENADOR = 'Coordenadores'
NOVA_COLUNA_FILIAL = 'Filial'

# --- 5. Listas para Regras de Negócio ---
FRANQUIAS = [
    'F CHR-AM', 'F CAC-RO', 'F PDR-GO', 'CZS -AC', 'F PVH-RO', 'GNT -MT', 'F ARQ - RO',
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

BASES_FLUXO_INVERSO = [
    "VLP -GO", "VHL-RO", "VGR-MT", "VGR 02-MT", "URC -GO", "TRD -GO", "TLL -MS", "TGT -DF",
    "TGA -MT", "TAR -AC", "SRS -MT", "SNP -MT", "SMD -AC", "SMB -GO", "SMA -GO", "SJA -GO",
    "SGO -MS", "SEN-GO", "SBN -DF", "SAMS -DF", "SAD -GO", "RVD -GO", "ROO -MT", "RFI -DF",
    "RDM -RO", "RBR -AC", "RBR 02-AC", "QUI -GO", "QRN -MT", "PVL -MT", "PVH -RO", "PVH 02-RO",
    "PTD -MT", "PRG -GO", "PRB -MS", "POS -GO", "PON -GO", "PNT-MS", "PLN -GO", "PLDF -DF",
    "PA GYN-GO", "OCD-GO", "NVT -MT", "NVR -MS", "NDI -MS", "MT CGB", "MDT -MT", "LUZ -GO",
    "LRV -MT", "JUI -MT", "JTI -GO", "JRD -MS", "JPN -RO", "ITR -GO", "IPR -GO", "GYN -GO",
    "GYN 07-GO", "GYN 06-GO", "GYN 05-GO", "GYN 04-GO", "GYN 03-GO", "GYN 02-GO", "GUA -DF", "GP",
    "GNT -MT", "GNA -GO", "GAM -DF", "FMA -GO", "FAI -GO", "F TRD-GO", "F RVD - GO", "F OCD - GO",
    "F GYN - GO", "F FMA-GO", "F CGR - MS", "F BSB-DF", "F BSB - DF", "F APG - GO", "F AGL-GO",
    "EMA -DF", "DOU -MS", "CZS -AC", "CXM -MS", "CTN -GO", "CTL -GO", "CRB -MS", "CNF -MT", "CMP-MT",
    "CHS -MS", "CGR -MS", "CGR 05-MS", "CGR 04-MS", "CGR 03-MS", "CGR 02-MS", "CGB 05-MT",
    "CGB 04-MT", "CGB 03-MT", "CGB 02-MT", "CEIS -DF", "CEIN -DF", "CCR -MT", "CAPI -GO", "CAN -GO",
    "BSB -DF", "BGA -MT", "ATF -MT", "ARQ -RO", "ARI -MT", "AQD -MS", "APG -GO", "ANP -GO",
    "AMB -MS", "AGL -GO", "AGB -MT"
]

DESTINOS_FLUXO_INVERSO = [
    "MAO -AM", "DC AGB-MT", "DC CGR-MS", "DC GYN-GO", "DC JUI-MT", "DC MAO-AM", "DC MRB-PA",
    "DC PMW-TO", "DC PVH-RO", "DC RBR-AC", "DC STM-PA", "DF BSB"
]

BASES_CD = BASES_FLUXO_INVERSO

# --- 6. Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ==============================================================================
# --- FUNÇÕES DE CARREGAMENTO ---
# ==============================================================================

def garantir_pastas():
    os.makedirs(PATH_OUTPUT_REPORTS, exist_ok=True)
    os.makedirs(PATH_OUTPUT_ARQUIVO_MORTO, exist_ok=True)


def encontrar_arquivo_principal(pasta: str, inicio_nome: str) -> Optional[str]:
    try:
        for nome_arquivo in os.listdir(pasta):
            if nome_arquivo.startswith(inicio_nome) and nome_arquivo.endswith(('.xlsx', '.xls')):
                logging.info(f"Arquivo principal encontrado: {nome_arquivo}")
                return os.path.join(pasta, nome_arquivo)
    except FileNotFoundError:
        logging.error(f"A pasta de leitura '{pasta}' não foi encontrada.")
        return None

    logging.warning(f"Nenhum arquivo começando com '{inicio_nome}' foi encontrado em '{pasta}'.")
    return None


def carregar_planilhas_de_pasta(caminho_pasta: str, descricao_tqdm: str) -> pd.DataFrame:
    """
    Lê todos os arquivos Excel de uma pasta (todas as abas) e consolida em 1 DataFrame.
    """
    lista_dfs: List[pd.DataFrame] = []
    nome_pasta = os.path.basename(caminho_pasta)
    logging.info(f"Lendo planilhas da pasta: {nome_pasta}")

    try:
        arquivos = [f for f in os.listdir(caminho_pasta) if f.endswith(('.xlsx', '.xls'))]
        if not arquivos:
            logging.warning(f"Nenhum arquivo Excel encontrado na pasta '{nome_pasta}'.")
            return pd.DataFrame()

        for arquivo in tqdm(arquivos, desc=descricao_tqdm):
            caminho_completo = os.path.join(caminho_pasta, arquivo)
            try:
                abas = pd.read_excel(caminho_completo, sheet_name=None)
                lista_dfs.extend([df for df in abas.values() if isinstance(df, pd.DataFrame) and not df.empty])
            except Exception as e:
                logging.error(f"Falha ao ler '{arquivo}' em '{nome_pasta}': {e}")
                continue

        if not lista_dfs:
            return pd.DataFrame()

        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        logging.info(f"Total de {len(df_consolidado)} registros consolidados de '{nome_pasta}'.")
        return df_consolidado

    except FileNotFoundError:
        logging.error(f"A pasta '{caminho_pasta}' não foi encontrada.")
        raise
# ==============================================================================
# --- REGRAS DE NEGÓCIO ---
# ==============================================================================

def aplicar_regras_transito(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Aplicando regras de trânsito...")

    for col in [COL_BASE_RECENTE, COL_ULTIMA_OPERACAO, COL_REGIONAL, COL_DIAS_PARADO]:
        if col not in df.columns:
            logging.warning(f"Coluna '{col}' não encontrada. Regras de trânsito não aplicadas.")
            df[COL_TRANSITO] = "DADOS INSUFICIENTES PARA TRÂNSITO"
            return df

    cond_em_transito = df[COL_ULTIMA_OPERACAO] == "发件扫描/Bipe de expedição"
    is_fluxo_inverso = df[COL_BASE_RECENTE].isin(BASES_FLUXO_INVERSO) & df[COL_REGIONAL].isin(DESTINOS_FLUXO_INVERSO)
    origem_sc_bre = df[COL_BASE_RECENTE] == 'SC BRE'
    destino_pvh = df[COL_REGIONAL].astype(str).str.contains('PVH-RO', na=False, case=False)

    prazo_fluxo_inverso_estourado = df[COL_DIAS_PARADO] >= 3
    prazo_5_dias_estourado = df[COL_DIAS_PARADO] >= 5
    prazo_3_dias_estourado = df[COL_DIAS_PARADO] >= 3

    conditions = [
        cond_em_transito & is_fluxo_inverso & prazo_fluxo_inverso_estourado,
        cond_em_transito & is_fluxo_inverso & ~prazo_fluxo_inverso_estourado,
        cond_em_transito & origem_sc_bre & prazo_5_dias_estourado,
        cond_em_transito & origem_sc_bre & ~prazo_5_dias_estourado,
        cond_em_transito & ~origem_sc_bre & destino_pvh & prazo_5_dias_estourado,
        cond_em_transito & ~origem_sc_bre & destino_pvh & ~prazo_5_dias_estourado,
        cond_em_transito & ~origem_sc_bre & ~destino_pvh & prazo_3_dias_estourado,
        cond_em_transito & ~origem_sc_bre & ~destino_pvh & ~prazo_3_dias_estourado,
    ]
    choices = [
        "VERIFICAR COM TRANSPORTE: VEÍCULO NÃO CHEGOU (FLUXO INVERSO)",
        "EM TRÂNSITO (FLUXO INVERSO)",
        "FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)",
        "EM TRÂNSITO PARA A BASE",
        "FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)",
        "EM TRÂNSITO PARA A BASE",
        "FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)",
        "EM TRÂNSITO PARA A BASE",
    ]

    df[COL_TRANSITO] = np.select(conditions, choices, default='')
    logging.info("Regras de trânsito aplicadas.")
    return df


def aplicar_regras_status(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Aplicando regras de status...")

    for col in [COL_ULTIMA_OPERACAO, COL_NOME_PROBLEMATICO, COL_DIAS_PARADO, COL_BASE_RECENTE, COL_REGIONAL]:
        if col not in df.columns:
            logging.warning(f"Coluna '{col}' não encontrada. Status ficará padrão.")
            df[COL_STATUS] = df.get(COL_ULTIMA_OPERACAO, pd.Series(index=df.index)).fillna("").astype(str).str.upper()
            return df

    is_problematico = df[COL_ULTIMA_OPERACAO] == "问题件扫描/Bipe de pacote problemático"
    is_envio_errado_cd = df[COL_BASE_RECENTE].isin(BASES_CD) & df[COL_REGIONAL].isin(BASES_CD)

    regras: List[Dict[str, Any]] = []

    # ================================================================
    # 1. PROBLEMÁTICOS
    # ================================================================
    regras += [
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Extravio.interno.内部遗失"),
         "status": "PEDIDO EXTRAVIADO"},
        {"condicao": is_problematico &
                    (df[COL_NOME_PROBLEMATICO] == "Encomenda.expedido.mas.não.chegou.有发未到件") &
                    (df[COL_DIAS_PARADO] >= 3),
         "status": "ALERTA DE EXTRAVIO: ABRIR CHAMADO INTERNO (HÁ MAIS DE 3 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Encomenda.expedido.mas.não.chegou.有发未到件"),
         "status": "ATENÇÃO: RISCO DE EXTRAVIO (AGUARDANDO CHEGADA)"},
    ]

    regras += [
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "retidos.留仓") & (df[COL_DIAS_PARADO] >= 3),
         "status": "ATENÇÃO: PACOTE RETIDO NO PISO (HÁ MAIS DE 3 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "retidos.留仓"),
         "status": "ATENÇÃO: PACOTE RETIDO NO PISO"},
    ]

    regras += [
        {"condicao": is_problematico &
                    (df[COL_NOME_PROBLEMATICO].isin([
                        "Endereço.incorreto地址信息错误",
                        "Impossibilidade.de.chegar.no.endereço.informado客户地址无法进入",
                        "Endereço.incompleto地址信息不详",
                        "Impossibilidade.de.chegar.no.endereço.informado.de.coleta.客户地址无法进入C"
                    ])) &
                    (df[COL_DIAS_PARADO] >= 8),
         "status": "SOLICITAR DEVOLUÇÃO (ENDEREÇO/ACESSO INCORRETO, HÁ MAIS DE 8 DIAS)"},
        {"condicao": is_problematico &
                    (df[COL_NOME_PROBLEMATICO].isin([
                        "Endereço.incorreto地址信息错误",
                        "Impossibilidade.de.chegar.no.endereço.informado客户地址无法进入",
                        "Endereço.incompleto地址信息不详",
                        "Impossibilidade.de.chegar.no.endereço.informado.de.coleta.客户地址无法进入C"
                    ])),
         "status": "ATENÇÃO: AGUARDANDO DEVOLUÇÃO (ENDEREÇO/ACESSO INCORRETO)"},
    ]

    regras += [
        {"condicao": is_problematico &
                    (df[COL_NOME_PROBLEMATICO] == "Ausência.de.destinatário.nas.várias.tentativas.de.entrega多次派送客户不在"),
         "status": "VERIFICAR 3 TENTATIVAS DE ENTREGA. SE OK, SOLICITAR DEVOLUÇÃO. SENÃO, REALIZAR NOVA TENTATIVA."},
        {"condicao": is_problematico &
                    (df[COL_NOME_PROBLEMATICO] == "Ausência.do.destinatário客户不在") &
                    (df[COL_DIAS_PARADO] >= 2),
         "status": "ATENÇÃO: DEVOLVER À BASE (AUSÊNCIA, HÁ MAIS DE 2 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Ausência.do.destinatário客户不在"),
         "status": "ATENÇÃO: DEVOLUÇÃO À BASE PENDENTE (AUSÊNCIA)"},
    ]

    regras += [
        {"condicao": is_problematico &
                    (df[COL_NOME_PROBLEMATICO].isin([
                        "Recusa.de.recebimento.pelo.cliente.(destinatário)无理由拒收",
                        "O.destinatário.mudou.o.endereço.收件人搬家"
                    ])) &
                    (df[COL_DIAS_PARADO] >= 2),
         "status": "ATENÇÃO: DEVOLVER À BASE (RECUSA/MUDANÇA DE ENDEREÇO, HÁ MAIS DE 2 DIAS)"},
        {"condicao": is_problematico &
                    (df[COL_NOME_PROBLEMATICO].isin([
                        "Recusa.de.recebimento.pelo.cliente.(destinatário)无理由拒收",
                        "O.destinatário.mudou.o.endereço.收件人搬家"
                    ])),
         "status": "ATENÇÃO: DEVOLUÇÃO À BASE PENDENTE (RECUSA/MUDANÇA DE ENDEREÇO)"},
    ]

    regras += [
        {"condicao": is_problematico &
                    (df[COL_NOME_PROBLEMATICO].isin([
                        "Pacote.fora.do.padrão.三边尺寸超限",
                        "Embalagem.não.conforme.包装不规范"
                    ])),
         "status": "SOLICITAR DEVOLUÇÃO IMEDIATA (FORA DO PADRÃO / EMBALAGEM NÃO CONFORME)"},
        {"condicao": is_problematico &
                    (df[COL_NOME_PROBLEMATICO] == "Mercadorias.que.chegam.incompletos货未到齐") &
                    (df[COL_DIAS_PARADO] >= 2),
         "status": "ENVIAR PARA O FLUXO INVERSO (INCOMPLETO, HÁ MAIS DE 2 DIAS)"},
        {"condicao": is_problematico &
                    (df[COL_NOME_PROBLEMATICO] == "Pacotes.retidos.por.anomalias.异常拦截件") &
                    (df[COL_DIAS_PARADO] >= 3),
         "status": "ENVIAR PARA A QUALIDADE (ANOMALIA, HÁ MAIS DE 3 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Pacotes.retidos.por.anomalias.异常拦截件"),
         "status": "ATENÇÃO: ANOMALIA EM ANÁLISE"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Devolução.退回件"),
         "status": "ENVIAR PARA SC/DC (DEVOLUÇÃO APROVADA)"},
    ]

    # ================================================================
    # 2. OPERAÇÕES NORMAIS
    # ================================================================
    regras += [
        {"condicao": (df[COL_ULTIMA_OPERACAO] == "出仓扫描/Bipe de saída para entrega") &
                    (df[COL_REGIONAL].isin(FRANQUIAS)) &
                    (df[COL_DIAS_PARADO] >= 2),
         "status": "ATRASO NA ENTREGA (FRANQUIA)"},
        {"condicao": (df[COL_ULTIMA_OPERACAO] == "出仓扫描/Bipe de saída para entrega") &
                    (df[COL_REGIONAL].isin(FRANQUIAS)),
         "status": "EM ROTA DE ENTREGA (FRANQUIA)"},
        {"condicao": (df[COL_ULTIMA_OPERACAO] == "出仓扫描/Bipe de saída para entrega") &
                    (~df[COL_REGIONAL].isin(FRANQUIAS)) &
                    (df[COL_DIAS_PARADO] >= 2),
         "status": "ATENÇÃO: ATRASO NA ENTREGA (BASE PRÓPRIA)"},
        {"condicao": (df[COL_ULTIMA_OPERACAO] == "出仓扫描/Bipe de saída para entrega"),
         "status": "EM ROTA DE ENTREGA (BASE PRÓPRIA)"},
    ]

    # ================================================================
    # 3. ENVIO ERRADO (CDs)
    # ================================================================
    regras += [
        {"condicao": is_envio_errado_cd &
                    (df[COL_NOME_PROBLEMATICO] == "Mercadorias.do.cliente.não.estão.completas.客户货物未备齐"),
         "status": "ENVIAR PARA O FLUXO INVERSO (INCOMPLETO, HÁ MAIS DE 2 DIAS)"},
        {"condicao": is_envio_errado_cd &
                    (df[COL_NOME_PROBLEMATICO] == "Ausência.do.destinatário客户不在"),
         "status": "VERIFICAR 3 TENTATIVAS DE ENTREGA. SE OK, SOLICITAR DEVOLUÇÃO. SENÃO, REALIZAR NOVA TENTATIVA."},
        {"condicao": is_envio_errado_cd,
         "status": "ENVIO ERRADO - ENTRE CDs"},
    ]

    conditions = [r["condicao"] for r in regras]
    choices = [r["status"] for r in regras]

    base_default = df[COL_ULTIMA_OPERACAO].fillna("").astype(str).str.upper()
    df[COL_STATUS] = np.select(conditions, choices, default=base_default)

    logging.info("Regras de status aplicadas.")
    return df


def calcular_multa(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df_copy = df.copy()
    condicoes_multa = [
        df_copy[COL_DIAS_PARADO] >= 30,
        df_copy[COL_DIAS_PARADO].between(14, 29),
        df_copy[COL_DIAS_PARADO].between(10, 13),
        df_copy[COL_DIAS_PARADO].between(7, 9),
        df_copy[COL_DIAS_PARADO] == 6,
        df_copy[COL_DIAS_PARADO] == 5,
        df_copy[COL_DIAS_PARADO] == 4,
        df_copy[COL_DIAS_PARADO] == 3,
        df_copy[COL_DIAS_PARADO] == 2,
    ]
    valores_multa = [30, 14, 10, 7, 6, 5, 4, 3, 2]
    df_copy[COL_MULTA] = np.select(condicoes_multa, valores_multa, default=0)
    return df_copy


def processar_dados(df_main: pd.DataFrame, df_problematicos: pd.DataFrame, df_devolucao: pd.DataFrame) -> pd.DataFrame:
    colunas_necessarias = [COL_REMESSA, COL_ULTIMA_OPERACAO, COL_REGIONAL, COL_NOME_PROBLEMATICO, COL_HORA_OPERACAO, COL_BASE_RECENTE]
    faltantes = [c for c in colunas_necessarias if c not in df_main.columns]
    if faltantes:
        logging.critical(f"Arquivo principal sem colunas obrigatórias: {faltantes}")
        return pd.DataFrame()

    df = df_main.copy()
    df[COL_REMESSA] = df[COL_REMESSA].astype(str)

    # --- Pacotes problemáticos ---
    df["Qtd Problemáticas"] = 0
    df["Última Problemática Detalhada"] = "-"

    if not df_problematicos.empty:
        needed_cols = ["Número de pedido JMS", "Tempo de digitalização", "Tipo de nível II de pacote problemático"]
        missing_pb = [c for c in needed_cols if c not in df_problematicos.columns]
        if missing_pb:
            logging.warning(f"Problemáticos sem colunas esperadas {missing_pb}. Pulando integração.")
        else:
            df_problematicos = df_problematicos.copy()
            df_problematicos["Número de pedido JMS"] = df_problematicos["Número de pedido JMS"].astype(str)
            df_problematicos["Tempo de digitalização"] = pd.to_datetime(df_problematicos["Tempo de digitalização"], errors="coerce")

            df_problematicos = (
                df_problematicos
                .dropna(subset=["Número de pedido JMS"])
                .sort_values("Tempo de digitalização")
            )

            summary = (
                df_problematicos
                .groupby("Número de pedido JMS", as_index=False)
                .agg(
                    **{
                        "Qtd Problemáticas": ("Número de pedido JMS", "size"),
                        "Última Problemática Detalhada": ("Tipo de nível II de pacote problemático", "last"),
                    }
                )
            )

            df = df.merge(summary, left_on=COL_REMESSA, right_on="Número de pedido JMS", how="left")
            df.drop(columns=["Número de pedido JMS"], inplace=True, errors="ignore")
            df["Qtd Problemáticas"] = df["Qtd Problemáticas"].fillna(0).astype(int)
            df["Última Problemática Detalhada"] = df["Última Problemática Detalhada"].fillna("-")
            logging.info("Dados de pacotes problemáticos integrados.")

    # --- Devoluções ---
    df[COL_DEVOLUCAO] = "DEVOLUÇÃO NÃO SOLICITADA"
    if not df_devolucao.empty:
        needed_cols = ["Número de pedido JMS", "Estado de solicitação"]
        missing_dev = [c for c in needed_cols if c not in df_devolucao.columns]
        if missing_dev:
            logging.warning(f"Devolução sem colunas esperadas {missing_dev}. Pulando integração.")
        else:
            df_devolucao = df_devolucao.copy()
            df_devolucao["Número de pedido JMS"] = df_devolucao["Número de pedido JMS"].astype(str)

            mapa_traducao = {
                "待审核": "EM PROCESSO DE APROVAÇÃO",
                "驳回": "PEDIDO DE DEVOLUÇÃO RECUSADO",
                "已审核": "DEVOLUÇÃO APROVADA"
            }
            df_devolucao["Status_Traduzido"] = df_devolucao["Estado de solicitação"].map(mapa_traducao)

            df_devolucao_info = (
                df_devolucao
                .dropna(subset=["Status_Traduzido"])
                [["Número de pedido JMS", "Status_Traduzido"]]
                .drop_duplicates(subset="Número de pedido JMS", keep="last")
            )

            df = df.merge(df_devolucao_info, left_on=COL_REMESSA, right_on="Número de pedido JMS", how="left")
            df[COL_DEVOLUCAO] = df["Status_Traduzido"].fillna(df[COL_DEVOLUCAO])
            df.drop(columns=["Número de pedido JMS", "Status_Traduzido"], inplace=True, errors="ignore")
            logging.info("Dados de devolução integrados.")

    # --- Dias Parado ---
    logging.info("Calculando dias parados...")
    df[COL_HORA_OPERACAO] = pd.to_datetime(df[COL_HORA_OPERACAO], errors="coerce")
    df[COL_DIAS_PARADO] = (datetime.now() - df[COL_HORA_OPERACAO]).dt.days.fillna(0).astype(int)

    # --- Regras ---
    df = aplicar_regras_status(df)
    df = aplicar_regras_transito(df)

    # --- Prioriza devolução ---
    df.loc[df[COL_DEVOLUCAO] != "DEVOLUÇÃO NÃO SOLICITADA", COL_STATUS] = df[COL_DEVOLUCAO]

    cond_aprovado_em_rota = (df[COL_STATUS] == "DEVOLUÇÃO APROVADA") & (df[COL_ULTIMA_OPERACAO] == "出仓扫描/Bipe de saída para entrega")
    df.loc[cond_aprovado_em_rota, COL_STATUS] = "DEVOLUÇÃO APROVADA, MAS O PACOTE ESTÁ EM ROTA"

    condicao_aprovado = df[COL_STATUS].isin(["DEVOLUÇÃO APROVADA", "DEVOLUÇÃO APROVADA, MAS O PACOTE ESTÁ EM ROTA"])
    df.loc[condicao_aprovado, COL_TRANSITO] = ""

    # Renomeia para casar com mapeamento
    df.rename(columns={COL_REGIONAL: COLUNA_CHAVE_PRINCIPAL}, inplace=True)

    # Ordem
    ordem_colunas = [
        COL_REMESSA, COLUNA_CHAVE_PRINCIPAL, COL_DIAS_PARADO, COL_ULTIMA_OPERACAO,
        COL_HORA_OPERACAO, COL_STATUS, COL_TRANSITO, COL_DEVOLUCAO,
        "Qtd Problemáticas", "Última Problemática Detalhada",
    ]
    colunas_existentes = [c for c in df.columns if c not in ordem_colunas]
    df = df[ordem_colunas + colunas_existentes]

    return df


def adicionar_info_coordenador(df_principal: pd.DataFrame) -> pd.DataFrame:
    if df_principal.empty:
        logging.warning("DataFrame vazio. Pulando coordenadores.")
        return df_principal

    try:
        df_mapeamento = pd.read_excel(ARQUIVO_MAPEAMENTO_COORDENADORES)
    except FileNotFoundError:
        logging.error(f"Arquivo de mapeamento não encontrado: {ARQUIVO_MAPEAMENTO_COORDENADORES}")
        raise

    for col in [COLUNA_CHAVE_MAPEAMENTO, COLUNA_INFO_COORDENADOR, COLUNA_INFO_FILIAL]:
        if col not in df_mapeamento.columns:
            logging.error(f"Mapeamento sem coluna '{col}'.")
            raise ValueError(f"Mapeamento sem coluna '{col}'.")

    df_mapeamento = df_mapeamento.copy()
    df_mapeamento[COLUNA_CHAVE_MAPEAMENTO] = df_mapeamento[COLUNA_CHAVE_MAPEAMENTO].astype(str).str.strip()

    mapa_coordenador = pd.Series(df_mapeamento[COLUNA_INFO_COORDENADOR].values, index=df_mapeamento[COLUNA_CHAVE_MAPEAMENTO]).to_dict()
    mapa_filial = pd.Series(df_mapeamento[COLUNA_INFO_FILIAL].values, index=df_mapeamento[COLUNA_CHAVE_MAPEAMENTO]).to_dict()

    key_series = df_principal[COLUNA_CHAVE_PRINCIPAL]
    if isinstance(key_series, pd.DataFrame):
        key_series = key_series.iloc[:, 0]

    key_series = key_series.astype(str).str.strip()

    df_principal[NOVA_COLUNA_COORDENADOR] = key_series.map(mapa_coordenador).fillna("NÃO ENCONTRADO")
    df_principal[NOVA_COLUNA_FILIAL] = key_series.map(mapa_filial).fillna("NÃO ENCONTRADA")
    logging.info("Coordenador e Filial adicionados.")
    return df_principal
# ==============================================================================
# --- RELATÓRIOS / ARQUIVO MORTO / MAIN ---
# ==============================================================================

def salvar_relatorios(df_final: pd.DataFrame, pasta_saida: str):
    if df_final.empty:
        logging.warning("Nenhum dado para salvar relatórios.")
        return

    if COL_DIAS_PARADO not in df_final.columns:
        logging.error(f"Coluna '{COL_DIAS_PARADO}' não existe no df_final. Abortando salvamento.")
        return

    data_hoje = datetime.now().strftime("%Y-%m-%d")

    # Debug resumo
    try:
        resumo = {
            "0-4 dias": int((df_final[COL_DIAS_PARADO] <= 4).sum()),
            "5+ dias": int((df_final[COL_DIAS_PARADO] >= 5).sum()),
            "Total": int(len(df_final))
        }
        logging.info(f"Resumo Dias Parados: {resumo}")
    except Exception as e:
        logging.error(f"Erro ao gerar resumo: {e}")

    # 1) 0-4 dias
    df_0_4 = df_final[df_final[COL_DIAS_PARADO] <= 4]
    if not df_0_4.empty:
        arquivo_0_4 = os.path.join(pasta_saida, f"Relatório Sem Movimentação (0-4 dias)_{data_hoje}.xlsx")
        df_0_4.to_excel(arquivo_0_4, index=False)
        logging.info(f"Relatório 0-4 salvo: {arquivo_0_4}")

    # 2) 5+ dias (com multa)
    df_5_plus = df_final[df_final[COL_DIAS_PARADO] >= 5]
    if not df_5_plus.empty:
        df_5_plus = calcular_multa(df_5_plus)
        arquivo_5_plus = os.path.join(pasta_saida, f"Relatório Sem Movimentação (5+ dias)_{data_hoje}.xlsx")
        df_5_plus.to_excel(arquivo_5_plus, index=False)
        logging.info(f"Relatório 5+ salvo: {arquivo_5_plus}")
    else:
        logging.warning("Nenhum pedido com 5+ dias.")

    # 3) Incompletos
    if COL_NOME_PROBLEMATICO in df_final.columns:
        df_incompletos = df_final[df_final[COL_NOME_PROBLEMATICO] == "Mercadorias.que.chegam.incompletos货未到齐"]
        if not df_incompletos.empty:
            arquivo_incompletos = os.path.join(pasta_saida, f"Relatório Mercadorias incompletas_{data_hoje}.xlsx")
            df_incompletos.to_excel(arquivo_incompletos, index=False)
            logging.info(f"Relatório incompletos salvo: {arquivo_incompletos}")


def mover_para_arquivo_morto(pasta_origem: str, pasta_destino: str):
    if not os.path.exists(pasta_origem):
        os.makedirs(pasta_origem, exist_ok=True)

    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino, exist_ok=True)

    hoje = datetime.now().strftime("%Y-%m-%d")
    arquivos = [f for f in os.listdir(pasta_origem) if f.endswith(('.xlsx', '.xls'))]
    arquivos_hoje = [f for f in arquivos if hoje in f]

    arquivos_hoje.sort(key=lambda f: os.path.getmtime(os.path.join(pasta_origem, f)))

    # Mantém o mais novo de hoje
    if len(arquivos_hoje) > 1:
        for arquivo in arquivos_hoje[:-1]:
            try:
                shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo))
                logging.info(f"Arquivo duplicado de hoje movido: {arquivo}")
            except Exception as e:
                logging.error(f"Erro ao mover duplicado {arquivo}: {e}")

    # Move tudo que não é de hoje
    for arquivo in arquivos:
        if arquivo not in arquivos_hoje:
            try:
                shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo))
                logging.info(f"Arquivo antigo movido: {arquivo}")
            except Exception as e:
                logging.error(f"Erro ao mover antigo {arquivo}: {e}")


def main():
    logging.info("--- INICIANDO PROCESSO DE GERAÇÃO DE RELATÓRIOS ---")
    garantir_pastas()

    caminho_arquivo_original = encontrar_arquivo_principal(PATH_INPUT_MAIN, FILENAME_START_MAIN)
    if not caminho_arquivo_original:
        logging.critical("Arquivo principal não encontrado. Processo interrompido.")
        return

    df_main = pd.read_excel(caminho_arquivo_original)
    df_problematicos = carregar_planilhas_de_pasta(PATH_INPUT_PROBLEMATICOS, "Consolidando problemáticos")
    df_devolucao = carregar_planilhas_de_pasta(PATH_INPUT_DEVOLUCAO, "Consolidando devoluções")

    df_final = processar_dados(df_main, df_problematicos, df_devolucao)
    df_final = adicionar_info_coordenador(df_final)

    mover_para_arquivo_morto(PATH_OUTPUT_REPORTS, PATH_OUTPUT_ARQUIVO_MORTO)
    salvar_relatorios(df_final, PATH_OUTPUT_REPORTS)

    logging.info("--- PROCESSO CONCLUÍDO COM SUCESSO! ---")


if __name__ == "__main__":
    main()
