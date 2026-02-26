# -*- coding: utf-8 -*-
import pandas as pd
import os
import numpy as np
from tqdm import tqdm
from datetime import datetime
import shutil
import logging
import time
from typing import List, Dict, Optional, Any

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

# Colunas principais
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

# Colunas para mapeamento de coordenadores
COLUNA_CHAVE_PRINCIPAL = 'Unidade responsável'
COLUNA_CHAVE_MAPEAMENTO = 'Nome da base'
COLUNA_INFO_COORDENADOR = 'Coordenadores'
COLUNA_INFO_FILIAL = 'Filial'
NOVA_COLUNA_COORDENADOR = 'Coordenadores'
NOVA_COLUNA_FILIAL = 'Filial'

# --- 5. Regra: “Mercadorias incompletas” (cria relatório separado e REMOVE do principal/Feishu) ---
INCOMPLETOS_KEY_EXATA = "Mercadorias.que.chegam.incompletos货未到齐"
INCOMPLETOS_KEY_PARTE_1 = "Mercadorias.que.chegam.incompletos"
INCOMPLETOS_KEY_PARTE_2 = "货未到齐"

# --- 6. Listas para Regras de Negócio ---
FRANQUIAS = ["F AGL-GO", "F ALV-AM", "F ALX-AM", "F AMB-MS", "F ANP-GO", "F APG - GO",
    "F ARQ - RO", "F BAO-PA", "F BSB - DF", "F BSB-DF", "F BSL-AC", "F CDN-AM",
    "F CEI-DF", "F CGR - MS", "F CGR 02-MS", "F CHR-AM", "F CMV-MT", "F CNC-PA",
    "F CNF-MT", "F DOM -PA", "F DOU-MS", "F ELD-PA", "F FMA-GO", "F GAI-TO",
    "F GRP-TO", "F GYN - GO", "F GYN 02-GO", "F GYN 03-GO", "F IGA-PA", "F ITI -PA",
    "F ITI-PA", "F JCD-PA", "F MCP 02-AP", "F MCP-AP", "F OCD - GO", "F OCD-GO",
    "F ORL-PA", "F PCA-PA", "F PDR-GO", "F PGM-PA", "F PLN-DF", "F PON-GO",
    "F POS-GO", "F PVH 02-RO", "F PVH-RO", "F PVL-MT", "F RDC -PA", "F RVD - GO",
    "F SEN-GO", "F SFX-PA", "F TGA-MT", "F TGT-DF", "F TLA-PA", "F TRD-GO",
    "F TUR-PA", "F VHL-RO", "F VLP-GO", "F XIG-PA", "F TRM-AM", "F STM-PA",
    "F JPN 02-RO", "F CAC-RO", "F SVC-RR", "F SNP-MT", "F SJA-GO", "F SBS-DF", "F SBN-DF", "F SAM-DF",
    "F ROO-MT", "F RFI-DF", "F RBR-AC", "F RBR 02-AC", "F PVL 02-MT", "F PVH 03-RO",
    "F PTD-MT", "F PPA-MS", "F PNA-TO", "F PLA-GO", "F PDT-TO", "F PDP-PA",
    "F PAZ-AM", "F NMB-PA", "F NDI-MS", "F MTB-PA", "F MRL-AM", "F MDR-PA",
    "F MDO-RO", "F MAC-AP", "F JRG-GO", "F JPN-RO", "F JAU-RO", "F IPX-PA",
    "F HMT-AM", "F GYN 04-GO", "F GUA-DF", "F GNS-PA", "F GFN-PA", "F EMA-DF",
    "F CTL-GO", "F CRX-GO", "F CRH-PA", "F CGR 04-MS", "F CGR 03-MS", "F CDN 02-AM",
    "F CCR-MT", "F BVB-RR", "F BTS-RO", "F ARQ 02-RO", "F ANA-PA", "F AGB-MT",
    "F AGB 02-MT"]

UNIDADES_SC_DC = ["DC AGB-MT", "DC CGR-MS", "DC GYN-GO", "DC JUI-MT", "DC PVH-RO", "DC RBR-AC", "DF BSB", "GYN -GO",
                  "MT CGB"]

BASES_FLUXO_INVERSO = ["VLP -GO", "VHL-RO", "VGR-MT", "VGR 02-MT", "URC -GO", "TRD -GO", "TLL -MS", "TGT -DF",
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
                       "AMB -MS", "AGL -GO", "AGB -MT"]

DESTINOS_FLUXO_INVERSO = ["MAO -AM", "DC AGB-MT", "DC CGR-MS", "DC GYN-GO", "DC JUI-MT", "DC MAO-AM", "DC MRB-PA",
                          "DC PMW-TO", "DC PVH-RO", "DC RBR-AC", "DC STM-PA", "DF BSB"]

BASES_CD = BASES_FLUXO_INVERSO

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# ==============================================================================
# --- AVISO DE TÉRMINO (LOG + PRINT + NOTIFICAÇÃO + BEEP) ---
# ==============================================================================
def avisar_termino(titulo: str, mensagem: str, sucesso: bool = True) -> None:
    """
    Aviso no fim do processo:
    - log + print
    - notificação do Windows (se plyer/win10toast estiver instalado)
    - beep (winsound) quando possível
    """
    try:
        # 1) Log + Print
        if sucesso:
            logging.info(f"✅ {titulo} - {mensagem}")
        else:
            logging.error(f"❌ {titulo} - {mensagem}")
        print(f"\n{titulo}\n{mensagem}\n")

        # 2) Notificação (opcional) via plyer
        try:
            from plyer import notification  # pip install plyer
            notification.notify(
                title=titulo,
                message=mensagem,
                app_name="Relatórios J&T",
                timeout=10
            )
        except Exception:
            pass

        # 3) Notificação (opcional) via win10toast
        try:
            from win10toast import ToastNotifier  # pip install win10toast
            toaster = ToastNotifier()
            toaster.show_toast(titulo, mensagem, duration=10, threaded=True)
        except Exception:
            pass

        # 4) Beep do Windows (quando disponível)
        try:
            import winsound
            # Sons diferentes para sucesso/erro
            winsound.MessageBeep(winsound.MB_ICONASTERISK if sucesso else winsound.MB_ICONHAND)
        except Exception:
            # fallback: bell no terminal
            try:
                print("\a", end="")
            except Exception:
                pass

    except Exception:
        # não deixa o processo falhar por causa do aviso
        pass


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
    lista_dfs = []
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
                lista_dfs.extend(abas.values())
            except Exception as e:
                logging.error(f"Falha ao ler o arquivo '{arquivo}' da pasta '{nome_pasta}': {e}")
                continue

        if not lista_dfs:
            return pd.DataFrame()

        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        logging.info(f"Total de {len(df_consolidado)} registros consolidados de '{nome_pasta}'.")
        return df_consolidado

    except FileNotFoundError:
        logging.error(f"A pasta '{caminho_pasta}' não foi encontrada. Processo interrompido.")
        raise
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado ao ler os arquivos da pasta '{nome_pasta}': {e}")
        raise


def aplicar_regras_transito(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Aplicando regras de trânsito...")

    if COL_BASE_RECENTE not in df.columns:
        logging.warning(f"Coluna '{COL_BASE_RECENTE}' não encontrada. As regras de trânsito não serão aplicadas.")
        df[COL_TRANSITO] = "COLUNA DE BASE RECENTE NÃO ENCONTRADA"
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
    logging.info("Regras de trânsito aplicadas com sucesso.")
    return df


def aplicar_regras_status(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Aplicando regras de status...")

    is_problematico = df[COL_ULTIMA_OPERACAO] == "问题件扫描/Bipe de pacote problemático"
    is_envio_errado_cd = df[COL_BASE_RECENTE].isin(BASES_CD) & df[COL_REGIONAL].isin(BASES_CD)

    regras: List[Dict[str, Any]] = []

    # 1) PROBLEMÁTICOS — Extravio
    regras += [
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Extravio.interno.内部遗失"),
         "status": "PEDIDO EXTRAVIADO"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Encomenda.expedido.mas.não.chegou.有发未到件") &
                     (df[COL_DIAS_PARADO] >= 3),
         "status": "ALERTA DE EXTRAVIO: ABRIR CHAMADO INTERNO (HÁ MAIS DE 3 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Encomenda.expedido.mas.não.chegou.有发未到件"),
         "status": "ATENÇÃO: RISCO DE EXTRAVIO (AGUARDANDO CHEGADA)"},
    ]

    # Retidos
    regras += [
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "retidos.留仓") & (df[COL_DIAS_PARADO] >= 3),
         "status": "ATENÇÃO: PACOTE RETIDO NO PISO (HÁ MAIS DE 3 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "retidos.留仓"),
         "status": "ATENÇÃO: PACOTE RETIDO NO PISO"},
    ]

    # Endereço
    regras += [
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO].isin([
            "Endereço.incorreto地址信息错误",
            "Impossibilidade.de.chegar.no.endereço.informado客户地址无法进入",
            "Endereço.incompleto地址信息不详",
            "Impossibilidade.de.chegar.no.endereço.informado.de.coleta.客户地址无法进入C"
        ])) & (df[COL_DIAS_PARADO] >= 8),
         "status": "SOLICITAR DEVOLUÇÃO (ENDEREÇO/ACESSO INCORRETO, HÁ MAIS DE 8 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO].isin([
            "Endereço.incorreto地址信息错误",
            "Impossibilidade.de.chegar.no.endereço.informado客户地址无法进入",
            "Endereço.incompleto地址信息不详",
            "Impossibilidade.de.chegar.no.endereço.informado.de.coleta.客户地址无法进入C"
        ])),
         "status": "ATENÇÃO: AGUARDANDO DEVOLUÇÃO (ENDEREÇO/ACESSO INCORRETO)"},
    ]

    # Tentativas / ausência
    regras += [
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] ==
                                       "Ausência.de.destinatário.nas.várias.tentativas.de.entrega多次派送客户不在"),
         "status": "VERIFICAR 3 TENTATIVAS DE ENTREGA. SE OK, SOLICITAR DEVOLUÇÃO. SENÃO, REALIZAR NOVA TENTATIVA."},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Ausência.do.destinatário客户不在") &
                     (df[COL_DIAS_PARADO] >= 2),
         "status": "ATENÇÃO: DEVOLVER À BASE (AUSÊNCIA, HÁ MAIS DE 2 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Ausência.do.destinatário客户不在"),
         "status": "ATENÇÃO: DEVOLUÇÃO À BASE PENDENTE (AUSÊNCIA)"},
    ]

    # Recusa / mudança
    regras += [
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO].isin([
            "Recusa.de.recebimento.pelo.cliente.(destinatário)无理由拒收",
            "O.destinatário.mudou.o.endereço.收件人搬家"
        ])) & (df[COL_DIAS_PARADO] >= 2),
         "status": "ATENÇÃO: DEVOLVER À BASE (RECUSA/MUDANÇA DE ENDEREÇO, HÁ MAIS DE 2 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO].isin([
            "Recusa.de.recebimento.pelo.cliente.(destinatário)无理由拒收",
            "O.destinatário.mudou.o.endereço.收件人搬家"
        ])),
         "status": "ATENÇÃO: DEVOLUÇÃO À BASE PENDENTE (RECUSA/MUDANÇA DE ENDEREÇO)"},
    ]

    # Outros problemáticos
    regras += [
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO].isin([
            "Pacote.fora.do.padrão.三边尺寸超限",
            "Embalagem.não.conforme.包装不规范"
        ])),
         "status": "SOLICITAR DEVOLUÇÃO IMEDIATA (FORA DO PADRÃO / EMBALAGEM NÃO CONFORME)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Mercadorias.que.chegam.incompletos货未到齐") &
                     (df[COL_DIAS_PARADO] >= 2),
         "status": "ENVIAR PARA O FLUXO INVERSO (INCOMPLETO, HÁ MAIS DE 2 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Pacotes.retidos.por.anomalias.异常拦截件") &
                     (df[COL_DIAS_PARADO] >= 3),
         "status": "ENVIAR PARA A QUALIDADE (ANOMALIA, HÁ MAIS DE 3 DIAS)"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Pacotes.retidos.por.anomalias.异常拦截件"),
         "status": "ATENÇÃO: ANOMALIA EM ANÁLISE"},
        {"condicao": is_problematico & (df[COL_NOME_PROBLEMATICO] == "Devolução.退回件"),
         "status": "ENVIAR PARA SC/DC (DEVOLUÇÃO APROVADA)"},
    ]

    # 2) OPERAÇÕES NORMAIS
    regras += [
        {"condicao": (df[COL_ULTIMA_OPERACAO] == "出仓扫描/Bipe de saída para entrega") &
                     (df[COL_REGIONAL].isin(FRANQUIAS)) & (df[COL_DIAS_PARADO] >= 2),
         "status": "ATRASO NA ENTREGA (FRANQUIA)"},
        {"condicao": (df[COL_ULTIMA_OPERACAO] == "出仓扫描/Bipe de saída para entrega") &
                     (df[COL_REGIONAL].isin(FRANQUIAS)),
         "status": "EM ROTA DE ENTREGA (FRANQUIA)"},
        {"condicao": (df[COL_ULTIMA_OPERACAO] == "出仓扫描/Bipe de saída para entrega") &
                     (~df[COL_REGIONAL].isin(FRANQUIAS)) & (df[COL_DIAS_PARADO] >= 2),
         "status": "ATENÇÃO: ATRASO NA ENTREGA (BASE PRÓPRIA)"},
        {"condicao": (df[COL_ULTIMA_OPERACAO] == "出仓扫描/Bipe de saída para entrega"),
         "status": "EM ROTA DE ENTREGA (BASE PRÓPRIA)"},
    ]

    # 3) ENVIO ERRADO (CDs)
    regras += [
        {"condicao": is_envio_errado_cd &
                     (df[COL_NOME_PROBLEMATICO] == "Mercadorias.do.cliente.não.estão.completas.客户货物未备齐"),
         "status": "ENVIAR PARA O FLUXO INVERSO (INCOMPLETO, HÁ MAIS DE 2 DIAS)"},
        {"condicao": is_envio_errado_cd & (df[COL_NOME_PROBLEMATICO] == "Ausência.do.destinatário客户不在"),
         "status": "VERIFICAR 3 TENTATIVAS DE ENTREGA. SE OK, SOLICITAR DEVOLUÇÃO. SENÃO, REALIZAR NOVA TENTATIVA."},
        {"condicao": is_envio_errado_cd,
         "status": "ENVIO ERRADO - ENTRE CDs"},
    ]

    conditions = [r["condicao"] for r in regras]
    choices = [r["status"] for r in regras]

    df[COL_STATUS] = np.select(
        conditions,
        choices,
        default=df[COL_ULTIMA_OPERACAO].astype(str).str.upper()
    )

    logging.info("Regras aplicadas com sucesso.")
    return df
def calcular_multa(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logging.info("Nenhum pacote com 6+ dias para cálculo de multa.")
        return df

    logging.info("Calculando multa para pacotes com 6 ou mais dias parados...")
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
    logging.info("Multa calculada por item.")
    return df_copy


def processar_dados(df_main: pd.DataFrame, df_problematicos: pd.DataFrame, df_devolucao: pd.DataFrame) -> pd.DataFrame:
    colunas_necessarias = [
        COL_REMESSA, COL_ULTIMA_OPERACAO, COL_REGIONAL, COL_NOME_PROBLEMATICO, COL_HORA_OPERACAO, COL_BASE_RECENTE
    ]
    if not all(col in df_main.columns for col in colunas_necessarias):
        colunas_faltantes = set(colunas_necessarias) - set(df_main.columns)
        logging.critical(f"O arquivo principal não contém as colunas obrigatórias: {colunas_faltantes}.")
        return pd.DataFrame()

    df = df_main.copy()
    df[COL_REMESSA] = df[COL_REMESSA].astype(str)

    # 1) Problemáticos
    if not df_problematicos.empty:
        if 'Número de pedido JMS' in df_problematicos.columns:
            df_problematicos['Número de pedido JMS'] = df_problematicos['Número de pedido JMS'].astype(str)

        if 'Tempo de digitalização' in df_problematicos.columns:
            df_problematicos['Tempo de digitalização'] = pd.to_datetime(df_problematicos['Tempo de digitalização'], errors='coerce')

        df_problematicos = df_problematicos.sort_values('Tempo de digitalização').dropna(subset=['Número de pedido JMS'])

        summary = df_problematicos.groupby('Número de pedido JMS').agg(
            Qtd_Problematicas=('Número de pedido JMS', 'size'),
            Ultima_Problematica_Detalhada=('Tipo de nível II de pacote problemático', 'last')
        ).reset_index()

        df = df.merge(summary, left_on=COL_REMESSA, right_on='Número de pedido JMS', how='left')
        df.drop(columns=['Número de pedido JMS'], inplace=True, errors='ignore')
        logging.info("Dados de pacotes problemáticos integrados.")
    else:
        logging.info("Sem dados de pacotes problemáticos para integrar.")

    df['Última Problemática Detalhada'] = df.get('Ultima_Problematica_Detalhada', pd.Series(index=df.index)).fillna('-')
    df['Qtd Problemáticas'] = df.get('Qtd_Problematicas', pd.Series(index=df.index)).fillna(0).astype(int)

    # 2) Devolução
    df[COL_DEVOLUCAO] = 'DEVOLUÇÃO NÃO SOLICITADA'
    if not df_devolucao.empty and 'Número de pedido JMS' in df_devolucao.columns:
        df_devolucao['Número de pedido JMS'] = df_devolucao['Número de pedido JMS'].astype(str)

        mapa_traducao = {'待审核': 'EM PROCESSO DE APROVAÇÃO', '驳回': 'PEDIDO DE DEVOLUÇÃO RECUSADO', '已审核': 'DEVOLUÇÃO APROVADA'}
        if 'Estado de solicitação' in df_devolucao.columns:
            df_devolucao['Status_Traduzido'] = df_devolucao['Estado de solicitação'].map(mapa_traducao)

            df_devolucao_info = (
                df_devolucao.dropna(subset=['Status_Traduzido'])[['Número de pedido JMS', 'Status_Traduzido']]
                .drop_duplicates(subset='Número de pedido JMS', keep='last')
            )

            df = df.merge(df_devolucao_info, left_on=COL_REMESSA, right_on='Número de pedido JMS', how='left')
            df[COL_DEVOLUCAO] = df['Status_Traduzido'].fillna(df[COL_DEVOLUCAO])
            df.drop(columns=['Número de pedido JMS', 'Status_Traduzido'], inplace=True, errors='ignore')
            logging.info("Dados de devolução integrados.")
    else:
        logging.info("Sem dados de devolução para integrar.")

    # 3) Dias Parado
    logging.info("Calculando dias parados...")
    df[COL_HORA_OPERACAO] = pd.to_datetime(df[COL_HORA_OPERACAO], errors='coerce')
    df[COL_DIAS_PARADO] = (datetime.now() - df[COL_HORA_OPERACAO]).dt.days.fillna(0).astype(int)

    # 4) Regras
    df = aplicar_regras_status(df)
    df = aplicar_regras_transito(df)

    # 5) Priorização devolução
    df.loc[df[COL_DEVOLUCAO] != 'DEVOLUÇÃO NÃO SOLICITADA', COL_STATUS] = df[COL_DEVOLUCAO]
    cond_aprovado_em_rota = (df[COL_STATUS] == 'DEVOLUÇÃO APROVADA') & (df[COL_ULTIMA_OPERACAO] == "出仓扫描/Bipe de saída para entrega")
    df.loc[cond_aprovado_em_rota, COL_STATUS] = 'DEVOLUÇÃO APROVADA, MAS O PACOTE ESTÁ EM ROTA'

    condicao_aprovado = df[COL_STATUS].isin(['DEVOLUÇÃO APROVADA', 'DEVOLUÇÃO APROVADA, MAS O PACOTE ESTÁ EM ROTA'])
    df.loc[condicao_aprovado, COL_TRANSITO] = ''

    df.rename(columns={COL_REGIONAL: COLUNA_CHAVE_PRINCIPAL}, inplace=True)

    ordem_colunas = [
        COL_REMESSA, COLUNA_CHAVE_PRINCIPAL, COL_DIAS_PARADO, COL_ULTIMA_OPERACAO,
        COL_HORA_OPERACAO, COL_STATUS, COL_TRANSITO, COL_DEVOLUCAO,
        'Qtd Problemáticas', 'Última Problemática Detalhada'
    ]
    colunas_existentes = [col for col in df.columns if col not in ordem_colunas]
    df = df[ordem_colunas + colunas_existentes]

    return df


def adicionar_info_coordenador(df_principal: pd.DataFrame) -> pd.DataFrame:
    if df_principal.empty:
        logging.warning("DataFrame de entrada está vazio. Pulando adição de coordenadores.")
        return df_principal

    try:
        logging.info(f"Lendo arquivo de mapeamento: {os.path.basename(ARQUIVO_MAPEAMENTO_COORDENADORES)}")
        df_mapeamento = pd.read_excel(ARQUIVO_MAPEAMENTO_COORDENADORES)
    except FileNotFoundError:
        logging.error(f"ERRO CRÍTICO: Arquivo de mapeamento '{ARQUIVO_MAPEAMENTO_COORDENADORES}' não encontrado.")
        raise
    except Exception as e:
        logging.error(f"Ocorreu um erro ao ler o arquivo de mapeamento: {e}.")
        raise

    mapa_coordenador = pd.Series(df_mapeamento[COLUNA_INFO_COORDENADOR].values,
                                 index=df_mapeamento[COLUNA_CHAVE_MAPEAMENTO]).to_dict()
    mapa_filial = pd.Series(df_mapeamento[COLUNA_INFO_FILIAL].values,
                            index=df_mapeamento[COLUNA_CHAVE_MAPEAMENTO]).to_dict()

    key_series = df_principal[COLUNA_CHAVE_PRINCIPAL]
    if isinstance(key_series, pd.DataFrame):
        logging.warning(f"Colunas duplicadas para '{COLUNA_CHAVE_PRINCIPAL}'. Usando a primeira ocorrência.")
        key_series = key_series.iloc[:, 0]

    df_principal[NOVA_COLUNA_COORDENADOR] = key_series.map(mapa_coordenador).fillna('NÃO ENCONTRADO')
    df_principal[NOVA_COLUNA_FILIAL] = key_series.map(mapa_filial).fillna('NÃO ENCONTRADA')

    logging.info("Informações de coordenador e filial adicionadas.")
    return df_principal


def _mask_incompletos(df: pd.DataFrame) -> pd.Series:
    """
    Máscara robusta para detectar "Mercadorias incompletas" na coluna Nome de pacote problemático.
    """
    if df.empty or COL_NOME_PROBLEMATICO not in df.columns:
        return pd.Series([False] * len(df), index=df.index)

    s = df[COL_NOME_PROBLEMATICO].astype(str).fillna("").str.strip()
    return (
        (s == INCOMPLETOS_KEY_EXATA) |
        (s.str.contains(INCOMPLETOS_KEY_PARTE_1, na=False)) |
        (s.str.contains(INCOMPLETOS_KEY_PARTE_2, na=False))
    )


def salvar_relatorios(df_final: pd.DataFrame, pasta_saida: str) -> pd.DataFrame:
    """
    - Salva relatório separado de Mercadorias incompletas
    - REMOVE incompletos do relatório principal (0-4 / 5+)
    - Retorna df_principal (já sem incompletos) para usar no card do Feishu
    """
    if df_final.empty:
        logging.warning("⚠️ Nenhum dado para salvar relatórios.")
        return df_final

    data_hoje = datetime.now().strftime("%Y-%m-%d")

    # 0) Separa INCOMPLETOS e remove do principal
    mask_incompletos = _mask_incompletos(df_final)
    df_incompletos = df_final[mask_incompletos].copy()
    df_principal = df_final[~mask_incompletos].copy()

    # 1) Salva relatório de INCOMPLETOS (se houver)
    if not df_incompletos.empty:
        arquivo_incompletos = os.path.join(pasta_saida, f"Relatório Mercadorias incompletas_{data_hoje}.xlsx")
        df_incompletos.to_excel(arquivo_incompletos, index=False)
        logging.info(f"✅ Relatório Mercadorias incompletas salvo: {arquivo_incompletos}")
    else:
        logging.info("Sem registros de Mercadorias incompletas para salvar.")

    # 🔍 Debug: resumo por faixa de dias (AGORA no principal, sem incompletos)
    try:
        resumo = {
            "0-4 dias (principal)": (df_principal[COL_DIAS_PARADO] <= 4).sum(),
            "5+ dias (principal)": (df_principal[COL_DIAS_PARADO] >= 5).sum(),
            "Total (principal)": len(df_principal),
            "Incompletos (separado)": len(df_incompletos)
        }
        logging.info(f"📊 Resumo Dias Parados: {resumo}")
    except Exception as e:
        logging.error(f"Erro ao gerar resumo de debug: {e}")

    # 2) Relatório 0–4 dias (SEM incompletos)
    df_0_4 = df_principal[df_principal[COL_DIAS_PARADO] <= 4]
    if not df_0_4.empty:
        arquivo_0_4 = os.path.join(pasta_saida, f"Relatório Sem Movimentação (0-4 dias)_{data_hoje}.xlsx")
        df_0_4.to_excel(arquivo_0_4, index=False)
        logging.info(f"Relatório 0-4 dias salvo: {arquivo_0_4}")

    # 3) Relatório 5+ dias (SEM incompletos)
    df_5_plus = df_principal[df_principal[COL_DIAS_PARADO] >= 5]
    if not df_5_plus.empty:
        df_5_plus = calcular_multa(df_5_plus)
        arquivo_5_plus = os.path.join(pasta_saida, f"Relatório Sem Movimentação (5+ dias)_{data_hoje}.xlsx")
        df_5_plus.to_excel(arquivo_5_plus, index=False)
        logging.info(f"✅ Relatório 5+ dias salvo: {arquivo_5_plus}")
    else:
        logging.warning("⚠️ Nenhum pedido encontrado com 5+ dias parados (principal).")

    # Retorna o principal já filtrado -> use isso no FEISHU CARD
    return df_principal


def mover_para_arquivo_morto(pasta_origem: str, pasta_destino: str):
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)

    hoje = datetime.now().strftime("%Y-%m-%d")
    arquivos = [f for f in os.listdir(pasta_origem) if f.endswith(('.xlsx', '.xls'))]

    arquivos_hoje = [f for f in arquivos if hoje in f]
    arquivos_hoje.sort(key=lambda f: os.path.getmtime(os.path.join(pasta_origem, f)))

    if len(arquivos_hoje) > 1:
        for arquivo in arquivos_hoje[:-1]:
            try:
                shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo))
                logging.info(f"📦 Arquivo duplicado de hoje movido: {arquivo}")
            except Exception as e:
                logging.error(f"Erro ao mover o arquivo {arquivo}: {e}")

    for arquivo in arquivos:
        if arquivo not in arquivos_hoje:
            try:
                shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo))
                logging.info(f"📦 Arquivo antigo movido para Arquivo Morto: {arquivo}")
            except Exception as e:
                logging.error(f"Erro ao mover o arquivo {arquivo}: {e}")


def main():
    t0 = time.perf_counter()
    logging.info("--- INICIANDO PROCESSO DE GERAÇÃO DE RELATÓRIOS ---")

    try:
        caminho_arquivo_original = encontrar_arquivo_principal(PATH_INPUT_MAIN, FILENAME_START_MAIN)
        if not caminho_arquivo_original:
            logging.critical("Arquivo principal não encontrado. Processo interrompido.")
            raise FileNotFoundError("Arquivo principal não encontrado.")

        df_main = pd.read_excel(caminho_arquivo_original)
        df_problematicos = carregar_planilhas_de_pasta(PATH_INPUT_PROBLEMATICOS, "Consolidando problemáticos")
        df_devolucao = carregar_planilhas_de_pasta(PATH_INPUT_DEVOLUCAO, "Consolidando devoluções")

        df_final = processar_dados(df_main, df_problematicos, df_devolucao)
        df_final = adicionar_info_coordenador(df_final)

        # Move relatórios antigos antes de salvar novos
        mover_para_arquivo_morto(PATH_OUTPUT_REPORTS, PATH_OUTPUT_ARQUIVO_MORTO)

        # Salva relatórios e pega o DF PRINCIPAL (SEM incompletos) para o Feishu
        df_para_feishu = salvar_relatorios(df_final, PATH_OUTPUT_REPORTS)

        logging.info(f"DF para Feishu (principal, sem incompletos): {len(df_para_feishu)} linhas")
        logging.info("--- PROCESSO CONCLUÍDO COM SUCESSO! ---")

        dt = time.perf_counter() - t0
        avisar_termino(
            titulo="Processo finalizado ✅",
            mensagem=f"Relatórios gerados com sucesso. Tempo total: {dt:.1f}s",
            sucesso=True
        )

    except Exception as e:
        dt = time.perf_counter() - t0
        logging.exception("❌ PROCESSO FINALIZADO COM ERRO.")
        avisar_termino(
            titulo="Processo finalizado com erro ❌",
            mensagem=f"Falha ao gerar relatórios: {e}\nTempo até falhar: {dt:.1f}s",
            sucesso=False
        )
        raise


if __name__ == "__main__":
    main()