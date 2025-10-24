import polars as pl
import os
import numpy as np
from tqdm import tqdm
from datetime import datetime
import shutil
import logging
from typing import List, Dict, Optional, Any

# ==============================================================================
# --- CONFIGURAÇÃO GERAL ---
# Todas as configurações dos scripts foram unificadas aqui para fácil acesso.
# ==============================================================================

# --- 1. Caminhos Principais ---
# Altere estes caminhos para corresponder à estrutura de pastas do seu ambiente.
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

# --- 5. Listas para Regras de Negócio ---
FRANQUIAS = ["F AGL-GO", "F ALV-AM", "F APG - GO", "F ARQ - RO", "F BSB-DF", "F CDN-AM", "F CGR - MS", "F FMA-GO",
             "F GYN - GO", "F ITI-PA", "F RVD - GO", "F TRD-GO", "F CGR 02-MS", "F GYN 02-GO", "F OCD-GO", "F PVH-RO",
             "F TGT-DF", "F DOM -PA", "F JCD-PA", "F MCP-AP", "F ORL-PA", "F PCA-PA", "F RDC -PA", "F SFX-PA",
             "F TLA-PA"]
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

# --- 6. Configuração do Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# ==============================================================================
# --- FUNÇÕES DE CARREGAMENTO DE DADOS ---
# ==============================================================================

def encontrar_arquivo_principal(pasta: str, inicio_nome: str) -> Optional[str]:
    """Busca por um arquivo Excel em uma pasta que comece com um determinado nome."""
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


def carregar_planilhas_de_pasta(caminho_pasta: str, descricao_tqdm: str) -> pl.DataFrame:
    """Lê todos os arquivos Excel de uma pasta e consolida seus dados."""
    lista_dfs = []
    nome_pasta = os.path.basename(caminho_pasta)
    logging.info(f"Lendo planilhas da pasta: {nome_pasta}")
    try:
        arquivos = [f for f in os.listdir(caminho_pasta) if f.endswith(('.xlsx', '.xls'))]
        if not arquivos:
            logging.warning(f"Nenhum arquivo Excel encontrado na pasta '{nome_pasta}'.")
            return pl.DataFrame()

        colunas_referencia = None
        for arquivo in tqdm(arquivos, desc=descricao_tqdm):
            caminho_completo = os.path.join(caminho_pasta, arquivo)
            try:
                df_aba = pl.read_excel(caminho_completo)
                if df_aba.height == 0:
                    continue

                if colunas_referencia is None:
                    colunas_referencia = df_aba.columns
                    logging.info(
                        f"Esquema de colunas de referência definido com {len(colunas_referencia)} colunas a partir do arquivo '{arquivo}'.")
                    lista_dfs.append(df_aba)
                    continue

                colunas_faltantes = [col for col in colunas_referencia if col not in df_aba.columns]
                if colunas_faltantes:
                    logging.warning(
                        f"Arquivo '{arquivo}' está faltando {len(colunas_faltantes)} colunas. Colunas adicionadas com nulos: {colunas_faltantes}")
                    for col in colunas_faltantes:
                        df_aba = df_aba.with_columns(pl.lit(None).alias(col))

                df_alinhado = df_aba.select(colunas_referencia)
                lista_dfs.append(df_alinhado)

            except Exception as e:
                logging.error(f"Falha ao ler o arquivo '{arquivo}' da pasta '{nome_pasta}': {e}")
                continue

        if not lista_dfs:
            return pl.DataFrame()

        df_consolidado = pl.concat(lista_dfs)
        logging.info(f"Total de {len(df_consolidado)} registros consolidados de '{nome_pasta}'.")
        return df_consolidado

    except FileNotFoundError:
        logging.error(f"A pasta '{caminho_pasta}' não foi encontrada. Processo interrompido.")
        raise
    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado ao ler os arquivos da pasta '{nome_pasta}': {e}")
        raise


# ==============================================================================
# --- FUNÇÕES DE PROCESSAMENTO E LÓGICA DE NEGÓCIO ---
# ==============================================================================

def aplicar_regras_transito(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica regras de negócio para definir o status de 'Trânsito' dos pacotes."""
    logging.info("Aplicando regras de trânsito...")
    if COL_BASE_RECENTE not in df.columns:
        logging.warning(f"Coluna '{COL_BASE_RECENTE}' não encontrada. As regras de trânsito não serão aplicadas.")
        df = df.with_columns(pl.lit("COLUNA DE BASE RECENTE NÃO ENCONTRADA").alias(COL_TRANSITO))
        return df

    cond_em_transito = pl.col(COL_ULTIMA_OPERACAO) == "发件扫描/Bipe de expedição"
    is_fluxo_inverso = pl.col(COL_BASE_RECENTE).is_in(BASES_FLUXO_INVERSO) & pl.col(COL_REGIONAL).is_in(
        DESTINOS_FLUXO_INVERSO)
    origem_sc_bre = pl.col(COL_BASE_RECENTE) == 'SC BRE'
    destino_pvh = pl.col(COL_REGIONAL).str.contains('PVH-RO', literal=False)
    prazo_fluxo_inverso_estourado = pl.col(COL_DIAS_PARADO) >= 3
    prazo_5_dias_estourado = pl.col(COL_DIAS_PARADO) >= 5
    prazo_3_dias_estourado = pl.col(COL_DIAS_PARADO) >= 3

    df = df.with_columns(
        pl.when(cond_em_transito & is_fluxo_inverso & prazo_fluxo_inverso_estourado)
        .then("VERIFICAR COM TRANSPORTE: VEÍCULO NÃO CHEGOU (FLUXO INVERSO)")
        .when(cond_em_transito & is_fluxo_inverso & ~prazo_fluxo_inverso_estourado)
        .then("EM TRÂNSITO (FLUXO INVERSO)")
        .when(cond_em_transito & origem_sc_bre & prazo_5_dias_estourado)
        .then("FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)")
        .when(cond_em_transito & origem_sc_bre & ~prazo_5_dias_estourado)
        .then("EM TRÂNSITO PARA A BASE")
        .when(cond_em_transito & ~origem_sc_bre & destino_pvh & prazo_5_dias_estourado)
        .then("FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)")
        .when(cond_em_transito & ~origem_sc_bre & destino_pvh & ~prazo_5_dias_estourado)
        .then("EM TRÂNSITO PARA A BASE")
        .when(cond_em_transito & ~origem_sc_bre & ~destino_pvh & prazo_3_dias_estourado)
        .then("FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)")
        .when(cond_em_transito & ~origem_sc_bre & ~destino_pvh & ~prazo_3_dias_estourado)
        .then("EM TRÂNSITO PARA A BASE")
        .otherwise("")
        .alias(COL_TRANSITO)
    )
    logging.info("Regras de trânsito aplicadas com sucesso.")
    return df


def aplicar_regras_status(df: pl.DataFrame) -> pl.DataFrame:
    """Aplica regras de negócio agrupadas por categoria para definir o 'Status'."""
    logging.info("Aplicando regras de status...")
    is_problematico = pl.col(COL_ULTIMA_OPERACAO) == "问题件扫描/Bipe de pacote problemático"
    is_envio_errado_cd = pl.col(COL_BASE_RECENTE).is_in(BASES_CD) & pl.col(COL_REGIONAL).is_in(BASES_CD)
    df = df.with_columns(pl.col(COL_ULTIMA_OPERACAO).str.to_uppercase().alias(COL_STATUS))

    # 1. PROBLEMÁTICOS
    df = df.with_columns(
        pl.when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "Extravio.interno.内部遗失"))
        .then("PEDIDO EXTRAVIADO")
        .when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "Encomenda.expedido.mas.não.chegou.有发未到件") & (
                    pl.col(COL_DIAS_PARADO) >= 3))
        .then("ALERTA DE EXTRAVIO: ABRIR CHAMADO INTERNO (HÁ MAIS DE 3 DIAS)")
        .when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "Encomenda.expedido.mas.não.chegou.有发未到件"))
        .then("ATENÇÃO: RISCO DE EXTRAVIO (AGUARDANDO CHEGADA)")
        .otherwise(pl.col(COL_STATUS)).alias(COL_STATUS)
    )
    df = df.with_columns(
        pl.when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "retidos.留仓") & (pl.col(COL_DIAS_PARADO) >= 3))
        .then("ATENÇÃO: PACOTE RETIDO NO PISO (HÁ MAIS DE 3 DIAS)")
        .when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "retidos.留仓"))
        .then("ATENÇÃO: PACOTE RETIDO NO PISO")
        .otherwise(pl.col(COL_STATUS)).alias(COL_STATUS)
    )
    df = df.with_columns(
        pl.when(is_problematico & pl.col(COL_NOME_PROBLEMATICO).is_in([
            "Endereço.incorreto地址信息错误", "Impossibilidade.de.chegar.no.endereço.informado客户地址无法进入",
            "Endereço.incompleto地址信息不详",
            "Impossibilidade.de.chegar.no.endereço.informado.de.coleta.客户地址无法进入C"]) & (
                            pl.col(COL_DIAS_PARADO) >= 8))
        .then("SOLICITAR DEVOLUÇÃO (ENDEREÇO/ACESSO INCORRETO, HÁ MAIS DE 8 DIAS)")
        .when(is_problematico & pl.col(COL_NOME_PROBLEMATICO).is_in([
            "Endereço.incorreto地址信息错误", "Impossibilidade.de.chegar.no.endereço.informado客户地址无法进入",
            "Endereço.incompleto地址信息不详",
            "Impossibilidade.de.chegar.no.endereço.informado.de.coleta.客户地址无法进入C"]))
        .then("ATENÇÃO: AGUARDANDO DEVOLUÇÃO (ENDEREÇO/ACESSO INCORRETO)")
        .otherwise(pl.col(COL_STATUS)).alias(COL_STATUS)
    )
    df = df.with_columns(
        pl.when(is_problematico & (pl.col(
            COL_NOME_PROBLEMATICO) == "Ausência.de.destinatário.nas.várias.tentativas.de.entrega多次派送客户不在"))
        .then("VERIFICAR 3 TENTATIVAS DE ENTREGA. SE OK, SOLICITAR DEVOLUÇÃO. SENÃO, REALIZAR NOVA TENTATIVA.")
        .when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "Ausência.do.destinatário客户不在") & (
                    pl.col(COL_DIAS_PARADO) >= 2))
        .then("ATENÇÃO: DEVOLVER À BASE (AUSÊNCIA, HÁ MAIS DE 2 DIAS)")
        .when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "Ausência.do.destinatário客户不在"))
        .then("ATENÇÃO: DEVOLUÇÃO À BASE PENDENTE (AUSÊNCIA)")
        .otherwise(pl.col(COL_STATUS)).alias(COL_STATUS)
    )
    df = df.with_columns(
        pl.when(is_problematico & pl.col(COL_NOME_PROBLEMATICO).is_in([
            "Recusa.de.recebimento.pelo.cliente.(destinatário)无理由拒收",
            "O.destinatário.mudou.o.endereço.收件人搬家"]) & (pl.col(COL_DIAS_PARADO) >= 2))
        .then("ATENÇÃO: DEVOLVER À BASE (RECUSA/MUDANÇA DE ENDEREÇO, HÁ MAIS DE 2 DIAS)")
        .when(is_problematico & pl.col(COL_NOME_PROBLEMATICO).is_in([
            "Recusa.de.recebimento.pelo.cliente.(destinatário)无理由拒收",
            "O.destinatário.mudou.o.endereço.收件人搬家"]))
        .then("ATENÇÃO: DEVOLUÇÃO À BASE PENDENTE (RECUSA/MUDANÇA DE ENDEREÇO)")
        .otherwise(pl.col(COL_STATUS)).alias(COL_STATUS)
    )
    df = df.with_columns(
        pl.when(is_problematico & pl.col(COL_NOME_PROBLEMATICO).is_in(
            ["Pacote.fora.do.padrão.三边尺寸超限", "Embalagem.não.conforme.包装不规范"]))
        .then("SOLICITAR DEVOLUÇÃO IMEDIATA (FORA DO PADRÃO / EMBALAGEM NÃO CONFORME)")
        .when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "Mercadorias.que.chegam.incompletos货未到齐") & (
                    pl.col(COL_DIAS_PARADO) >= 2))
        .then("ENVIAR PARA O FLUXO INVERSO (INCOMPLETO, HÁ MAIS DE 2 DIAS)")
        .when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "Pacotes.retidos.por.anomalias.异常拦截件") & (
                    pl.col(COL_DIAS_PARADO) >= 3))
        .then("ENVIAR PARA A QUALIDADE (ANOMALIA, HÁ MAIS DE 3 DIAS)")
        .when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "Pacotes.retidos.por.anomalias.异常拦截件"))
        .then("ATENÇÃO: ANOMALIA EM ANÁLISE")
        .when(is_problematico & (pl.col(COL_NOME_PROBLEMATICO) == "Devolução.退回件"))
        .then("ENVIAR PARA SC/DC (DEVOLUÇÃO APROVADA)")
        .otherwise(pl.col(COL_STATUS)).alias(COL_STATUS)
    )

    # 2. OPERAÇÕES NORMAIS
    df = df.with_columns(
        pl.when((pl.col(COL_ULTIMA_OPERACAO) == "出仓扫描/Bipe de saída para entrega") & (
            pl.col(COL_REGIONAL).is_in(FRANQUIAS)) & (pl.col(COL_DIAS_PARADO) >= 2))
        .then("ATRASO NA ENTREGA (FRANQUIA)")
        .when((pl.col(COL_ULTIMA_OPERACAO) == "出仓扫描/Bipe de saída para entrega") & (
            pl.col(COL_REGIONAL).is_in(FRANQUIAS)))
        .then("EM ROTA DE ENTREGA (FRANQUIA)")
        .when((pl.col(COL_ULTIMA_OPERACAO) == "出仓扫描/Bipe de saída para entrega") & (
            ~pl.col(COL_REGIONAL).is_in(FRANQUIAS)) & (pl.col(COL_DIAS_PARADO) >= 2))
        .then("ATENÇÃO: ATRASO NA ENTREGA (BASE PRÓPRIA)")
        .when((pl.col(COL_ULTIMA_OPERACAO) == "出仓扫描/Bipe de saída para entrega"))
        .then("EM ROTA DE ENTREGA (BASE PRÓPRIA)")
        .otherwise(pl.col(COL_STATUS)).alias(COL_STATUS)
    )

    # 3. ENVIO ERRADO (CDs)
    df = df.with_columns(
        pl.when(is_envio_errado_cd & (
                    pl.col(COL_NOME_PROBLEMATICO) == "Mercadorias.do.cliente.não.estão.completas.客户货物未备齐"))
        .then("ENVIAR PARA O FLUXO INVERSO (INCOMPLETO, HÁ MAIS DE 2 DIAS)")
        .when(is_envio_errado_cd & (pl.col(COL_NOME_PROBLEMATICO) == "Ausência.do.destinatário客户不在"))
        .then("VERIFICAR 3 TENTATIVAS DE ENTREGA. SE OK, SOLICITAR DEVOLUÇÃO. SENÃO, REALIZAR NOVA TENTATIVA.")
        .when(is_envio_errado_cd)
        .then("ENVIO ERRADO - ENTRE CDs")
        .otherwise(pl.col(COL_STATUS)).alias(COL_STATUS)
    )
    logging.info("Regras aplicadas com sucesso.")
    return df


def calcular_multa(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        logging.info("Nenhum pacote com 6+ dias para cálculo de multa.")
        return df
    logging.info("Calculando multa para pacotes com 6 ou mais dias parados...")
    df = df.with_columns(
        pl.when(pl.col(COL_DIAS_PARADO) >= 30).then(30)
        .when(pl.col(COL_DIAS_PARADO).is_between(14, 29)).then(14)
        .when(pl.col(COL_DIAS_PARADO).is_between(10, 13)).then(10)
        .when(pl.col(COL_DIAS_PARADO).is_between(7, 9)).then(7)
        .when(pl.col(COL_DIAS_PARADO) == 6).then(6)
        .when(pl.col(COL_DIAS_PARADO) == 5).then(5)
        .when(pl.col(COL_DIAS_PARADO) == 4).then(4)
        .when(pl.col(COL_DIAS_PARADO) == 3).then(3)
        .when(pl.col(COL_DIAS_PARADO) == 2).then(2)
        .otherwise(0).alias(COL_MULTA)
    )
    logging.info("Multa calculada por item.")
    return df


def processar_dados(df_main: pl.DataFrame, df_problematicos: pl.DataFrame, df_devolucao: pl.DataFrame) -> pl.DataFrame:
    """
    Função principal de processamento que orquestra a junção de dados e a aplicação de regras.
    FLUXO:
    1. Inicia com os dados do arquivo 'Sem Movimentação'.
    2. Enriquece com dados de 'Pacotes Problematicos'.
    3. Enriquece com dados de 'Devolução'.
    4. Aplica todas as regras de negócio (cálculo de dias, status, trânsito).
    """
    colunas_necessarias = [COL_REMESSA, COL_ULTIMA_OPERACAO, COL_REGIONAL, COL_NOME_PROBLEMATICO, COL_HORA_OPERACAO,
                           COL_BASE_RECENTE]
    if not all(col in df_main.columns for col in colunas_necessarias):
        colunas_faltantes = set(colunas_necessarias) - set(df_main.columns)
        logging.critical(f"O arquivo principal não contém as colunas obrigatórias: {colunas_faltantes}.")
        return pl.DataFrame()

    # Passo 1: Iniciar com o DataFrame principal (Sem Movimentação)
    df = df_main.clone().with_columns(pl.col(COL_REMESSA).cast(pl.Utf8))
    logging.info(f"Processamento iniciado com {len(df)} registros da planilha principal.")

    # Passo 2: Juntar com dados de Pacotes Problemáticos
    if df_problematicos.height > 0:
        logging.info("Enriquecendo dados com informações de pacotes problemáticos...")
        # CORREÇÃO 1: Adicionado .cast(pl.Utf8) antes de .str.to_datetime() para evitar erro se a coluna já for datetime.
        df_problematicos = df_problematicos.with_columns(
            pl.col('Número de pedido JMS').cast(pl.Utf8),
            pl.col('Tempo de digitalização').cast(pl.Utf8).str.to_datetime(strict=False)
        ).sort('Tempo de digitalização').filter(pl.col('Número de pedido JMS').is_not_null())

        summary = df_problematicos.group_by('Número de pedido JMS').agg([
            pl.len().alias('Qtd_Problematicas'),
            pl.last('Tipo de nível II de pacote problemático').alias('Ultima_Problematica_Detalhada')
        ])
        df = df.join(summary, left_on=COL_REMESSA, right_on='Número de pedido JMS', how='left')
        df = df.with_columns(
            pl.col('Ultima_Problematica_Detalhada').fill_null('-').alias('Última Problemática Detalhada'),
            pl.col('Qtd_Problematicas').fill_null(0).cast(pl.Int32).alias('Qtd Problemáticas')
        )
        logging.info("Dados de pacotes problemáticos integrados.")

    # Passo 3: Juntar com dados de Devolução
    df = df.with_columns(pl.lit('DEVOLUÇÃO NÃO SOLICITADA').alias(COL_DEVOLUCAO))
    if df_devolucao.height > 0:
        logging.info("Enriquecendo dados com informações de devolução...")
        df_devolucao = df_devolucao.with_columns(pl.col('Número de pedido JMS').cast(pl.Utf8))
        mapa_traducao = {'待审核': 'EM PROCESSO DE APROVAÇÃO', '驳回': 'PEDIDO DE DEVOLUÇÃO RECUSADO',
                         '已审核': 'DEVOLUÇÃO APROVADA'}
        df_devolucao = df_devolucao.with_columns(
            pl.col('Estado de solicitação').replace(mapa_traducao).alias('Status_Traduzido'))
        df_devolucao_info = df_devolucao.filter(pl.col('Status_Traduzido').is_not_null()).select(
            ['Número de pedido JMS', 'Status_Traduzido']).unique(subset=['Número de pedido JMS'], keep='last')
        df = df.join(df_devolucao_info, left_on=COL_REMESSA, right_on='Número de pedido JMS', how='left')
        df = df.with_columns(pl.coalesce([pl.col('Status_Traduzido'), pl.col(COL_DEVOLUCAO)]).alias(COL_DEVOLUCAO))

        # CORREÇÃO: Remover apenas a coluna 'Status_Traduzido', que foi a coluna temporária adicionada.
        df = df.drop('Status_Traduzido')
        logging.info("Dados de devolução integrados.")

    # Passo 4: Aplicar as regras de negócio
    logging.info("Aplicando regras de negócio no DataFrame consolidado...")
    # CORREÇÃO 2: Adicionado .cast(pl.Utf8) antes de .str.to_datetime() para evitar erro se a coluna já for datetime.
    df = df.with_columns(pl.col(COL_HORA_OPERACAO).cast(pl.Utf8).str.to_datetime(strict=False))
    df = df.with_columns(
        (datetime.now() - pl.col(COL_HORA_OPERACAO)).dt.days().fill_null(0).cast(pl.Int32).alias(COL_DIAS_PARADO))
    df = aplicar_regras_status(df)
    df = aplicar_regras_transito(df)

    # Ajustes finais de status
    df = df.with_columns(
        pl.when(pl.col(COL_DEVOLUCAO) != 'DEVOLUÇÃO NÃO SOLICITADA').then(pl.col(COL_DEVOLUCAO)).otherwise(
            pl.col(COL_STATUS)).alias(COL_STATUS)
    )
    cond_aprovado_em_rota = (pl.col(COL_STATUS) == 'DEVOLUÇÃO APROVADA') & (
                pl.col(COL_ULTIMA_OPERACAO) == "出仓扫描/Bipe de saída para entrega")
    df = df.with_columns(pl.when(cond_aprovado_em_rota).then('DEVOLUÇÃO APROVADA, MAS O PACOTE ESTÁ EM ROTA').otherwise(
        pl.col(COL_STATUS)).alias(COL_STATUS))
    condicao_aprovado = pl.col(COL_STATUS).is_in(
        ['DEVOLUÇÃO APROVADA', 'DEVOLUÇÃO APROVADA, MAS O PACOTE ESTÁ EM ROTA'])
    df = df.with_columns(pl.when(condicao_aprovado).then('').otherwise(pl.col(COL_TRANSITO)).alias(COL_TRANSITO))

    df = df.rename({COL_REGIONAL: COLUNA_CHAVE_PRINCIPAL})

    # Reordenar colunas para melhor visualização
    ordem_colunas = [COL_REMESSA, COLUNA_CHAVE_PRINCIPAL, COL_DIAS_PARADO, COL_ULTIMA_OPERACAO, COL_HORA_OPERACAO,
                     COL_STATUS, COL_TRANSITO, COL_DEVOLUCAO, 'Qtd Problemáticas', 'Última Problemática Detalhada']
    colunas_existentes = [col for col in df.columns if col not in ordem_colunas]
    df = df.select(ordem_colunas + colunas_existentes)

    return df


def adicionar_info_coordenador(df_principal: pl.DataFrame) -> pl.DataFrame:
    if df_principal.height == 0:
        logging.warning("DataFrame de entrada está vazio. Pulando adição de coordenadores.")
        return df_principal
    try:
        logging.info(f"Lendo arquivo de mapeamento: {os.path.basename(ARQUIVO_MAPEAMENTO_COORDENADORES)}")
        df_mapeamento = pl.read_excel(ARQUIVO_MAPEAMENTO_COORDENADORES)
    except FileNotFoundError:
        logging.error(
            f"ERRO CRÍTICO: Arquivo de mapeamento '{ARQUIVO_MAPEAMENTO_COORDENADORES}' não encontrado. O processo será interrompido.")
        raise
    except Exception as e:
        logging.error(f"Ocorreu um erro ao ler o arquivo de mapeamento: {e}. O processo será interrompido.")
        raise
    mapa_coordenador = dict(zip(df_mapeamento[COLUNA_CHAVE_MAPEAMENTO], df_mapeamento[COLUNA_INFO_COORDENADOR]))
    mapa_filial = dict(zip(df_mapeamento[COLUNA_CHAVE_MAPEAMENTO], df_mapeamento[COLUNA_INFO_FILIAL]))
    logging.info("Adicionando informações de Coordenador e Filial...")
    df_principal = df_principal.with_columns(
        pl.col(COLUNA_CHAVE_PRINCIPAL).map_dict(mapa_coordenador, default='NÃO ENCONTRADO').alias(
            NOVA_COLUNA_COORDENADOR),
        pl.col(COLUNA_CHAVE_PRINCIPAL).map_dict(mapa_filial, default='NÃO ENCONTRADA').alias(NOVA_COLUNA_FILIAL)
    )
    logging.info("Informações de coordenador e filial adicionadas.")
    return df_principal


# ==============================================================================
# --- FUNÇÃO PARA SALVAR RELATÓRIOS ---
# ==============================================================================
def salvar_relatorios(df_final: pl.DataFrame, pasta_saida: str):
    if df_final.height == 0:
        logging.warning("⚠️ Nenhum dado para salvar relatórios.")
        return
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    resumo = {"0-4 dias": df_final.filter(pl.col(COL_DIAS_PARADO) <= 4).height,
              "5+ dias": df_final.filter(pl.col(COL_DIAS_PARADO) >= 5).height, "Total": df_final.height}
    logging.info(f"📊 Resumo Dias Parados: {resumo}")

    df_0_4 = df_final.filter(pl.col(COL_DIAS_PARADO) <= 4)
    if df_0_4.height > 0:
        arquivo_0_4 = os.path.join(pasta_saida, f"Relatório Sem Movimentação (0-4 dias)_{data_hoje}.xlsx")
        df_0_4.write_excel(arquivo_0_4)
        logging.info(f"Relatório 0-4 dias salvo: {arquivo_0_4}")

    df_5_plus = df_final.filter(pl.col(COL_DIAS_PARADO) >= 5)
    if df_5_plus.height > 0:
        df_5_plus = calcular_multa(df_5_plus)
        arquivo_5_plus = os.path.join(pasta_saida, f"Relatório Sem Movimentação (5+ dias)_{data_hoje}.xlsx")
        df_5_plus.write_excel(arquivo_5_plus)
        logging.info(f"✅ Relatório 5+ dias salvo: {arquivo_5_plus}")
    else:
        logging.warning("⚠️ Nenhum pedido encontrado com 5+ dias parados.")

    df_incompletos = df_final.filter(pl.col(COL_NOME_PROBLEMATICO) == "Mercadorias.que.chegam.incompletos货未到齐")
    if df_incompletos.height > 0:
        arquivo_incompletos = os.path.join(pasta_saida, f"Relatório Mercadorias incompletas_{data_hoje}.xlsx")
        df_incompletos.write_excel(arquivo_incompletos)
        logging.info(f"✅ Relatório Mercadorias incompletas salvo: {arquivo_incompletos}")


# ==============================================================================
# --- FUNÇÃO PARA MOVER RELATÓRIOS ANTIGOS PARA ARQUIVO MORTO ---
# ==============================================================================
def mover_para_arquivo_morto(pasta_origem: str, pasta_destino: str):
    if not os.path.exists(pasta_destino): os.makedirs(pasta_destino)
    hoje = datetime.now().strftime("%Y-%m-%d")
    arquivos = [f for f in os.listdir(pasta_origem) if f.endswith(('.xlsx', '.xls'))]
    arquivos_hoje = [f for f in arquivos if hoje in f]
    arquivos_hoje.sort(key=lambda f: os.path.getmtime(os.path.join(pasta_origem, f)))
    if len(arquivos_hoje) > 1:
        for arquivo in arquivos_hoje[:-1]:
            try:
                shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo)); logging.info(
                    f"📦 Arquivo duplicado de hoje movido: {arquivo}")
            except Exception as e:
                logging.error(f"Erro ao mover o arquivo {arquivo}: {e}")
    for arquivo in arquivos:
        if arquivo not in arquivos_hoje:
            try:
                shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo)); logging.info(
                    f"📦 Arquivo antigo movido para Arquivo Morto: {arquivo}")
            except Exception as e:
                logging.error(f"Erro ao mover o arquivo {arquivo}: {e}")


# ==============================================================================
# --- MAIN ---
# ==============================================================================
def main():
    """
    Função principal que executa o fluxo completo do script.
    1. Carrega todos os dados de entrada.
    2. Processa e junta os dados na função 'processar_dados'.
    3. Adiciona informações de coordenadores.
    4. Salva os relatórios finais.
    """
    logging.info("--- INICIANDO PROCESSO DE GERAÇÃO DE RELATÓRIOS ---")

    # 1. CARREGAR DADOS
    caminho_arquivo_original = encontrar_arquivo_principal(PATH_INPUT_MAIN, FILENAME_START_MAIN)
    if not caminho_arquivo_original:
        logging.critical("Arquivo principal não encontrado. Processo interrompido.")
        return
    df_main = pl.read_excel(caminho_arquivo_original)
    df_problematicos = carregar_planilhas_de_pasta(PATH_INPUT_PROBLEMATICOS, "Consolidando problemáticos")
    df_devolucao = carregar_planilhas_de_pasta(PATH_INPUT_DEVOLUCAO, "Consolidando devoluções")

    # 2. PROCESSAR DADOS (Juntar e Aplicar Regras)
    df_final = processar_dados(df_main, df_problematicos, df_devolucao)

    # 3. ADICIONAR INFO COORDENADOR
    df_final = adicionar_info_coordenador(df_final)

    # 4. MOVER RELATÓRIOS ANTIGOS E SALVAR NOVOS
    mover_para_arquivo_morto(PATH_OUTPUT_REPORTS, PATH_OUTPUT_ARQUIVO_MORTO)
    salvar_relatorios(df_final, PATH_OUTPUT_REPORTS)

    logging.info("--- PROCESSO CONCLUÍDO COM SUCESSO! ---")


if __name__ == "__main__":
    main()