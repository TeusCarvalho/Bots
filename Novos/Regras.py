import polars as pl
import os
import numpy as np
from tqdm import tqdm
from datetime import datetime
import shutil
import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field

# ==============================================================================
# --- CONFIGURAÇÃO GERAL ---
# Todas as configurações foram movidas para uma classe de dados (dataclass).
# Isso centraliza as configurações e facilita a passagem para as funções.
# ==============================================================================

@dataclass
class Config:
    """Classe para armazenar todas as configurações do script."""
    # --- 1. Caminhos Principais ---
    base_path: str = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Sem Movimentação'
    output_base_path: str = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Jt - Relatórios'
    coordenador_base_path: str = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador'

    # --- 2. Pastas e Arquivos de Entrada ---
    @property
    def path_input_main(self) -> str:
        return os.path.join(self.base_path, 'Sem_Movimentação')

    @property
    def path_input_problematicos(self) -> str:
        return os.path.join(self.base_path, 'Pacotes Problematicos')

    @property
    def path_input_devolucao(self) -> str:
        return os.path.join(self.base_path, 'Devolução')

    @property
    def arquivo_mapeamento_coordenadores(self) -> str:
        return os.path.join(self.coordenador_base_path, 'Base_Atualizada.xlsx')

    # --- 3. Pastas de Saída ---
    @property
    def path_output_reports(self) -> str:
        return self.output_base_path

    @property
    def path_output_arquivo_morto(self) -> str:
        return os.path.join(self.output_base_path, "Arquivo Morto")

    # --- 4. Nomes de Arquivos e Colunas ---
    filename_start_main: str = 'Monitoramento de movimentação em tempo real'

    # Colunas principais
    col_remetessa: str = 'Remessa'
    col_dias_parado: str = 'Dias Parado'
    col_ultima_operacao: str = 'Tipo da última operação'
    col_regional: str = 'Regional responsável'
    col_nome_problematico: str = 'Nome de pacote problemático'
    col_hora_operacao: str = 'Horário da última operação'
    col_devolucao: str = 'Devolução'
    col_status: str = 'Status'
    col_multa: str = 'Multa (R$)'
    col_base_recente: str = 'Nome da base mais recente'
    col_transito: str = 'Trânsito'

    # Colunas para mapeamento de coordenadores
    coluna_chave_principal: str = 'Unidade responsável'
    coluna_chave_mapeamento: str = 'Nome da base'
    coluna_info_coordenador: str = 'Coordenadores'
    coluna_info_filial: str = 'Filial'
    nova_coluna_coordenador: str = 'Coordenadores'
    nova_coluna_filial: str = 'Filial'

    # --- 5. Listas para Regras de Negócio ---
    # CORREÇÃO: Usar field(default_factory=lambda: [...]) para listas
    franquias: List[str] = field(default_factory=lambda: ["F AGL-GO", "F ALV-AM", "F APG - GO", "F ARQ - RO", "F BSB-DF", "F CDN-AM", "F CGR - MS", "F FMA-GO",
                                                          "F GYN - GO", "F ITI-PA", "F RVD - GO", "F TRD-GO", "F CGR 02-MS", "F GYN 02-GO", "F OCD-GO", "F PVH-RO",
                                                          "F TGT-DF", "F DOM -PA", "F JCD-PA", "F MCP-AP", "F ORL-PA", "F PCA-PA", "F RDC -PA", "F SFX-PA",
                                                          "F TLA-PA"])
    unidades_sc_dc: List[str] = field(default_factory=lambda: ["DC AGB-MT", "DC CGR-MS", "DC GYN-GO", "DC JUI-MT", "DC PVH-RO", "DC RBR-AC", "DF BSB", "GYN -GO",
                                                               "MT CGB"])
    bases_fluxo_inverso: List[str] = field(default_factory=lambda: ["VLP -GO", "VHL-RO", "VGR-MT", "VGR 02-MT", "URC -GO", "TRD -GO", "TLL -MS", "TGT -DF",
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
                                                                    "AMB -MS", "AGL -GO", "AGB -MT"])
    destinos_fluxo_inverso: List[str] = field(default_factory=lambda: ["MAO -AM", "DC AGB-MT", "DC CGR-MS", "DC GYN-GO", "DC JUI-MT", "DC MAO-AM", "DC MRB-PA",
                                                                       "DC PMW-TO", "DC PVH-RO", "DC RBR-AC", "DC STM-PA", "DF BSB"])
    bases_cd: List[str] = None # Será preenchido com bases_fluxo_inverso

    def __post_init__(self):
        # Preenche bases_cd após a inicialização
        self.bases_cd = self.bases_fluxo_inverso
        # Configura o logging uma única vez
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )


# ==============================================================================
# --- CLASSE DE PROCESSAMENTO PRINCIPAL ---
# Encapsula toda a lógica de carregamento, processamento e salvamento.
# ==============================================================================

class PackageProcessor:
    """Orquestra o fluxo completo de processamento de pacotes."""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)

    # --- MÉTODOS DE CARREGAMENTO DE DADOS ---
    def _encontrar_arquivo_principal(self, pasta: str, inicio_nome: str) -> Optional[str]:
        """Busca por um arquivo Excel em uma pasta que comece com um determinado nome."""
        try:
            for nome_arquivo in os.listdir(pasta):
                if nome_arquivo.startswith(inicio_nome) and nome_arquivo.endswith(('.xlsx', '.xls')):
                    self.logger.info(f"Arquivo principal encontrado: {nome_arquivo}")
                    return os.path.join(pasta, nome_arquivo)
        except FileNotFoundError:
            self.logger.error(f"A pasta de leitura '{pasta}' não foi encontrada.")
            return None
        self.logger.warning(f"Nenhum arquivo começando com '{inicio_nome}' foi encontrado em '{pasta}'.")
        return None

    def _carregar_planilhas_de_pasta(self, caminho_pasta: str, descricao_tqdm: str) -> pl.DataFrame:
        """Lê todos os arquivos Excel de uma pasta e consolida seus dados."""
        lista_dfs = []
        nome_pasta = os.path.basename(caminho_pasta)
        self.logger.info(f"Lendo planilhas da pasta: {nome_pasta}")
        try:
            arquivos = [f for f in os.listdir(caminho_pasta) if f.endswith(('.xlsx', '.xls'))]
            if not arquivos:
                self.logger.warning(f"Nenhum arquivo Excel encontrado na pasta '{nome_pasta}'.")
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
                        self.logger.info(f"Esquema de colunas de referência definido com {len(colunas_referencia)} colunas a partir do arquivo '{arquivo}'.")
                        lista_dfs.append(df_aba)
                        continue

                    colunas_faltantes = [col for col in colunas_referencia if col not in df_aba.columns]
                    if colunas_faltantes:
                        self.logger.warning(f"Arquivo '{arquivo}' está faltando colunas. Colunas adicionadas com nulos: {colunas_faltantes}")
                        for col in colunas_faltantes:
                            df_aba = df_aba.with_columns(pl.lit(None).alias(col))

                    df_alinhado = df_aba.select(colunas_referencia)
                    lista_dfs.append(df_alinhado)

                except Exception as e:
                    self.logger.error(f"Falha ao ler o arquivo '{arquivo}': {e}")
                    continue

            if not lista_dfs:
                return pl.DataFrame()

            df_consolidado = pl.concat(lista_dfs)
            self.logger.info(f"Total de {len(df_consolidado)} registros consolidados de '{nome_pasta}'.")
            return df_consolidado

        except FileNotFoundError:
            self.logger.error(f"A pasta '{caminho_pasta}' não foi encontrada. Processo interrompido.")
            raise
        except Exception as e:
            self.logger.error(f"Ocorreu um erro inesperado ao ler os arquivos da pasta '{nome_pasta}': {e}")
            raise

    # --- MÉTODOS DE PROCESSAMENTO E LÓGICA DE NEGÓCIO ---
    def _aplicar_regras_transito(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica regras de negócio para definir o status de 'Trânsito' dos pacotes."""
        self.logger.info("Aplicando regras de trânsito...")
        if self.config.col_base_recente not in df.columns:
            self.logger.warning(f"Coluna '{self.config.col_base_recente}' não encontrada. As regras de trânsito não serão aplicadas.")
            return df.with_columns(pl.lit("COLUNA DE BASE RECENTE NÃO ENCONTRADA").alias(self.config.col_transito))

        cond_em_transito = pl.col(self.config.col_ultima_operacao) == "发件扫描/Bipe de expedição"
        is_fluxo_inverso = pl.col(self.config.col_base_recente).is_in(self.config.bases_fluxo_inverso) & pl.col(self.config.col_regional).is_in(self.config.destinos_fluxo_inverso)
        origem_sc_bre = pl.col(self.config.col_base_recente) == 'SC BRE'
        destino_pvh = pl.col(self.config.col_regional).str.contains('PVH-RO', literal=False)
        prazo_fluxo_inverso_estourado = pl.col(self.config.col_dias_parado) >= 3
        prazo_5_dias_estourado = pl.col(self.config.col_dias_parado) >= 5
        prazo_3_dias_estourado = pl.col(self.config.col_dias_parado) >= 3

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
            .alias(self.config.col_transito)
        )
        self.logger.info("Regras de trânsito aplicadas com sucesso.")
        return df

    def _aplicar_regras_status_problematicos(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica regras de status para pacotes problemáticos."""
        is_problematico = pl.col(self.config.col_ultima_operacao) == "问题件扫描/Bipe de pacote problemático"

        # Regras de Extravio
        df = df.with_columns(
            pl.when(is_problematico & (pl.col(self.config.col_nome_problematico) == "Extravio.interno.内部遗失"))
            .then("PEDIDO EXTRAVIADO")
            .when(is_problematico & (pl.col(self.config.col_nome_problematico) == "Encomenda.expedido.mas.não.chegou.有发未到件") & (pl.col(self.config.col_dias_parado) >= 3))
            .then("ALERTA DE EXTRAVIO: ABRIR CHAMADO INTERNO (HÁ MAIS DE 3 DIAS)")
            .when(is_problematico & (pl.col(self.config.col_nome_problematico) == "Encomenda.expedido.mas.não.chegou.有发未到件"))
            .then("ATENÇÃO: RISCO DE EXTRAVIO (AGUARDANDO CHEGADA)")
            .otherwise(pl.col(self.config.col_status)).alias(self.config.col_status)
        )
        # Regras de Retidos
        df = df.with_columns(
            pl.when(is_problematico & (pl.col(self.config.col_nome_problematico) == "retidos.留仓") & (pl.col(self.config.col_dias_parado) >= 3))
            .then("ATENÇÃO: PACOTE RETIDO NO PISO (HÁ MAIS DE 3 DIAS)")
            .when(is_problematico & (pl.col(self.config.col_nome_problematico) == "retidos.留仓"))
            .then("ATENÇÃO: PACOTE RETIDO NO PISO")
            .otherwise(pl.col(self.config.col_status)).alias(self.config.col_status)
        )
        # Regras de Endereço
        df = df.with_columns(
            pl.when(is_problematico & pl.col(self.config.col_nome_problematico).is_in([
                "Endereço.incorreto地址信息错误", "Impossibilidade.de.chegar.no.endereço.informado客户地址无法进入",
                "Endereço.incompleto地址信息不详", "Impossibilidade.de.chegar.no.endereço.informado.de.coleta.客户地址无法进入C"]) & (pl.col(self.config.col_dias_parado) >= 8))
            .then("SOLICITAR DEVOLUÇÃO (ENDEREÇO/ACESSO INCORRETO, HÁ MAIS DE 8 DIAS)")
            .when(is_problematico & pl.col(self.config.col_nome_problematico).is_in([
                "Endereço.incorreto地址信息错误", "Impossibilidade.de.chegar.no.endereço.informado客户地址无法进入",
                "Endereço.incompleto地址信息不详", "Impossibilidade.de.chegar.no.endereço.informado.de.coleta.客户地址无法进入C"]))
            .then("ATENÇÃO: AGUARDANDO DEVOLUÇÃO (ENDEREÇO/ACESSO INCORRETO)")
            .otherwise(pl.col(self.config.col_status)).alias(self.config.col_status)
        )
        # Regras de Ausência de Destinatário
        df = df.with_columns(
            pl.when(is_problematico & (pl.col(self.config.col_nome_problematico) == "Ausência.de.destinatário.nas.várias.tentativas.de.entrega多次派送客户不在"))
            .then("VERIFICAR 3 TENTATIVAS DE ENTREGA. SE OK, SOLICITAR DEVOLUÇÃO. SENÃO, REALIZAR NOVA TENTATIVA.")
            .when(is_problematico & (pl.col(self.config.col_nome_problematico) == "Ausência.do.destinatário客户不在") & (pl.col(self.config.col_dias_parado) >= 2))
            .then("ATENÇÃO: DEVOLVER À BASE (AUSÊNCIA, HÁ MAIS DE 2 DIAS)")
            .when(is_problematico & (pl.col(self.config.col_nome_problematico) == "Ausência.do.destinatário客户不在"))
            .then("ATENÇÃO: DEVOLUÇÃO À BASE PENDENTE (AUSÊNCIA)")
            .otherwise(pl.col(self.config.col_status)).alias(self.config.col_status)
        )
        # Regras de Recusa/Mudança
        df = df.with_columns(
            pl.when(is_problematico & pl.col(self.config.col_nome_problematico).is_in([
                "Recusa.de.recebimento.pelo.cliente.(destinatário)无理由拒收", "O.destinatário.mudou.o.endereço.收件人搬家"]) & (pl.col(self.config.col_dias_parado) >= 2))
            .then("ATENÇÃO: DEVOLVER À BASE (RECUSA/MUDANÇA DE ENDEREÇO, HÁ MAIS DE 2 DIAS)")
            .when(is_problematico & pl.col(self.config.col_nome_problematico).is_in([
                "Recusa.de.recebimento.pelo.cliente.(destinatário)无理由拒收", "O.destinatário.mudou.o.endereço.收件人搬家"]))
            .then("ATENÇÃO: DEVOLUÇÃO À BASE PENDENTE (RECUSA/MUDANÇA DE ENDEREÇO)")
            .otherwise(pl.col(self.config.col_status)).alias(self.config.col_status)
        )
        # Outras regras problemáticas
        df = df.with_columns(
            pl.when(is_problematico & pl.col(self.config.col_nome_problematico).is_in(["Pacote.fora.do.padrão.三边尺寸超限", "Embalagem.não.conforme.包装不规范"]))
            .then("SOLICITAR DEVOLUÇÃO IMEDIATA (FORA DO PADRÃO / EMBALAGEM NÃO CONFORME)")
            .when(is_problematico & (pl.col(self.config.col_nome_problematico) == "Mercadorias.que.chegam.incompletos货未到齐") & (pl.col(self.config.col_dias_parado) >= 2))
            .then("ENVIAR PARA O FLUXO INVERSO (INCOMPLETO, HÁ MAIS DE 2 DIAS)")
            .when(is_problematico & (pl.col(self.config.col_nome_problematico) == "Pacotes.retidos.por.anomalias.异常拦截件") & (pl.col(self.config.col_dias_parado) >= 3))
            .then("ENVIAR PARA A QUALIDADE (ANOMALIA, HÁ MAIS DE 3 DIAS)")
            .when(is_problematico & (pl.col(self.config.col_nome_problematico) == "Pacotes.retidos.por.anomalias.异常拦截件"))
            .then("ATENÇÃO: ANOMALIA EM ANÁLISE")
            .when(is_problematico & (pl.col(self.config.col_nome_problematico) == "Devolução.退回件"))
            .then("ENVIAR PARA SC/DC (DEVOLUÇÃO APROVADA)")
            .otherwise(pl.col(self.config.col_status)).alias(self.config.col_status)
        )
        return df

    def _aplicar_regras_status_normais(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica regras de status para operações normais de entrega."""
        df = df.with_columns(
            pl.when((pl.col(self.config.col_ultima_operacao) == "出仓扫描/Bipe de saída para entrega") & (pl.col(self.config.col_regional).is_in(self.config.franquias)) & (pl.col(self.config.col_dias_parado) >= 2))
            .then("ATRASO NA ENTREGA (FRANQUIA)")
            .when((pl.col(self.config.col_ultima_operacao) == "出仓扫描/Bipe de saída para entrega") & (pl.col(self.config.col_regional).is_in(self.config.franquias)))
            .then("EM ROTA DE ENTREGA (FRANQUIA)")
            .when((pl.col(self.config.col_ultima_operacao) == "出仓扫描/Bipe de saída para entrega") & (~pl.col(self.config.col_regional).is_in(self.config.franquias)) & (pl.col(self.config.col_dias_parado) >= 2))
            .then("ATENÇÃO: ATRASO NA ENTREGA (BASE PRÓPRIA)")
            .when((pl.col(self.config.col_ultima_operacao) == "出仓扫描/Bipe de saída para entrega"))
            .then("EM ROTA DE ENTREGA (BASE PRÓPRIA)")
            .otherwise(pl.col(self.config.col_status)).alias(self.config.col_status)
        )
        return df

    def _aplicar_regras_status_envio_errado(self, df: pl.DataFrame) -> pl.DataFrame:
        """Aplica regras de status para envios errados entre CDs."""
        is_envio_errado_cd = pl.col(self.config.col_base_recente).is_in(self.config.bases_cd) & pl.col(self.config.col_regional).is_in(self.config.bases_cd)
        df = df.with_columns(
            pl.when(is_envio_errado_cd & (pl.col(self.config.col_nome_problematico) == "Mercadorias.do.cliente.não.estão.completas.客户货物未备齐"))
            .then("ENVIAR PARA O FLUXO INVERSO (INCOMPLETO, HÁ MAIS DE 2 DIAS)")
            .when(is_envio_errado_cd & (pl.col(self.config.col_nome_problematico) == "Ausência.do.destinatário客户不在"))
            .then("VERIFICAR 3 TENTATIVAS DE ENTREGA. SE OK, SOLICITAR DEVOLUÇÃO. SENÃO, REALIZAR NOVA TENTATIVA.")
            .when(is_envio_errado_cd)
            .then("ENVIO ERRADO - ENTRE CDs")
            .otherwise(pl.col(self.config.col_status)).alias(self.config.col_status)
        )
        return df

    def _aplicar_regras_status(self, df: pl.DataFrame) -> pl.DataFrame:
        """Orquestra a aplicação de todas as regras de status."""
        self.logger.info("Aplicando regras de status...")
        df = df.with_columns(pl.col(self.config.col_ultima_operacao).str.to_uppercase().alias(self.config.col_status))

        df = self._aplicar_regras_status_problematicos(df)
        df = self._aplicar_regras_status_normais(df)
        df = self._aplicar_regras_status_envio_errado(df)

        self.logger.info("Regras de status aplicadas com sucesso.")
        return df

    def _calcular_multa(self, df: pl.DataFrame) -> pl.DataFrame:
        if df.height == 0:
            self.logger.info("Nenhum pacote com 6+ dias para cálculo de multa.")
            return df
        self.logger.info("Calculando multa para pacotes com 6 ou mais dias parados...")
        df = df.with_columns(
            pl.when(pl.col(self.config.col_dias_parado) >= 30).then(30)
            .when(pl.col(self.config.col_dias_parado).is_between(14, 29)).then(14)
            .when(pl.col(self.config.col_dias_parado).is_between(10, 13)).then(10)
            .when(pl.col(self.config.col_dias_parado).is_between(7, 9)).then(7)
            .when(pl.col(self.config.col_dias_parado) == 6).then(6)
            .when(pl.col(self.config.col_dias_parado) == 5).then(5)
            .when(pl.col(self.config.col_dias_parado) == 4).then(4)
            .when(pl.col(self.config.col_dias_parado) == 3).then(3)
            .when(pl.col(self.config.col_dias_parado) == 2).then(2)
            .otherwise(0).alias(self.config.col_multa)
        )
        self.logger.info("Multa calculada por item.")
        return df

    def _processar_dados(self, df_main: pl.DataFrame, df_problematicos: pl.DataFrame, df_devolucao: pl.DataFrame) -> pl.DataFrame:
        """Função principal de processamento que orquestra a junção de dados e a aplicação de regras."""
        colunas_necessarias = [self.config.col_remetessa, self.config.col_ultima_operacao, self.config.col_regional, self.config.col_nome_problematico, self.config.col_hora_operacao, self.config.col_base_recente]
        if not all(col in df_main.columns for col in colunas_necessarias):
            colunas_faltantes = set(colunas_necessarias) - set(df_main.columns)
            self.logger.critical(f"O arquivo principal não contém as colunas obrigatórias: {colunas_faltantes}.")
            return pl.DataFrame()

        df = df_main.clone().with_columns(pl.col(self.config.col_remetessa).cast(pl.Utf8))
        self.logger.info(f"Processamento iniciado com {len(df)} registros da planilha principal.")

        # Juntar com dados de Pacotes Problemáticos
        if df_problematicos.height > 0:
            self.logger.info("Enriquecendo dados com informações de pacotes problemáticos...")
            df_problematicos = df_problematicos.with_columns(
                pl.col('Número de pedido JMS').cast(pl.Utf8),
                pl.col('Tempo de digitalização').cast(pl.Utf8).str.to_datetime(strict=False)
            ).sort('Tempo de digitalização').filter(pl.col('Número de pedido JMS').is_not_null())
            summary = df_problematicos.group_by('Número de pedido JMS').agg([
                pl.len().alias('Qtd_Problematicas'),
                pl.last('Tipo de nível II de pacote problemático').alias('Ultima_Problematica_Detalhada')
            ])
            df = df.join(summary, left_on=self.config.col_remetessa, right_on='Número de pedido JMS', how='left')
            df = df.with_columns(
                pl.col('Ultima_Problematica_Detalhada').fill_null('-').alias('Última Problemática Detalhada'),
                pl.col('Qtd_Problematicas').fill_null(0).cast(pl.Int32).alias('Qtd Problemáticas')
            )
            self.logger.info("Dados de pacotes problemáticos integrados.")

        # Juntar com dados de Devolução
        df = df.with_columns(pl.lit('DEVOLUÇÃO NÃO SOLICITADA').alias(self.config.col_devolucao))
        if df_devolucao.height > 0:
            self.logger.info("Enriquecendo dados com informações de devolução...")
            df_devolucao = df_devolucao.with_columns(pl.col('Número de pedido JMS').cast(pl.Utf8))
            mapa_traducao = {'待审核': 'EM PROCESSO DE APROVAÇÃO', '驳回': 'PEDIDO DE DEVOLUÇÃO RECUSADO', '已审核': 'DEVOLUÇÃO APROVADA'}
            df_devolucao = df_devolucao.with_columns(pl.col('Estado de solicitação').replace(mapa_traducao).alias('Status_Traduzido'))
            df_devolucao_info = df_devolucao.filter(pl.col('Status_Traduzido').is_not_null()).select(['Número de pedido JMS', 'Status_Traduzido']).unique(subset=['Número de pedido JMS'], keep='last')
            df = df.join(df_devolucao_info, left_on=self.config.col_remetessa, right_on='Número de pedido JMS', how='left')
            df = df.with_columns(pl.coalesce([pl.col('Status_Traduzido'), pl.col(self.config.col_devolucao)]).alias(self.config.col_devolucao))
            df = df.drop('Status_Traduzido')
            self.logger.info("Dados de devolução integrados.")

        # Aplicar as regras de negócio
        self.logger.info("Aplicando regras de negócio no DataFrame consolidado...")
        df = df.with_columns(pl.col(self.config.col_hora_operacao).cast(pl.Utf8).str.to_datetime(strict=False))

        # --- LINHA CORRIGIDA DEFINITIVAMENTE ABAIXO ---
        df = df.with_columns(
            (pl.lit(datetime.now()) - pl.col(self.config.col_hora_operacao)).dt.total_days().fill_null(0).cast(pl.Int32).alias(self.config.col_dias_parado)
        )

        df = self._aplicar_regras_status(df)
        df = self._aplicar_regras_transito(df)

        # Ajustes finais de status
        df = df.with_columns(
            pl.when(pl.col(self.config.col_devolucao) != 'DEVOLUÇÃO NÃO SOLICITADA').then(pl.col(self.config.col_devolucao)).otherwise(pl.col(self.config.col_status)).alias(self.config.col_status)
        )
        cond_aprovado_em_rota = (pl.col(self.config.col_status) == 'DEVOLUÇÃO APROVADA') & (pl.col(self.config.col_ultima_operacao) == "出仓扫描/Bipe de saída para entrega")
        df = df.with_columns(pl.when(cond_aprovado_em_rota).then('DEVOLUÇÃO APROVADA, MAS O PACOTE ESTÁ EM ROTA').otherwise(pl.col(self.config.col_status)).alias(self.config.col_status))
        condicao_aprovado = pl.col(self.config.col_status).is_in(['DEVOLUÇÃO APROVADA', 'DEVOLUÇÃO APROVADA, MAS O PACOTE ESTÁ EM ROTA'])
        df = df.with_columns(pl.when(condicao_aprovado).then('').otherwise(pl.col(self.config.col_transito)).alias(self.config.col_transito))

        df = df.rename({self.config.col_regional: self.config.coluna_chave_principal})

        # Reordenar colunas
        ordem_colunas = [self.config.col_remetessa, self.config.coluna_chave_principal, self.config.col_dias_parado, self.config.col_ultima_operacao, self.config.col_hora_operacao, self.config.col_status, self.config.col_transito, self.config.col_devolucao, 'Qtd Problemáticas', 'Última Problemática Detalhada']
        colunas_existentes = [col for col in df.columns if col not in ordem_colunas]
        df = df.select(ordem_colunas + colunas_existentes)

        return df

    def _adicionar_info_coordenador(self, df_principal: pl.DataFrame) -> pl.DataFrame:
        if df_principal.height == 0:
            self.logger.warning("DataFrame de entrada está vazio. Pulando adição de coordenadores.")
            return df_principal
        try:
            self.logger.info(f"Lendo arquivo de mapeamento: {os.path.basename(self.config.arquivo_mapeamento_coordenadores)}")
            df_mapeamento = pl.read_excel(self.config.arquivo_mapeamento_coordenadores)
        except FileNotFoundError:
            self.logger.error(f"ERRO CRÍTICO: Arquivo de mapeamento '{self.config.arquivo_mapeamento_coordenadores}' não encontrado. O processo será interrompido.")
            raise
        except Exception as e:
            self.logger.error(f"Ocorreu um erro ao ler o arquivo de mapeamento: {e}. O processo será interrompido.")
            raise
        mapa_coordenador = dict(zip(df_mapeamento[self.config.coluna_chave_mapeamento], df_mapeamento[self.config.coluna_info_coordenador]))
        mapa_filial = dict(zip(df_mapeamento[self.config.coluna_chave_mapeamento], df_mapeamento[self.config.coluna_info_filial]))
        self.logger.info("Adicionando informações de Coordenador e Filial...")
        df_principal = df_principal.with_columns(
            pl.col(self.config.coluna_chave_principal).map_dict(mapa_coordenador, default='NÃO ENCONTRADO').alias(self.config.nova_coluna_coordenador),
            pl.col(self.config.coluna_chave_principal).map_dict(mapa_filial, default='NÃO ENCONTRADA').alias(self.config.nova_coluna_filial)
        )
        self.logger.info("Informações de coordenador e filial adicionadas.")
        return df_principal

    # --- MÉTODOS PARA SALVAR RELATÓRIOS ---
    def _salvar_relatorios(self, df_final: pl.DataFrame):
        if df_final.height == 0:
            self.logger.warning("⚠️ Nenhum dado para salvar relatórios.")
            return
        data_hoje = datetime.now().strftime("%Y-%m-%d")
        resumo = {"0-4 dias": df_final.filter(pl.col(self.config.col_dias_parado) <= 4).height,
                  "5+ dias": df_final.filter(pl.col(self.config.col_dias_parado) >= 5).height, "Total": df_final.height}
        self.logger.info(f"📊 Resumo Dias Parados: {resumo}")

        df_0_4 = df_final.filter(pl.col(self.config.col_dias_parado) <= 4)
        if df_0_4.height > 0:
            arquivo_0_4 = os.path.join(self.config.path_output_reports, f"Relatório Sem Movimentação (0-4 dias)_{data_hoje}.xlsx")
            df_0_4.write_excel(arquivo_0_4)
            self.logger.info(f"Relatório 0-4 dias salvo: {arquivo_0_4}")

        df_5_plus = df_final.filter(pl.col(self.config.col_dias_parado) >= 5)
        if df_5_plus.height > 0:
            df_5_plus = self._calcular_multa(df_5_plus)
            arquivo_5_plus = os.path.join(self.config.path_output_reports, f"Relatório Sem Movimentação (5+ dias)_{data_hoje}.xlsx")
            df_5_plus.write_excel(arquivo_5_plus)
            self.logger.info(f"✅ Relatório 5+ dias salvo: {arquivo_5_plus}")
        else:
            self.logger.warning("⚠️ Nenhum pedido encontrado com 5+ dias parados.")

        df_incompletos = df_final.filter(pl.col(self.config.col_nome_problematico) == "Mercadorias.que.chegam.incompletos货未到齐")
        if df_incompletos.height > 0:
            arquivo_incompletos = os.path.join(self.config.path_output_reports, f"Relatório Mercadorias incompletas_{data_hoje}.xlsx")
            df_incompletos.write_excel(arquivo_incompletos)
            self.logger.info(f"✅ Relatório Mercadorias incompletas salvo: {arquivo_incompletos}")

    def _mover_para_arquivo_morto(self):
        pasta_origem = self.config.path_output_reports
        pasta_destino = self.config.path_output_arquivo_morto
        if not os.path.exists(pasta_destino): os.makedirs(pasta_destino)
        hoje = datetime.now().strftime("%Y-%m-%d")
        arquivos = [f for f in os.listdir(pasta_origem) if f.endswith(('.xlsx', '.xls'))]
        arquivos_hoje = [f for f in arquivos if hoje in f]
        arquivos_hoje.sort(key=lambda f: os.path.getmtime(os.path.join(pasta_origem, f)))
        if len(arquivos_hoje) > 1:
            for arquivo in arquivos_hoje[:-1]:
                try:
                    shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo)); self.logger.info(f"📦 Arquivo duplicado de hoje movido: {arquivo}")
                except Exception as e:
                    self.logger.error(f"Erro ao mover o arquivo {arquivo}: {e}")
        for arquivo in arquivos:
            if arquivo not in arquivos_hoje:
                try:
                    shutil.move(os.path.join(pasta_origem, arquivo), os.path.join(pasta_destino, arquivo)); self.logger.info(f"📦 Arquivo antigo movido para Arquivo Morto: {arquivo}")
                except Exception as e:
                    self.logger.error(f"Erro ao mover o arquivo {arquivo}: {e}")

    # --- MÉTODO PRINCIPAL DE EXECUÇÃO ---
    def run(self):
        """Executa o fluxo completo de processamento."""
        self.logger.info("--- INICIANDO PROCESSO DE GERAÇÃO DE RELATÓRIOS ---")
        try:
            # 1. CARREGAR DADOS
            caminho_arquivo_original = self._encontrar_arquivo_principal(self.config.path_input_main, self.config.filename_start_main)
            if not caminho_arquivo_original:
                self.logger.critical("Arquivo principal não encontrado. Processo interrompido.")
                return
            df_main = pl.read_excel(caminho_arquivo_original)
            df_problematicos = self._carregar_planilhas_de_pasta(self.config.path_input_problematicos, "Consolidando problemáticos")
            df_devolucao = self._carregar_planilhas_de_pasta(self.config.path_input_devolucao, "Consolidando devoluções")

            # 2. PROCESSAR DADOS
            df_final = self._processar_dados(df_main, df_problematicos, df_devolucao)

            # 3. ADICIONAR INFO COORDENADOR
            df_final = self._adicionar_info_coordenador(df_final)

            # 4. MOVER RELATÓRIOS ANTIGOS E SALVAR NOVOS
            self._mover_para_arquivo_morto()
            self._salvar_relatorios(df_final)

            self.logger.info("--- PROCESSO CONCLUÍDO COM SUCESSO! ---")
        except Exception as e:
            self.logger.critical(f"O processo falhou com um erro crítico: {e}", exc_info=True)


# ==============================================================================
# --- MAIN ---
# ==============================================================================
def main():
    """
    Função principal que instancia a configuração e o processador,
    e inicia a execução.
    """
    # Crie a instância de configuração. Você pode passar valores diferentes aqui
    # se precisar rodar o script com outro conjunto de pastas/regras.
    config = Config()

    # Instancie o processador com a configuração
    processor = PackageProcessor(config)

    # Execute o processo
    processor.run()


if __name__ == "__main__":
    main()