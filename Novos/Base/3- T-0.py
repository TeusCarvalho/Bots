# -*- coding: utf-8 -*-
# T-0 - Processamento
import pandas as pd
from datetime import datetime, timedelta
import os
import glob
from unidecode import unidecode
from pathlib import Path
import numpy as np
import traceback

# --- Configurações de Caminhos e Constantes ---
CAMINHO_PASTA_RELATORIO = Path(
    r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\T-0'
)
CAMINHO_PASTA_COORDENADORES = Path(
    r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador'
)
NOME_ARQUIVO_COORDENADORES = 'Base_Atualizada.xlsx'

# --- Caminho fixo para salvar versão de compartilhamento ---
CAMINHO_RELATORIO_COMPARTILHADO = Path(
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Relatorios\T-0\Relatorio_Processado.xlsx"
)

# --- Parâmetros ---
DIAS_PARA_MANTER_RELATORIOS = 7
TRADUCOES_STATUS = {'是': 'ENTREGUE', '否': 'EM ROTA'}

# --- Colunas ---
COL_NOME_BASE = 'Nome da base'
COL_COORDENADOR = 'Coordenadores'
COL_NOME_BASE_ANTIGO_COORD = 'Nova Base - Nome da Base'
COL_STATUS_ENTREGA = 'Status de Entrega'
COL_REMESSA = 'Remessa'
COL_MOTORISTA = 'Motorista de entrega'
COL_CHAVE_NORMALIZADA = 'Nome_base_norm'
COLUNAS_DATA = [
    'Horário de término do prazo de coleta', 'tempo de chegada de veículo em PDD',
    'Horário bipagem de recebimento', 'Horário da entrega'
]


class ReportProcessor:
    def __init__(self, relatorio_path: Path, coordenadores_path: Path):
        self.relatorio_path = relatorio_path
        self.coordenadores_path = coordenadores_path
        self.pasta_resultados = self.relatorio_path / 'Resultados'
        os.makedirs(self.pasta_resultados, exist_ok=True)
        os.makedirs(CAMINHO_RELATORIO_COMPARTILHADO.parent, exist_ok=True)
        print(f"Pasta de resultados pronta em: {self.pasta_resultados}")

    def _limpar_relatorios_antigos(self, dias_a_manter: int):
        limite_tempo = datetime.now() - timedelta(days=dias_a_manter)
        arquivos_antigos = glob.glob(str(self.pasta_resultados / 'Relatorio_Processado_*.xlsx'))
        for arquivo_path in arquivos_antigos:
            try:
                arquivo = Path(arquivo_path)
                data_modificacao = datetime.fromtimestamp(arquivo.stat().st_mtime)
                if data_modificacao < limite_tempo:
                    os.remove(arquivo)
                    print(f"Arquivo antigo excluído: {arquivo.name}")
            except OSError as e:
                print(f"Erro ao excluir arquivo antigo {arquivo_path}: {e}")

    def _carregar_relatorio(self) -> pd.DataFrame or None:
        padrao_nome_arquivo = '*Relatório da taxa de assinatura T0(Lista)*.xls*'
        lista_arquivos = sorted(glob.glob(str(self.relatorio_path / padrao_nome_arquivo)),
                                key=os.path.getmtime, reverse=True)
        if not lista_arquivos:
            print("Nenhum arquivo de relatório encontrado!")
            return None
        caminho_arquivo = Path(lista_arquivos[0])
        print(f"Arquivo encontrado: {caminho_arquivo.name}")
        try:
            return pd.read_excel(caminho_arquivo)
        except Exception as e:
            print(f"Erro ao ler o arquivo Excel: {e}")
            return None

    def _carregar_coordenadores(self) -> pd.DataFrame or None:
        caminho_arquivo = self.coordenadores_path / NOME_ARQUIVO_COORDENADORES
        if caminho_arquivo.exists():
            try:
                df = pd.read_excel(caminho_arquivo)
                print(f"Arquivo de coordenadores carregado com sucesso!")
                return df
            except Exception as e:
                print(f"Erro ao carregar coordenadores: {e}")
                return None
        else:
            print("Arquivo de coordenadores não encontrado.")
            return None

    def _processar_dados(self, df: pd.DataFrame, df_coordenadores: pd.DataFrame or None):
        for col in COLUNAS_DATA:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        if COL_STATUS_ENTREGA in df.columns:
            df[COL_STATUS_ENTREGA] = df[COL_STATUS_ENTREGA].replace(TRADUCOES_STATUS)
        if COL_REMESSA in df.columns:
            df[COL_REMESSA] = df[COL_REMESSA].astype(str)
            df = df[~df[COL_REMESSA].str.contains(r'-\d{3}$', regex=True, na=False)]
        if COL_STATUS_ENTREGA in df.columns and COL_MOTORISTA in df.columns:
            df.loc[df[COL_MOTORISTA].isna(), COL_STATUS_ENTREGA] = 'EM PISO'
        if df_coordenadores is not None:
            if COL_NOME_BASE_ANTIGO_COORD in df_coordenadores.columns:
                df_coordenadores = df_coordenadores.rename(columns={COL_NOME_BASE_ANTIGO_COORD: COL_NOME_BASE})
            df[COL_CHAVE_NORMALIZADA] = df[COL_NOME_BASE].astype(str).str.strip().str.upper().apply(unidecode)
            df_coordenadores[COL_CHAVE_NORMALIZADA] = df_coordenadores[COL_NOME_BASE].astype(str).str.strip().str.upper().apply(unidecode)
            df = pd.merge(df, df_coordenadores[[COL_CHAVE_NORMALIZADA, COL_COORDENADOR]],
                          on=COL_CHAVE_NORMALIZADA, how='left')
            df.drop(columns=[COL_CHAVE_NORMALIZADA], inplace=True)
        return df

    def _gerar_resumo_numerico(self, df: pd.DataFrame) -> pd.DataFrame:
        contagem_pedidos_numerico = pd.DataFrame()
        if COL_COORDENADOR in df.columns and COL_STATUS_ENTREGA in df.columns and COL_REMESSA in df.columns:
            df_resumo = df.dropna(subset=[COL_COORDENADOR])
            if not df_resumo.empty:
                contagem_pedidos = df_resumo.groupby([COL_COORDENADOR, COL_STATUS_ENTREGA])[COL_REMESSA].nunique().unstack(fill_value=0)
                contagem_pedidos['TOTAL GERAL'] = contagem_pedidos.sum(axis=1)
                if 'ENTREGUE' not in contagem_pedidos.columns:
                    contagem_pedidos['ENTREGUE'] = 0
                sla = np.where(contagem_pedidos['TOTAL GERAL'] > 0,
                               (contagem_pedidos['ENTREGUE'] / contagem_pedidos['TOTAL GERAL']) * 100,
                               0)
                contagem_pedidos['SLA (%)'] = sla
                contagem_pedidos_numerico = contagem_pedidos
        return contagem_pedidos_numerico

    def run_processing_flow(self):
        self._limpar_relatorios_antigos(DIAS_PARA_MANTER_RELATORIOS)
        df_principal = self._carregar_relatorio()
        if df_principal is None:
            return
        df_coordenadores = self._carregar_coordenadores()
        df_processado = self._processar_dados(df_principal, df_coordenadores)
        df_resumo_numerico = self._gerar_resumo_numerico(df_processado)

        # --- Histórico na pasta Resultados ---
        nome_arquivo = f"Relatorio_Processado_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.xlsx"
        caminho_saida = self.pasta_resultados / nome_arquivo
        with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
            df_processado.to_excel(writer, sheet_name='Dados_Completos', index=False)
            if not df_resumo_numerico.empty:
                df_resumo_numerico.to_excel(writer, sheet_name='ResumoNumerico')

        # --- Versão fixa para compartilhar no OneDrive ---
        with pd.ExcelWriter(CAMINHO_RELATORIO_COMPARTILHADO, engine='openpyxl') as writer:
            df_processado.to_excel(writer, sheet_name='Dados_Completos', index=False)
            if not df_resumo_numerico.empty:
                df_resumo_numerico.to_excel(writer, sheet_name='ResumoNumerico')


if __name__ == "__main__":
    try:
        processor = ReportProcessor(
            relatorio_path=CAMINHO_PASTA_RELATORIO,
            coordenadores_path=CAMINHO_PASTA_COORDENADORES
        )
        processor.run_processing_flow()
    except Exception as e:
        print("\n--- ERRO FATAL ---")
        traceback.print_exc()