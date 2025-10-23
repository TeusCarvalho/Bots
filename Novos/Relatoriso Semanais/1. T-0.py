# -*- coding: utf-8 -*-
# 🚀 T-0 - Processamento Consolidado (lê todas as planilhas)
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import traceback
import glob

# --- Caminho da pasta ---
CAMINHO_PASTA_RELATORIO = Path(
    r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal\2. Taxa T0'
)

# --- Traduções e parâmetros ---
TRADUCOES_STATUS = {'是': 'ENTREGUE', '否': 'EM ROTA'}

# --- Colunas relevantes ---
COL_STATUS_ENTREGA = 'Status de Entrega'
COL_REMESSA = 'Remessa'
COL_MOTORISTA = 'Motorista de entrega'
COL_NOME_BASE = 'Nome da base'
COLUNAS_DATA = [
    'Horário de término do prazo de coleta',
    'tempo de chegada de veículo em PDD',
    'Horário bipagem de recebimento',
    'Horário da entrega'
]


class ReportProcessor:
    def __init__(self, relatorio_path: Path):
        self.relatorio_path = relatorio_path
        print(f"🚀 Iniciando processamento T-0 consolidado")
        print(f"📂 Pasta: {self.relatorio_path}")

    # --------------------------------------------------
    def _carregar_todos_relatorios(self) -> pd.DataFrame | None:
        """Lê todas as planilhas .xls e .xlsx da pasta"""
        padrao_arquivos = str(self.relatorio_path / '*.xls*')
        arquivos = sorted(glob.glob(padrao_arquivos))
        if not arquivos:
            print("⚠️ Nenhum arquivo encontrado na pasta!")
            return None

        print(f"📄 {len(arquivos)} arquivo(s) encontrado(s). Lendo todos...")

        dfs = []
        for arq in arquivos:
            try:
                print(f"→ Lendo: {Path(arq).name}")
                df = pd.read_excel(arq)
                df['Arquivo_Origem'] = Path(arq).name
                dfs.append(df)
            except Exception as e:
                print(f"❌ Erro ao ler {arq}: {e}")

        if not dfs:
            print("⚠️ Nenhuma planilha pôde ser lida.")
            return None

        df_final = pd.concat(dfs, ignore_index=True)
        print(f"✅ Total combinado: {len(df_final)} linhas.")
        return df_final

    # --------------------------------------------------
    def _processar_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpeza e normalização dos dados"""
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

        return df

    # --------------------------------------------------
    def _gerar_resumo_geral(self, df: pd.DataFrame):
        """Gera e exibe o SLA geral consolidado"""
        if COL_STATUS_ENTREGA not in df.columns or COL_REMESSA not in df.columns:
            print("⚠️ Colunas essenciais ausentes. Não é possível gerar resumo.")
            return

        total_geral = df[COL_REMESSA].nunique()
        total_entregue = df.loc[df[COL_STATUS_ENTREGA] == 'ENTREGUE', COL_REMESSA].nunique()

        sla = (total_entregue / total_geral * 100) if total_geral > 0 else 0

        print("\n📊 --- RESUMO GERAL (Consolidado) ---")
        print(f"Total de pedidos únicos: {total_geral:,}")
        print(f"Total entregues: {total_entregue:,}")
        print(f"SLA Geral Consolidado: {sla:.2f}%")

        # Agrupado por base (se existir)
        if COL_NOME_BASE in df.columns:
            print("\n📋 --- SLA por Base ---")
            resumo_base = (
                df.groupby(COL_NOME_BASE)[COL_REMESSA]
                .nunique()
                .reset_index(name='Total')
            )
            entregues_base = (
                df.loc[df[COL_STATUS_ENTREGA] == 'ENTREGUE']
                .groupby(COL_NOME_BASE)[COL_REMESSA]
                .nunique()
                .reset_index(name='Entregues')
            )
            resumo = pd.merge(resumo_base, entregues_base, on=COL_NOME_BASE, how='left').fillna(0)
            resumo['SLA (%)'] = np.where(resumo['Total'] > 0, resumo['Entregues'] / resumo['Total'] * 100, 0)
            print(resumo.head(15).to_string(index=False))

    # --------------------------------------------------
    def run(self):
        df = self._carregar_todos_relatorios()
        if df is None:
            return
        df_processado = self._processar_dados(df)
        self._gerar_resumo_geral(df_processado)


# ======================================================
if __name__ == "__main__":
    try:
        processor = ReportProcessor(relatorio_path=CAMINHO_PASTA_RELATORIO)
        processor.run()
    except Exception:
        print("\n--- ERRO FATAL ---")
        traceback.print_exc()