# -*- coding: utf-8 -*-
# T-0 - Relatório Consolidado (Top 5 piores e melhores)
import pandas as pd
from datetime import datetime, timedelta
import os
import glob
from pathlib import Path
import numpy as np
import requests
import traceback

# --- Configurações de Caminhos e Constantes ---
CAMINHO_PASTA_RELATORIO = Path(
    r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\T-0'
)
NOME_ARQUIVO_COORDENADORES = 'Base_Atualizada.xlsx'

# --- Caminho fixo para salvar versão de compartilhamento ---
CAMINHO_RELATORIO_COMPARTILHADO = Path(
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Relatorios\T-0\Relatorio_Processado.xlsx"
)

# --- Link público fixo do OneDrive (para abrir no Feishu) ---
LINK_RELATORIO = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/Ek3KdqMIdX5EodE-3JwCQnsBAMiJ574BsxAR--oYBNN0-g?e=v83LYG"

# --- Webhook único ---
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/18eed487-c172-4b86-95cf-bfbe1cd21df1"

# --- Bases que devem ser enviadas ---
BASES_PERMITIDAS = [
    "F BSB - DF", "F BSB-DF", "F FMA-GO", "F TGT-DF", "F VLP-GO", "F AGL-GO",
    "F ANP-GO", "F APG - GO", "F GYN - GO", "F GYN 02-GO", "F OCD - GO", "F OCD-GO",
    "F PDR-GO", "F PON-GO", "F RVD - GO", "F SEN-GO", "F TRD-GO", "F ARQ - RO",
    "F PVH-RO", "F VHL-RO", "F CMV-MT", "F CNF-MT", "F PVL-MT", "F AMB-MS",
    "F CGR - MS", "F CGR 02-MS", "F DOU-MS", "F ALV-AM", "F ALX-AM", "F BAO-PA",
    "F CDN-AM", "F CHR-AM", "F DOM -PA", "F GAI-TO", "F GRP-TO", "F ITI -PA",
    "F ITI-PA", "F JCD-PA", "F MCP-AP", "F ORL-PA", "F PCA-PA", "F PGM-PA",
    "F RDC -PA", "F SFX-PA", "F TLA-PA", "F TUR-PA", "F MCP 02-AP"
]

# --- Parâmetros ---
DIAS_PARA_MANTER_RELATORIOS = 7
TRADUCOES_STATUS = {'是': 'ENTREGUE', '否': 'EM ROTA'}

# --- Colunas ---
COL_NOME_BASE = 'Nome da base'
COL_STATUS_ENTREGA = 'Status de Entrega'
COL_REMESSA = 'Remessa'
COL_MOTORISTA = 'Motorista de entrega'
COLUNAS_DATA = [
    'Horário de término do prazo de coleta', 'tempo de chegada de veículo em PDD',
    'Horário bipagem de recebimento', 'Horário da entrega'
]


class ReportProcessor:
    def __init__(self, relatorio_path: Path):
        self.relatorio_path = relatorio_path
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
            df = pd.read_excel(caminho_arquivo)
            df.columns = df.columns.str.strip().str.replace('\n', ' ').str.replace('\r', ' ')
            print("📌 Colunas encontradas no relatório:")
            for i, col in enumerate(df.columns, 1):
                print(f"{i}. {col}")
            return df
        except Exception as e:
            print(f"Erro ao ler o arquivo Excel: {e}")
            return None

    def _processar_dados(self, df: pd.DataFrame):
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

    def _enviar_card_top10(self, df_processado: pd.DataFrame):
        if COL_NOME_BASE not in df_processado.columns:
            print(f"⚠ Coluna '{COL_NOME_BASE}' não encontrada.")
            return

        data_coleta_str = datetime.now().strftime('%d/%m/%Y %H:%M')

        # ============================
        # 🔎 Relatório anterior
        # ============================
        arquivos = sorted(
            glob.glob(str(self.pasta_resultados / "Relatorio_Processado_*.xlsx")),
            key=os.path.getmtime,
            reverse=True
        )

        df_anterior = None
        if len(arquivos) > 1:
            try:
                df_anterior = pd.read_excel(arquivos[1], sheet_name="Dados_Completos")
            except Exception as e:
                print(f"⚠ Erro ao carregar relatório anterior: {e}")

        # ============================
        # 📊 Resumo atual
        # ============================
        df_bases = df_processado[df_processado[COL_NOME_BASE].isin(BASES_PERMITIDAS)]
        if df_bases.empty:
            print("⚠ Nenhuma base permitida encontrada no relatório.")
            return

        df_resumo = (
            df_bases.groupby(COL_NOME_BASE)
            .agg(pedidos=(COL_REMESSA, "nunique"))
        )
        total_pedidos = df_resumo["pedidos"].sum()

        # ============================
        # 📊 Resumo anterior
        # ============================
        if df_anterior is not None and COL_NOME_BASE in df_anterior.columns:
            df_anterior_resumo = (
                df_anterior[df_anterior[COL_NOME_BASE].isin(BASES_PERMITIDAS)]
                .groupby(COL_NOME_BASE)
                .agg(pedidos_ant=(COL_REMESSA, "nunique"))
            )
            total_ant = df_anterior_resumo["pedidos_ant"].sum()
            variacao_total = total_ant - total_pedidos

            df_resumo = df_resumo.merge(df_anterior_resumo, on=COL_NOME_BASE, how="left")
            df_resumo["pedidos_ant"] = df_resumo["pedidos_ant"].fillna(df_resumo["pedidos"])
            df_resumo["reducao"] = df_resumo["pedidos_ant"] - df_resumo["pedidos"]
        else:
            total_ant = total_pedidos
            variacao_total = 0
            df_resumo["pedidos_ant"] = df_resumo["pedidos"]
            df_resumo["reducao"] = 0

        # ============================
        # 🔴 Top 5 piores
        # ============================
        top5_piores = df_resumo.sort_values("pedidos", ascending=False).head(5).reset_index()
        piores_text = "\n".join([f"- {row[COL_NOME_BASE]}: {row['pedidos']}" for _, row in top5_piores.iterrows()])

        # 🟢 Top 5 melhores reduções
        top5_melhores = df_resumo.sort_values("reducao", ascending=False).head(5).reset_index()
        melhores_text = "\n".join([
            f"- {row[COL_NOME_BASE]}: reduziu {int(row['reducao'])} (de {int(row['pedidos_ant'])} → {int(row['pedidos'])})"
            for _, row in top5_melhores.iterrows()
            if row["reducao"] > 0
        ])

        # ============================
        # 📝 Montar card
        # ============================
        seta = "🔼 Aumentou" if variacao_total < 0 else "🔽 Diminuiu"
        payload = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": "📊 Relatório Sem Movimentação (5+ dias) - Franquias"},
                    "template": "red"
                },
                "elements": [
                    {
                        "tag": "div",
                        "fields": [
                            {"is_short": True, "text": {"tag": "lark_md", "content": f"📅 **Data:**\n{data_coleta_str}"}},
                            {"is_short": True, "text": {"tag": "lark_md", "content": f"📦 **Total Pacotes:**\n{total_pedidos}"}}
                        ]
                    },
                    {"tag": "div", "text": {"tag": "lark_md", "content": f"🔄 **Variação:** {seta} {abs(variacao_total)} pacotes"}},
                    {"tag": "hr"},
                    {"tag": "div", "text": {"tag": "lark_md", "content": "🔴 **5 Piores Franquias (Mais Pacotes)**"}},
                    {"tag": "div", "text": {"tag": "lark_md", "content": piores_text}},
                    {"tag": "hr"},
                    {"tag": "div", "text": {"tag": "lark_md", "content": "🟢 **5 Melhores Reduções**"}},
                    {"tag": "div", "text": {"tag": "lark_md", "content": melhores_text if melhores_text else "Nenhuma redução registrada."}},
                    {"tag": "hr"},
                    {
                        "tag": "action",
                        "actions": [
                            {
                                "tag": "button",
                                "text": {"tag": "plain_text", "content": "📎 Abrir Relatório"},
                                "url": LINK_RELATORIO,
                                "type": "default"
                            }
                        ]
                    }
                ]
            }
        }

        try:
            response = requests.post(WEBHOOK_URL, json=payload, timeout=10)
            response.raise_for_status()
            print("✅ Card consolidado enviado com sucesso!")
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao enviar card consolidado: {e}")

    def run_processing_flow(self):
        self._limpar_relatorios_antigos(DIAS_PARA_MANTER_RELATORIOS)
        df_principal = self._carregar_relatorio()
        if df_principal is None:
            return
        df_processado = self._processar_dados(df_principal)

        # --- Histórico na pasta Resultados ---
        nome_arquivo = f"Relatorio_Processado_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.xlsx"
        caminho_saida = self.pasta_resultados / nome_arquivo
        with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
            df_processado.to_excel(writer, sheet_name='Dados_Completos', index=False)

        # --- Versão fixa para compartilhar no OneDrive ---
        with pd.ExcelWriter(CAMINHO_RELATORIO_COMPARTILHADO, engine='openpyxl') as writer:
            df_processado.to_excel(writer, sheet_name='Dados_Completos', index=False)

        # --- Envia card consolidado ---
        if not df_processado.empty:
            self._enviar_card_top10(df_processado)


if __name__ == "__main__":
    try:
        processor = ReportProcessor(
            relatorio_path=CAMINHO_PASTA_RELATORIO
        )
        processor.run_processing_flow()
    except Exception as e:
        print("\n--- ERRO FATAL ---")
        traceback.print_exc()