# -*- coding: utf-8 -*-
# Custo e Arbitragem - versão automática

import pandas as pd
import os
import requests
import json
from datetime import datetime

def format_currency(value):
    """Formata número em formato BRL"""
    formatted_value = f"{value:,.2f}"
    return formatted_value.replace(",", "X").replace(".", ",").replace("X", ".")

# --- CONFIGURAÇÕES ---
COORDENADOR_WEBHOOKS = {
    "Franquias": "https://open.feishu.cn/open-apis/bot/v2/hook/328a86ed-6c6f-4b61-acc4-aa33bd1b8254"
}

# --- LINK PARA CARD ---
LINK_RELATORIO = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EtbZs3AZ0_BHtx7KGJOAVGcBvxaAJM-8vINYH7PJG43W-w?e=Su1J2P"

# --- PASTAS ---
BASE_DIR = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Local de Teste\Custo'
OUTPUT_FILE = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Franquias\Custo\Minha_responsabilidade_atualizada.xlsx"

# --- BASES PERMITIDAS ---
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

# --- COLUNAS ESPERADAS ---
COLUMN_NAMES = [
    'Número de declaração', 'Remessa', 'Tipo de produto', 'Tipo de anomalia primária',
    'Tipo de anomalia secundária', 'Dias de atraso', 'Status de arbitragem', 'Base remetente',
    'Regional Remetente', 'Declarante', 'Declarante No.', 'Data de declaração',
    'Origem da Solicitação', 'Regional de declaração', 'Data de recebimento da arbitragem',
    'Data de distribuição da arbitragem', 'Data de decisão de arbitragem', 'Data de contestação',
    'Data da última edição', 'Data de distribuição da contestação', 'Data de decisão da contestação',
    'Data de processamento de retorno', 'Valor do item', 'Processador de arbitragem',
    'Processador de contestação', 'Tipo de produto', 'Conteúdo do pacote',
    'Descrição de anomalia', 'Data de fechamento', 'Tipo de decisão', 'Base responsável',
    'Regional responsável', 'Valor a pagar (yuan)', 'Taxa de manuseio (yuan)',
    'Valor da arbitragem (yuan)', 'Base de liquidação financeira',
    'Comentários de decisão de arbitragem', 'Comentários de decisão de contestação',
    'Processador de retorno', 'Comentário de processamento de retorno', 'Tempo de processamento de retorno',
    'Resposta da parte responsável', 'Fonte', 'Origem do Pedido', 'Hora de envio',
    'Horário de coleta', 'Horário de Previsão de Entrega SLA Cadeia',
    'Responsável pela entrega', 'Horário da entrega', 'Peso cobrável',
    'Tempo restante de processamento', 'Número do cliente', 'Nome do cliente',
    'Etapa de decisão de responsabilidade'
]

def create_feishu_card_payload(title: str, body: str) -> dict:
    """Monta o card interativo do Feishu."""
    return {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "blue"
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": body}},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "📎 Abrir Relatório Completo"},
                            "url": LINK_RELATORIO,
                            "type": "default"
                        }
                    ]
                },
                {"tag": "note", "elements": [{"tag": "plain_text", "content": "Resumo automático."}]}
            ]
        }
    }

# --- ENCONTRAR O ARQUIVO MAIS RECENTE ---
def get_latest_file(folder: str):
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(('.xls', '.xlsx'))]
    if not files:
        return None
    return max(files, key=os.path.getmtime)

# --- PROCESSAMENTO ---
latest_file = get_latest_file(BASE_DIR)

if not latest_file:
    print("⚠️ Nenhum arquivo encontrado na pasta de entrada.")
else:
    try:
        print(f"📂 Lendo arquivo: {os.path.basename(latest_file)}")
        df = pd.read_excel(latest_file, header=None, names=COLUMN_NAMES)

        # 🔹 Remover remessas com traço
        if 'Remessa' in df.columns:
            df = df[~df['Remessa'].astype(str).str.contains('-')]

        # 🔹 Normalizar base
        if 'Base responsável' in df.columns:
            df['Base responsável'] = df['Base responsável'].astype(str).str.strip()
            df['Base responsável'] = df['Base responsável'].replace({"VHL -RO": "F VHL-RO"})

        # 🔹 Filtrar regionais GP e bases permitidas
        df = df[df['Regional responsável'] == 'GP']
        df = df[df['Base responsável'].isin(BASES_PERMITIDAS)]

        # --- AGRUPAR E CALCULAR ---
        resumo_bases = df.groupby('Base responsável').agg(
            Qtd_Pedidos=('Remessa', 'count'),
            Valor_Total=('Valor a pagar (yuan)', 'sum')
        ).reset_index().sort_values(by="Valor_Total", ascending=False)

        valor_total_geral = resumo_bases['Valor_Total'].sum()
        top5 = resumo_bases.head(5)

        # --- MONTAR MENSAGEM ---
        data_geracao = datetime.now().strftime("%d/%m/%Y %H:%M")
        mensagem = f"📊 **Relatório de Resarcimento - TOP 5 Piores Bases**\n📅 Data de geração: {data_geracao}\n\n"

        for _, row in top5.iterrows():
            mensagem += f"🔴 {row['Base responsável']} - {row['Qtd_Pedidos']} pedidos - R$ {format_currency(row['Valor_Total'])}\n"

        mensagem += f"\n💰 **Valor Total Geral:** R$ {format_currency(valor_total_geral)}"

        # --- ENVIAR CARD ---
        payload = create_feishu_card_payload("📊 Relatório de Resarcimento - Franquias", mensagem)
        webhook_url = COORDENADOR_WEBHOOKS.get("Franquias")

        if webhook_url:
            resp = requests.post(webhook_url, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
            if resp.status_code == 200:
                print(f"✅ Card enviado com sucesso para Franquias")
            else:
                print(f"⚠️ Erro {resp.status_code}: {resp.text}")

        # --- SALVAR ---
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        df.to_excel(OUTPUT_FILE, index=False)
        print(f"📎 Arquivo salvo em: {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ Erro ao processar: {e}")