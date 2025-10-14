# -*- coding: utf-8 -*-
# Envio Unificado (Custo + T0) para Feishu, com logs separados

import pandas as pd
import requests
import json
from pathlib import Path
from datetime import datetime, timedelta

# --- Arquivos processados ---
RELATORIO_CUSTOS = Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Relatorios\Custos\Relatorio_Custos.xlsx")
RELATORIO_T0 = Path(r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Relatorios\T-0\Relatorio_Processado.xlsx")

# --- Links fixos OneDrive ---
LINK_CUSTOS = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/ErvERW1fRVxAheWDyZ9QOx4BWApWIpVaVNfusQYcMZev3w?e=bBbfjI"
LINK_T0 = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EgPls3u4i9FIlul4oyiV4BoBi8RUXLNub8W9oyKwL1sI2w?e=OO3v34"

# --- Webhooks separados ---
COORDENADOR_WEBHOOKS_CUSTOS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/1f3f48d7-b60c-45c1-87ee-6cc8ab9f6467",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/b448a316-f146-49d0-9f0a-90b1f086b8a7",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/fa768680-b4ab-4d87-bf2c-285c91034dad",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/e14d0307-c6d6-472b-bea1-d83a5573dc1b",
    "Odária Fereira": "https://open.feishu.cn/open-apis/bot/v2/hook/4cfd01be-defa-4adb-936e-6bfbee5326a6",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/e3e31e14-79ab-4a95-8a2d-be99e1fc9b10",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/9ce83b77-04ad-4558-ab83-39929b30f092",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/d624dcc1-73c7-4d36-8f63-5c43d0e5259b",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/eb777d25-f454-4db7-9364-edf95ee37063",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/99557a7f-ca4e-4ede-b9e5-ccd7ad85b96a"
}

COORDENADOR_WEBHOOKS_T0 = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/3663dd30-722c-45d6-9e3c-1d4e2838f112",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/0b907801-c73e-4de8-9f84-682d7b54f6fd",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/261cefd4-5528-4760-b18e-49a0249718c7",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/b749fd36-d287-460e-b1e2-c78bfb4c1946",
    "Odária Fereira": "https://open.feishu.cn/open-apis/bot/v2/hook/48c4db73-b5a4-4007-96af-f5d28301f0c1",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/606ed22b-dc49-451d-9bfe-0a8829dbe76e",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/840f79b0-1eff-42fe-aae0-433c9edbad80",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/95c8e4d2-27aa-4811-b6bf-ebf99cdfd42d",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/63751a67-efe8-40e4-b841-b290a4819836",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/3ddc5962-2d32-4b2d-92d9-a4bc95ac3393"
}

# --- Funções utilitárias ---
def format_currency(value):
    try:
        formatted_value = f"{value:,.2f}"
        return formatted_value.replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "0,00"

def create_card(title: str, body: str, link: str, color: str = "blue") -> dict:
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": color
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": body}},
                {"tag": "hr"},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "📎 Abrir Relatório Completo"},
                            "url": link,
                            "type": "default"
                        }
                    ]
                }
            ]
        }
    }

def enviar_card(coordenador: str, payload: dict, webhooks: dict, categoria: str):
    webhook = webhooks.get(coordenador)
    if not webhook:
        print(f"⚠ Nenhum Webhook configurado para {coordenador} ({categoria})")
        return
    try:
        resp = requests.post(webhook, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
        if resp.status_code == 200:
            print(f"✅ [{categoria}] Card enviado para {coordenador}")
        else:
            print(f"⚠ [{categoria}] Erro {resp.status_code} para {coordenador}: {resp.text}")
    except Exception as e:
        print(f"❌ [{categoria}] Falha ao enviar para {coordenador}: {e}")

# ======================================================
# 📊 ENVIO DE CUSTO E ARBITRAGEM
# ======================================================
print("\n==============================")
print("📡 Iniciando envio: CUSTO E ARBITRAGEM")
print("==============================")

if RELATORIO_CUSTOS.exists():
    try:
        df_custos = pd.read_excel(RELATORIO_CUSTOS)
        data_relatorio = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")  # dia anterior (fechamento)

        for coord in df_custos['Coordenadores'].dropna().unique():
            df_coord = df_custos[df_custos['Coordenadores'] == coord]

            qtd_pedidos = df_coord.shape[0]
            valor_total = df_coord['Valor a pagar (yuan)'].sum()

            body = f"📊 **Relatório de Custo e Arbitragem**\n"
            body += f"📅 **Data do relatório:** {data_relatorio}\n"
            body += f"👤 **Coordenador:** {coord}\n"
            body += f"📦 **Qtd Pedidos:** {qtd_pedidos}\n"
            body += f"💰 **Valor Total (R$):** {format_currency(valor_total)}\n\n"

            problemas = df_coord.groupby('Tipo de anomalia primária').agg(
                Quantidade=('Tipo de anomalia primária', 'count'),
                Valor=('Valor a pagar (yuan)', 'sum')
            ).reset_index()

            if not problemas.empty:
                body += "⚠ **Problemáticas:**\n"
                for _, row in problemas.iterrows():
                    body += f"- {row['Tipo de anomalia primária']}: {row['Quantidade']} pedidos – R$ {format_currency(row['Valor'])}\n"
            else:
                body += "✅ Sem problemáticas registradas.\n"

            payload = create_card(f"📊 Custos - {coord}", body, LINK_CUSTOS, color="turquoise")
            enviar_card(coord, payload, COORDENADOR_WEBHOOKS_CUSTOS, "CUSTOS")

    except Exception as e:
        print(f"❌ Erro ao processar envio de CUSTOS: {e}")
else:
    print("⚠ Relatório de Custos não encontrado.")

# ======================================================
# 📊 ENVIO DE T-0
# ======================================================
print("\n==============================")
print("📡 Iniciando envio: T-0")
print("==============================")

if RELATORIO_T0.exists():
    try:
        df_t0_resumo = pd.read_excel(RELATORIO_T0, sheet_name="ResumoNumerico", index_col=0)
        df_t0_dados = pd.read_excel(RELATORIO_T0, sheet_name="Dados_Completos")

        # --- Extrai a data real da coluna ---
        if "Horário bipagem de recebimento" in df_t0_dados.columns:
            df_t0_dados["Horário bipagem de recebimento"] = pd.to_datetime(
                df_t0_dados["Horário bipagem de recebimento"], errors="coerce"
            )
            ultima_data = df_t0_dados["Horário bipagem de recebimento"].dropna().max()
            data_relatorio_t0 = ultima_data.strftime("%d/%m/%Y") if pd.notna(ultima_data) else (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")
        else:
            data_relatorio_t0 = (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y")

        # --- Envio por coordenador ---
        for coord in df_t0_resumo.index:
            if coord not in COORDENADOR_WEBHOOKS_T0:
                print(f"⚠ Coordenador {coord} não tem webhook configurado, pulando.")
                continue

            row = df_t0_resumo.loc[coord]

            body = f"📊 **Relatório T-0**\n"
            body += f"📅 **Data do relatório:** {data_relatorio_t0}\n"
            body += f"👤 **Coordenador:** {coord}\n"
            body += f"📦 **Total Pedidos:** {int(row['TOTAL GERAL'])}\n"
            body += f"✅ **SLA Geral:** {row['SLA (%)']:.2f}%\n\n"

            df_coord = df_t0_dados[df_t0_dados['Coordenadores'] == coord]
            df_base = (
                df_coord.groupby('Nome da base')
                .agg(
                    pedidos=('Remessa', 'nunique'),
                    entregues=('Status de Entrega', lambda x: (x == "ENTREGUE").sum())
                )
            )

            if not df_base.empty:
                df_base["SLA (%)"] = (df_base["entregues"] / df_base["pedidos"]) * 100
                top_bases = df_base.sort_values("SLA (%)", ascending=True).head(4)

                if not top_bases.empty:
                    body += "🔻 **Top 4 Piores Bases:**\n"
                    for base, dados in top_bases.iterrows():
                        body += f"- {base}: {dados['SLA (%)']:.2f}% ({dados['pedidos']} pedidos)\n"
            else:
                body += "✅ Nenhuma base problemática encontrada.\n"

            payload = create_card(f"📊 T-0 - {coord}", body, LINK_T0, color="red")
            enviar_card(coord, payload, COORDENADOR_WEBHOOKS_T0, "T-0")

    except Exception as e:
        print(f"❌ Erro ao processar envio de T-0: {e}")
else:
    print("⚠ Relatório T-0 não encontrado.")