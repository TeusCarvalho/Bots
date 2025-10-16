# -*- coding: utf-8 -*-

import requests
import json
from datetime import datetime
import time
import os
import pandas as pd
from typing import Dict, Any, Optional

# ==============================================================================
# CONFIGURAÇÕES GERAIS
# ==============================================================================

COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/1d9bbacf-79ed-4eb3-8046-26d7480893c3",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/5c2bb460-1971-4770-9b37-98b6e4ba3cd9",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/ac4a5800-44b5-45d5-b0d2-f4d88a677967",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/20a61c63-6db7-4e83-9e44-ae6b545495cc",
    "Odária Fereira": "https://open.feishu.cn/open-apis/bot/v2/hook/914ce9f9-35ab-4869-860f-d2bef7d933fb",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/16414836-5020-49bd-b3d3-ded4f34878ab",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/62cd648c-ecd5-406a-903d-b596944c1919",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/62518b67-f897-4341-98e6-2db87f4fdee2",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/e502bc10-3cb3-4b46-872e-eb73ef1c5ee0",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/db18d309-8f26-41b5-b911-1a9f27449c83"
}

REPORTS_FOLDER_PATH = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Jt - Relatórios"
ARQUIVO_MORTO_FOLDER = os.path.join(REPORTS_FOLDER_PATH, "Arquivo Morto")

LINK_RELATORIO = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/Ek3KdqMIdX5EodE-3JwCQnsBAMiJ574BsxAR--oYBNN0-g?e=dfqBzT"


# ==============================================================================
# FUNÇÕES DE APOIO
# ==============================================================================

def format_currency_brl(value: float) -> str:
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def carregar_ultimo_arquivo_morto() -> Optional[pd.DataFrame]:
    """Procura no Arquivo Morto o último arquivo que contenha '5+ dias' no nome"""
    if not os.path.exists(ARQUIVO_MORTO_FOLDER):
        print("⚠️ Pasta Arquivo Morto não encontrada.")
        return None

    arquivos = [
        f for f in os.listdir(ARQUIVO_MORTO_FOLDER)
        if f.lower().endswith((".xlsx", ".xls")) and "5+ dias" in f
    ]
    if not arquivos:
        print("⚠️ Nenhum arquivo compatível em Arquivo Morto contendo '5+ dias'.")
        return None

    arquivos.sort(key=lambda x: os.path.getmtime(os.path.join(ARQUIVO_MORTO_FOLDER, x)), reverse=True)
    ultimo_arquivo = os.path.join(ARQUIVO_MORTO_FOLDER, arquivos[0])

    print(f"📂 Usando {ultimo_arquivo} como relatório anterior.")
    try:
        return pd.read_excel(ultimo_arquivo)
    except Exception as e:
        print(f"⚠️ Erro ao ler {ultimo_arquivo}: {e}")
        return None


# ==============================================================================
# PREPARAÇÃO DO RELATÓRIO
# ==============================================================================

def prepare_report_data(df_current: pd.DataFrame, df_old: Optional[pd.DataFrame], report_title: str) -> Dict[str, Any]:
    total_current = df_current["Remessa"].nunique() if "Remessa" in df_current.columns else len(df_current)
    current_fines = df_current["Multa (R$)"].sum() if "Multa (R$)" in df_current.columns else 0

    var_text, var_f_text = "N/A", "N/A"

    if df_old is not None and not df_old.empty:
        total_old = df_old["Remessa"].nunique() if "Remessa" in df_old.columns else len(df_old)
        difference = total_current - total_old
        if difference < 0:
            var_text = f"📉 Diminuiu {abs(difference)} pedidos"
        elif difference > 0:
            var_text = f"📈 Aumentou {difference} pedidos"
        else:
            var_text = "➖ Sem alteração"

        if "Multa (R$)" in df_old.columns:
            old_fines = df_old["Multa (R$)"].sum()
            fines_diff = current_fines - old_fines
            if fines_diff < 0:
                var_f_text = f"📉 Diminuiu {format_currency_brl(abs(fines_diff))}"
            elif fines_diff > 0:
                var_f_text = f"📈 Aumentou {format_currency_brl(fines_diff)}"
            else:
                var_f_text = "➖ Sem alteração"

    # ======================================================================
    # TOP BASES (piores atuais e melhores reduções)
    # ======================================================================
    metrics_text = ""

    if "Unidade responsável" in df_current.columns and "Remessa" in df_current.columns:
        # Contagem atual por base
        base_counts = (
            df_current.groupby("Unidade responsável")["Remessa"]
            .nunique()
            .sort_values(ascending=False)
        )

        # 🔴 3 piores (maiores quantidades atuais)
        worst_bases = base_counts.head(3)
        metrics_text += "**🔴 3 Piores Bases (maior nº de pedidos):**\n"
        for unit, count in worst_bases.items():
            metrics_text += f"- 🔴 **{unit}**: {count} pedidos\n"

        metrics_text += "\n"

        # 🟢 3 melhores (maiores reduções em relação ao relatório anterior)
        if df_old is not None and "Unidade responsável" in df_old.columns:
            old_counts = df_old.groupby("Unidade responsável")["Remessa"].nunique()

            # Diferença: pedidos antigos - atuais (positivo = redução)
            diffs = (old_counts - base_counts).dropna().sort_values(ascending=False)

            best_reductions = diffs.head(3)
            metrics_text += "**🟢 3 Melhores Reduções:**\n"
            for unit, reduction in best_reductions.items():
                atual = base_counts.get(unit, 0)
                anterior = old_counts.get(unit, 0)
                metrics_text += f"- 🟢 **{unit}**: reduziu {int(reduction)} (de {anterior} → {atual})\n"

    return {
        "title": f"{report_title}",
        "metrics_text": metrics_text,
        "observation": "Resumo automático.",
        "total_pacotes": total_current,
        "variacao_pacotes": var_text,
        "multa_atual": format_currency_brl(current_fines),
        "variacao_multa": var_f_text,
    }


# ==============================================================================
# FORMATAÇÃO DO CARD PARA FEISHU
# ==============================================================================

def create_feishu_card_payload(report_data: Dict[str, Any]) -> Dict[str, Any]:
    elements = [
        {"tag": "div", "fields": [
            {"is_short": True, "text": {"tag": "lark_md",
                "content": (
                    f"**Data de Geração:**\n{report_data.get('date', 'N/A')}\n\n"
                    f"**Qtd de Pacotes:**\n{report_data.get('total_pacotes', 'N/A')}\n"
                    f"**Variação Pacotes:**\n{report_data.get('variacao_pacotes', 'N/A')}"
                )}},
            {"is_short": True, "text": {"tag": "lark_md",
                "content": (
                    f"**Multa Atual:**\n{report_data.get('multa_atual', 'N/A')}\n"
                    f"**Variação Multa:**\n{report_data.get('variacao_multa', 'N/A')}"
                )}}
        ]},
        {"tag": "hr"},
        {"tag": "div", "text": {"tag": "lark_md", "content": report_data.get("metrics_text", "")}},
        {"tag": "hr"},
        {"tag": "action", "actions": [
            {"tag": "button",
             "text": {"tag": "plain_text", "content": "📎 Abrir Relatório Completo"},
             "url": LINK_RELATORIO,
             "type": "primary"}
        ]},
        {"tag": "note", "elements": [{"tag": "plain_text", "content": report_data.get("observation", "")}]}
    ]

    return {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"🚨 Sem Movimentação - {report_data.get('title', '')}"},
                "template": "red"   # 🔴 Header vermelho
            },
            "elements": elements
        }
    }


# ==============================================================================
# FUNÇÕES DE ENVIO E PROCESSAMENTO
# ==============================================================================

def send_report_to_feishu(webhook_url: str, report_data: Dict[str, Any]):
    headers = {"Content-Type": "application/json"}
    payload = create_feishu_card_payload(report_data)
    try:
        r = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
        r.raise_for_status()
        print(f"✅ Enviado para {webhook_url[:50]}...")
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")


def process_report_file(file_path: str) -> Optional[pd.DataFrame]:
    try:
        return pd.read_excel(file_path)
    except Exception as e:
        print(f"Erro ao ler {file_path}: {e}")
        return None


def dispatch_reports_by_coordinator(df: pd.DataFrame, report_title: str):
    if "Coordenadores" not in df.columns:
        print("⚠️ Coluna 'Coordenadores' não encontrada.")
        return

    df_old = carregar_ultimo_arquivo_morto()

    for coordenador, webhook_url in COORDENADOR_WEBHOOKS.items():
        df_coord = df[df["Coordenadores"] == coordenador]
        if df_coord.empty:
            continue

        df_coord_old = None
        if df_old is not None and "Coordenadores" in df_old.columns:
            df_coord_old = df_old[df_old["Coordenadores"] == coordenador]

        report_data = prepare_report_data(df_coord, df_coord_old, f"{coordenador}")
        report_data["date"] = datetime.now().strftime("%d/%m/%Y %H:%M")

        send_report_to_feishu(webhook_url, report_data)
        time.sleep(1)


# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================

def run_main_task():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Procurando relatórios em {REPORTS_FOLDER_PATH}")

    arquivos = [
        f for f in os.listdir(REPORTS_FOLDER_PATH)
        if f.endswith(".xlsx") and "5+ dias" in f and not f.startswith("~")
    ]
    if not arquivos:
        print("⚠️ Nenhum relatório encontrado.")
        return

    arquivos.sort(key=lambda x: os.path.getmtime(os.path.join(REPORTS_FOLDER_PATH, x)), reverse=True)
    file_name = arquivos[0]

    full_path = os.path.join(REPORTS_FOLDER_PATH, file_name)
    df_current = process_report_file(full_path)
    if df_current is not None:
        dispatch_reports_by_coordinator(df_current, os.path.splitext(file_name)[0])


if __name__ == "__main__":
    run_main_task()