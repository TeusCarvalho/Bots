# -*- coding: utf-8 -*-

import requests
import json
from datetime import datetime
import time
import os
import pandas as pd
import shutil
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
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/db18d309-8f26-41b5-b911-1a9f27449c83",
}

# Pastas
REPORTS_FOLDER_PATH = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Jt - Relatórios"
ARQUIVO_MORTO_FOLDER = os.path.join(REPORTS_FOLDER_PATH, "Arquivo Morto")

# Caminho do hash local (na mesma pasta do script)
HASH_FILE = os.path.join(os.path.dirname(__file__), "ultimo_relatorio.json")

LINK_RELATORIO = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/"
    "matheus_carvalho_jtexpressdf_onmicrosoft_com/Ek3KdqMIdX5EodE-3JwCQnsBAMiJ574BsxAR--oYBNN0-g?e=dfqBzT"
)

# ==============================================================================
# FUNÇÕES DE APOIO
# ==============================================================================

def format_currency_brl(value: float) -> str:
    try:
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


def process_report_file(file_path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_excel(file_path)
        df.columns = df.columns.str.strip()
        df.rename(columns={"运单号": "Remessa", "Coordenador": "Coordenadores"}, inplace=True)
        return df
    except Exception as e:
        print(f"❌ Erro ao ler {file_path}: {e}")
        return None


def gerar_snapshot(df: pd.DataFrame) -> Dict[str, Any]:
    """Cria o dicionário base com informações resumidas por coordenador e base."""
    snapshot = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "coordenadores": {}}

    if "Coordenadores" not in df.columns:
        return snapshot

    for coord in df["Coordenadores"].unique():
        dfc = df[df["Coordenadores"] == coord]
        total_pacotes = dfc["Remessa"].nunique()
        total_multa = dfc["Multa (R$)"].sum() if "Multa (R$)" in dfc.columns else 0
        bases = dfc.groupby("Unidade responsável")["Remessa"].nunique().to_dict()

        snapshot["coordenadores"][coord] = {
            "total_pacotes": total_pacotes,
            "total_multa": float(total_multa),
            "bases": bases,
        }
    return snapshot


def carregar_snapshot_antigo() -> Optional[Dict[str, Any]]:
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def salvar_snapshot(snapshot: Dict[str, Any]):
    with open(HASH_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=4)


def comparar_coordenador(snapshot_atual: Dict[str, Any], snapshot_antigo: Optional[Dict[str, Any]], coord: str):
    atual = snapshot_atual["coordenadores"].get(coord, {})
    antigo = snapshot_antigo["coordenadores"].get(coord, {}) if snapshot_antigo else {}

    diff_pacotes = atual.get("total_pacotes", 0) - antigo.get("total_pacotes", 0)
    diff_multa = atual.get("total_multa", 0) - antigo.get("total_multa", 0)

    if diff_pacotes > 0:
        var_pacotes = f"📈 Aumentou {diff_pacotes} pedidos"
    elif diff_pacotes < 0:
        var_pacotes = f"📉 Diminuiu {abs(diff_pacotes)} pedidos"
    else:
        var_pacotes = "➖ Sem alteração"

    if diff_multa > 0:
        var_multa = f"📈 Aumentou {format_currency_brl(diff_multa)}"
    elif diff_multa < 0:
        var_multa = f"📉 Diminuiu {format_currency_brl(abs(diff_multa))}"
    else:
        var_multa = "➖ Sem alteração"

    # --- Bases ---
    bases_txt = ""
    bases_atuais = atual.get("bases", {})
    bases_antigas = antigo.get("bases", {}) if antigo else {}

    # Ordena pelas piores bases
    sorted_bases = sorted(bases_atuais.items(), key=lambda x: x[1], reverse=True)[:3]
    bases_txt += "**🔴 3 Piores Bases:**\n"
    for base, count in sorted_bases:
        diff = count - bases_antigas.get(base, 0)
        if diff < 0:
            bases_txt += f"- 🟢 **{base}**: {count} pedidos (reduziu {abs(diff)})\n"
        elif diff > 0:
            bases_txt += f"- 🔺 **{base}**: {count} pedidos (aumentou {diff})\n"
        else:
            bases_txt += f"- ⚪ **{base}**: {count} pedidos (sem alteração)\n"

    return {
        "total_pacotes": atual.get("total_pacotes", 0),
        "total_multa": atual.get("total_multa", 0),
        "var_pacotes": var_pacotes,
        "var_multa": var_multa,
        "bases_text": bases_txt,
    }

# ==============================================================================
# ENVIO AO FEISHU
# ==============================================================================

def create_feishu_payload(coordenador: str, data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": f"🚨 Sem Movimentação - {coordenador}"},
                "template": "red",
            },
            "elements": [
                {
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": (
                                    f"**Data de Geração:**\n{datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
                                    f"**Qtd de Pacotes:**\n{data['total_pacotes']}\n"
                                    f"**Variação Pacotes:**\n{data['var_pacotes']}"
                                ),
                            },
                        },
                        {
                            "is_short": True,
                            "text": {
                                "tag": "lark_md",
                                "content": (
                                    f"**Multa Atual:**\n{format_currency_brl(data['total_multa'])}\n"
                                    f"**Variação Multa:**\n{data['var_multa']}"
                                ),
                            },
                        },
                    ],
                },
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": data["bases_text"]}},
                {"tag": "hr"},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "📎 Abrir Relatório Completo"},
                            "url": LINK_RELATORIO,
                            "type": "primary",
                        }
                    ],
                },
            ],
        },
    }


def send_to_feishu(url: str, data: Dict[str, Any]):
    try:
        r = requests.post(url, json=data, timeout=10)
        r.raise_for_status()
        print(f"✅ Enviado para {url[:60]}...")
    except Exception as e:
        print(f"❌ Erro ao enviar: {e}")


# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================

def run_main_task():
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Procurando relatórios em {REPORTS_FOLDER_PATH}")

    arquivos = [
        f for f in os.listdir(REPORTS_FOLDER_PATH)
        if f.endswith(".xlsx") and "5+ dias" in f.lower() and not f.startswith("~")
    ]
    if not arquivos:
        print("⚠️ Nenhum relatório encontrado.")
        return

    arquivos.sort(key=lambda x: os.path.getmtime(os.path.join(REPORTS_FOLDER_PATH, x)), reverse=True)
    file_name = arquivos[0]
    full_path = os.path.join(REPORTS_FOLDER_PATH, file_name)

    df = process_report_file(full_path)
    if df is None:
        return

    snapshot_atual = gerar_snapshot(df)
    snapshot_antigo = carregar_snapshot_antigo()

    for coord, url in COORDENADOR_WEBHOOKS.items():
        if coord not in snapshot_atual["coordenadores"]:
            continue

        dados = comparar_coordenador(snapshot_atual, snapshot_antigo, coord)
        payload = create_feishu_payload(coord, dados)
        send_to_feishu(url, payload)
        time.sleep(1)

    # 🔁 Atualiza o hash
    salvar_snapshot(snapshot_atual)

    # 📦 Move relatório atual para Arquivo Morto
    if not os.path.exists(ARQUIVO_MORTO_FOLDER):
        os.makedirs(ARQUIVO_MORTO_FOLDER)

    destino = os.path.join(ARQUIVO_MORTO_FOLDER, file_name)
    try:
        shutil.move(full_path, destino)
        print(f"📦 Relatório movido para Arquivo Morto: {destino}")
    except Exception as e:
        print(f"⚠️ Falha ao mover relatório: {e}")


if __name__ == "__main__":
    run_main_task()
