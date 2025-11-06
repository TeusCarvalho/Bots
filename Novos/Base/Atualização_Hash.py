# -*- coding: utf-8 -*-

import os
import json
import hashlib
import shutil
import time
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional
from colorama import init, Fore, Style

init(autoreset=True)

# ==============================================================================
# CONFIGURAÇÕES GERAIS
# ==============================================================================

COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
    "Odária Fereira": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b",
}

REPORTS_FOLDER_PATH = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Jt - Relatórios"
ARQUIVO_MORTO_FOLDER = os.path.join(REPORTS_FOLDER_PATH, "Arquivo Morto")
HASH_FILE = os.path.join(os.path.dirname(__file__), "ultimo_relatorio.json")

LINK_RELATORIO = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/"
    "matheus_carvalho_jtexpressdf_onmicrosoft_com/Ek3KdqMIdX5EodE-3JwCQnsBAMiJ574BsxAR--oYBNN0-g?e=dfqBzT"
)

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

def format_currency_brl(value: float) -> str:
    try:
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


def calcular_hash_md5(file_path: str) -> str:
    """Calcula o hash MD5 de um arquivo para detectar mudanças."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def carregar_snapshot_antigo() -> Optional[Dict[str, Any]]:
    if os.path.exists(HASH_FILE):
        with open(HASH_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def salvar_snapshot(snapshot: Dict[str, Any]):
    with open(HASH_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=4)


def process_report_file(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path)
    df.columns = df.columns.str.strip()
    df.rename(columns={"运单号": "Remessa", "Coordenador": "Coordenadores"}, inplace=True)
    return df


def gerar_snapshot(df: pd.DataFrame) -> Dict[str, Any]:
    snapshot = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "coordenadores": {}}
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


def comparar_coordenador(snapshot_atual, snapshot_antigo, coord):
    atual = snapshot_atual["coordenadores"].get(coord, {})
    antigo = snapshot_antigo["coordenadores"].get(coord, {}) if snapshot_antigo else {}

    diff_pacotes = atual.get("total_pacotes", 0) - antigo.get("total_pacotes", 0)
    diff_multa = atual.get("total_multa", 0) - antigo.get("total_multa", 0)

    if diff_pacotes > 0:
        var_pacotes = f"📈 Aumentou {diff_pacotes} pedidos"
        cor_pacotes = Fore.RED
    elif diff_pacotes < 0:
        var_pacotes = f"📉 Diminuiu {abs(diff_pacotes)} pedidos"
        cor_pacotes = Fore.GREEN
    else:
        var_pacotes = "➖ Sem alteração"
        cor_pacotes = Fore.YELLOW

    if diff_multa > 0:
        var_multa = f"📈 Aumentou {format_currency_brl(diff_multa)}"
    elif diff_multa < 0:
        var_multa = f"📉 Diminuiu {format_currency_brl(abs(diff_multa))}"
    else:
        var_multa = "➖ Sem alteração"

    bases_txt = "**🔴 3 Piores Bases:**\n"
    bases_atuais = atual.get("bases", {})
    bases_antigas = antigo.get("bases", {}) if antigo else {}
    sorted_bases = sorted(bases_atuais.items(), key=lambda x: x[1], reverse=True)[:3]

    for base, count in sorted_bases:
        diff = count - bases_antigas.get(base, 0)
        if diff < 0:
            bases_txt += f"- 🟢 **{base}**: {count} pedidos (reduziu {abs(diff)})\n"
        elif diff > 0:
            bases_txt += f"- 🔺 **{base}**: {count} pedidos (aumentou {diff})\n"
        else:
            bases_txt += f"- ⚪ **{base}**: {count} pedidos (sem alteração)\n"

    print(f"{cor_pacotes}📊 {coord:<20} | {var_pacotes:<25} | Multa: {var_multa}")
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
                                    f"**Data de Geração:**\n{datetime.now():%d/%m/%Y %H:%M}\n\n"
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
        print(f"{Fore.CYAN}✅ Enviado com sucesso → {url[:55]}...")
    except Exception as e:
        print(f"{Fore.RED}❌ Erro ao enviar: {e}")

# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================

def run_main_task():
    print(f"{Fore.CYAN}[{datetime.now():%Y-%m-%d %H:%M:%S}] Procurando relatórios em {REPORTS_FOLDER_PATH}")

    arquivos = [f for f in os.listdir(REPORTS_FOLDER_PATH)
                if f.endswith(".xlsx") and "5+ dias" in f.lower() and not f.startswith("~")]
    if not arquivos:
        print(f"{Fore.YELLOW}⚠️ Nenhum relatório encontrado.")
        return

    arquivos.sort(key=lambda x: os.path.getmtime(os.path.join(REPORTS_FOLDER_PATH, x)), reverse=True)
    file_name = arquivos[0]
    full_path = os.path.join(REPORTS_FOLDER_PATH, file_name)

    print(f"{Fore.CYAN}📄 Último relatório detectado: {file_name}")

    file_hash = calcular_hash_md5(full_path)
    snapshot_antigo = carregar_snapshot_antigo()

    # Se o hash do arquivo for igual ao último processado, não refaz o envio
    if snapshot_antigo and snapshot_antigo.get("file_hash") == file_hash:
        print(f"{Fore.YELLOW}⚠️ Nenhuma mudança detectada no relatório. Nada será reenviado.")
        return

    df = process_report_file(full_path)
    snapshot_atual = gerar_snapshot(df)
    snapshot_atual["file_hash"] = file_hash

    for coord, url in COORDENADOR_WEBHOOKS.items():
        if coord not in snapshot_atual["coordenadores"]:
            continue
        dados = comparar_coordenador(snapshot_atual, snapshot_antigo, coord)
        payload = create_feishu_payload(coord, dados)
        send_to_feishu(url, payload)
        time.sleep(1)

    salvar_snapshot(snapshot_atual)

    # Move o relatório para Arquivo Morto
    if not os.path.exists(ARQUIVO_MORTO_FOLDER):
        os.makedirs(ARQUIVO_MORTO_FOLDER)
    destino = os.path.join(ARQUIVO_MORTO_FOLDER, file_name)
    try:
        shutil.move(full_path, destino)
        print(f"{Fore.GREEN}📦 Relatório movido para Arquivo Morto: {destino}")
    except Exception as e:
        print(f"{Fore.RED}⚠️ Falha ao mover relatório: {e}")

    print(f"{Fore.CYAN}✅ Processo concluído!")


if __name__ == "__main__":
    run_main_task()
