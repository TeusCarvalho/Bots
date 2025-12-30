# -*- coding: utf-8 -*-
"""
Script de cálculo de fretes - Melhor Envio (Sandbox + Produção)
---------------------------------------------------------------
✅ Alterna automaticamente entre ambientes de teste e produção.
✅ Totalmente compatível com a documentação oficial:
   https://docs.melhorenvio.com.br/reference/calculo-de-fretes-por-produtos
✅ Testa o primeiro envio antes de processar o restante.
✅ Interrompe se houver falha de conexão ou autenticação.

Autor: bb-assistente 😎
"""

import pandas as pd
import requests
import json
import time
import sys
from datetime import datetime
from requests.exceptions import RequestException

# ============================================================
# ⚙️ CONFIGURAÇÕES GERAIS
# ============================================================

# Caminhos dos arquivos
ARQUIVO_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Melhor Envio\modelo_upload_envios.xls"
ARQUIVO_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Melhor Envio\Fretes_Calculados.xlsx"
ARQUIVO_LOG = "melhor_envio.log"

# Ambiente de execução
# 👉 Mude para "sandbox" ou "producao"
AMBIENTE = "sandbox"

# Tokens de acesso (copie o seu do painel do Melhor Envio)
TOKEN_SANDBOX = "SEU_TOKEN_SANDBOX_AQUI"
TOKEN_PRODUCAO = "SEU_TOKEN_PRODUCAO_AQUI"

# CEP de origem padrão
CEP_ORIGEM = "70000000"

# ============================================================
# 🌐 CONFIGURAÇÃO DO AMBIENTE
# ============================================================

if AMBIENTE.lower() == "sandbox":
    URL_BASE = "https://sandbox.melhorenvio.com.br"
    TOKEN_API = TOKEN_SANDBOX
else:
    URL_BASE = "https://api.melhorenvio.com.br"
    TOKEN_API = TOKEN_PRODUCAO

URL_CALCULO = f"{URL_BASE}/api/v2/me/shipment/calculate"

HEADERS = {
    "Authorization": f"Bearer {TOKEN_API}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

TIMEOUT = 15

# ============================================================
# 🧠 FUNÇÕES AUXILIARES
# ============================================================

def log(msg):
    """Imprime e salva no arquivo de log"""
    print(msg)
    with open(ARQUIVO_LOG, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%H:%M:%S} - {msg}\n")

def try_post(payload, verify=True):
    """Envia requisição POST para o Melhor Envio"""
    try:
        resp = requests.post(URL_CALCULO, headers=HEADERS,
                             data=json.dumps(payload), timeout=TIMEOUT, verify=verify)
        try:
            return resp.status_code, resp.json(), resp.text
        except Exception:
            return resp.status_code, None, resp.text
    except RequestException as e:
        return None, None, str(e)

def testar_primeira_requisicao(payload_teste):
    """Testa o primeiro envio antes de processar tudo"""
    log(f"🧠 Testando ambiente {AMBIENTE.upper()} com primeira requisição...")
    status, j, txt = try_post(payload_teste, verify=True)

    if status == 200:
        log("✅ Conexão e autenticação bem-sucedidas.")
        return True
    elif status == 401:
        log("❌ Erro 401: Token inválido ou expirado.")
    elif status == 403:
        log("❌ Erro 403: Acesso negado. Verifique permissões da conta.")
    elif status == 404:
        log("❌ Erro 404: Endpoint não encontrado (verifique URL).")
    elif status:
        log(f"⚠️ Erro {status}: {txt[:300]}")
    else:
        log(f"❌ Falha de conexão: {txt[:200]}")
    log("🚫 Encerrando script após falha inicial.")
    return False

# ============================================================
# 🚀 LÓGICA PRINCIPAL
# ============================================================

def calcular_fretes():
    log(f"🚀 Iniciando cálculo de fretes no ambiente: {AMBIENTE.upper()}")
    try:
        df = pd.read_excel(ARQUIVO_ENTRADA, engine="xlrd")
    except Exception as e:
        log(f"❌ Erro ao abrir planilha: {e}")
        sys.exit(1)

    if df.empty:
        log("⚠️ Planilha vazia. Encerrando.")
        sys.exit(1)

    # Cria payload do primeiro item
    primeira = df.iloc[0]
    payload_teste = {
        "from": {"postal_code": CEP_ORIGEM},
        "to": {"postal_code": str(primeira.get("CEP DESTINO", "")).zfill(8)},
        "products": [
            {
                "id": "teste_1",
                "width": float(primeira.get("LARGURA (CM)", 1)),
                "height": float(primeira.get("ALTURA (CM)", 1)),
                "length": float(primeira.get("COMPRIMENTO (CM)", 1)),
                "weight": float(primeira.get("PESO (KG)", 0.1)),
                "insurance_value": float(primeira.get("VALOR SEGURADO", 0.0)),
                "quantity": 1
            }
        ],
        "options": {"receipt": False, "own_hand": False}
    }

    # Testa a primeira requisição
    if not testar_primeira_requisicao(payload_teste):
        sys.exit(1)

    log(f"✅ Ambiente {AMBIENTE.upper()} validado com sucesso. Iniciando envios...\n")

    resultados = []
    total = len(df)
    for idx, row in df.iterrows():
        i = idx + 1
        log(f"📦 ({i}/{total}) Calculando envio...")

        payload = {
            "from": {"postal_code": CEP_ORIGEM},
            "to": {"postal_code": str(row.get('CEP DESTINO', '')).zfill(8)},
            "products": [
                {
                    "id": f"item_{i}",
                    "width": float(row.get("LARGURA (CM)", 1)),
                    "height": float(row.get("ALTURA (CM)", 1)),
                    "length": float(row.get("COMPRIMENTO (CM)", 1)),
                    "weight": float(row.get("PESO (KG)", 0.1)),
                    "insurance_value": float(row.get("VALOR SEGURADO", 0.0)),
                    "quantity": 1
                }
            ],
            "options": {"receipt": False, "own_hand": False}
        }

        status, j, txt = try_post(payload, verify=True)

        if status == 200 and isinstance(j, list):
            log(f"✅ Frete calculado com sucesso ({len(j)} opções).")
            for c in j:
                resultados.append({
                    "Ambiente": AMBIENTE.capitalize(),
                    "CEP_DESTINO": payload["to"]["postal_code"],
                    "Transportadora": c.get("company", {}).get("name"),
                    "Serviço": c.get("name"),
                    "Preço (R$)": c.get("custom_price") or c.get("price"),
                    "Prazo (dias úteis)": c.get("custom_delivery_time") or c.get("delivery_time")
                })
        else:
            log(f"⚠️ Falha no cálculo ({status}): {txt[:250]}")

        time.sleep(0.4)

    if resultados:
        pd.DataFrame(resultados).to_excel(ARQUIVO_SAIDA, index=False)
        log(f"\n✅ Concluído! Planilha salva em:\n{ARQUIVO_SAIDA}")
    else:
        log("🚫 Nenhum frete foi calculado com sucesso.")

# ============================================================
# ▶️ EXECUÇÃO
# ============================================================
if __name__ == "__main__":
    calcular_fretes()
