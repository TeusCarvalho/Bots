# -*- coding: utf-8 -*-
"""
💬 Envio Automático via WhatsApp Cloud API (Meta)
Autor: bb-assistente 😎
Descrição:
 - Tenta enviar o template "ateno_motoristas"
 - Se já houver sessão aberta (últimas 24h), envia texto simples
"""

import requests
import time
from datetime import datetime

# ==========================================================
# ⚙️ CONFIGURAÇÕES
# ==========================================================
ACCESS_TOKEN = "EAALqfksfv9UBP7KTbeYTWgpCTs0eh6iTHZB7CFIunJSZASJN3OGav9EUg8uss2rm3texT0VfHsEcRd4U9HFffZAHGm85EGG8NZBuDDLox9XfrwglyfTsNMEhZCSy3ZCgbZAYZCexPdcvDKIyt77VcAdoepLlcp0BllAZBdZBKT2BpcngnX6AGF1ugdDdMujw2ZBabCArAP44ZCxktjzBJfFZB0ZAhFcoNZCuhIkwfli81bV65CThqi8xM7bgisowVHB5zXnylD5EsyZBhmXo3I8rpVdJjfTZC"
PHONE_NUMBER_ID = "899378896582685"  # ID do número da Meta
DESTINATARIO = "5561994335940"       # Número do motorista

# ==========================================================
# 💬 MENSAGEM PERSONALIZADA
# ==========================================================
data = datetime.now().strftime("%d/%m/%Y %H:%M")

mensagem = f"""⚡ *BLACK FRIDAY EM ANDAMENTO!* ⚡

🚚 Precisamos de motoristas disponíveis para carregamento *IMEDIATO*! 💨

📍 Compareça à base o quanto antes e garanta sua rota.
🕓 Atualizado em: {data}

💪 Contamos com você!
— *Equipe de Operações | J&T Express Brasil*
"""

# ==========================================================
# 🚀 ENVIO DE TEMPLATE
# ==========================================================
def enviar_template():
    url = f"https://graph.facebook.com/v22.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": DESTINATARIO,
        "type": "template",
        "template": {
            "name": "ateno_motoristas",  # 👈 seu template aprovado
            "language": {"code": "pt_BR"}
        }
    }

    r = requests.post(url, headers=headers, json=payload)
    return r

# ==========================================================
# 🚀 ENVIO DE TEXTO SIMPLES
# ==========================================================
def enviar_texto():
    url = f"https://graph.facebook.com/v22.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": DESTINATARIO,
        "type": "text",
        "text": {"body": mensagem}
    }

    r = requests.post(url, headers=headers, json=payload)
    return r

# ==========================================================
# 🧩 FLUXO INTELIGENTE
# ==========================================================
print("🚀 Tentando enviar mensagem via template 'ateno_motoristas'...")
res = enviar_template()

if res.status_code == 200:
    print("✅ Template enviado com sucesso!")
else:
    print(f"⚠️ Falha ao enviar template ({res.status_code}): {res.text}")
    print("➡️ Tentando envio direto de mensagem (sessão pode estar aberta)...")
    time.sleep(2)
    res2 = enviar_texto()

    if res2.status_code == 200:
        print("✅ Mensagem de texto enviada com sucesso!")
    else:
        print(f"❌ Falha no envio de texto ({res2.status_code}): {res2.text}")

print("\n🏁 Processo finalizado.")
