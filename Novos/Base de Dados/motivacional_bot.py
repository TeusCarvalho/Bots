# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------
💬 BOT DE FRASES MOTIVACIONAIS & DESMOTIVACIONAIS — Feishu
-----------------------------------------------------------
✅ Envia automaticamente frases inspiradoras ou sarcásticas 😅
✅ Escolhe aleatoriamente o tipo (mot ou desmot)
✅ Gera cards interativos e coloridos
✅ Executa automaticamente a cada 1 hora
===========================================================
"""

import requests
import time
import random
from datetime import datetime

# ==========================================================
# ⚙️ CONFIGURAÇÃO GERAL
# ==========================================================
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/f3b2a254-5e45-431e-a574-5b949c94ebbc"

# ==========================================================
# 💪 FRASES MOTIVACIONAIS
# ==========================================================
FRASES_MOTIVACIONAIS = [
    "A persistência realiza o impossível.",
    "Você é mais forte do que imagina.",
    "Seja o motivo do seu próprio sorriso.",
    "Acredite: cada passo te aproxima do seu sonho.",
    "A força de vontade move montanhas.",
    "Desistir não é uma opção.",
    "O sucesso é construído com pequenos esforços diários.",
    "Você pode não estar lá ainda, mas está mais perto do que ontem.",
    "Nada é impossível quando você acredita.",
    "Coragem é agir mesmo com medo.",
    "A disciplina vence a motivação.",
    "Os dias difíceis também te fortalecem.",
    "O único fracasso é desistir de tentar.",
    "Transforme dúvidas em atitude.",
    "Você é capaz de muito mais do que imagina.",
    "Nunca é tarde demais para recomeçar.",
    "Acredite em si mesmo e vá além.",
    "Pequenas vitórias diárias geram grandes conquistas.",
    "Seja constante, não perfeito.",
    "Tudo o que você busca também está te buscando.",
    "Você já superou tanto — não pare agora.",
    "A jornada é longa, mas cada passo vale a pena.",
    "Trabalhe em silêncio, deixe o sucesso fazer barulho.",
    "O impossível é apenas o possível que ainda não foi tentado.",
    "Seja a energia que você quer atrair.",
    "Nada muda se você não mudar.",
    "Vença o cansaço com propósito.",
    "Seja seu próprio motivo para continuar.",
    "Acreditar é o primeiro passo para conquistar.",
    "O esforço de hoje é o sucesso de amanhã.",
    "Você está exatamente onde deveria estar.",
    "Caminhe, mesmo devagar, mas nunca pare.",
    "Seu futuro depende do que você faz agora.",
    "Foque no progresso, não na perfeição.",
    "Você é o projeto mais importante da sua vida.",
    "Grandes resultados exigem paciência.",
    "Acredite na sua capacidade de se reinventar.",
    "A determinação transforma sonhos em realidade.",
    "Você não veio até aqui só para chegar até aqui.",
    "O limite é uma ilusão.",
    "Confie no processo.",
    "A vitória começa na mente.",
    "Pequenas ações geram grandes mudanças.",
    "Quem acredita, sempre alcança.",
    "Siga firme, mesmo quando ninguém entende seu caminho.",
    "Você tem tudo o que precisa para começar.",
    "Desafios são oportunidades disfarçadas.",
    "Nada supera um coração determinado.",
    "Seja grato pelo que tem enquanto trabalha pelo que quer.",
    "Continue — o amanhã pode ser incrível."
]

# ==========================================================
# 😩 FRASES DESMOTIVACIONAIS
# ==========================================================
FRASES_DESMOTIVACIONAIS = [
    "Acordar cedo não traz sucesso, só sono.",
    "Nem todo esforço é recompensado — mas o cansaço vem garantido.",
    "Um dia você vai olhar pra trás… e ainda vai estar cansado.",
    "Nem sempre dá certo, mas o boleto chega igual.",
    "Trabalhar duro não é o mesmo que trabalhar feliz.",
    "Errar é humano. Repetir o erro é rotina.",
    "Não desista dos seus sonhos — durma mais.",
    "A vida é curta. Mas o expediente é longo.",
    "Você não é preguiçoso, o mundo é que exige demais.",
    "Motivação não paga contas.",
    "Siga seus sonhos… depois do café.",
    "Amanhã você tenta de novo. Ou não.",
    "O sucesso alheio pode ser apenas sorte. Ou competência, vai saber.",
    "Lute pelos seus sonhos, mas leve um lanche, pode demorar.",
    "Nem todo esforço traz resultado — às vezes só dor nas costas.",
    "O fracasso é garantido para quem tenta. Para quem não tenta, é mais rápido.",
    "Trabalhar é ótimo, pena que tem que fazer isso todo dia.",
    "A esperança é o café da alma cansada.",
    "A vida é feita de altos e baixos… e boletos.",
    "Não há caminho fácil, só atalhos que dão errado.",
    "A paciência é uma virtude. Pena que acaba rápido.",
    "Não se preocupe com o amanhã, ele vai te preocupar sozinho.",
    "Você não está atrasado — o mundo é que anda rápido demais.",
    "A motivação de hoje é o arrependimento de amanhã.",
    "Seja você mesmo — mas talvez melhore um pouco.",
    "Grandes conquistas exigem grandes cafés.",
    "Erros nos ensinam… mas o chefe não entende assim.",
    "Nem todo herói usa capa. Alguns só querem férias.",
    "A persistência leva ao sucesso — ou ao burnout.",
    "Tudo é possível. Principalmente o impossível.",
    "Um passo de cada vez… até o abismo.",
    "Acreditar é importante. Mas pagar as contas é prioridade.",
    "A vida é feita de escolhas — quase todas erradas.",
    "Você pode tudo. Exceto descansar.",
    "Se tudo der errado, pelo menos deu experiência.",
    "Amanhã é outro dia… igual a hoje.",
    "Nada é tão ruim que não possa piorar.",
    "Nem todo dia é bom — e tudo bem.",
    "O otimismo é o perfume da ignorância.",
    "Quem cedo madruga… dorme no transporte.",
    "Sorria, pode piorar.",
    "A motivação vem e vai — mais vai do que vem.",
    "Tudo passa. Inclusive a vontade de tentar.",
    "Às vezes o universo só quer te ensinar paciência.",
    "Não é preguiça, é economia de energia.",
    "Seja positivo: pelo menos você tentou.",
    "O cansaço é a prova de que você está vivo. E exausto.",
    "Amanhã vai ser melhor. Talvez.",
    "Nada como um dia ruim para valorizar o anterior."
]

# ==========================================================
# 🧠 FUNÇÃO DE ENVIO
# ==========================================================
def enviar_card_frase():
    """Escolhe uma frase aleatória (mot ou desmot) e envia para o Feishu."""
    tipo = random.choice(["mot", "desmot"])
    frase = random.choice(FRASES_MOTIVACIONAIS if tipo == "mot" else FRASES_DESMOTIVACIONAIS)
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")

    titulo = "💪 Frase Motivacional do Dia" if tipo == "mot" else "😩 Frase Desmotivacional do Dia"
    cor = "turquoise" if tipo == "mot" else "red"

    card_payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"template": cor, "title": {"tag": "plain_text", "content": titulo}},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": f"_{frase}_"}},
                {"tag": "hr"},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": f"🕒 Enviado em {agora}"}]}
            ]
        }
    }

    try:
        resp = requests.post(WEBHOOK_URL, json=card_payload, timeout=10)
        if resp.status_code == 200:
            print(f"✅ Card enviado com sucesso às {agora} ({tipo.upper()})")
        else:
            print(f"⚠️ Erro ao enviar card ({resp.status_code}): {resp.text}")
    except requests.RequestException as e:
        print(f"🚨 Falha de conexão com Feishu: {e}")


# ==========================================================
# ⏰ LOOP DE ENVIO AUTOMÁTICO
# ==========================================================
if __name__ == "__main__":
    print("🚀 Bot de frases iniciado! Enviando aleatoriamente a cada 1 hora...\n")
    while True:
        enviar_card_frase()
        time.sleep(3600)  # espera 1 hora (3600 segundos)
