# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------
💬 MotivaBB v3.1 — Mente & Humor (Feishu)
-----------------------------------------------------------
✅ Dias úteis apenas (pausa total sábado/domingo)
✅ Horários ativos configuráveis (ex.: 08–22)
✅ Evita repetição (histórico circular das últimas 20)
✅ Bom dia (08:00) e Boa noite (22:00) com frases randômicas
✅ Intervalo inteligente: manhã 1h, tarde 2h, noite 3h
✅ Cores & emojis dinâmicos por tipo
✅ Logs CSV + stats agregadas
✅ Frases lidas de arquivos (fallback embutido)
===========================================================
"""

import os
import csv
import json
import time
import random
import requests
from datetime import datetime, timedelta

# =========================
# ⚙️ CONFIGURAÇÕES GERAIS
# =========================
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/f3b2a254-5e45-431e-a574-5b949c94ebbc"

# Horas ativas (24h). Ex.: range(8, 22) → 08:00..21:59
HORAS_ATIVAS = range(8, 22)

# Persistência
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(BASE_DIR, "data")
LOGS_DIR   = os.path.join(BASE_DIR, "logs")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)

HIST_ARQ   = os.path.join(DATA_DIR, "historico.json")    # histórico de últimas mensagens
LOG_CSV    = os.path.join(LOGS_DIR, "motivaBB_log.csv")  # log detalhado
STATS_CSV  = os.path.join(LOGS_DIR, "motivaBB_stats.csv")# agregados

ARQ_MOT    = os.path.join(DATA_DIR, "frases_motivacionais.txt")
ARQ_DES    = os.path.join(DATA_DIR, "frases_desmotivacionais.txt")

HIST_MAX   = 20  # quantas mensagens recentes evitar

# =========================
# 🎨 TEMA DINÂMICO
# =========================
THEME = {
    "mot":   {"header": "turquoise", "emoji": "💪"},
    "desmot":{"header": "red",       "emoji": "😩"},
    "misto": {"header": "yellow",    "emoji": "☯️"}
}
HEADERS_ROTATIVOS = ["turquoise", "blue", "green", "wathet", "orange", "yellow", "red", "purple"]

# =========================
# 🗂️ FRASES (fallback embutido)
# — Para 300+ frases, coloque uma por linha nos .txt:
#   data/frases_motivacionais.txt
#   data/frases_desmotivacionais.txt
# =========================
FALLBACK_MOT = [
    "A persistência realiza o impossível.",
    "Você é mais forte do que imagina.",
    "Coragem é agir mesmo com medo.",
    "Acredite em si mesmo e vá além.",
    "A disciplina vence a motivação.",
    "Transforme dúvidas em atitude.",
    "Nada muda se você não mudar.",
    "O sucesso é a soma de pequenos esforços diários.",
    "Você pode não estar lá ainda, mas está mais perto do que ontem.",
    "Seja constante, não perfeito.",
    "Seu futuro começa quando você decide agir.",
    "A cada amanhecer, uma nova chance de recomeçar.",
    "Persistência é o caminho do êxito.",
    "Pequenas vitórias constroem grandes histórias.",
    "Vença o desânimo com propósito.",
    "A vitória começa na mente.",
    "Você é o seu próprio limite.",
    "Cair é humano; levantar é escolha.",
    "A vida premia quem não desiste.",
    "Desafios são oportunidades disfarçadas.",
]
FALLBACK_DES = [
    "Acordar cedo não traz sucesso, só sono.",
    "Nem todo esforço é recompensado — mas o cansaço vem garantido.",
    "Nem sempre dá certo, mas o boleto chega igual.",
    "Trabalhar é ótimo, pena que tem que fazer isso todo dia.",
    "Não desista dos seus sonhos — durma mais.",
    "A vida é curta, mas o expediente é longo.",
    "Você não é preguiçoso, o mundo é que exige demais.",
    "Motivação não paga contas.",
    "Amanhã você tenta de novo. Ou não.",
    "Lute pelos seus sonhos, mas leve um lanche — pode demorar.",
    "Sorria, pode piorar.",
    "Tudo passa. Inclusive a vontade de tentar.",
    "A persistência leva ao sucesso — ou ao burnout.",
    "Nada é tão ruim que não possa piorar.",
    "Não é preguiça, é economia de energia.",
    "A motivação vem e vai — mais vai do que vem.",
]

def carregar_frases(caminho, fallback):
    frases = []
    if os.path.exists(caminho):
        with open(caminho, "r", encoding="utf-8") as f:
            for linha in f:
                s = linha.strip()
                if s:
                    frases.append(s)
    if not frases:
        frases = fallback[:]
    return frases

# =========================
# 📅 UTILITÁRIOS DE TEMPO
# =========================
def dia_util(dt=None) -> bool:
    if dt is None:
        dt = datetime.now()
    return dt.weekday() < 5  # 0=Seg, 6=Dom

def hora_ativa(hora_int: int) -> bool:
    return hora_int in HORAS_ATIVAS

def proximo_topo_hora():
    agora = datetime.now()
    return (agora.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))

def dormir_ate(dt: datetime):
    delta = (dt - datetime.now()).total_seconds()
    time.sleep(max(1, delta))

def intervalo_inteligente(hora: int) -> int:
    """
    Retorna intervalo (em horas) com base no horário:
    - Manhã (08–11): 1h
    - Tarde (12–17): 2h
    - Noite (18–21): 3h
    Fora da janela ativa, não envia.
    """
    if 8 <= hora <= 11:
        return 1
    if 12 <= hora <= 17:
        return 2
    if 18 <= hora <= 21:
        return 3
    return 1  # fallback

# =========================
# 💾 PERSISTÊNCIA / LOGS
# =========================
def load_hist():
    if os.path.exists(HIST_ARQ):
        try:
            with open(HIST_ARQ, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"recentes": []}

def save_hist(hist):
    hist["recentes"] = hist.get("recentes", [])[-HIST_MAX:]
    with open(HIST_ARQ, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=2)

def push_recent(msg: str, hist: dict):
    hist.setdefault("recentes", []).append(msg)
    save_hist(hist)

def append_log(ts_iso: str, tipo: str, conteudo: str, status: str):
    novo = not os.path.exists(LOG_CSV)
    with open(LOG_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        if novo:
            w.writerow(["timestamp", "tipo", "conteudo", "status"])
        w.writerow([ts_iso, tipo, conteudo, status])

def upsert_stats(tipo: str):
    # lê tudo
    stats = {}
    if os.path.exists(STATS_CSV):
        with open(STATS_CSV, "r", encoding="utf-8") as f:
            r = csv.reader(f, delimiter=";")
            for i, row in enumerate(r):
                if i == 0:  # header
                    continue
                if len(row) >= 2:
                    stats[row[0]] = int(row[1])
    # atualiza
    stats[tipo] = stats.get(tipo, 0) + 1
    stats["_total"] = stats.get("_total", 0) + 1
    # escreve
    with open(STATS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["tipo", "quantidade"])
        for k, v in sorted(stats.items()):
            w.writerow([k, v])

# =========================
# 🔁 SELEÇÃO SEM REPETIÇÃO
# =========================
def escolher_sem_repetir(pool, hist, tentativas=30):
    recentes = set(hist.get("recentes", []))
    for _ in range(tentativas):
        s = random.choice(pool)
        if s not in recentes:
            return s
    # fallback se tudo quase repetido
    return random.choice(pool)

# =========================
# 📨 CARD FEISHU
# =========================
def enviar_card(tipo: str, titulo: str, markdown: str, header=None):
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    header_color = header if header else THEME.get(tipo, THEME["misto"])["header"]
    card = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"template": header_color, "title": {"tag": "plain_text", "content": titulo}},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": markdown}},
                {"tag": "hr"},
                {"tag": "note", "elements": [{"tag": "plain_text", "content": f"🕒 Enviado em {agora}"}]}
            ]
        }
    }
    try:
        r = requests.post(WEBHOOK_URL, json=card, timeout=10)
        return r.status_code == 200, ("" if r.status_code == 200 else f"{r.status_code}: {r.text}")
    except requests.RequestException as e:
        return False, str(e)

# =========================
# 🚀 LOOP PRINCIPAL
# =========================
def rodar():
    print("🚀 MotivaBB v3.1 — iniciado.")
    hist = load_hist()

    # Carrega frases de arquivo (ou fallback)
    frases_mot = carregar_frases(ARQ_MOT, FALLBACK_MOT)
    frases_des = carregar_frases(ARQ_DES, FALLBACK_DES)

    print(f"📚 Frases carregadas — MOT: {len(frases_mot)} | DES: {len(frases_des)}")
    print(f"🕒 Janela ativa: {min(HORAS_ATIVAS):02d}:00–{max(HORAS_ATIVAS):02d}:59 | Dias úteis apenas\n")

    while True:
        agora = datetime.now()
        ts_iso = agora.isoformat(timespec="seconds")

        # pausa fins de semana
        if not dia_util(agora):
            print(f"⏸️ {ts_iso} fim de semana — dormindo 6h.")
            time.sleep(6 * 3600)
            continue

        # respeita janela ativa
        if not hora_ativa(agora.hour):
            prox = proximo_topo_hora()
            print(f"⏸️ {ts_iso} fora da janela ativa — dormindo até {prox.strftime('%H:%M')}.")
            dormir_ate(proximo_topo_hora())
            continue

        # mensagens especiais fixas por horário (mas aleatórias no conteúdo)
        header_rot = random.choice(HEADERS_ROTATIVOS)

        if agora.hour == 8:
            frase = escolher_sem_repetir(frases_mot, hist)
            titulo = "☀️ Bom dia, bb!"
            md     = f"_{frase}_"
            ok, err = enviar_card("mot", titulo, md, header=header_rot)
            append_log(ts_iso, "bom_dia", frase, "ok" if ok else f"erro: {err}")
            upsert_stats("bom_dia")
            if ok: push_recent(frase, hist)
            dormir_ate(proximo_topo_hora())
            continue

        if agora.hour == 22:
            frase = escolher_sem_repetir(frases_des, hist)
            titulo = "🌙 Boa noite, bb!"
            md     = f"_{frase}_"
            ok, err = enviar_card("misto", titulo, md, header=header_rot)
            append_log(ts_iso, "boa_noite", frase, "ok" if ok else f"erro: {err}")
            upsert_stats("boa_noite")
            if ok: push_recent(frase, hist)
            dormir_ate(proximo_topo_hora())
            continue

        # mensagem normal (mot / desmot / misto)
        modo = random.choice(["mot", "desmot", "misto"])
        if modo == "mot":
            frase = escolher_sem_repetir(frases_mot, hist)
            titulo = f"{THEME['mot']['emoji']} Frase Motivacional"
            md     = f"_{frase}_"
            content_to_log = frase

        elif modo == "desmot":
            frase = escolher_sem_repetir(frases_des, hist)
            titulo = f"{THEME['desmot']['emoji']} Frase Desmotivacional"
            md     = f"_{frase}_"
            content_to_log = frase

        else:  # misto
            mot = escolher_sem_repetir(frases_mot, hist)
            des = escolher_sem_repetir(frases_des, hist)
            titulo = f"{THEME['misto']['emoji']} Yin-Yang do Dia"
            md     = f"**💪 Motivacional:** _{mot}_\n\n**😩 Desmotivacional:** _{des}_"
            content_to_log = f"{mot} || {des}"

        ok, err = enviar_card(modo, titulo, md, header=header_rot)
        append_log(ts_iso, modo, content_to_log, "ok" if ok else f"erro: {err}")
        upsert_stats(modo)
        if ok:
            push_recent(content_to_log, hist)
            print(f"✅ {ts_iso} [{modo}] enviado.")
        else:
            print(f"⚠️ {ts_iso} falha ao enviar: {err}")

        # intervalo inteligente
        horas = intervalo_inteligente(agora.hour)
        proxima = agora.replace(minute=0, second=0, microsecond=0) + timedelta(hours=horas)
        print(f"⏳ Próximo envio previsto ~ {proxima.strftime('%d/%m %H:%M')} ({horas}h).")
        dormir_ate(proxima)

# =========================
# ▶️ MAIN
# =========================
if __name__ == "__main__":
    rodar()
