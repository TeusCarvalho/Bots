# ==============================================================
# 📡 Monitor de Rede - Visual Profissional
# ==============================================================

import os
import socket
import time
import csv
import speedtest
import pandas as pd
from scapy.all import sniff
from datetime import datetime
import streamlit as st
import threading

# ==============================================================
# ⚙️ CONFIGURAÇÕES
# ==============================================================
PASTA_LOG = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Teste Base\Redes"
ARQUIVO_LOG = os.path.join(PASTA_LOG, "monitor_rede.csv")

HOSTS_PARA_PING = ["8.8.8.8", "1.1.1.1"]
PORTAS = [22, 80, 443]
QTD_PACOTES = 5
INTERVALO = 3600  # 1 hora

DOWNLOAD_CONTRATADO = 200  # Mbps
UPLOAD_CONTRATADO = 100    # Mbps


# ==============================================================
# 🔍 FUNÇÕES DE COLETA
# ==============================================================
def verifica_ping(host):
    return os.system(f"ping -n 1 {host} > nul") == 0  # Windows


def verifica_porta(host, porta):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    resultado = sock.connect_ex((host, porta))
    sock.close()
    return resultado == 0


def captura_pacotes(qtd=QTD_PACOTES):
    pacotes = sniff(count=qtd)
    return [p.summary() for p in pacotes]


def mede_velocidade():
    tester = speedtest.Speedtest()
    tester.get_best_server()
    download = tester.download() / 1_000_000
    upload = tester.upload() / 1_000_000
    return download, upload


# ==============================================================
# 📝 LOG E COLETA
# ==============================================================
def salvar_log(dados):
    existe = os.path.isfile(ARQUIVO_LOG)
    with open(ARQUIVO_LOG, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        if not existe:
            writer.writerow([
                "timestamp", "host", "ping", "porta", "aberta",
                "download_Mbps", "upload_Mbps", "trafego", "status", "alerta"
            ])
        # Normaliza valores
        dados[2] = int(dados[2]) if isinstance(dados[2], (bool, str)) else dados[2]
        dados[4] = int(dados[4]) if isinstance(dados[4], (bool, str)) else dados[4]
        dados[8] = int(dados[8]) if isinstance(dados[8], (bool, str)) else dados[8]
        writer.writerow(dados)


def coleta():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sucesso = True

    for host in HOSTS_PARA_PING:
        try:
            status_ping = verifica_ping(host)
        except Exception:
            status_ping = False
            sucesso = False

        for porta in PORTAS:
            try:
                aberta = verifica_porta(host, porta)
            except Exception:
                aberta = False
                sucesso = False

            try:
                download, upload = mede_velocidade()
            except Exception:
                download, upload = 0, 0
                sucesso = False

            try:
                pacotes = captura_pacotes()
                pacotes_str = " | ".join(pacotes[:3])
            except Exception:
                pacotes_str = "Erro ao capturar pacotes"
                sucesso = False

            alerta_msg = ""
            if not status_ping:
                alerta_msg += "Ping falhou; "
            if download < 0.8 * DOWNLOAD_CONTRATADO:
                alerta_msg += f"Download baixo ({download:.2f} Mbps); "
            if upload < 0.8 * UPLOAD_CONTRATADO:
                alerta_msg += f"Upload baixo ({upload:.2f} Mbps); "
            if aberta:
                alerta_msg += f"Porta {porta} aberta; "

            salvar_log([
                timestamp, host, status_ping, porta, aberta,
                f"{download:.2f}", f"{upload:.2f}", pacotes_str, sucesso, alerta_msg
            ])


def loop_coleta():
    while True:
        time.sleep(INTERVALO)
        coleta()


# ==============================================================
# 📊 DASHBOARD PROFISSIONAL
# ==============================================================
def dashboard():
    st.set_page_config(page_title="Monitor de Rede", layout="wide")
    st.title("📡 Monitoramento de Rede")

    # --- SIDEBAR ---
    st.sidebar.header("⚙️ Painel de Controle")
    if st.sidebar.button("🔄 Coletar Agora"):
        coleta()
        st.sidebar.success("✅ Coleta concluída!")
    if st.sidebar.button("▶️ Iniciar Coleta Automática"):
        threading.Thread(target=loop_coleta, daemon=True).start()
        st.sidebar.info("⏱️ Coleta automática iniciada")
    refresh = st.sidebar.checkbox("🔁 Auto Refresh")
    intervalo = st.sidebar.slider("Intervalo (s)", 10, 300, 60)
    if refresh:
        time.sleep(intervalo)
        st.experimental_rerun()

    # --- LEITURA DE DADOS ---
    if not os.path.exists(ARQUIVO_LOG):
        st.warning("Nenhum dado coletado ainda. Faça a primeira coleta.")
        st.stop()
    df = pd.read_csv(ARQUIVO_LOG, sep=";")
    for col in ["ping", "aberta", "status", "download_Mbps", "upload_Mbps"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- INDICADORES EM CARDS ---
    st.subheader("📈 Indicadores de Rede")
    col1, col2, col3 = st.columns(3)
    col1.metric("📶 Uptime", f"{df['ping'].mean()*100:.2f} %")
    col2.metric("⬇️ Download", f"{df['download_Mbps'].mean():.2f} Mbps")
    col3.metric("⬆️ Upload", f"{df['upload_Mbps'].mean():.2f} Mbps")

    # --- ALERTAS ---
    st.subheader("⚠️ Alertas Recentes")
    alertas_df = df[df["alerta"].astype(str).str.strip() != ""].tail(5)
    if alertas_df.empty:
        st.success("Nenhum alerta registrado ✅")
    else:
        for _, row in alertas_df.iterrows():
            st.error(f"**{row['timestamp']} | {row['host']}** → {row['alerta']}")

    # --- GRÁFICO DE VELOCIDADE ---
    st.subheader("🚀 Velocidade da Conexão")
    vel = df[["timestamp", "download_Mbps", "upload_Mbps"]].set_index("timestamp")
    st.line_chart(vel)

    # --- STATUS DE PORTAS ---
    st.subheader("🔒 Status de Portas")
    portas_chart = df.pivot_table(index="timestamp", columns="porta", values="aberta", aggfunc="mean")
    st.line_chart(portas_chart)

    # --- PACOTES CAPTURADOS ---
    st.subheader("📦 Últimos Pacotes Capturados")
    pacotes = df[["timestamp", "host", "trafego"]].tail(5)
    for _, row in pacotes.iterrows():
        texto = row["trafego"]
        if "TCP" in texto:
            st.markdown(f"🟢 **{row['timestamp']}** → {texto}")
        elif "UDP" in texto:
            st.markdown(f"🔵 **{row['timestamp']}** → {texto}")
        elif "ICMP" in texto:
            st.markdown(f"🟠 **{row['timestamp']}** → {texto}")
        else:
            st.markdown(f"⚪ **{row['timestamp']}** → {texto}")


# ==============================================================
# 🚀 EXECUÇÃO
# ==============================================================
dashboard()