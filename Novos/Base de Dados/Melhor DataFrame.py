import pandas as pd
import polars as pl
import numpy as np
import time
import psutil
import os
from tqdm import tqdm
import threading

# =========================================================
# 🎨 Cores para terminal
# =========================================================
class Color:
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


# =========================================================
# ⚙️ Funções auxiliares
# =========================================================
def memory_usage_mb():
    """Retorna o uso atual de memória em MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 ** 2


def cpu_percent():
    """Retorna o uso atual de CPU (%)"""
    return psutil.cpu_percent(interval=0.5)


def monitor_recursos(stop_event):
    """Exibe o uso de CPU e memória a cada segundo."""
    while not stop_event.is_set():
        cpu = cpu_percent()
        mem = memory_usage_mb()
        cor_cpu = Color.GREEN if cpu < 50 else Color.YELLOW if cpu < 80 else Color.RED
        cor_mem = Color.GREEN if mem < 8000 else Color.YELLOW if mem < 16000 else Color.RED
        print(f"{cor_cpu}⚙️ CPU: {cpu:5.1f}%{Color.RESET} | {cor_mem}💾 RAM: {mem:8.2f} MB{Color.RESET}", end='\r')
        time.sleep(1)


def medir_tempo_memoria(nome_teste, cor, func):
    """Executa função com monitoramento de CPU/RAM em tempo real."""
    print(f"\n{cor}{Color.BOLD}🚀 Iniciando teste: {nome_teste}{Color.RESET}")
    inicio = time.time()
    stop_event = threading.Event()
    monitor_thread = threading.Thread(target=monitor_recursos, args=(stop_event,))
    monitor_thread.start()

    resultado = func()

    stop_event.set()
    monitor_thread.join()

    tempo = time.time() - inicio
    mem_final = memory_usage_mb()
    print(f"\n{cor}⏱️ Tempo total: {tempo:.2f} segundos | 💾 Memória final: {mem_final:.2f} MB{Color.RESET}")
    return resultado, tempo, mem_final


# =========================================================
# 1️⃣ Geração do Dataset (automático)
# =========================================================
N_TOTAL = 30_000_000  # ajuste livremente
BLOCO = 1_000_000
arquivo_csv = "dados_teste_auto.csv"

print(f"{Color.BOLD}{Color.YELLOW}📦 Gerando dataset de {N_TOTAL:,} linhas em blocos de {BLOCO:,}...{Color.RESET}")

if os.path.exists(arquivo_csv):
    os.remove(arquivo_csv)

for i in tqdm(range(0, N_TOTAL, BLOCO), desc="Gerando blocos", colour="cyan"):
    data = {
        "cidade": np.random.choice(["SP", "RJ", "MG", "RS", "BA"], size=BLOCO),
        "vendas": np.random.randint(1, 1000, size=BLOCO),
        "ano": np.random.choice([2023, 2024, 2025], size=BLOCO)
    }
    df = pd.DataFrame(data)
    df.to_csv(arquivo_csv, index=False, mode="a", header=not os.path.exists(arquivo_csv))

print(f"{Color.GREEN}✅ CSV completo criado: {arquivo_csv}{Color.RESET}\n")

# =========================================================
# 2️⃣ Escolha dinâmica do modo de execução
# =========================================================
if N_TOTAL < 100_000_000:
    modos = ["pandas", "polars", "lazy"]
    print(f"{Color.CYAN}🧮 Base pequena detectada — executando todos os modos (Pandas, Polars, Lazy).{Color.RESET}")
else:
    modos = ["lazy"]
    print(f"{Color.YELLOW}⚡ Base muito grande — executando apenas o modo Polars Lazy (streaming).{Color.RESET}")


# =========================================================
# 3️⃣ Funções de teste
# =========================================================
def teste_pandas():
    df = pd.read_csv(arquivo_csv)
    return df.groupby("cidade")["vendas"].sum()


def teste_polars():
    df = pl.read_csv(arquivo_csv)
    return df.group_by("cidade").agg(pl.col("vendas").sum())


def teste_polars_lazy():
    lazy_df = pl.scan_csv(arquivo_csv)
    return (
        lazy_df.group_by("cidade")
        .agg(pl.col("vendas").sum())
        .collect(streaming=True)
    )


# =========================================================
# 4️⃣ Execução dos testes
# =========================================================
resultados = {}

if "pandas" in modos:
    res, tempo, mem = medir_tempo_memoria("🐼 Pandas", Color.BLUE, teste_pandas)
    resultados["Pandas"] = (tempo, mem)
    print(res)

if "polars" in modos:
    res, tempo, mem = medir_tempo_memoria("⚡ Polars", Color.CYAN, teste_polars)
    resultados["Polars"] = (tempo, mem)
    print(res)

if "lazy" in modos:
    res, tempo, mem = medir_tempo_memoria("🧠 Polars Lazy (Streaming)", Color.GREEN, teste_polars_lazy)
    resultados["Polars Lazy"] = (tempo, mem)
    print(res)


# =========================================================
# 5️⃣ Resumo final e ranking
# =========================================================
print(f"\n{Color.BOLD}{Color.YELLOW}🏁 BENCHMARK FINAL - RESUMO{Color.RESET}")
print(f"{Color.CYAN}{'='*60}{Color.RESET}")
print(f"{Color.BOLD}Biblioteca{' '*10}Tempo (s){' '*5}Memória (MB){Color.RESET}")

for nome, (tempo, mem) in resultados.items():
    print(f"{Color.GREEN if nome=='Polars Lazy' else Color.CYAN}{nome:<18}{tempo:<10.2f}{mem:<10.2f}{Color.RESET}")

# 🏆 Melhor desempenho
melhor = min(resultados.items(), key=lambda x: x[1][0])
print(f"{Color.CYAN}{'='*60}{Color.RESET}")
print(f"{Color.BOLD}{Color.GREEN}🏆 Melhor desempenho: {melhor[0]} ({melhor[1][0]:.2f}s){Color.RESET}")

print(f"\n{Color.YELLOW}📊 Observações:{Color.RESET}")
print(f"{Color.CYAN}- Pandas processa tudo em memória (um núcleo, alto consumo)")
print(f"- Polars usa CPU paralela, ótimo desempenho médio")
print(f"- Lazy (Streaming) é o mais eficiente em grandes volumes 🚀{Color.RESET}")
