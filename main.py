# main.py
import os
import subprocess

# Caminho base do projeto
BASE_PATH = r"C:\Users\J&T-099\PycharmProjects\Bots"

# Scripts disponíveis
SCRIPTS = {
    "1": ("📦 Sem Movimentação", os.path.join(BASE_PATH, "Comparação.py")),
    "2": ("💰 Custo e Arbitragem", os.path.join(BASE_PATH, "Custo_Arbitragem.py")),
    "3": ("⏱️ T-0", os.path.join(BASE_PATH, "Resumo_Semanal.py")),
    "4": ("📊 ShippingTime", os.path.join(BASE_PATH, "4. ShippingTime.py")),
    "5": ("✅ Entrega Realizada – Dia", os.path.join(BASE_PATH, "Entrega_Realizada.py")),
    "6": ("📂 Mover Arquivos Antigos", os.path.join(BASE_PATH, "Mover_Antigos.py")),
}

def exibir_menu():
    print("\n=== MENU BOTS J&T EXPRESS ===")
    for key, (nome, _) in SCRIPTS.items():
        print(f"{key}. {nome}")
    print("0. ❌ Sair")

def rodar_script(escolha):
    nome, caminho = SCRIPTS[escolha]
    print(f"\n▶️ Executando: {nome}\n")
    try:
        subprocess.run(["python", caminho], check=True)
    except Exception as e:
        print(f"⚠️ Erro ao executar {nome}: {e}")

if __name__ == "__main__":
    while True:
        exibir_menu()
        escolha = input("\nEscolha uma opção: ")
        if escolha == "0":
            print("Saindo... 👋")
            break
        elif escolha in SCRIPTS:
            rodar_script(escolha)
        else:
            print("Opção inválida! Tente novamente.")
