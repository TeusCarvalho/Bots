import os
import subprocess
import sys
from pathlib import Path

# Caminho base do projeto
BASE_PATH = Path(r"C:\Users\J&T-099\PycharmProjects\Bots")

# Scripts disponíveis
SCRIPTS = {
    "1": ("📦 Sem Movimentação", "Comparação.py"),
    "2": ("💰 Custo e Arbitragem", "Custo_Arbitragem.py"),
    "3": ("⏱️ T-0", "Resumo_Semanal.py"),
    "4": ("📊 ShippingTime", "4. ShippingTime.py"),
    "5": ("✅ Entrega Realizada – Dia", "Entrega_Realizada.py"),
    "6": ("📂 Mover Arquivos Antigos", "Mover_Antigos.py"),
    "7": ("📱 WhatsApp Bot", "Novos/WhatsApp/WhatsApp.py"),
}


def exibir_menu():
    """Exibe o menu de opções disponíveis."""
    print("\n" + "=" * 40)
    print("    MENU BOTS J&T EXPRESS")
    print("=" * 40)
    for key, (nome, _) in SCRIPTS.items():
        print(f"{key}. {nome}")
    print("0. ❌ Sair")
    print("=" * 40)


def verificar_script(caminho_script):
    """Verifica se o script existe e pode ser executado."""
    if not os.path.exists(caminho_script):
        print(f"❌ Erro: O script '{caminho_script}' não foi encontrado.")
        return False

    if not os.access(caminho_script, os.R_OK):
        print(f"❌ Erro: Sem permissão para ler o script '{caminho_script}'.")
        return False

    return True


def rodar_script(escolha):
    """Executa o script selecionado com tratamento de erros aprimorado."""
    nome, nome_arquivo = SCRIPTS[escolha]
    caminho_script = BASE_PATH / nome_arquivo

    print(f"\n▶️ Executando: {nome}")
    print(f"📂 Caminho: {caminho_script}")
    print("-" * 40)

    if not verificar_script(caminho_script):
        return

    try:
        # Usa o mesmo interpretador Python que está executando este script
        processo = subprocess.run(
            [sys.executable, str(caminho_script)],
            check=True,
            capture_output=False,  # Mostra a saída em tempo real
            text=True
        )
        print(f"\n✅ Script '{nome}' executado com sucesso!")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Erro ao executar '{nome}':")
        print(f"Código de saída: {e.returncode}")
        print(f"Erro: {e}")
    except FileNotFoundError:
        print(f"\n❌ Erro: Interpretador Python não encontrado em '{sys.executable}'")
    except Exception as e:
        print(f"\n❌ Erro inesperado ao executar '{nome}': {type(e).__name__}: {e}")


def main():
    """Função principal do programa."""
    print("Bem-vindo ao sistema de automação J&T Express!")

    while True:
        try:
            exibir_menu()
            escolha = input("\nEscolha uma opção: ").strip()

            if escolha == "0":
                print("\nSaindo... 👋\n")
                break
            elif escolha in SCRIPTS:
                rodar_script(escolha)
                input("\nPressione Enter para continuar...")
            else:
                print("\n⚠️ Opção inválida! Tente novamente.")
                input("Pressione Enter para continuar...")
        except KeyboardInterrupt:
            print("\n\nOperação cancelada pelo usuário. Saindo... 👋\n")
            break
        except Exception as e:
            print(f"\n❌ Erro inesperado: {type(e).__name__}: {e}")
            input("Pressione Enter para continuar...")


if __name__ == "__main__":
    main()