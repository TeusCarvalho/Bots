import subprocess
import os

def revisar_codigo(caminho_arquivo, modelo="codellama"):
    # Lê o conteúdo do código
    with open(caminho_arquivo, "r", encoding="utf-8") as f:
        codigo = f.read()

    # Monta o prompt para o Ollama
    prompt = f"""
Revise o seguinte código Python:
- Corrija erros.
- Aplique boas práticas PEP8.
- Sugira melhorias de desempenho, se possível.
- Responda sempre com o código corrigido completo.

Código:
{codigo}
"""

    # Executa o Ollama com subprocess
    comando = ["ollama", "run", modelo, prompt]
    resultado = subprocess.run(comando, capture_output=True, text=True)

    # Saída do modelo
    resposta = resultado.stdout.strip()

    # Salva em outro arquivo para não sobrescrever o original
    caminho_saida = caminho_arquivo.replace(".py", "_revisto.py")
    with open(caminho_saida, "w", encoding="utf-8") as f:
        f.write(resposta)

    print(f"✅ Revisão concluída! Arquivo salvo em: {caminho_saida}")


# Exemplo de uso:
if __name__ == "__main__":
    arquivo = "Local_Para_Teste.py"  # coloque aqui o nome do seu script
    revisar_codigo(arquivo)
