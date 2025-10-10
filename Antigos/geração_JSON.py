import pandas as pd
import os

# Caminho da pasta com os arquivos
folder_path = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Teste Base\Alterações"

# Verifica se a pasta existe
if not os.path.exists(folder_path):
    raise FileNotFoundError(f"Pasta não encontrada: {folder_path}")

# Lista todos os arquivos na pasta, incluindo .xls e .csv
files = [f for f in os.listdir(folder_path) if f.endswith(('.xls', '.xlsx', '.csv'))]

if not files:
    print("Nenhum arquivo .xlsx encontrado na pasta.")
else:
    print(f"{len(files)} arquivo(s) encontrado(s).")

dfs = []
for file in files:
    file_path = os.path.join(folder_path, file)
    try:
        df_temp = pd.read_excel(file_path, engine='openpyxl')  # lê o arquivo
        df_temp['Fonte'] = file  # adiciona coluna com o nome do arquivo
        dfs.append(df_temp)
        print(f"{file} lido com sucesso!")
    except Exception as e:
        print(f"Erro ao ler {file}: {e}")

# Concatena todos os DataFrames em um único
if dfs:
    df = pd.concat(dfs, ignore_index=True)
    print("Todos os arquivos foram concatenados com sucesso!")
    # Exemplo: salvar em CSV
    df.to_csv(os.path.join(folder_path, "base_concatenada.csv"), index=False, encoding='utf-8-sig')
    print("Arquivo 'base_concatenada.csv' gerado na pasta.")
else:
    print("Nenhum DataFrame para concatenar.")
