# Passo 1: Instale as bibliotecas (se ainda não tiver)
# pip install pandas openpyxl tqdm

import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

import pandas as pd
import os
from tqdm import tqdm

# --- INÍCIO: ÁREA DE CONFIGURAÇÃO ---
# 1. Caminho da pasta onde estão suas planilhas e onde o arquivo será salvo.
caminho_da_pasta = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Nova pasta\Entrega Realizada'

# 2. Status que você quer filtrar.
status_desejado = 'recebimento com assinatura normal'

# 3. Nome do arquivo de saída. Ele será salvo DENTRO da pasta acima.
nome_arquivo_saida = 'resumo_entregas_por_motorista.xlsx'
# --- FIM: ÁREA DE CONFIGURAÇÃO ---


# Verifica se a pasta existe
if not os.path.isdir(caminho_da_pasta):
    print(f"Erro: A pasta '{caminho_da_pasta}' não foi encontrada.")
else:
    lista_de_dataframes_filtrados = []

    # Prepara a lista de arquivos para a barra de progresso
    arquivos_para_processar = [f for f in os.listdir(caminho_da_pasta) if f.endswith(('.xlsx', '.xls'))]

    # Inicia a barra de progresso
    for nome_do_arquivo in tqdm(arquivos_para_processar, desc="Processando arquivos"):
        caminho_completo = os.path.join(caminho_da_pasta, nome_do_arquivo)
        try:
            # <<< MUDANÇA AQUI >>>
            # Lemos APENAS as colunas que são absolutamente necessárias.
            df = pd.read_excel(caminho_completo, usecols=['Responsável pela entrega', 'Marca de assinatura'])

            # Limpar e filtrar a coluna de status
            df['Marca de assinatura'] = df['Marca de assinatura'].str.strip().str.lower()
            df_filtrado = df[df['Marca de assinatura'] == status_desejado]

            if not df_filtrado.empty:
                lista_de_dataframes_filtrados.append(df_filtrado)
        except Exception as e:
            # Erros são exibidos sem quebrar a barra de progresso
            tqdm.write(f"   -> Erro ao ler o arquivo '{nome_do_arquivo}': {e}")

    if not lista_de_dataframes_filtrados:
        print("\nNenhum dado encontrado com o filtro especificado.")
    else:
        print("\nConsolidando dados e criando resumo...")
        dataframe_consolidado = pd.concat(lista_de_dataframes_filtrados, ignore_index=True)

        # <<< MUDANÇA AQUI >>>
        # Agrupamos por motorista e contamos as linhas.
        # Como não lemos mais a coluna 'Número de pedido JMS', usamos qualquer outra coluna para a contagem.
        # Usar a própria coluna de agrupamento ('Responsável pela entrega') é uma boa prática.
        resumo_por_motorista = dataframe_consolidado.groupby('Responsável pela entrega').agg(
            Quantidade_de_Pedidos=('Responsável pela entrega', 'size')
        ).reset_index()

        # Define o caminho completo de saída
        caminho_saida = os.path.join(caminho_da_pasta, nome_arquivo_saida)

        # Salva o resultado na pasta de origem
        resumo_por_motorista.to_excel(caminho_saida, index=False)

        print(f"\nProcesso concluído!")
        print(
            f"O arquivo de resumo '{nome_arquivo_saida}' foi criado em '{caminho_da_pasta}' para {len(resumo_por_motorista)} motoristas.")
