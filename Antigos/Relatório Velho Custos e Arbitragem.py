import pandas as pd
import os
import requests

def format_currency(value):
     # Formata o número com separador de milhares e duas casas decimais
    formatted_value = f"{value:,.2f}"
    # Substitui o separador de milhares por ponto e o decimal por vírgula
    return formatted_value.replace(",", "X").replace(".", ",").replace("X", ".")


# A nova lista de nomes de colunas fornecida.
column_names = [
    'Número de declaração', 'Remessa', 'Tipo de produto', 'Tipo de anomalia primária',
    'Tipo de anomalia secundária', 'Dias de atraso', 'Status de arbitragem', 'Base remetente',
    'Regional Remetente', 'Declarante', 'Declarante No.', 'Data de declaração',
    'Origem da Solicitação', 'Regional de declaração', 'Data de recebimento da arbitragem',
    'Data de distribuição da arbitragem', 'Data de decisão de arbitragem', 'Data de contestação',
    'Data da última edição', 'Data de distribuição da contestação', 'Data de decisão da contestação',
    'Data de processamento de retorno', 'Valor do item', 'Processador de arbitragem',
    'Processador de contestação', 'Tipo de produto', 'Conteúdo do pacote',
    'Descrição de anomalia', 'Data de fechamento', 'Tipo de decisão', 'Base responsável',
    'Regional responsável', 'Valor a pagar (yuan)', 'Taxa de manuseio (yuan)',
    'Valor da arbitragem (yuan)', 'Base de liquidação financeira',
    'Comentários de decisão de arbitragem', 'Comentários de decisão de contestação',
    'Processador de retorno', 'Comentário de processamento de retorno', 'Tempo de processamento de retorno',
    'Resposta da parte responsável', 'Fonte', 'Origem do Pedido', 'Hora de envio',
    'Horário de coleta', 'Horário de Previsão de Entrega SLA Cadeia',
    'Responsável pela entrega', 'Horário da entrega', 'Peso cobrável',
    'Tempo restante de processamento', 'Número do cliente', 'Nome do cliente',
    'Etapa de decisão de responsabilidade'
]

# Define o diretório base onde o arquivo de origem está e onde o novo arquivo será salvo.
base_directory = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Teste Base\Custo'

# Define o nome do arquivo fixo.
file_name = 'Minha responsabilidade.xls'

# Constrói o caminho completo para o arquivo de entrada.
file_path = os.path.join(base_directory, file_name)

# Define o nome e o caminho para o novo arquivo Excel de saída.
output_file_name = 'Minha_responsabilidade_atualizada.xlsx'
output_file_path = os.path.join(base_directory, output_file_name)

# Verifique se o arquivo de entrada existe antes de tentar ler.
if not os.path.exists(file_path):
    print(f"Erro: O arquivo '{file_path}' não foi encontrado.")
    print("Por favor, verifique se o nome do arquivo está correto e se ele está no diretório especificado.")
else:
    try:
        # Carregue os dados do arquivo Excel para um DataFrame do pandas.
        # Use 'header=None' e 'names=column_names' para aplicar os nomes de coluna fornecidos.
        df = pd.read_excel(file_path, header=None, names=column_names)

        print("Dados carregados com sucesso!")
        print("\n--- Informações iniciais do DataFrame ---")
        print(df.info())

        print("\n--- Primeiras 5 linhas do DataFrame ---")
        print(df.head())

        # --- Seção para Manipulação e Análise de Dados ---

        # Filtra os dados para manter apenas a "Regional responsável" como "GP"
        print("\n--- Filtrando dados para manter apenas a região 'GP' ---")
        df = df[df['Regional responsável'] == 'GP']
        print("Filtro aplicado com sucesso. O DataFrame agora contém apenas a região 'GP'.")

        # Adicionar uma nova coluna 'Custo Estimado'
        print("\n--- Adicionando a nova coluna 'Custo Estimado' ---")
        df['Custo Estimado'] = 0
        df.loc[df['Tipo de anomalia primária'] == 'Dano', 'Custo Estimado'] = 50.00
        df.loc[df['Tipo de anomalia primária'] == 'Perdido', 'Custo Estimado'] = 150.00
        df.loc[df['Tipo de anomalia primária'] == 'Atraso', 'Custo Estimado'] = 10.00
        print("Nova coluna 'Custo Estimado' adicionada com sucesso!")

        # Juntar a planilha de Coordenadores
        print("\n--- Adicionando a coluna 'Coordenadores' ---")

        # Define o caminho do arquivo de coordenadores
        coordenador_file_path = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Teste Base\Coordenador\Base_Atualizada.xlsx'

        if not os.path.exists(coordenador_file_path):
            print(f"Erro: O arquivo de coordenadores '{coordenador_file_path}' não foi encontrado.")
            print("Verifique se o nome e o caminho do arquivo estão corretos.")
        else:
            # Carrega a planilha de coordenadores
            df_coordenadores = pd.read_excel(coordenador_file_path)

            # Junta os dois DataFrames
            # 'how=left' mantém todas as linhas do DataFrame original (df)
            df = pd.merge(df, df_coordenadores, left_on='Base responsável', right_on='Nome da base', how='left')

            # Opcional: Remova a coluna 'Nome da base' da planilha de coordenadores, já que ela é uma duplicata
            df.drop('Nome da base', axis=1, inplace=True)

            print("Coluna 'Coordenadores' adicionada com sucesso!")

            # --- Seção de Geração do Relatório e Envio para o Feishu ---

            # Inicializa a string que conterá o relatório completo com o título.
            report_message = "Relatório de Custo e Arbitragem\n"

            # Calcula a quantidade total de pedidos processados
            total_pedidos = len(df)
            report_message += f"Qtd de Pedidos Processados: {total_pedidos}\n"

            # Calcula o valor total por cada tipo de anomalia
            valor_por_problema = df.groupby('Tipo de anomalia primária')['Valor a pagar (yuan)'].sum().reset_index(
                name='Valor Total')
            report_message += "\n--- Valor Total por Tipo de Problemática ---\n"
            for index, row in valor_por_problema.iterrows():
                report_message += f"📍 {row['Tipo de anomalia primária']}: R$ {format_currency(row['Valor Total'])}\n"

            # Agrupa por Coordenador e conta os pedidos
            pedidos_por_coordenador = df.groupby('Coordenadores').size().reset_index(name='Qtd de Pedidos')

            # Agrupa por Coordenador e Tipo Problemático para a lista de problemas e seus valores
            problemas_por_coordenador = df.groupby(['Coordenadores', 'Tipo de anomalia primária']).agg(
                Quantidade=('Tipo de anomalia primária', 'count'),
                Valor=('Valor a pagar (yuan)', 'sum')
            ).reset_index()

            # Agrupa por Coordenador e soma o valor a pagar
            valor_por_coordenador = df.groupby('Coordenadores')['Valor a pagar (yuan)'].sum().reset_index(
                name='Valor Total')

            # Itera sobre o resultado do total de pedidos por coordenador e adiciona ao relatório
            for index, row in pedidos_por_coordenador.iterrows():
                coordenador = row['Coordenadores']
                qtd_pedidos = row['Qtd de Pedidos']
                # Encontra o valor total para o coordenador atual
                valor_total = \
                valor_por_coordenador.loc[valor_por_coordenador['Coordenadores'] == coordenador, 'Valor Total'].iloc[0]

                report_message += "---\n"
                report_message += f"📍 Coordenador: {coordenador}\n"
                report_message += f"Qtd de Pedidos: {qtd_pedidos}\n"
                report_message += f"Valor a Pagar (R$): {format_currency(valor_total)}\n"

                # Filtra os problemas para o coordenador atual
                problemas_do_coordenador = problemas_por_coordenador[
                    problemas_por_coordenador['Coordenadores'] == coordenador]

                # Adiciona os problemas e suas quantidades/valores
                report_message += "Problemáticas:\n"
                if not problemas_do_coordenador.empty:
                    for _, prob_row in problemas_do_coordenador.iterrows():
                        problema = prob_row['Tipo de anomalia primária']
                        quantidade = prob_row['Quantidade']
                        valor = prob_row['Valor']
                        report_message += f"  - {problema}: {quantidade} pedidos - R$ {format_currency(valor)}\n"
                else:
                    report_message += "  - Nenhuma problemática registrada.\n"

            report_message += "---\n"

            # Imprime o relatório no console.
            print("\n--- Relatório Final por Coordenador (no console) ---")
            print(report_message)

            # Envia a mensagem para o Feishu
            feishu_url = "https://open.feishu.cn/open-apis/bot/v2/hook/28c742fc-affd-49d7-926b-253fceb42e22"

            # O payload deve ser um JSON com o campo 'text'
            payload = {
                "msg_type": "text",
                "content": {
                    "text": report_message
                }
            }

            try:
                response = requests.post(feishu_url, json=payload)
                response.raise_for_status()  # Levanta um erro para status de erro (4xx ou 5xx)
                print("\n--- Status de Envio para o Feishu ---")
                print("Mensagem enviada com sucesso para o Feishu!")
            except requests.exceptions.HTTPError as errh:
                print(f"Erro HTTP: {errh}")
            except requests.exceptions.ConnectionError as errc:
                print(f"Erro de Conexão: {errc}")
            except requests.exceptions.Timeout as errt:
                print(f"Timeout: {errt}")
            except requests.exceptions.RequestException as err:
                print(f"Ocorreu um erro ao enviar a mensagem para o Feishu: {err}")


            # --- Seção para salvar o novo arquivo Excel ---
            print(f"\n--- Salvando o DataFrame atualizado em '{output_file_name}' ---")

            # Salva o DataFrame final em um novo arquivo Excel
            # O 'index=False' evita que o pandas salve o índice do DataFrame como uma coluna.
            df.to_excel(output_file_path, index=False)

            print(f"O arquivo '{output_file_name}' foi salvo com sucesso em '{base_directory}'.")


    except Exception as e:
        print(f"Ocorreu um erro ao processar o arquivo: {e}")