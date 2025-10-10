import pandas as pd
import requests
from pathlib import Path


def send_to_feishu_bot(webhook_url: str, message_content: dict) -> None:
    """
    Envia uma mensagem formatada para um bot do Feishu via webhook.

    Args:
        webhook_url (str): O URL do webhook do bot do Feishu.
        message_content (dict): O conteúdo da mensagem no formato JSON esperado pelo Feishu.
    """
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(webhook_url, headers=headers, json=message_content)
        response.raise_for_status()  # Lança uma exceção para erros HTTP
        print("Mensagem enviada com sucesso para o bot do Feishu.")
    except requests.exceptions.RequestException as e:
        print(f"ERRO ao enviar mensagem para o bot do Feishu: {e}")


def analyze_excel_data(directory_path: Path, filename_start: str, sheet_name: str, feishu_webhook_url: str) -> None:
    """
    Processa um arquivo Excel para analisar pacotes retidos e envia os resultados para o Feishu.

    O script busca o arquivo que começa com 'filename_start' no diretório fornecido.
    Em seguida, carrega a base de coordenadores, une as duas planilhas, filtra os
    dados pela regional 'GP', conta o número total de pacotes por 'Base de Entrega'
    e detalha essa contagem por dia ('Cluster Retidos'). Os resultados da análise
    são impressos no console, exportados para um novo arquivo Excel e enviados para
    um bot do Feishu.

    Args:
        directory_path (Path): O caminho da pasta onde o arquivo principal está.
        filename_start (str): O nome do arquivo principal a ser buscado.
        sheet_name (str): O nome da aba (sheet) que contém os dados.
        feishu_webhook_url (str): O URL do webhook do bot do Feishu para envio dos resultados.
    """
    # Define o caminho do arquivo de coordenadores
    coordinators_file_path = Path(r'C:\Users\JT-244\Desktop\Testes\Teste Base\Coordenador\Base_Atualizada.xlsx')

    # Verifica se o arquivo de coordenadores existe
    if not coordinators_file_path.is_file():
        print(f"ERRO: O arquivo de coordenadores não foi encontrado em '{coordinators_file_path}'.")
        print("Por favor, verifique o caminho e o nome do arquivo 'Base_Atualizada.xlsx'.")
        return

    # Procura pelo arquivo principal no diretório
    file_path = next((f for f in directory_path.iterdir() if f.name.startswith(filename_start) and f.suffix == '.xlsx'),
                     None)

    if not file_path:
        print(f"ERRO: Nenhum arquivo que começa com '{filename_start}' foi encontrado no diretório '{directory_path}'.")
        print("Por favor, verifique se o caminho da pasta e o nome do arquivo estão corretos.")
        return

    print(f"Lendo o arquivo Excel principal: '{file_path}'...")

    try:
        # --- LEITURA DOS ARQUIVOS ---
        df_main = pd.read_excel(file_path, sheet_name=sheet_name)
        df_coordenadores = pd.read_excel(coordinators_file_path)
        print("Arquivos lidos com sucesso.\n")

        # --- PREPARAÇÃO DOS DADOS: LIMPAR NOMES DE COLUNAS ---
        # Remove espaços em branco extras dos nomes das colunas para evitar erros de Key
        df_main.columns = df_main.columns.str.strip()
        df_coordenadores.columns = df_coordenadores.columns.str.strip()

        # --- VERIFICAÇÃO DAS COLUNAS NECESSÁRIAS ---
        required_main_cols = ['Base de Entrega 派件网点', 'regional nova 区域', 'Número do Pedido JMS 运单号',
                              'Cluster Retidos 分类']
        required_coordinators_cols = ['Nome da base', 'Coordenadores']

        if not all(col in df_main.columns for col in required_main_cols):
            missing_cols = [col for col in required_main_cols if col not in df_main.columns]
            print(f"ERRO: As seguintes colunas não foram encontradas no arquivo principal: {missing_cols}")
            return

        if not all(col in df_coordenadores.columns for col in required_coordinators_cols):
            missing_cols = [col for col in required_coordinators_cols if col not in df_coordenadores.columns]
            print(f"ERRO: As seguintes colunas não foram encontradas no arquivo de coordenadores: {missing_cols}")
            return

        # --- ETAPA 1: UNIR OS DADOS COM A TABELA DE COORDENADORES ---
        # A união é feita com base nas colunas especificadas. O 'how=left' garante
        # que todos os dados do arquivo principal sejam mantidos.
        df_merged = df_main.merge(
            df_coordenadores,
            left_on='Base de Entrega 派件网点',
            right_on='Nome da base',
            how='left'
        )

        # --- ETAPA 2: FILTRAR PELA COLUNA 'regional nova 区域' ---
        df_gp = df_merged[df_merged['regional nova 区域'] == 'GP'].copy()
        print("Filtragem pela coluna 'regional nova 区域' = 'GP' realizada com sucesso.")
        print(f"Número de registros após a filtragem: {len(df_gp)}\n")

        # Se não houver dados após a filtragem, interrompe a execução.
        if df_gp.empty:
            print("Nenhum registro encontrado para a regional 'GP'.")
            return

        # --- ETAPA 3: CONTAR O TOTAL DE PACOTES POR COORDENADOR ---
        # Agora o agrupamento é feito pela coluna 'Coordenadores'
        contagem_total_por_coordenador = df_gp.groupby('Coordenadores')['Número do Pedido JMS 运单号'].count()

        print("--- Contagem Total de Pacotes por Coordenador (Apenas 'GP') ---")
        print(contagem_total_por_coordenador.to_string())
        print("\n" + "=" * 50 + "\n")

        # --- ETAPA 4: CONTAR PACOTES POR COORDENADOR E POR DIA ---
        # Agrupamento feito por 'Coordenadores' e 'Cluster Retidos'
        contagem_por_coordenador_e_dia = df_gp.groupby(
            ['Coordenadores', 'Cluster Retidos 分类']
        )['Número do Pedido JMS 运单号'].count()

        print("--- Contagem de Pacotes por Coordenador e por Dia ('Cluster Retidos') ---")
        print(contagem_por_coordenador_e_dia.to_string())
        print("\n" + "=" * 50 + "\n")

        # --- ETAPA 5: EXPORTAR OS RESULTADOS PARA UM NOVO ARQUIVO EXCEL ---
        output_path = directory_path / 'Resultados_Processados.xlsx'
        with pd.ExcelWriter(output_path) as writer:
            contagem_total_por_coordenador.to_excel(writer, sheet_name='Contagem Total por Coordenador')
            contagem_por_coordenador_e_dia.to_excel(writer, sheet_name='Contagem por Coordenador e Dia')
            # Exporta os dados filtrados e com a nova coluna 'Coordenadores'
            df_gp.to_excel(writer, sheet_name='Dados Filtrados e Coordenadores', index=False)

        print(f"Resultados salvos com sucesso em '{output_path}'")

        # --- ETAPA 6: PREPARAR E ENVIAR MENSAGEM PARA O FEISHU ---

        # Formata a mensagem com a contagem total
        total_pacotes = len(df_gp)
        total_message = f"Total de Pacotes Retidos na regional GP: {total_pacotes}"

        # Formata a contagem por coordenador
        coordenador_messages = []
        for coordenador, qtd in contagem_total_por_coordenador.items():
            qtd_por_dia_str = ""
            try:
                # O .loc[coordenador] retorna uma série, onde o índice é 'Cluster Retidos'
                for dias, qtd_dia in contagem_por_coordenador_e_dia.loc[coordenador].items():
                    qtd_por_dia_str += f"- {dias}: {qtd_dia} pedidos\n"
            except KeyError:
                qtd_por_dia_str = "Nenhum dado de retenção por dia encontrado.\n"

            coordenador_messages.append(
                f"📍 Coordenador: {coordenador}\nQtd de Pacotes: {qtd}\n{qtd_por_dia_str}\n---\n")

        # Monta o conteúdo final da mensagem para o Feishu
        feishu_message_content = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": "Relatório de Pacotes Retidos - Regional GP",
                        "content": [
                            [
                                {
                                    "tag": "text",
                                    "text": total_message
                                }
                            ],
                            *[[{"tag": "text", "text": msg}] for msg in coordenador_messages]
                        ]
                    }
                }
            }
        }
        send_to_feishu_bot(feishu_webhook_url, feishu_message_content)

    except Exception as e:
        print(f"Ocorreu um erro ao processar o arquivo: {e}")
        print("Verifique se o nome das colunas ou da aba nos arquivos Excel está exatamente igual ao do código.")


# --- BLOCO DE EXECUÇÃO PRINCIPAL ---
if __name__ == '__main__':
    # Defina o caminho completo da pasta que contém o arquivo.
    caminho_da_pasta = Path(r'C:\Users\JT-244\Desktop\Testes\Retidos')

    # O script procurará por um arquivo que comece com este nome nesta pasta.
    nome_do_arquivo_base = 'Pacotes Retidos 网点滞留'

    # Nome da aba que contém os dados.
    nome_da_aba = '滞留明细表'

    # URL do webhook do bot do Feishu
    feishu_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b'

    analyze_excel_data(caminho_da_pasta, nome_do_arquivo_base, nome_da_aba, feishu_url)