# Para usar este script, você precisa ter as bibliotecas pandas, openpyxl e requests instaladas.
# Se não tiver, abra o terminal ou prompt de comando e execute:
# pip install pandas openpyxl requests

import pandas as pd
import os
import requests
import json

# --- 1. CONFIGURAÇÕES GERAIS ---
# Mude as variáveis aqui para ajustar o comportamento do script.

# Caminho da pasta onde o arquivo de SLA está localizado e onde o resultado será salvo
PASTA_SLA = r'C:\Users\JT-244\Desktop\Testes\SLA'

# Prefixo do nome do arquivo (para encontrá-lo mesmo com variações)
PREFIXO_ARQUIVO_SLA = '实际签收(T-1)(detalhes)'

# --- NOVAS CONFIGURAÇÕES ---
# Caminho da pasta onde o arquivo da Base Principal está localizado
PASTA_COORDENADOR = r'C:\Users\JT-244\Desktop\Testes\Teste Base\Coordenador'

# Nome do arquivo da planilha principal que será usada para obter o Coordenador
ARQUIVO_BASE_PRINCIPAL = 'Base_Atualizada.xlsx'

# Nome das colunas que serão utilizadas para a junção dos dados
COLUNA_BASE_ENTREGA = 'Base de entrega'  # Esta coluna já existe, mas é a chave de junção
COLUNA_COORDENADOR = 'Coordenador'

# --- FIM DAS NOVAS CONFIGURAÇÕES ---

# Nome do arquivo de saída que será gerado
NOME_ARQUIVO_SAIDA = 'Analise_SLA_Motorista.xlsx'

# Nomes das colunas que serão utilizadas na análise
COLUNA_ENTREGADOR = 'Entregador'
COLUNA_REMESSA = 'Remessa'
COLUNA_NO_PRAZO = 'Entregue no prazo？'

# Valor na coluna que indica uma entrega "no prazo".
VALOR_NO_PRAZO = 'Y'

# Meta de SLA em porcentagem
SLA_META = 95.0

# URL do webhook do Feishu
FEISHU_WEBHOOK_URL = 'https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b'


# --- 2. FUNÇÕES DO SCRIPT ---

def carregar_dados_sla(caminho_pasta, prefixo):
    """Procura e carrega o arquivo de SLA com base em um prefixo."""
    try:
        for arquivo in os.listdir(caminho_pasta):
            if arquivo.startswith(prefixo) and not arquivo.startswith('~'):  # Ignora arquivos temporários
                caminho_completo = os.path.join(caminho_pasta, arquivo)
                print(f"--- Carregando dados de: {caminho_completo} ---")
                if arquivo.endswith(('.xlsx', '.xls')):
                    return pd.read_excel(caminho_completo)
                else:  # Assume CSV
                    try:
                        return pd.read_csv(caminho_completo, sep=';', on_bad_lines='skip', encoding='utf-8')
                    except Exception:
                        return pd.read_csv(caminho_completo, sep=',', on_bad_lines='skip', encoding='utf-8')
    except FileNotFoundError:
        print(f"ERRO CRÍTICO: A pasta '{caminho_pasta}' não foi encontrada.")
        return None
    except Exception as e:
        print(f"ERRO CRÍTICO ao ler arquivo na pasta '{caminho_pasta}': {e}")
        return None

    print(f"ERRO CRÍTICO: Nenhum arquivo começando com '{prefixo}' foi encontrado.")
    return None


def carregar_e_processar_base_principal(caminho_pasta, nome_arquivo):
    """Carrega o arquivo da base principal, seleciona as colunas 'Base de entrega' e 'Coordenador'."""
    caminho_completo = os.path.join(caminho_pasta, nome_arquivo)
    print(f"\n--- Carregando dados da base principal: {caminho_completo} ---")
    try:
        df_base = pd.read_excel(caminho_completo)
        # Seleciona as colunas necessárias e remove duplicatas
        df_base_coordenador = df_base[[COLUNA_BASE_ENTREGA, COLUNA_COORDENADOR]].drop_duplicates()
        return df_base_coordenador
    except FileNotFoundError:
        print(
            f"AVISO: O arquivo da base principal '{caminho_completo}' não foi encontrado. A coluna 'Coordenador' não será incluída.")
        return None
    except KeyError:
        print(
            f"AVISO: Uma das colunas '{COLUNA_BASE_ENTREGA}' ou '{COLUNA_COORDENADOR}' não foi encontrada na base principal. A coluna 'Coordenador' não será incluída.")
        return None
    except Exception as e:
        print(f"ERRO ao ler a base principal: {e}. A coluna 'Coordenador' não será incluída.")
        return None


def analisar_sla_completo(df, df_base_coordenador=None):
    """
    Executa a análise de SLA e retorna dois DataFrames:
    1. Resumo das bases abaixo da meta.
    2. Detalhe dos entregadores abaixo da meta.
    """
    # Verifica se as colunas necessárias existem
    colunas_necessarias = [COLUNA_ENTREGADOR, COLUNA_BASE_ENTREGA, COLUNA_REMESSA, COLUNA_NO_PRAZO]
    if not all(col in df.columns for col in colunas_necessarias):
        print(f"ERRO: Faltam colunas essenciais para a análise. Verifique se {colunas_necessarias} existem no arquivo.")
        return pd.DataFrame(), pd.DataFrame()  # Retorna DataFrames vazios para evitar erros

    # Função interna para calcular SLA
    def calcular_sla(dataframe, grupo_por):
        df_agrupado = dataframe.groupby(grupo_por)
        total_pedidos = df_agrupado[COLUNA_REMESSA].count()
        entregas_prazo = dataframe[dataframe[COLUNA_NO_PRAZO] == VALOR_NO_PRAZO].groupby(grupo_por)[
            COLUNA_REMESSA].count()

        resultado = pd.DataFrame({'Total de Pedidos': total_pedidos, 'Entregues no Prazo': entregas_prazo})
        resultado['Entregues no Prazo'] = resultado['Entregues no Prazo'].fillna(0).astype(int)
        resultado['SLA_Raw'] = (resultado['Entregues no Prazo'] / resultado['Total de Pedidos'].replace(0, 1)) * 100
        return resultado

    # 1. Calcular SLA por Base e filtrar pelos resultados abaixo da meta
    sla_bases = calcular_sla(df, COLUNA_BASE_ENTREGA)
    sla_bases_abaixo_meta = sla_bases[sla_bases['SLA_Raw'] < SLA_META].sort_values(by='SLA_Raw', ascending=True)

    # Adiciona a coluna Coordenador se a base principal estiver disponível
    if df_base_coordenador is not None:
        sla_bases_abaixo_meta = pd.merge(sla_bases_abaixo_meta, df_base_coordenador, on=COLUNA_BASE_ENTREGA, how='left')
        sla_bases_abaixo_meta.set_index(COLUNA_BASE_ENTREGA, inplace=True)  # Mantém a Base como índice

    if not sla_bases_abaixo_meta.empty:
        sla_bases_abaixo_meta['SLA (%)'] = sla_bases_abaixo_meta['SLA_Raw'].map('{:.2f}%'.format)
        # Reorganiza as colunas para incluir o Coordenador
        if df_base_coordenador is not None:
            resumo_bases_formatado = sla_bases_abaixo_meta[
                [COLUNA_COORDENADOR, 'Total de Pedidos', 'Entregues no Prazo', 'SLA (%)']]
        else:
            resumo_bases_formatado = sla_bases_abaixo_meta[['Total de Pedidos', 'Entregues no Prazo', 'SLA (%)']]
        print(f"\n--- Resumo do SLA por Base de Entrega abaixo de {SLA_META}% (Ordenado por Performance) ---")
        print(resumo_bases_formatado.to_string())
    else:
        resumo_bases_formatado = pd.DataFrame()  # Cria um DataFrame vazio se não houver bases abaixo da meta
        print(f"\n--- Todas as bases atingiram a meta de {SLA_META}% de SLA. ---")

    # 2. Calcular SLA por Entregador
    sla_entregadores = calcular_sla(df, COLUNA_ENTREGADOR)

    # 3. Filtrar apenas os entregadores abaixo da meta para o relatório de problemas
    entregadores_abaixo_meta_raw = sla_entregadores[sla_entregadores['SLA_Raw'] < SLA_META]
    df_problemas_consolidados = pd.DataFrame()  # Inicializa um DataFrame vazio

    if not entregadores_abaixo_meta_raw.empty:
        # Prepara o DataFrame com os detalhes dos entregadores abaixo da meta
        mapa_entregador_base = df[[COLUNA_ENTREGADOR, COLUNA_BASE_ENTREGA]].drop_duplicates()
        df_problemas_consolidados = pd.merge(
            entregadores_abaixo_meta_raw.reset_index(),
            mapa_entregador_base,
            on=COLUNA_ENTREGADOR,
            how='left'
        ).sort_values(by='SLA_Raw', ascending=True)

        # Adiciona a coluna Coordenador se a base principal estiver disponível
        if df_base_coordenador is not None:
            df_problemas_consolidados = pd.merge(df_problemas_consolidados, df_base_coordenador, on=COLUNA_BASE_ENTREGA,
                                                 how='left')

        df_problemas_consolidados['SLA (%)'] = df_problemas_consolidados['SLA_Raw'].map('{:.2f}%'.format)

        # Reorganiza as colunas para incluir o Coordenador
        if df_base_coordenador is not None:
            colunas_finais = [COLUNA_COORDENADOR, COLUNA_BASE_ENTREGA, COLUNA_ENTREGADOR, 'Total de Pedidos',
                              'Entregues no Prazo', 'SLA (%)']
        else:
            colunas_finais = [COLUNA_BASE_ENTREGA, COLUNA_ENTREGADOR, 'Total de Pedidos', 'Entregues no Prazo',
                              'SLA (%)']

        df_problemas_consolidados = df_problemas_consolidados[colunas_finais]

        print(f"\n\n--- DETALHAMENTO: Entregadores com SLA abaixo de {SLA_META}% (Ordenado por Performance) ---")
        print(df_problemas_consolidados.to_string(index=False))
    else:
        print(f"\n\n--- Todos os entregadores atingiram a meta de {SLA_META}% de SLA. ---")

    return resumo_bases_formatado, df_problemas_consolidados


def salvar_resultados_excel(caminho_arquivo, df_resumo_bases_abaixo_meta, df_detalhes_entregadores):
    """
    Salva apenas os dataframes das bases e entregadores abaixo da meta em abas
    separadas de um arquivo Excel.
    """
    try:
        with pd.ExcelWriter(caminho_arquivo, engine='openpyxl') as writer:
            # Aba 1: Resumo das Bases Abaixo da Meta (se houver)
            if not df_resumo_bases_abaixo_meta.empty:
                df_resumo_bases_abaixo_meta.to_excel(writer, sheet_name='Bases Abaixo da Meta', index=True)

            # Aba 2: Entregadores Abaixo da Meta (se houver)
            if not df_detalhes_entregadores.empty:
                df_detalhes_entregadores.to_excel(writer, sheet_name='Entregadores Abaixo da Meta', index=False)

        print(f"\n--- Relatório salvo com sucesso em: {caminho_arquivo} ---")
    except Exception as e:
        print(f"\nERRO CRÍTICO ao salvar o arquivo Excel: {e}")


def enviar_notificacao_feishu(df_bases, df_entregadores):
    """
    Formata e envia uma mensagem para o webhook do Feishu com as 3 piores bases
    e os 3 piores entregadores, agrupados por base.
    """
    print("\n--- Enviando notificação para o Feishu ---")

    # Início da mensagem
    conteudo_mensagem = "Análise de SLA Concluída!\n\n"

    # Se todas as bases atingiram a meta, informa e encerra
    if df_bases.empty:
        conteudo_mensagem += "Todas as bases e entregadores atingiram a meta de SLA."
    else:
        # Pega as 3 primeiras (piores) bases
        top_3_bases = df_bases.head(3)
        conteudo_mensagem += "Relatório de Bases e Entregadores Abaixo da Meta:\n\n"

        # Reseta o índice para poder iterar (incluindo o nome da Base)
        top_3_bases = top_3_bases.reset_index()

        for _, base_info in top_3_bases.iterrows():
            base_name = base_info[COLUNA_BASE_ENTREGA]
            # Adiciona a base e seu SLA
            conteudo_mensagem += f"Base de Entrega: {base_name} ({base_info['SLA (%)']})\n"

            # Adiciona o Coordenador se a coluna existir
            if COLUNA_COORDENADOR in base_info:
                coordenador = base_info.get(COLUNA_COORDENADOR, 'N/A')
                conteudo_mensagem += f"Coordenador: {coordenador}\n"

            # Filtra os entregadores dessa base e pega os 3 piores
            entregadores_da_base = df_entregadores[df_entregadores[COLUNA_BASE_ENTREGA] == base_name].head(3)

            # Adiciona os entregadores
            if not entregadores_da_base.empty:
                for _, entregador_info in entregadores_da_base.iterrows():
                    conteudo_mensagem += f" - Entregador: {entregador_info[COLUNA_ENTREGADOR]} - {entregador_info['SLA (%)']}\n"
            else:
                conteudo_mensagem += " - Nenhum entregador com SLA abaixo da meta.\n"

            conteudo_mensagem += "\n"  # Adiciona uma linha em branco para separar as bases

    # Dados para o webhook
    payload = {
        "msg_type": "text",
        "content": {
            "text": conteudo_mensagem
        }
    }

    try:
        response = requests.post(FEISHU_WEBHOOK_URL, json=payload)
        response.raise_for_status()  # Levanta um erro se a resposta não for 200
        print("Notificação enviada com sucesso!")
    except requests.exceptions.HTTPError as http_err:
        print(f"Erro HTTP ao enviar notificação: {http_err}")
    except Exception as err:
        print(f"Outro erro ao enviar notificação: {err}")


# --- 3. EXECUÇÃO PRINCIPAL ---

def main():
    """Orquestra a execução do script de análise de SLA."""
    print("Iniciando processo de análise de SLA...")

    df_sla = carregar_dados_sla(PASTA_SLA, PREFIXO_ARQUIVO_SLA)

    if df_sla is None:
        print("\nProcesso interrompido. Não foi possível carregar o arquivo de SLA.")
        return

    # NOVO PASSO: Carrega e processa a base principal
    df_base_principal = carregar_e_processar_base_principal(PASTA_COORDENADOR, ARQUIVO_BASE_PRINCIPAL)

    # A função de análise agora recebe a base principal como argumento opcional
    resumo_bases_abaixo, detalhes_entregadores = analisar_sla_completo(df_sla, df_base_principal)

    if not resumo_bases_abaixo.empty or not detalhes_entregadores.empty:
        caminho_saida_excel = os.path.join(PASTA_SLA, NOME_ARQUIVO_SAIDA)
        salvar_resultados_excel(caminho_saida_excel, resumo_bases_abaixo, detalhes_entregadores)

        # Chama a nova função para enviar a notificação ao Feishu
        enviar_notificacao_feishu(resumo_bases_abaixo, detalhes_entregadores)


if __name__ == "__main__":
    main()