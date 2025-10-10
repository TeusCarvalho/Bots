import pandas as pd
import numpy as np
import os

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================

# Pasta fixa com os dados de ShippingTime
MAIN_DATA_DIRECTORY = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\ShippingTime"

# Pasta fixa com os dados de Coordenadores
COORDINATOR_DIRECTORY = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador"

# Prefixo esperado para o arquivo principal
MAIN_FILE_PREFIX = "全网时效分析(新)-Lista"

# =============================================================================
# Funções de Processamento de Dados
# =============================================================================

def find_latest_file(directory, prefix=None):
    """Encontra o arquivo mais recente em um diretório, opcionalmente filtrando por prefixo."""
    try:
        files_in_dir = os.listdir(directory)
        matching_files = [
            f for f in files_in_dir
            if f.lower().endswith('.xlsx') and (prefix is None or f.startswith(prefix))
        ]

        if not matching_files:
            print(f"Nenhum arquivo encontrado em '{directory}' (prefixo={prefix}).")
            return None

        latest_file = max(matching_files, key=lambda f: os.path.getmtime(os.path.join(directory, f)))
        return os.path.join(directory, latest_file)
    except FileNotFoundError:
        print(f"Erro: O diretório especificado não foi encontrado: '{directory}'")
        return None
    except Exception as e:
        print(f"Ocorreu um erro ao procurar o arquivo: {e}")
        return None


def load_data_from_path(filepath, nome="Arquivo"):
    """Lê um arquivo XLSX de um caminho local e o converte para um DataFrame."""
    if not filepath:
        print(f"Erro: {nome} não encontrado.")
        return None
    try:
        df = pd.read_excel(filepath, engine='openpyxl')
        print(f"{nome} '{os.path.basename(filepath)}' carregado com sucesso.")
        return df
    except Exception as e:
        print(f"Erro ao ler {nome}: {e}")
        return None


def map_data(df):
    """Processa o DataFrame bruto para extrair as colunas necessárias para a análise."""
    df_mapped = pd.DataFrame({
        'id': df.get('Número de pedido JMS', pd.Series(dtype='str')),
        'base_entrega': df.get('PDD de Entrega', pd.Series(dtype='str')),
        'coordenador': df.get('Coordenadores', pd.Series(dtype='str')),
        # Colunas para cálculo de médias
        'tempo_transito_sc_base': df.get('Tempo trânsito SC Destino->Base Entrega'),
        'tempo_proc_base': df.get('Tempo médio processamento Base Entrega'),
        'tempo_saida_entrega': df.get('Tempo médio Saída para Entrega->Entrega')
    })

    return df_mapped.fillna('N/D')


def calculate_metrics(df):
    """Calcula a quantidade de remessas e as médias de tempo e retorna um dicionário."""
    if df.empty:
        return {
            'quantidade_remessas': 0,
            'media_transito': np.nan,
            'media_proc': np.nan,
            'media_saida': np.nan,
        }

    quantidade_remessas = df['id'].nunique()
    media_transito = pd.to_numeric(df['tempo_transito_sc_base'], errors='coerce').mean()
    media_proc = pd.to_numeric(df['tempo_proc_base'], errors='coerce').mean()
    media_saida = pd.to_numeric(df['tempo_saida_entrega'], errors='coerce').mean()

    return {
        'quantidade_remessas': quantidade_remessas,
        'media_transito': media_transito,
        'media_proc': media_proc,
        'media_saida': media_saida,
    }


def display_metrics(metrics, title="Resumo"):
    """Exibe um conjunto de métricas formatadas no console."""
    media_transito_str = f"{metrics['media_transito']:.1f} h" if not pd.isna(metrics['media_transito']) else "N/D"
    media_proc_str = f"{metrics['media_proc']:.1f} h" if not pd.isna(metrics['media_proc']) else "N/D"
    media_saida_str = f"{metrics['media_saida']:.1f} h" if not pd.isna(metrics['media_saida']) else "N/D"

    print("\n" + "=" * 55)
    print(f"  {title}")
    print("=" * 55)
    print(f"  Quantidade de Remessas              : {metrics['quantidade_remessas']}")
    print("-" * 55)
    print(f"  T. Médio Trânsito SC->Base          : {media_transito_str}")
    print(f"  T. Médio Processamento Base         : {media_proc_str}")
    print(f"  T. Médio Saída para Entrega->Entrega : {media_saida_str}")
    print("=" * 55)


# =============================================================================
# Execução Principal do Script
# =============================================================================
def main():
    print("=" * 50)
    print("   Analisador de Dados de Logística - JMS Express")
    print("=" * 50)

    print(f"Procurando o arquivo de dados mais recente em: {MAIN_DATA_DIRECTORY}")
    filepath = find_latest_file(MAIN_DATA_DIRECTORY, MAIN_FILE_PREFIX)

    print(f"Procurando o arquivo de coordenadores em: {COORDINATOR_DIRECTORY}")
    coordinator_filepath = find_latest_file(COORDINATOR_DIRECTORY)

    # Carregar arquivos
    df_raw = load_data_from_path(filepath, nome="Arquivo Principal")
    df_coordinators = load_data_from_path(coordinator_filepath, nome="Arquivo de Coordenadores")

    # Filtragem GP
    if df_raw is not None and 'Regional de Entrega' in df_raw.columns:
        print("\nFiltrando dados para manter apenas a Regional 'GP'...")
        df_raw = df_raw[df_raw['Regional de Entrega'] == 'GP']
        print("Filtragem concluída.")
        if df_raw.empty:
            print("Aviso: A planilha filtrada para 'GP' está vazia.")

    # Processamento
    if df_raw is not None and df_coordinators is not None:
        df_merged = pd.merge(df_raw, df_coordinators,
                             left_on='PDD de Entrega',
                             right_on='Nome da base',
                             how='left')
        df_processed = map_data(df_merged)

        # Resultados gerais
        general_metrics = calculate_metrics(df_processed)
        display_metrics(general_metrics, title="Resultados Gerais (Regional GP)")

        summary_list = []
        if 'coordenador' in df_processed.columns and df_processed['coordenador'].nunique() > 1:
            coordinators = sorted([c for c in df_processed['coordenador'].unique() if c != 'N/D'])

            print("\n\n" + "=" * 60)
            print(f"{'Análise Detalhada por Coordenador (Regional GP)':^60}")
            print("=" * 60)

            for coordinator in coordinators:
                df_coordinator = df_processed[df_processed['coordenador'] == coordinator]
                coordinator_metrics = calculate_metrics(df_coordinator)

                display_metrics(coordinator_metrics, title=f"Coordenador: {coordinator}")

                coordinator_metrics['Coordenador'] = coordinator
                summary_list.append(coordinator_metrics)

            if summary_list:
                summary_df = pd.DataFrame(summary_list)
                total_row = {
                    'Coordenador': 'Total Geral',
                    'quantidade_remessas': general_metrics['quantidade_remessas'],
                    'media_transito': general_metrics['media_transito'],
                    'media_proc': general_metrics['media_proc'],
                    'media_saida': general_metrics['media_saida'],
                }
                summary_df = pd.concat([summary_df, pd.DataFrame([total_row])], ignore_index=True)

                summary_df.rename(columns={
                    'quantidade_remessas': 'Qtd. Remessas',
                    'media_transito': 'T. Médio Trânsito (h)',
                    'media_proc': 'T. Médio Proc. (h)',
                    'media_saida': 'T. Médio Saída (h)'
                }, inplace=True)

                for col in ['T. Médio Trânsito (h)', 'T. Médio Proc. (h)', 'T. Médio Saída (h)']:
                    summary_df[col] = summary_df[col].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/D")

                summary_df = summary_df[
                    ['Coordenador', 'Qtd. Remessas', 'T. Médio Trânsito (h)',
                     'T. Médio Proc. (h)', 'T. Médio Saída (h)']]

                print("\n\n" + "=" * 80)
                print(f"{'Resumo Final por Coordenador (Regional GP)':^80}")
                print("=" * 80)
                print(summary_df.to_string(index=False))
                print("=" * 80)
        else:
            print("\nNão foi possível separar por coordenador.")


if __name__ == '__main__':
    main()
