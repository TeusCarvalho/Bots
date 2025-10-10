# -*- coding: utf-8 -*-
# Custo e Arbitragem - Processamento
import pandas as pd
import os
from datetime import datetime

def format_currency(value):
    """Formata número em formato BRL"""
    formatted_value = f"{value:,.2f}"
    return formatted_value.replace(",", "X").replace(".", ",").replace("X", ".")

# --- PASTAS E ARQUIVOS ---
base_directory = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Custo'
file_name = 'Minha responsabilidade.xls'
file_path = os.path.join(base_directory, file_name)

output_file_name = 'Minha_responsabilidade_atualizada.xlsx'
output_file_path = os.path.join(base_directory, output_file_name)

# Pasta fixa para salvar versão de compartilhamento
output_shared = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Relatorios\Custos\Relatorio_Custos.xlsx"

coordenador_file_path = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx'

# --- COLUNAS ESPERADAS ---
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

# --- PROCESSAMENTO ---
if not os.path.exists(file_path):
    print(f"Erro: O arquivo '{file_path}' não foi encontrado.")
else:
    try:
        df = pd.read_excel(file_path, header=None, names=column_names)
        print("Dados carregados com sucesso!")

        # Filtrar só regionais GP
        df = df[df['Regional responsável'] == 'GP']

        # Adicionar coluna de custo estimado
        df['Custo Estimado'] = 0
        df.loc[df['Tipo de anomalia primária'] == 'Dano', 'Custo Estimado'] = 50.00
        df.loc[df['Tipo de anomalia primária'] == 'Perdido', 'Custo Estimado'] = 150.00
        df.loc[df['Tipo de anomalia primária'] == 'Atraso', 'Custo Estimado'] = 10.00

        # Se existir a planilha de coordenadores, junta só Nome da base + Coordenadores
        if os.path.exists(coordenador_file_path):
            df_coordenadores = pd.read_excel(coordenador_file_path)

            if 'Nome da base' in df_coordenadores.columns and 'Coordenadores' in df_coordenadores.columns:
                df = pd.merge(
                    df,
                    df_coordenadores[['Nome da base', 'Coordenadores']],
                    left_on='Base responsável',
                    right_on='Nome da base',
                    how='left'
                )
                df.drop('Nome da base', axis=1, inplace=True)
            else:
                print("⚠️ Planilha de coordenadores não tem as colunas esperadas (Nome da base, Coordenadores).")
        else:
            print(f"Arquivo de coordenadores não encontrado: {coordenador_file_path}")

        # --- ADICIONAR DATA DE PROCESSAMENTO (APENAS SE ESTIVER VAZIA) ---
        data_atual = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        if 'Data de processamento de retorno' in df.columns:
            df['Data de processamento de retorno'] = df['Data de processamento de retorno'].fillna(data_atual)
        else:
            df['Data de processamento de retorno'] = data_atual
        print(f"📌 Data de processamento registrada: {data_atual}")

        # --- REORDENAR COLUNAS ---
        try:
            colunas = list(df.columns)
            colunas.remove("Base responsável")
            colunas.remove("Coordenadores")
            nova_ordem = [df.columns[0], "Base responsável", "Coordenadores"] + [c for c in colunas if c not in [df.columns[0]]]
            df = df[nova_ordem]
            print("✅ Colunas reordenadas: 'Base responsável' em 2º e 'Coordenadores' em 3º lugar.")
        except Exception as e:
            print(f"⚠️ Não foi possível reordenar colunas: {e}")

        # Salvar Excel atualizado (original + atualizado)
        df.to_excel(output_file_path, index=False)
        print(f"\nArquivo salvo em {output_file_path}")

        # Salvar Excel fixo para compartilhamento no OneDrive
        os.makedirs(os.path.dirname(output_shared), exist_ok=True)
        df.to_excel(output_shared, index=False)
        print(f"📎 Arquivo compartilhado salvo em {output_shared}")

    except Exception as e:
        print(f"Ocorreu um erro ao processar o arquivo: {e}")