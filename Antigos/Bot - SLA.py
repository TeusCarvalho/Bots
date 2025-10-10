# -*- coding: utf-8 -*-
"""
Script para gerar relatórios diários de SLA (Service Level Agreement),
calcular métricas por coordenador e regional, e enviar resumos via Feishu.
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List
import time
import hmac
import hashlib
import base64
import json
import requests
from datetime import datetime, timedelta
import os
import traceback

# ==============================================================================
# --- BLOCO DE CONFIGURAÇÃO ---
# ==============================================================================
# ATENÇÃO: Altere os caminhos e valores abaixo para os locais corretos.

# --- 1. Configurações de Arquivos ---
PASTA_DO_RELATORIO = Path(r'C:\Users\JT-244\Desktop\Testes\SLA')
PASTA_DESTINO_RELATORIO = Path(r'C:\Users\JT-244\OneDrive - Speed Rabbit Express Ltda\SLA - Relatórios')
PREFIXO_ARQUIVO_ENTRADA = '实际签收(T-1)(detalhes)'
NOME_ARQUIVO_SAIDA = 'Relatorio_SLA_Simplificado.xlsx'
NOME_ARQUIVO_NAO_MAPEADOS = 'Relatorio_Pedidos_Nao_Mapeados.xlsx'

# --- 2. Configurações de Mapeamento ---
PASTA_BASE_COORDENADORES = Path(r'C:\Users\JT-244\Desktop\Testes\Teste Base\Coordenador')
COL_MAP_BASE = 'Nome da base'
COL_MAP_COORDENADOR = 'Coordenadores'

# --- 3. Nomes das Colunas nos Dados Principais ---
COL_REMESSA = 'Remessa'
COL_BASE_ENTREGA = 'Base de entrega'
COL_REGIONAL_ENTREGA = 'Regional de entrega'
COL_ENTREGUE_NO_PRAZO = 'Entregue no prazo？'
COL_DATA_PREVISTA = 'Data prevista de entrega'
COL_TURNO = 'Turno de linha secundária'

# --- 4. Valores de Status e Filtros ---
STATUS_NO_PRAZO = 'Y'
STATUS_FORA_DO_PRAZO = 'N'
STATUS_FORA_DO_PRAZO_2 = ' '
VALOR_TURNO_FILTRO = '1Rit'

# --- 5. Configuração do Feishu (Múltiplos Grupos) ---
FEISHU_GROUPS = [
{
        "name": "Indicadores Operacionais | Qualidade e Redes",
        "url": os.getenv("FEISHU_WEBHOOK_URL_SLA_2",
                         "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b"),
        "secret": os.getenv("FEISHU_SECRET_KEY_SLA_2", "GOreUcd9cTaWTVE4IHiAbh")
    },
#
#{
#        "name": "Grupo de Dados GO",
#        "url": os.getenv("FEISHU_WEBHOOK_URL_SLA_2",
#                         "https://open.feishu.cn/open-apis/bot/v2/hook/7b9ae3c3-e645-4367-85e5-e8e8aa11d808"),
#        "secret": os.getenv("FEISHU_SECRET_KEY_SLA_2", "GOreUcd9cTaWTVE4IHiAbh")
#    },
#{
#        "name": "Ronaldinho do BOT",
#        "url": os.getenv("FEISHU_WEBHOOK_URL_SLA_2",
#                         "https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b"),
#        "secret": os.getenv("FEISHU_SECRET_KEY_SLA_2", "GOreUcd9cTaWTVE4IHiAbh")
#    }

    # Adicione mais grupos aqui se necessário
]

# --- 6. Configuração do Link da Pasta ---
ONEDRIVE_FOLDER_URL = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EnavuWCRgAxOuo4-3dpPdJkBAOwHV-XPWXJ0eamBhUS4FA?e=LKpkwg"

# --- 7. Configuração da Limpeza de Arquivos ---
DIAS_PARA_MANTER_RELATORIOS = 7  # Apaga arquivos .xlsx mais antigos que 7 dias


# ==============================================================================
# --- FUNÇÕES DE NOTIFICAÇÃO (FEISHU) ---
# ==============================================================================

def _gerar_assinatura_feishu(secret: str, timestamp: int) -> str:
    """Gera a assinatura HMAC-SHA256 para autenticação do webhook do Feishu."""
    string_to_sign = f'{timestamp}\n{secret}'
    hmac_code = hmac.new(string_to_sign.encode('utf-8'), digestmod=hashlib.sha256).digest()
    return base64.b64encode(hmac_code).decode('utf-8')


def enviar_notificacao_feishu(webhook_url: str, secret_key: Optional[str], texto_metricas: str,
                              data_relatorio: datetime):
    """Constrói e envia a notificação de resumo de SLA para um webhook específico do Feishu."""
    if not webhook_url or "COLOQUE_A_URL" in webhook_url:
        print(f"AVISO: URL do webhook inválida ou não configurada. Pulando envio. URL: {webhook_url}")
        return

    titulo = f"📊 Resumo Diário de SLA - {data_relatorio.strftime('%d/%m/%Y')} (Turno: {VALOR_TURNO_FILTRO})"

    elements = [{"tag": "div", "text": {"tag": "lark_md", "content": texto_metricas}}]

    actions = []
    if ONEDRIVE_FOLDER_URL:
        actions.append({
            "tag": "button",
            "text": {"tag": "plain_text", "content": "📂 Acessar Pasta de Relatórios"},
            "type": "primary",
            "url": ONEDRIVE_FOLDER_URL
        })

    payload = {
        "msg_type": "interactive",
        "card": {
            "header": {"title": {"tag": "plain_text", "content": titulo}, "template": "blue"},
            "elements": elements
        }
    }

    if actions:
        payload["card"]["elements"].append({"tag": "action", "actions": actions})

    if secret_key and "COLOQUE_O_SEGREDO" not in secret_key:
        timestamp = int(time.time())
        payload['timestamp'] = timestamp
        payload['sign'] = _gerar_assinatura_feishu(secret_key, timestamp)

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        response_json = response.json()
        if response_json.get("StatusCode") == 0 or response_json.get("code") == 0:
            print("Resumo do SLA enviado com sucesso!")
        else:
            print(f"AVISO: Feishu retornou um erro: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"ERRO: Falha ao enviar notificação para o Feishu: {e}")


# ==============================================================================
# --- FUNÇÕES AUXILIARES ---
# ==============================================================================

def limpar_relatorios_antigos(pasta: Path, dias_a_manter: int):
    """Apaga arquivos .xlsx na pasta de destino que são mais antigos que o número de dias especificado."""
    print(f"--- Iniciando limpeza de relatórios antigos (mais de {dias_a_manter} dias) ---")
    if not pasta.is_dir():
        print(f"AVISO: A pasta de destino '{pasta}' não existe. Nenhuma limpeza será feita.")
        return

    limite_de_tempo = datetime.now() - timedelta(days=dias_a_manter)
    arquivos_apagados = 0
    for arquivo in pasta.glob('*.xlsx'):
        try:
            data_modificacao = datetime.fromtimestamp(arquivo.stat().st_mtime)
            if data_modificacao < limite_de_tempo:
                print(f"  - Apagando relatório antigo: {arquivo.name}")
                os.remove(arquivo)
                arquivos_apagados += 1
        except Exception as e:
            print(f"  - ERRO: Não foi possível apagar o arquivo {arquivo.name}. Detalhes: {e}")

    if arquivos_apagados == 0:
        print("Nenhum relatório antigo encontrado para apagar.")
    else:
        print(f"{arquivos_apagados} relatório(s) antigo(s) foram apagados.")
    print("--- Limpeza Concluída ---")


def encontrar_arquivo_recente(pasta: Path, prefixo: str = "") -> Optional[Path]:
    """Encontra o arquivo Excel mais recente em uma pasta com base em um prefixo."""
    if not pasta.is_dir():
        print(f"ERRO: A pasta '{pasta}' não foi encontrada.")
        return None
    arquivos_candidatos = list(pasta.glob(f'{prefixo}*.xls*'))
    if not arquivos_candidatos:
        print(f"AVISO: Nenhum arquivo com o prefixo '{prefixo}' foi encontrado na pasta '{pasta}'.")
        return None
    arquivo_mais_recente = max(arquivos_candidatos, key=lambda f: f.stat().st_mtime)
    print(f"Arquivo encontrado: '{arquivo_mais_recente.name}'")
    return arquivo_mais_recente


def aplicar_mapeamento_coordenadores(df_dados: pd.DataFrame) -> pd.DataFrame:
    """Carrega o arquivo de mapeamento e adiciona a coluna de coordenador ao DataFrame principal."""
    caminho_map = encontrar_arquivo_recente(PASTA_BASE_COORDENADORES)
    if not caminho_map:
        print("AVISO: Arquivo de mapeamento de coordenadores não encontrado. A análise por coordenador será ignorada.")
        df_dados[COL_MAP_COORDENADOR] = "Não Mapeado"
        return df_dados
    try:
        df_map = pd.read_excel(caminho_map)
        if COL_MAP_BASE not in df_map.columns or COL_MAP_COORDENADOR not in df_map.columns:
            print(f"ERRO: Arquivo de mapeamento deve conter as colunas '{COL_MAP_BASE}' e '{COL_MAP_COORDENADOR}'.")
            df_dados[COL_MAP_COORDENADOR] = "Erro no Mapeamento"
            return df_dados

        df_map = df_map[[COL_MAP_BASE, COL_MAP_COORDENADOR]].dropna()
        df_merged = pd.merge(
            df_dados,
            df_map,
            left_on=COL_BASE_ENTREGA,
            right_on=COL_MAP_BASE,
            how='left'
        )
        df_merged[COL_MAP_COORDENADOR].fillna("Não Mapeado", inplace=True)
        print("Mapeamento de coordenadores aplicado com sucesso.")
        return df_merged

    except Exception as e:
        print(f"ERRO: Falha ao ler ou aplicar o arquivo de mapeamento de coordenadores. Detalhes: {e}")
        df_dados[COL_MAP_COORDENADOR] = "Erro no Mapeamento"
        return df_dados


def calcular_sla_agrupado(df: pd.DataFrame, coluna_agrupamento: str) -> pd.DataFrame:
    """Calcula as métricas de SLA agrupadas por uma coluna específica."""
    colunas_essenciais = [coluna_agrupamento, COL_ENTREGUE_NO_PRAZO, COL_REMESSA]
    if not all(col in df.columns for col in colunas_essenciais):
        print(f"ERRO: O arquivo de entrada não contém as colunas necessárias: {colunas_essenciais}.")
        return pd.DataFrame()

    total = df.groupby(coluna_agrupamento)[COL_REMESSA].nunique().rename('Total de Pedidos')
    condicao_fora_prazo = df[COL_ENTREGUE_NO_PRAZO].isin([STATUS_FORA_DO_PRAZO, STATUS_FORA_DO_PRAZO_2]) | df[
        COL_ENTREGUE_NO_PRAZO].isna()
    fora_prazo = df[condicao_fora_prazo] \
        .groupby(coluna_agrupamento)[COL_REMESSA].nunique().rename('Pedidos Fora do Prazo')

    df_sla = pd.concat([total, fora_prazo], axis=1).fillna(0).astype(int).reset_index()
    df_sla['Pedidos no Prazo'] = df_sla['Total de Pedidos'] - df_sla['Pedidos Fora do Prazo']
    df_sla['SLA (%)'] = (df_sla['Pedidos no Prazo'] / df_sla['Total de Pedidos'].replace(0, 1)) * 100
    return df_sla


def gerar_relatorio_nao_mapeados(df_dados: pd.DataFrame, nome_arquivo_final: str):
    """Filtra e salva um relatório com todos os pedidos não mapeados."""
    df_nao_mapeados = df_dados[df_dados[COL_MAP_COORDENADOR] == "Não Mapeado"]

    if df_nao_mapeados.empty:
        print("Nenhum pedido 'Não Mapeado' encontrado. Nenhum relatório adicional será gerado.")
        return

    caminho_saida = PASTA_DESTINO_RELATORIO / nome_arquivo_final
    try:
        PASTA_DESTINO_RELATORIO.mkdir(parents=True, exist_ok=True)
        print(f"Encontrados {len(df_nao_mapeados)} pedidos não mapeados. Gerando relatório em '{caminho_saida}'...")
        df_nao_mapeados.to_excel(caminho_saida, index=False)
        print("Relatório de pedidos não mapeados gerado com sucesso.")
    except Exception as e:
        print(f"ERRO: Falha ao salvar o relatório de pedidos não mapeados. Detalhes: {e}")


def formatar_texto_notificacao(df_coordenador: pd.DataFrame, df_dados_completos: pd.DataFrame) -> str:
    """Formata o texto de resumo para a notificação do Feishu."""
    if df_coordenador.empty:
        return "Não foi possível gerar as métricas de SLA para os coordenadores mapeados."

    total_pedidos_geral = df_dados_completos[COL_REMESSA].nunique()
    pedidos_no_prazo_geral = df_dados_completos[df_dados_completos[COL_ENTREGUE_NO_PRAZO] == STATUS_NO_PRAZO][
        COL_REMESSA].nunique()
    sla_geral = (pedidos_no_prazo_geral / total_pedidos_geral) * 100 if total_pedidos_geral > 0 else 0

    linhas_texto = [
        f"**SLA Geral do Dia (Turno {VALOR_TURNO_FILTRO}):** {sla_geral:.2f}%",
        f"**Total de Pedidos:** {int(total_pedidos_geral)}",
        "\n**📊 Desempenho por Coordenador:**"
    ]

    for _, row in df_coordenador.iterrows():
        pedidos_fora_prazo = int(row['Pedidos Fora do Prazo'])
        linhas_texto.append(
            f"- **{row[COL_MAP_COORDENADOR]}:** {row['SLA (%)']:.2f}% ({pedidos_fora_prazo} pedidos fora do prazo)"
        )

    return "\n".join(linhas_texto)


def salvar_detalhes_por_coordenador(writer: pd.ExcelWriter, df_dados: pd.DataFrame):
    """
    Salva uma aba separada no arquivo Excel para cada coordenador,
    contendo os detalhes completos dos seus respectivos pedidos.
    """
    print("Iniciando a gravação dos detalhes por coordenador em abas separadas...")
    coordenadores = df_dados[COL_MAP_COORDENADOR].unique()
    coordenadores_validos = [c for c in coordenadores if c not in ["Não Mapeado", "Erro no Mapeamento"]]

    if not coordenadores_validos:
        print("Nenhum coordenador válido encontrado para gerar abas detalhadas.")
        return

    for coordenador in coordenadores_validos:
        nome_aba = str(coordenador)[:31]
        print(f"  - Criando aba para o coordenador: '{nome_aba}'")
        df_filtrado = df_dados[df_dados[COL_MAP_COORDENADOR] == coordenador]
        df_filtrado.to_excel(writer, sheet_name=nome_aba, index=False)

    print("Gravação dos detalhes por coordenador concluída.")


# ==============================================================================
# --- FUNÇÃO PRINCIPAL ---
# ==============================================================================

def gerar_relatorio_sla():
    """Orquestra o processo: encontrar arquivo, calcular SLA, salvar Excel e notificar."""
    print("--- Iniciando a Geração do Relatório de SLA ---")

    limpar_relatorios_antigos(PASTA_DESTINO_RELATORIO, DIAS_PARA_MANTER_RELATORIOS)

    caminho_arquivo_entrada = encontrar_arquivo_recente(PASTA_DO_RELATORIO, PREFIXO_ARQUIVO_ENTRADA)
    if not caminho_arquivo_entrada:
        print("Processo interrompido: nenhum arquivo de entrada válido foi encontrado.")
        return

    try:
        df_dados = pd.read_excel(caminho_arquivo_entrada, dtype={COL_ENTREGUE_NO_PRAZO: str})
        print(f"Arquivo '{caminho_arquivo_entrada.name}' lido com sucesso. Total de {len(df_dados)} linhas.")
    except Exception as e:
        print(f"ERRO: Falha ao ler o arquivo Excel '{caminho_arquivo_entrada.name}'. Detalhes: {e}")
        return

    if COL_TURNO in df_dados.columns:
        print(f"Aplicando filtro: '{COL_TURNO}' == '{VALOR_TURNO_FILTRO}'...")
        df_dados = df_dados[df_dados[COL_TURNO] == VALOR_TURNO_FILTRO].copy()
        print(f"{len(df_dados)} linhas restantes após o filtro.")
        if df_dados.empty:
            print("AVISO: Nenhum dado encontrado após aplicar o filtro de turno. O processo será interrompido.")
            return
    else:
        print(
            f"AVISO: A coluna de filtro '{COL_TURNO}' não foi encontrada no arquivo. O relatório será gerado com os dados completos.")

    data_do_relatorio = datetime.now()
    if COL_DATA_PREVISTA in df_dados.columns:
        datas_previstas = pd.to_datetime(df_dados[COL_DATA_PREVISTA], errors='coerce')
        if not datas_previstas.dropna().empty:
            data_do_relatorio = datas_previstas.dropna().max()
            print(f"Data do relatório definida como: {data_do_relatorio.strftime('%d/%m/%Y')}")
        else:
            print(f"AVISO: Coluna '{COL_DATA_PREVISTA}' não contém datas válidas. Usando a data atual.")
    else:
        print(f"AVISO: Coluna '{COL_DATA_PREVISTA}' não encontrada. Usando a data atual.")

    data_formatada = data_do_relatorio.strftime('%Y-%m-%d')
    nome_arquivo_saida_datado = f"{data_formatada}_{NOME_ARQUIVO_SAIDA}"
    nome_arquivo_nao_mapeados_datado = f"{data_formatada}_{NOME_ARQUIVO_NAO_MAPEADOS}"

    df_dados = aplicar_mapeamento_coordenadores(df_dados)

    print("Calculando SLA por Coordenador...")
    df_sla_coordenador_completo = calcular_sla_agrupado(df_dados, COL_MAP_COORDENADOR)
    valores_a_remover = ["Não Mapeado", "Erro no Mapeamento"]
    df_sla_coordenador = df_sla_coordenador_completo[
        ~df_sla_coordenador_completo[COL_MAP_COORDENADOR].isin(valores_a_remover)]
    df_sla_coordenador = df_sla_coordenador.sort_values(by='SLA (%)', ascending=True)
    print("Coordenadores não mapeados foram removidos do relatório principal de SLA.")

    print("Calculando SLA por Regional de Entrega...")
    df_sla_regional = calcular_sla_agrupado(df_dados, COL_REGIONAL_ENTREGA).sort_values(by='SLA (%)', ascending=True)

    if df_sla_coordenador.empty and df_sla_regional.empty:
        print("Nenhum dado de SLA foi gerado para os coordenadores mapeados. O relatório principal não será salvo.")
    else:
        caminho_saida = PASTA_DESTINO_RELATORIO / nome_arquivo_saida_datado
        try:
            PASTA_DESTINO_RELATORIO.mkdir(parents=True, exist_ok=True)

            with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
                print(f"Salvando relatório de SLA em: '{caminho_saida}'")
                df_sla_coordenador.to_excel(writer, sheet_name='SLA por Coordenador', index=False)
                df_sla_regional.to_excel(writer, sheet_name='SLA por Regional', index=False)
                salvar_detalhes_por_coordenador(writer, df_dados)

            print("Relatório de SLA e detalhes por coordenador gerados com sucesso!")
        except Exception as e:
            print(f"ERRO: Falha ao salvar o arquivo Excel de SLA. Detalhes: {e}")

    gerar_relatorio_nao_mapeados(df_dados, nome_arquivo_nao_mapeados_datado)

    texto_notificacao = formatar_texto_notificacao(df_sla_coordenador, df_dados)

    # --- ALTERAÇÃO: Loop para enviar para múltiplos grupos ---
    print("\n--- INICIANDO ENVIO PARA OS GRUPOS FEISHU ---")
    for grupo in FEISHU_GROUPS:
        print(f"-- Enviando para o grupo: '{grupo['name']}' --")
        enviar_notificacao_feishu(
            webhook_url=grupo['url'],
            secret_key=grupo['secret'],
            texto_metricas=texto_notificacao,
            data_relatorio=data_do_relatorio
        )
        time.sleep(1)  # Pausa para evitar sobrecarga da API
    print("--- ENVIO CONCLUÍDO ---")

    print("\n--- Processo Geral Concluído ---")


# ==============================================================================
# --- EXECUÇÃO DO SCRIPT ---
# ==============================================================================
if __name__ == "__main__":
    try:
        gerar_relatorio_sla()
    except Exception as e:
        print("\n--- ERRO FATAL ---")
        print("Ocorreu um erro fatal que interrompeu a execução do script.")
        print("Verifique os detalhes do erro abaixo:")
        traceback.print_exc()

