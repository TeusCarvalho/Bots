# -*- coding: utf-8 -*-
import pandas as pd
from datetime import datetime, timedelta
import os
import glob
from unidecode import unidecode
from pathlib import Path
import numpy as np
import requests
import traceback

# --- Configurações de Caminhos e Constantes ---
# ATENÇÃO: Atualize estes caminhos para os seus diretórios locais
CAMINHO_PASTA_RELATORIO = Path(r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Teste Base\T-0')
CAMINHO_PASTA_COORDENADORES = Path(r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Teste Base\Coordenador')
NOME_ARQUIVO_COORDENADORES = 'Base_Atualizada.xlsx'

#Grupo de Indicadores Operacionais | Qualidade e Redes - GP
FEISHU_WEBHOOK_URL = 'https://open.feishu.cn/open-apis/bot/v2/hook/77c4243e-6876-4e1f-ab96-59003f733dce'

# Parâmetros de processamento
DIAS_PARA_MANTER_RELATORIOS = 7  # Apaga relatórios com mais de X dias
TRADUCOES_STATUS = {'是': 'ENTREGUE', '否': 'EM ROTA'}

# Nomes de colunas para padronização
COL_NOME_BASE = 'Nome da base'
COL_COORDENADOR = 'Coordenadores'
COL_NOME_BASE_ANTIGO_COORD = 'Nova Base - Nome da Base'
COL_STATUS_ENTREGA = 'Status de Entrega'
COL_REMESSA = 'Remessa'
COL_MOTORISTA = 'Motorista de entrega'
COL_ID_PACOTE = 'Número do ID'
COL_CHAVE_NORMALIZADA = 'Nome_base_norm'
COLUNAS_DATA = [
    'Horário de término do prazo de coleta', 'tempo de chegada de veículo em PDD',
    'Horário bipagem de recebimento', 'Horário da entrega'
]


class ReportProcessor:
    """
    Classe para processar relatórios de entrega, adicionar dados de coordenadores,
    gerar resumos e enviar notificações para o Feishu.
    """

    def __init__(self, relatorio_path: Path, coordenadores_path: Path, feishu_url: str):
        self.relatorio_path = relatorio_path
        self.coordenadores_path = coordenadores_path
        self.feishu_url = feishu_url
        self.pasta_resultados = self.relatorio_path / 'Resultados'
        os.makedirs(self.pasta_resultados, exist_ok=True)
        print(f"Pasta de resultados pronta em: {self.pasta_resultados}")

    def _limpar_relatorios_antigos(self, dias_a_manter: int):
        """Exclui relatórios processados com mais de X dias da pasta de destino."""
        print(f"--- Limpeza de relatórios com mais de {dias_a_manter} dias... ---")
        if not self.pasta_resultados.is_dir():
            print("Pasta 'Resultados' não encontrada, nenhuma limpeza a ser feita.")
            return

        limite_tempo = datetime.now() - timedelta(days=dias_a_manter)
        arquivos_antigos = glob.glob(str(self.pasta_resultados / 'Relatorio_Processado_*.xlsx'))

        apagados = 0
        if not arquivos_antigos:
            print("Nenhum relatório antigo para limpar.")

        for arquivo_path in arquivos_antigos:
            try:
                arquivo = Path(arquivo_path)
                data_modificacao = datetime.fromtimestamp(arquivo.stat().st_mtime)
                if data_modificacao < limite_tempo:
                    os.remove(arquivo)
                    print(f"Arquivo antigo excluído: {arquivo.name}")
                    apagados += 1
            except OSError as e:
                print(f"ERRO ao excluir arquivo antigo {arquivo_path}: {e}")

        if apagados == 0 and arquivos_antigos:
            print(f"Nenhum relatório antigo (com mais de {dias_a_manter} dias) encontrado para limpar.")

        print("=" * 50 + "\n")

    def _carregar_relatorio(self) -> pd.DataFrame or None:
        """Carrega o arquivo de relatório principal da pasta especificada."""
        print("--- Buscando arquivo de relatório... ---")
        padrao_nome_arquivo = '*Relatório da taxa de assinatura T0(Lista)*.xls*'
        lista_arquivos = sorted(glob.glob(str(self.relatorio_path / padrao_nome_arquivo)), key=os.path.getmtime,
                                reverse=True)
        if not lista_arquivos:
            print("ERRO: Nenhum arquivo de relatório encontrado!")
            return None

        caminho_arquivo = Path(lista_arquivos[0])
        print(f"Arquivo encontrado: {caminho_arquivo.name}\n")
        try:
            return pd.read_excel(caminho_arquivo)
        except Exception as e:
            print(f"ERRO ao ler o arquivo Excel do relatório: {e}")
            return None

    def _carregar_coordenadores(self) -> pd.DataFrame or None:
        """Carrega o arquivo de coordenadores a partir da pasta e nome do arquivo."""
        caminho_arquivo = self.coordenadores_path / NOME_ARQUIVO_COORDENADORES
        if caminho_arquivo.exists():
            try:
                df = pd.read_excel(caminho_arquivo)
                print(f"Arquivo de coordenadores '{NOME_ARQUIVO_COORDENADORES}' carregado com sucesso!\n")
                return df
            except Exception as e:
                print(f"AVISO: Erro ao carregar o arquivo de coordenadores: {e}\n")
                return None
        else:
            print(f"AVISO: Arquivo de coordenadores '{NOME_ARQUIVO_COORDENADORES}' não encontrado.\n")
            return None

    def _carregar_resumo_anterior(self) -> pd.DataFrame or None:
        """
        Carrega o DataFrame de resumo do relatório processado mais recente para comparação.
        Retorna o **arquivo anterior ao mais recente** (ou None se não houver).
        """
        print("--- Buscando relatório anterior para comparação... ---")
        padrao = str(self.pasta_resultados / 'Relatorio_Processado_*.xlsx')
        arquivos = sorted(glob.glob(padrao), key=os.path.getmtime, reverse=True)

        if not arquivos or len(arquivos) < 2:
            print("AVISO: Nenhum relatório anterior encontrado para comparação.")
            return None

        caminho_anterior = Path(arquivos[1])

        try:
            print(f"Carregando resumo anterior a partir do arquivo: {caminho_anterior.name}")
            xls = pd.ExcelFile(caminho_anterior)
            if 'ResumoNumerico' not in xls.sheet_names:
                print(f"AVISO: O relatório anterior '{caminho_anterior.name}' não contém a aba 'ResumoNumerico'.")
                return None

            df_anterior = pd.read_excel(caminho_anterior, sheet_name='ResumoNumerico', index_col=0)
            for col in df_anterior.columns:
                df_anterior[col] = pd.to_numeric(df_anterior[col], errors='coerce')
            return df_anterior
        except Exception as e:
            print(f"ERRO ao carregar ou ler o resumo do relatório anterior '{caminho_anterior.name}': {e}")
            return None

    def _get_total_pedidos_anterior(self) -> int:
        """Carrega o relatório completo anterior e retorna o total de pedidos únicos."""
        padrao = str(self.pasta_resultados / 'Relatorio_Processado_*.xlsx')
        arquivos = sorted(glob.glob(padrao), key=os.path.getmtime, reverse=True)

        if len(arquivos) < 2:
            print("AVISO: Não foi possível encontrar um relatório completo anterior para comparação de totais.")
            return 0

        caminho_anterior = Path(arquivos[1])
        try:
            df_anterior = pd.read_excel(caminho_anterior, sheet_name='Dados_Completos')
            if COL_REMESSA in df_anterior.columns:
                return df_anterior[COL_REMESSA].nunique()
            else:
                return 0
        except Exception as e:
            print(f"ERRO ao ler o total de pedidos do relatório anterior '{caminho_anterior.name}': {e}")
            return 0

    def _enviar_para_feishu(self, df_processado: pd.DataFrame, df_resumo_numerico: pd.DataFrame,
                            df_resumo_anterior: pd.DataFrame or None):
        """
        Formata o resumo de SLA com comparação (D0 vs D-1) e envia para o webhook do Feishu.
        A mensagem agora usa um formato mais visual e organizado.
        """
        print("--- Tentando enviar resumo para o Feishu... ---")
        if df_resumo_numerico.empty or df_processado.empty:
            print("Resumo ou dados vazios, nada para enviar.")
            return

        df_comp = df_resumo_numerico.copy()
        if df_resumo_anterior is not None:
            df_comp = df_comp.join(df_resumo_anterior.add_suffix(' (D-1)'), how='left')
        else:
            df_comp['SLA (%) (D-1)'] = np.nan

        df_comp['SLA (%)'] = pd.to_numeric(df_comp['SLA (%)'], errors='coerce').fillna(0)
        df_comp['SLA (%) (D-1)'] = pd.to_numeric(df_comp['SLA (%) (D-1)'], errors='coerce')
        df_comp['Δ SLA (%)'] = df_comp['SLA (%)'] - df_comp['SLA (%) (D-1)']

        # --- Cálculo dos totais e variação ---
        qtd_pedidos_atual = df_processado[COL_REMESSA].nunique()
        qtd_pedidos_anterior = self._get_total_pedidos_anterior()
        variacao_pedidos = qtd_pedidos_atual - qtd_pedidos_anterior

        emoji_variacao = ""
        if variacao_pedidos > 0:
            emoji_variacao = "📈 Aumentou"
        elif variacao_pedidos < 0:
            emoji_variacao = "📉 Diminuiu"
        else:
            emoji_variacao = "➖ Sem variação"

        # --- Extrair a data da coluna 'Horário de término do prazo de coleta' para o título ---
        if 'Horário de término do prazo de coleta' in df_processado.columns:
            datas_coleta = pd.to_datetime(df_processado['Horário de término do prazo de coleta'],
                                          errors='coerce').dt.date
            datas_unicas = sorted(set(datas_coleta.dropna()))
            if datas_unicas:
                data_coleta_str = ', '.join(date.strftime('%d/%m/%Y') for date in datas_unicas)
            else:
                data_coleta_str = datetime.now().strftime('%d/%m/%Y')
        else:
            data_coleta_str = datetime.now().strftime('%d/%m/%Y')
        # --- Construção do corpo da mensagem ---
        content_blocks = []

        # Bloco de Resumo Geral
        content_blocks.append([{"tag": "text", "text": f"Qtd de Pedidos Processados: {qtd_pedidos_atual}"}])
        content_blocks.append([{"tag": "text", "text": f"Variação: {emoji_variacao} {abs(variacao_pedidos)} pedidos"}])

        # Bloco de Resumo por Coordenador (loop)
        df_coordenadores_resumo = df_comp[df_comp.index != 'TOTAL']
        for index, row in df_coordenadores_resumo.iterrows():
            variacao_sla = row['Δ SLA (%)']
            emoji_sla = "➖"
            if variacao_sla > 0:
                emoji_sla = "🔼"
            elif variacao_sla < 0:
                emoji_sla = "🔽"

            # Formata a variação de SLA
            texto_variacao_sla = f"{emoji_sla} {variacao_sla:+.2f}%" if not pd.isna(variacao_sla) else "N/A"

            content_blocks.append([{"tag": "text", "text": "---"}])  # Adicionando um separador visual
            content_blocks.append([{"tag": "text", "text": f"📍 Coordenador: {index}"}])
            content_blocks.append([{"tag": "text", "text": f"Qtd de Pedidos: {int(row['TOTAL GERAL'])}"}])
            content_blocks.append([{"tag": "text", "text": f"SLA (%): {row['SLA (%)']:.2f}%"}])
            content_blocks.append([{"tag": "text", "text": f"Variação SLA: {texto_variacao_sla}"}])

        # Bloco de Total Geral
        total_row = df_comp.loc['TOTAL']
        variacao_sla_total = total_row['Δ SLA (%)']
        emoji_total_sla = "➖"
        if variacao_sla_total > 0:
            emoji_total_sla = "🔼"
        elif variacao_sla_total < 0:
            emoji_total_sla = "🔽"

        texto_variacao_total_sla = f"{emoji_total_sla} {variacao_sla_total:+.2f}%" if not pd.isna(
            variacao_sla_total) else "N/A"

        content_blocks.append([{"tag": "text", "text": "---"}])  # Adicionando um separador visual
        content_blocks.append([{"tag": "text", "text": "Resumo Total"}])
        content_blocks.append(
            [{"tag": "text", "text": f"Total de Pedidos Processados: {int(total_row['TOTAL GERAL'])}"}])
        content_blocks.append([{"tag": "text", "text": f"SLA Total (%): {total_row['SLA (%)']:.2f}%"}])
        content_blocks.append([{"tag": "text", "text": f"Variação SLA Total: {texto_variacao_total_sla}"}])

        payload = {
            "msg_type": "post",
            "content": {
                "post": {
                    "pt_br": {
                        "title": f"Resumo de SLA e Desempenho Diário ({data_coleta_str})",
                        "content": content_blocks
                    }
                }
            }
        }

        try:
            response = requests.post(self.feishu_url, json=payload, timeout=10)
            response.raise_for_status()
            print("Resumo (comparação) enviado para o Feishu com sucesso!")
            print(f"Status da resposta do Feishu: {response.status_code}")
            print(f"Resposta do Feishu: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"ERRO de conexão ao enviar para o Feishu: {e}")
            print(f"Detalhes do erro: {e}")
        except Exception as e:
            print(f"ERRO inesperado ao enviar para o Feishu: {e}")
            print(f"Detalhes do erro: {e}")
        finally:
            print("=" * 50 + "\n")
    def _processar_dados(self, df: pd.DataFrame, df_coordenadores: pd.DataFrame or None):
        """Executa as etapas de processamento do DataFrame."""
        # Conversão de datas
        for col in COLUNAS_DATA:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        print("Colunas de data convertidas.")

        # Tradução de status
        if COL_STATUS_ENTREGA in df.columns:
            df[COL_STATUS_ENTREGA] = df[COL_STATUS_ENTREGA].replace(TRADUCOES_STATUS)
            print("Status de entrega traduzidos.")

        # Filtragem de remessas com sufixo numérico -xxx
        if COL_REMESSA in df.columns:
            df[COL_REMESSA] = df[COL_REMESSA].astype(str)
            df = df[~df[COL_REMESSA].str.contains(r'-\d{3}$', regex=True, na=False)]
            print("Remessas com sufixo numérico filtradas.")

        # Motorista vazio => EM PISO
        if COL_STATUS_ENTREGA in df.columns and COL_MOTORISTA in df.columns:
            df.loc[df[COL_MOTORISTA].isna(), COL_STATUS_ENTREGA] = 'EM PISO'
            print("Status 'EM PISO' preenchido para motoristas vazios.")

        # Adicionar coordenadores (merge)
        if df_coordenadores is not None:
            try:
                print("--- Adicionando dados dos coordenadores... ---")
                if COL_NOME_BASE_ANTIGO_COORD in df_coordenadores.columns:
                    df_coordenadores = df_coordenadores.rename(columns={COL_NOME_BASE_ANTIGO_COORD: COL_NOME_BASE})
                colunas_necessarias_coord = [COL_NOME_BASE, COL_COORDENADOR]
                if not all(col in df_coordenadores.columns for col in colunas_necessarias_coord):
                    raise KeyError(
                        f"Arquivo de coordenadores deve conter as colunas: {', '.join(colunas_necessarias_coord)}")

                df[COL_CHAVE_NORMALIZADA] = df[COL_NOME_BASE].astype(str).str.strip().str.upper().apply(unidecode)
                df_coordenadores[COL_CHAVE_NORMALIZADA] = df_coordenadores[COL_NOME_BASE].astype(
                    str).str.strip().str.upper().apply(unidecode)
                df = pd.merge(df, df_coordenadores[[COL_CHAVE_NORMALIZADA, COL_COORDENADOR]], on=COL_CHAVE_NORMALIZADA,
                              how='left')
                df.drop(columns=[COL_CHAVE_NORMALIZADA], inplace=True)

                preenchidos = df[COL_COORDENADOR].notna().sum()
                faltantes = df[COL_COORDENADOR].isna().sum()
                print(f"Coluna '{COL_COORDENADOR}' adicionada. {preenchidos} preenchidos, {faltantes} sem coordenador.")

                if faltantes > 0:
                    df_sem_coord = df[df[COL_COORDENADOR].isna()].copy()
                    nome_sem_coord = f"Bases_Sem_Coordenador_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.xlsx"
                    caminho_sem_coord = self.pasta_resultados / nome_sem_coord
                    df_sem_coord.to_excel(caminho_sem_coord, index=False, engine='openpyxl')
                    print(f"Relatório auxiliar salvo: {caminho_sem_coord}")
            except Exception as e:
                print(f"ERRO ao adicionar coordenador: {e}")

        return df

    def _gerar_resumo_numerico(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gera o resumo numérico de SLA por coordenador."""
        print("\n" + "=" * 20 + " RESUMO POR COORDENADOR " + "=" * 20)
        contagem_pedidos_numerico = pd.DataFrame()
        if COL_COORDENADOR in df.columns and COL_STATUS_ENTREGA in df.columns and COL_REMESSA in df.columns:
            df_resumo = df.dropna(subset=[COL_COORDENADOR])
            if not df_resumo.empty:
                contagem_pedidos = df_resumo.groupby([COL_COORDENADOR, COL_STATUS_ENTREGA])[
                    COL_REMESSA].nunique().unstack(fill_value=0)
                contagem_pedidos['TOTAL GERAL'] = contagem_pedidos.sum(axis=1)

                if 'ENTREGUE' not in contagem_pedidos.columns:
                    contagem_pedidos['ENTREGUE'] = 0

                sla = np.where(contagem_pedidos['TOTAL GERAL'] > 0,
                               (contagem_pedidos['ENTREGUE'] / contagem_pedidos['TOTAL GERAL']) * 100, 0)
                contagem_pedidos['SLA (%)'] = sla

                total_row = contagem_pedidos.sum(numeric_only=True).to_frame('TOTAL').T
                total_entregue = int(total_row.at['TOTAL', 'ENTREGUE']) if 'ENTREGUE' in total_row.columns else 0
                total_geral = int(total_row.at['TOTAL', 'TOTAL GERAL']) if 'TOTAL GERAL' in total_row.columns else 0
                total_row['SLA (%)'] = (total_entregue / total_geral * 100) if total_geral > 0 else 0
                contagem_pedidos_numerico = pd.concat([contagem_pedidos, total_row])

                print("Contagem de pedidos por coordenador e status:")
                contagem_pedidos_formatado = contagem_pedidos_numerico.copy()
                contagem_pedidos_formatado['SLA (%)'] = contagem_pedidos_formatado['SLA (%)'].map('{:.2f}%'.format)
                print(contagem_pedidos_formatado.to_string())
            else:
                print("Nenhum pedido com coordenador definido para gerar resumo.")
        else:
            print(
                "Não foi possível gerar o resumo. Verifique se as colunas 'Coordenadores', 'Status de Entrega' e 'Remessa' existem.")

        print("=" * 62 + "\n")
        return contagem_pedidos_numerico

    def run_processing_flow(self):
        """Executa o fluxo completo de processamento do relatório."""
        self._limpar_relatorios_antigos(DIAS_PARA_MANTER_RELATORIOS)

        df_principal = self._carregar_relatorio()
        if df_principal is None:
            print("Processamento encerrado devido a falha no carregamento do relatório principal.")
            return

        df_coordenadores = self._carregar_coordenadores()

        df_processado = self._processar_dados(df_principal, df_coordenadores)
        df_resumo_numerico = self._gerar_resumo_numerico(df_processado)

        # Salva o relatório do dia
        nome_arquivo = f"Relatorio_Processado_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.xlsx"
        caminho_saida = self.pasta_resultados / nome_arquivo
        with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
            df_processado.to_excel(writer, sheet_name='Dados_Completos', index=False)
            if not df_resumo_numerico.empty:
                df_resumo_numerico.to_excel(writer, sheet_name='ResumoNumerico')
        print(f"\nRelatório final salvo com sucesso em: {caminho_saida}")

        # Envia o resumo (usa o resumo do arquivo anterior para comparação)
        if not df_resumo_numerico.empty:
            df_resumo_anterior = self._carregar_resumo_anterior()
            self._enviar_para_feishu(df_processado, df_resumo_numerico, df_resumo_anterior)


if __name__ == "__main__":
    try:
        processor = ReportProcessor(
            relatorio_path=CAMINHO_PASTA_RELATORIO,
            coordenadores_path=CAMINHO_PASTA_COORDENADORES,
            feishu_url=FEISHU_WEBHOOK_URL
        )
        processor.run_processing_flow()
    except Exception as e:
        print("\n--- ERRO FATAL ---")
        print("Ocorreu um erro fatal que interrompeu a execução do script.")
        print("Verifique os detalhes do erro abaixo:")
        traceback.print_exc()