# -*- coding: utf-8 -*-
"""
📦 Relatório de Pacotes Retidos - Regional GP
Versão 2.5 Multi-Auto Read Pro 💼
Autor: bb (ChatGPT ❤️ Matheus)

✨ Recursos:
- Lê automaticamente TODOS os arquivos .xlsx da pasta
- Busca automática da Base_Atualizada.xlsx
- Reconhecimento flexível de nomes de colunas
- Exportação para OneDrive
- Card interativo no Feishu com link do SharePoint
- Criação automática de pastas
"""

import pandas as pd
import requests
import logging
from datetime import datetime
from pathlib import Path

# ==========================================================
# 🎨 CONFIGURAÇÃO DE LOGS
# ==========================================================
logging.basicConfig(format="%(asctime)s - %(levelname)s: %(message)s", level=logging.INFO)
log = logging.getLogger()

# ==========================================================
# 🚀 ENVIO DE CARD PARA FEISHU
# ==========================================================
def send_to_feishu_bot(webhook_url: str, message_content: dict) -> None:
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(webhook_url, headers=headers, json=message_content)
        response.raise_for_status()
        log.info("✅ Card enviado com sucesso ao Feishu.")
    except requests.exceptions.RequestException as e:
        log.error(f"❌ Erro ao enviar mensagem ao Feishu: {e}")

# ==========================================================
# 🔍 BUSCA AUTOMÁTICA DE PLANILHA DE COORDENADORES
# ==========================================================
def find_coordinators_file(base_dir: Path, filename: str = "Base_Atualizada.xlsx") -> Path | None:
    log.info(f"🔍 Procurando '{filename}' dentro de '{base_dir}' ...")
    for file in base_dir.rglob(filename):
        log.info(f"✅ Arquivo encontrado: {file}")
        return file
    log.error(f"❌ Arquivo '{filename}' não foi encontrado em '{base_dir}' ou subpastas.")
    return None

# ==========================================================
# 🧠 FUNÇÃO DE PROCESSAMENTO DE UM ARQUIVO
# ==========================================================
def process_file(file_path: Path, df_coordenadores: pd.DataFrame, sheet_name: str, output_dir: Path,
                 feishu_webhook_url: str, sharepoint_link: str) -> None:
    try:
        log.info(f"📄 Processando arquivo: {file_path.name}")
        excel = pd.ExcelFile(file_path)
        if sheet_name not in excel.sheet_names:
            sheet_name = excel.sheet_names[0]
            log.warning(f"Aba '{sheet_name}' detectada automaticamente.")

        df_main = pd.read_excel(excel, sheet_name=sheet_name)
        df_main.columns = df_main.columns.str.strip()
        df_coordenadores.columns = df_coordenadores.columns.str.strip()

        # ==========================================================
        # 🔠 DETECÇÃO AUTOMÁTICA DE NOMES DE COLUNAS
        # ==========================================================
        possible_columns = {
            'regional nova 区域': ['regional nova 区域', 'Regional 区域', 'regional 区域', 'Regional Nova 区域'],
            'Base de Entrega 派件网点': ['Base de Entrega 派件网点', 'Base Entrega 派件网点', '网点名称'],
            'Número do Pedido JMS 运单号': ['Número do Pedido JMS 运单号', '运单号', 'Número Pedido JMS'],
            'Cluster Retidos 分类': ['Cluster Retidos 分类', '分类', 'Cluster Retido']
        }

        for canonical, variations in possible_columns.items():
            for var in variations:
                if var in df_main.columns:
                    df_main.rename(columns={var: canonical}, inplace=True)
                    log.info(f"📋 Coluna '{var}' reconhecida como '{canonical}'")
                    break
            else:
                log.error(f"❌ Nenhuma variação encontrada para a coluna '{canonical}' no arquivo {file_path.name}")
                return

        # ==========================================================
        # 🔢 PROCESSAMENTO
        # ==========================================================
        df_merged = df_main.merge(
            df_coordenadores,
            left_on='Base de Entrega 派件网点',
            right_on='Nome da base',
            how='left'
        )

        df_gp = df_merged[df_merged['regional nova 区域'] == 'GP'].copy()
        log.info(f"🎯 Regional 'GP' — {len(df_gp)} registros encontrados.")

        if df_gp.empty:
            log.warning(f"⚠️ Nenhum registro 'GP' em {file_path.name}")
            return

        contagem_total = df_gp.groupby('Coordenadores')['Número do Pedido JMS 运单号'].count()
        contagem_detalhada = df_gp.groupby(['Coordenadores', 'Cluster Retidos 分类'])['Número do Pedido JMS 运单号'].count()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_path = output_dir / f"Resultados_{file_path.stem}_{timestamp}.xlsx"
        with pd.ExcelWriter(output_path) as writer:
            contagem_total.to_excel(writer, sheet_name='Contagem Total por Coordenador')
            contagem_detalhada.to_excel(writer, sheet_name='Contagem por Coordenador e Dia')
            df_gp.to_excel(writer, sheet_name='Dados Filtrados', index=False)
        log.info(f"💾 Resultado salvo: {output_path}")

        # ==========================================================
        # 💬 CARD FEISHU
        # ==========================================================
        total_pacotes = len(df_gp)
        coordenador_cards = []
        for coordenador, qtd in contagem_total.items():
            qtd_por_dia = ""
            try:
                for dia, qtd_dia in contagem_detalhada.loc[coordenador].items():
                    qtd_por_dia += f"- {dia}: {qtd_dia} pedidos\n"
            except KeyError:
                qtd_por_dia = "Nenhum detalhe por dia.\n"
            coordenador_cards.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**📍 {coordenador}** — {qtd} pacotes\n{qtd_por_dia}"
                }
            })

        feishu_message_content = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {"tag": "plain_text", "content": f"📦 Relatório - {file_path.stem}"},
                    "template": "blue"
                },
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md",
                                            "content": f"**Total de Pacotes Retidos:** {total_pacotes} 📦"}},
                    {"tag": "hr"},
                    *coordenador_cards,
                    {"tag": "div", "text": {"tag": "lark_md",
                                            "content": f"📎 [Acessar no SharePoint]({sharepoint_link})"}}
                ]
            }
        }

        send_to_feishu_bot(feishu_webhook_url, feishu_message_content)

    except Exception as e:
        log.error(f"❌ Erro ao processar {file_path.name}: {e}")

# ==========================================================
# 🧩 EXECUÇÃO PRINCIPAL
# ==========================================================
if __name__ == '__main__':
    # Caminho onde estão os arquivos a processar
    caminho_da_pasta = Path(
        r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Teste\Retidos'
    )

    if not caminho_da_pasta.exists():
        caminho_da_pasta.mkdir(parents=True, exist_ok=True)
        log.warning(f"📁 Pasta '{caminho_da_pasta}' não existia e foi criada automaticamente.")

    # Pasta base onde está o arquivo de coordenadores
    base_test_dir = Path(
        r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes'
    )

    feishu_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/b8328e19-9b9f-40d5-bce0-6af7f4612f1b'
    sharepoint_link = (
        "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/"
        "matheus_carvalho_jtexpressdf_onmicrosoft_com/"
        "Ep7sv6B_nKBMg_S_Tdibe0MB4x--uJseBYT52EiRTEqzyA?e=hcTca7"
    )
    nome_da_aba = '滞留明细表'

    # Localiza planilha de coordenadores
    coordinators_file_path = find_coordinators_file(base_test_dir)
    if not coordinators_file_path:
        exit()

    df_coordenadores = pd.read_excel(coordinators_file_path)
    arquivos = list(caminho_da_pasta.glob('*.xlsx'))

    if not arquivos:
        log.warning("⚠️ Nenhum arquivo .xlsx encontrado na pasta de Retidos.")
        exit()

    log.info(f"📊 {len(arquivos)} arquivo(s) encontrados. Iniciando processamento...\n")

    for arquivo in arquivos:
        process_file(arquivo, df_coordenadores, nome_da_aba, caminho_da_pasta,
                     feishu_url, sharepoint_link)

    log.info("✅ Processamento finalizado com sucesso!")
