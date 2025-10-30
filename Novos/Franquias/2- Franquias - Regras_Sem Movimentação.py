
import polars as pl
import os
import logging
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# =====================================================================
# 🧩 CONFIGURAÇÕES GERAIS
# =====================================================================

BASE_PATH = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Sem Movimentação'
OUTPUT_BASE_PATH = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Franquias\Sem Movimentação'
COORDENADOR_BASE_PATH = r'C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador'

PATH_INPUT_MAIN = os.path.join(BASE_PATH, 'Sem_Movimentação')
PATH_OUTPUT_REPORTS = OUTPUT_BASE_PATH
PATH_OUTPUT_ARQUIVO_MORTO = os.path.join(OUTPUT_BASE_PATH, "Arquivo Morto")

FILENAME_START_MAIN = 'Monitoramento de movimentação em tempo real'
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/18eed487-c172-4b86-95cf-bfbe1cd21df1"

# Colunas principais
COL_REMESSA = 'Remessa'
COL_DIAS_PARADO = 'Dias Parado'
COL_ULTIMA_OPERACAO = 'Tipo da última operação'
COL_REGIONAL = 'Regional responsável'
COL_NOME_PROBLEMATICO = 'Nome de pacote problemático'
COL_HORA_OPERACAO = 'Horário da última operação'
COL_STATUS = 'Status'
COL_BASE_RECENTE = 'Nome da base mais recente'
COL_TRANSITO = 'Trânsito'

# Bases válidas
BASES_VALIDAS = [
    "F AGL-GO", "F ALV-AM", "F ALX-AM", "F AMB-MS", "F ANP-GO", "F APG - GO",
    "F ARQ - RO", "F BAO-PA", "F BSB - DF", "F BSB-DF", "F BSL-AC", "F CDN-AM",
    "F CEI-DF", "F CGR - MS", "F CGR 02-MS", "F CHR-AM", "F CMV-MT", "F CNC-PA",
    "F CNF-MT", "F DOM -PA", "F DOU-MS", "F ELD-PA", "F FMA-GO", "F GAI-TO",
    "F GRP-TO", "F GYN - GO", "F GYN 02-GO", "F GYN 03-GO", "F IGA-PA", "F ITI -PA",
    "F ITI-PA", "F JCD-PA", "F MCP 02-AP", "F MCP-AP", "F OCD - GO", "F OCD-GO",
    "F ORL-PA", "F PCA-PA", "F PDR-GO", "F PGM-PA", "F PLN-DF", "F PON-GO",
    "F POS-GO", "F PVH 02-RO", "F PVH-RO", "F PVL-MT", "F RDC -PA", "F RVD - GO",
    "F SEN-GO", "F SFX-PA", "F TGA-MT", "F TGT-DF", "F TLA-PA", "F TRD-GO",
    "F TUR-PA", "F VHL-RO", "F VLP-GO", "F XIG-PA", "F TRM-AM", "F STM-PA",
    "F JPN 02-RO", "F CAC-RO"
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# =====================================================================
# ⚙️ FUNÇÕES DE NEGÓCIO — POLARS
# =====================================================================

def aplicar_regras_transito(df: pl.DataFrame) -> pl.DataFrame:
    if COL_BASE_RECENTE not in df.columns:
        return df.with_columns(pl.lit("COLUNA DE BASE RECENTE NÃO ENCONTRADA").alias(COL_TRANSITO))

    cond_em_transito = df[COL_ULTIMA_OPERACAO] == "发件扫描/Bipe de expedição"
    origem_sc_bre = df[COL_BASE_RECENTE] == 'SC BRE'
    destino_pvh = df[COL_REGIONAL].str.contains('PVH-RO', literal=False)
    prazo_5 = df[COL_DIAS_PARADO] >= 5
    prazo_3 = df[COL_DIAS_PARADO] >= 3

    return df.with_columns(
        pl.when(cond_em_transito & origem_sc_bre & prazo_5)
        .then(pl.lit("FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)"))
        .when(cond_em_transito & origem_sc_bre & ~prazo_5)
        .then(pl.lit("EM TRÂNSITO PARA A BASE"))
        .when(cond_em_transito & ~origem_sc_bre & destino_pvh & prazo_5)
        .then(pl.lit("FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)"))
        .when(cond_em_transito & ~origem_sc_bre & destino_pvh & ~prazo_5)
        .then(pl.lit("EM TRÂNSITO PARA A BASE"))
        .when(cond_em_transito & ~origem_sc_bre & ~destino_pvh & prazo_3)
        .then(pl.lit("FALTA BIPE DE RECEBIMENTO (EXPEDIDO E NÃO CHEGOU)"))
        .when(cond_em_transito & ~origem_sc_bre & ~destino_pvh & ~prazo_3)
        .then(pl.lit("EM TRÂNSITO PARA A BASE"))
        .otherwise(pl.lit(""))
        .alias(COL_TRANSITO)
    )

def aplicar_regras_status(df: pl.DataFrame) -> pl.DataFrame:
    is_problematico = df[COL_ULTIMA_OPERACAO] == "问题件扫描/Bipe de pacote problemático"
    cond_extravio = is_problematico & (df[COL_NOME_PROBLEMATICO] == "Extravio.interno.内部遗失")
    return df.with_columns(
        pl.when(cond_extravio)
        .then(pl.lit("PEDIDO EXTRAVIADO"))
        .otherwise(pl.col(COL_ULTIMA_OPERACAO).str.to_uppercase())
        .alias(COL_STATUS)
    )

# =====================================================================
# 🧮 APOIO E COMPARAÇÃO
# =====================================================================

def carregar_relatorio_anterior(pasta: str) -> Optional[pl.DataFrame]:
    """Carrega o relatório do dia anterior (D-1) com base na data no nome."""
    ontem = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    candidatos = [f for f in os.listdir(pasta) if ontem in f and f.endswith(".xlsx")]

    if not candidatos:
        logging.warning(f"Nenhum relatório encontrado para {ontem} — usando o mais recente disponível.")
        arquivos = [f for f in os.listdir(pasta) if f.endswith(".xlsx")]
        if not arquivos:
            return None
        arquivo_mais_recente = max([os.path.join(pasta, f) for f in arquivos], key=os.path.getctime)
    else:
        arquivo_mais_recente = os.path.join(pasta, candidatos[0])

    logging.info(f"📂 Comparando com relatório anterior: {arquivo_mais_recente}")
    return pl.read_excel(arquivo_mais_recente)

def comparar_relatorios(df_atual: pl.DataFrame, df_anterior: Optional[pl.DataFrame]):
    atual = (
        df_atual
        .group_by(COL_BASE_RECENTE)
        .count()
        .rename({"count": "QtdAtual"})
        .with_columns(pl.col("QtdAtual").cast(pl.Int64))
    )

    if df_anterior is not None:
        df_anterior = df_anterior.unique(subset=[COL_REMESSA])
        anterior = (
            df_anterior
            .group_by(COL_BASE_RECENTE)
            .count()
            .rename({"count": "QtdAnterior"})
            .with_columns(pl.col("QtdAnterior").cast(pl.Int64))
        )
        df_comp = atual.join(anterior, on=COL_BASE_RECENTE, how="outer").fill_null(0)
    else:
        df_comp = atual.with_columns(pl.lit(0).alias("QtdAnterior"))

    # Calcular diferença
    df_comp = df_comp.with_columns([
        (pl.col("QtdAtual") - pl.col("QtdAnterior")).cast(pl.Int64).alias("Diferenca")
    ])

    qtd_total = int(df_comp["QtdAtual"].sum())
    variacao_total = int(df_comp["Diferenca"].sum())

    # 🔴 5 piores (maior quantidade atual)
    piores = (
        df_comp
        .sort("QtdAtual", descending=True)
        .head(5)
        .select([COL_BASE_RECENTE, "QtdAtual"])
    )

    # 🟢 5 melhores reduções (maior queda)
    melhores = (
        df_comp
        .filter(pl.col("Diferenca") < 0)
        .sort("Diferenca")  # mais negativo = maior redução
        .head(5)
        .select([COL_BASE_RECENTE, "Diferenca"])
    )

    piores_list = [(r[COL_BASE_RECENTE], int(r["QtdAtual"])) for r in piores.iter_rows(named=True)]
    melhores_list = [(r[COL_BASE_RECENTE], int(r["Diferenca"])) for r in melhores.iter_rows(named=True)]

    return qtd_total, variacao_total, piores_list, melhores_list

# =====================================================================
# 💬 CARD FEISHU
# =====================================================================

def montar_card_franquias(data, qtd_total, variacao, piores, melhores, link):
    return {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content":
                    f"**📊 Relatório Sem Movimentação (5+ dias)**\n"
                    f"**Data:** {data}\n"
                    f"**Total Pacotes:** {qtd_total}\n"
                    f"**Variação:** {variacao}\n"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": "**🔴 5 Piores Franquias (Mais Pacotes)**"}},
                {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join([f"- {b}: {q}" for b, q in piores])}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": "**🟢 5 Maiores Reduções (Melhoria)**"}},
                {"tag": "div", "text": {"tag": "lark_md", "content": "\n".join([f"- {b}: {q}" for b, q in melhores])}},
                {"tag": "hr"},
                {"tag": "action", "actions": [
                    {"tag": "button", "text": {"tag": "plain_text", "content": "📂 Abrir Relatório"},
                     "url": link, "type": "default"}]}
            ],
            "header": {"title": {"tag": "plain_text", "content": "📦 Sem Movimentação - Franquias (5+ dias)"}}
        }
    }

def enviar_card(payload: Dict[str, Any], webhook: str):
    r = requests.post(webhook, json=payload)
    if r.status_code == 200:
        logging.info("✅ Card enviado com sucesso ao Feishu!")
    else:
        logging.error(f"❌ Erro ao enviar card: {r.status_code} - {r.text}")

# =====================================================================
# 🚀 MAIN
# =====================================================================

def main():
    logging.info("Iniciando processamento...")

    arquivos = [f for f in os.listdir(PATH_INPUT_MAIN) if f.startswith(FILENAME_START_MAIN) and f.endswith('.xlsx')]
    if not arquivos:
        logging.error("Nenhum arquivo encontrado.")
        return

    arquivo = max([os.path.join(PATH_INPUT_MAIN, f) for f in arquivos], key=os.path.getctime)
    logging.info(f"Lendo arquivo principal: {arquivo}")

    # Lazy load (otimizado)
    df_lazy = pl.read_excel(arquivo, infer_schema_length=1000).lazy()
    df_lazy = df_lazy.filter(pl.col(COL_BASE_RECENTE).is_in(BASES_VALIDAS))

    # 🕒 Calcular dias parado
    df_lazy = df_lazy.with_columns([
        pl.col(COL_HORA_OPERACAO).cast(pl.Datetime).alias(COL_HORA_OPERACAO),
        (pl.lit(datetime.now()) - pl.col(COL_HORA_OPERACAO)).dt.total_days().fill_null(0).cast(pl.Int64).alias(COL_DIAS_PARADO)
    ])

    df_main = df_lazy.collect()

    # Aplicar regras
    df_final = aplicar_regras_status(df_main)
    df_final = aplicar_regras_transito(df_final)

    # Salvar relatório completo
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    output_path = os.path.join(PATH_OUTPUT_REPORTS, f"Relatório_SemMovimentação_Completo_{data_hoje}.xlsx")
    df_final.write_excel(output_path)

    # Filtro 5+ dias
    df_card = df_final.filter(pl.col(COL_DIAS_PARADO) >= 5)

    # Comparar e gerar card
    df_ant = carregar_relatorio_anterior(PATH_OUTPUT_ARQUIVO_MORTO)
    qtd_total, variacao_total, piores, melhores = comparar_relatorios(df_card, df_ant)

    variacao = (
        f"⬇️ Diminuiu {abs(variacao_total)} pacotes" if variacao_total < 0 else
        f"⬆️ Aumentou {variacao_total} pacotes" if variacao_total > 0 else
        "➖ Sem variação"
    )

    data_atual = datetime.now().strftime("%d/%m/%Y %H:%M")
    card_payload = montar_card_franquias(data_atual, qtd_total, variacao, piores, melhores, "https://link-relatorio")
    enviar_card(card_payload, WEBHOOK_URL)

    logging.info("✅ Processo concluído com sucesso.")

if __name__ == "__main__":
    main()
