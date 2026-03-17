import polars as pl
import os
import logging
import requests
import shutil
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# =====================================================================
# CONFIGURAÇÕES GERAIS
# =====================================================================

BASE_PATH = r'C:\Users\mathe_70oz1qs\OneDrive\Desktop\Testes\02 - Sem Movimentação'
OUTPUT_BASE_PATH = r'C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Sem_Movimentação - Franquia'
COORDENADOR_BASE_PATH = r'C:\Users\mathe_70oz1qs\OneDrive\Desktop\Testes\01 - Coordenador'

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
    'F CHR-AM', 'F CAC-RO', 'F PDR-GO', 'F PVH-RO', 'F ARQ - RO',
    'F AGB-MT', 'F GYN 03-GO', 'F RBR-AC','F GYN - GO', 'F VHL-RO', 'F PON-GO', 'F ANP-GO', 'F GYN 02-GO', 'F CDN-AM',
    'F AGL-GO', 'F APG - GO', 'F RVD - GO', 'F PDT-TO', 'F PLN-DF', 'F SEN-GO', 'F PVL-MT',
    'F TRD-GO', 'F CEI-DF', 'F CNF-MT', 'F FMA-GO', 'F ALV-AM', 'F POS-GO', 'F PPA-MS', 'F MAC-AP',
    'F GAI-TO', 'F CRX-GO', 'F DOM -PA', 'F CCR-MT', 'F GRP-TO', 'F PVL 02-MT', 'F AMB-MS', 'F BVB-RR',
    'F SVC-RR', 'F MCP-AP', 'F JPN 02-RO', 'F MCP 02-AP', 'F BSL-AC', 'F PVH 02-RO', 'F JPN-RO',
    'F CMV-MT', 'F DOU-MS', 'F PGM-PA', 'F RDC -PA', 'F XIG-PA', 'F TGT-DF', 'F CGR - MS', 'F VLP-GO',
    'F CGR 02-MS', 'F PLA-GO', 'F TGA-MT', 'F RFI-DF', 'F ORL-PA', 'F ITI-PA', 'F PCA-PA',
    'F CNC-PA', 'F SJA-GO', 'F IGA-PA', 'F PAZ-AM', 'F TUR-PA', 'F JCD-PA', 'F TLA-PA',
    'F ELD-PA', 'F BSB-DF', 'F OCD-GO', 'F EMA-DF', 'F GUA-DF', 'F STM-PA', 'F SBN-DF',
    'F AGB 02-MT', 'F ANA-PA', 'F ARQ 02-RO', 'F BAO-PA', 'F BGA-MT', 'F BTS-RO', 'F CDN 02-AM',
    'F CGR 03-MS', 'F CGR 04-MS', 'F CRH-PA', 'F CTL-GO', 'F DOU 02-MS', 'F GFN-PA', 'F GNS-PA',
    'F GYN 04-GO', 'F HMT-AM', 'F IGM-PA', 'F IPX-PA', 'F ITT-PA', 'F JAU-RO', 'F JRG-GO',
    'F MDO-RO', 'F MDR-PA', 'F MRL-AM', 'F MTB-PA', 'F NDI-MS', 'F NMB-PA', 'F PDP-PA', 'F PMW-TO',
    'F PNA-TO', 'F PTD-MT', 'F PVH 03-RO', 'F QUI-GO', 'F RBR 02-AC', 'F ROO-MT', 'F SAM-DF', 'F SBS-DF',
    'F SBZ-PA', 'F SFX-PA', 'F SNP-MT', 'F TPN-PA','F ANP 02-GO', 'F APG 02-GO', 'F BBG-MT', 'F BRV-PA', 'F CAM-PA',
    'F CDN 03-AM', 'F CGR 05-MS', 'F CNA-PA', 'F CNP-MT', 'F CRJ-RO',
    'F GAM-DF', 'F GYN 06-GO', 'F GYN 07-GO', 'F JTI-GO', 'F MCP 04-AP',
    'F MDT-MT', 'F PMG-GO', 'F PVH 04-RO', 'F RDM-RO', 'F TGT 02-DF'
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# =====================================================================
# FUNÇÕES DE NEGÓCIO — POLARS
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
# APOIO E COMPARAÇÃO
# =====================================================================

def carregar_relatorio_anterior(pasta: str) -> Optional[pl.DataFrame]:
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

    df_comp = df_comp.with_columns([
        (pl.col("QtdAtual") - pl.col("QtdAnterior")).cast(pl.Int64).alias("Diferenca")
    ])

    qtd_total = int(df_comp["QtdAtual"].sum())
    variacao_total = int(df_comp["Diferenca"].sum())

    piores = (
        df_comp
        .sort("QtdAtual", descending=True)
        .head(5)
        .select([COL_BASE_RECENTE, "QtdAtual"])
    )

    melhores = (
        df_comp
        .filter(pl.col("Diferenca") < 0)
        .sort("Diferenca")
        .head(5)
        .select([COL_BASE_RECENTE, "Diferenca"])
    )

    piores_list = [(r[COL_BASE_RECENTE], int(r["QtdAtual"])) for r in piores.iter_rows(named=True)]
    melhores_list = [(r[COL_BASE_RECENTE], int(r["Diferenca"])) for r in melhores.iter_rows(named=True)]

    return qtd_total, variacao_total, piores_list, melhores_list


# =====================================================================
# MOVER ARQUIVOS PARA ARQUIVO MORTO
# =====================================================================

def mover_para_arquivo_morto():
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    arquivos = [f for f in os.listdir(PATH_OUTPUT_REPORTS) if f.endswith(".xlsx")]

    os.makedirs(PATH_OUTPUT_ARQUIVO_MORTO, exist_ok=True)

    for f in arquivos:
        if data_hoje not in f:  # move apenas os dias anteriores
            origem = os.path.join(PATH_OUTPUT_REPORTS, f)
            destino = os.path.join(PATH_OUTPUT_ARQUIVO_MORTO, f)
            try:
                shutil.move(origem, destino)
                logging.info(f"📁 Movido para Arquivo Morto: {f}")
            except Exception as e:
                logging.error(f"❌ Erro ao mover {f}: {e}")


# =====================================================================
# CARD FEISHU
# =====================================================================

def _formatar_piores(piores: List[tuple]) -> str:
    if not piores:
        return "- Sem dados para exibir."
    return "\n".join([f"- {b}: {q}" for b, q in piores])


def _formatar_melhores(melhores: List[tuple]) -> str:
    if not melhores:
        return "- Nenhuma redução identificada."
    # Mantém o número com sinal e adiciona o sufixo "(Redução)"
    return "\n".join([f"- {b}: {q} (Redução)" for b, q in melhores])


def montar_card_franquias(data, qtd_total, variacao, piores, melhores, link):
    texto_piores = _formatar_piores(piores)
    texto_melhores = _formatar_melhores(melhores)

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
                {"tag": "div", "text": {"tag": "lark_md", "content": texto_piores}},
                {"tag": "hr"},
                {"tag": "div", "text": {"tag": "lark_md", "content": "**🟢 5 Maiores Reduções (Melhoria)**"}},
                {"tag": "div", "text": {"tag": "lark_md", "content": texto_melhores}},
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
# MAIN
# =====================================================================

def main():
    logging.info("Iniciando processamento...")

    arquivos = [f for f in os.listdir(PATH_INPUT_MAIN) if f.startswith(FILENAME_START_MAIN) and f.endswith('.xlsx')]
    if not arquivos:
        logging.error("Nenhum arquivo encontrado.")
        return

    arquivo = max([os.path.join(PATH_INPUT_MAIN, f) for f in arquivos], key=os.path.getctime)
    logging.info(f"Lendo arquivo principal: {arquivo}")

    df_lazy = pl.read_excel(arquivo, infer_schema_length=1000).lazy()
    df_lazy = df_lazy.filter(pl.col(COL_BASE_RECENTE).is_in(BASES_VALIDAS))

    df_lazy = df_lazy.with_columns([
        pl.col(COL_HORA_OPERACAO).cast(pl.Datetime).alias(COL_HORA_OPERACAO),
        (pl.lit(datetime.now()) - pl.col(COL_HORA_OPERACAO)).dt.total_days().fill_null(0).cast(pl.Int64).alias(
            COL_DIAS_PARADO)
    ])

    df_main = df_lazy.collect()

    df_final = aplicar_regras_status(df_main)
    df_final = aplicar_regras_transito(df_final)

    # Carrega o relatório anterior antes de mover os arquivos
    df_ant = carregar_relatorio_anterior(PATH_OUTPUT_REPORTS)

    # Move arquivos antigos para o Arquivo Morto
    mover_para_arquivo_morto()

    # Salva o relatório novo
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    output_path = os.path.join(PATH_OUTPUT_REPORTS, f"Relatório_SemMovimentação_Completo_{data_hoje}.xlsx")
    df_final.write_excel(output_path)
    logging.info(f"✅ Relatório salvo: {output_path}")

    # Comparativo (somente 5+ dias)
    df_card = df_final.filter(pl.col(COL_DIAS_PARADO) >= 5)
    qtd_total, variacao_total, piores, melhores = comparar_relatorios(df_card, df_ant)

    variacao = (
        f"⬇️ Diminuiu {abs(variacao_total)} pacotes" if variacao_total < 0 else
        f"⬆️ Aumentou {variacao_total} pacotes" if variacao_total > 0 else
        "➖ Sem variação"
    )

    data_atual = datetime.now().strftime("%d/%m/%Y %H:%M")
    card_payload = montar_card_franquias(
        data_atual, qtd_total, variacao, piores, melhores,
        "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EoLsAM3uwAJKiLmuU53XrzMBUqXMQQOvGtGJeVpp8JLLFA?e=N31EhA"
    )

    enviar_card(card_payload, WEBHOOK_URL)
    logging.info("✅ Processo concluído com sucesso.")


if __name__ == "__main__":
    main()
