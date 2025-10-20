# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------
📦 RELATÓRIO - CONSOLIDADO SLA (MODO TURBO + MEDALHAS)
-----------------------------------------------------------
✅ Lê automaticamente todas as planilhas Excel/CSV da pasta configurada
✅ Usa processamento paralelo (multiprocessing) + Polars (super rápido)
✅ Filtra apenas bases válidas, entregues e do dia atual
✅ Calcula o percentual por base (SLA) e gera ranking automático
✅ Envia card no Feishu (7 piores, 3 melhores com 🥇🥈🥉, média geral)
✅ Salva o Excel consolidado no OneDrive (backup)
===========================================================
"""

import os
import requests
import warnings
import polars as pl
import pandas as pd
import multiprocessing
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ==========================================================
# ⚙️ CONFIGURAÇÕES GERAIS
# ==========================================================
os.environ["POLARS_MAX_THREADS"] = str(multiprocessing.cpu_count())  # usa todos os núcleos

PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Local de Teste\Entrega Realizada - Dia"
PASTA_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Franquias\Entrega Realizada"

DATA_HOJE = datetime.now().strftime("%Y%m%d")
ARQUIVO_SAIDA = os.path.join(PASTA_SAIDA, f"Resumo_Consolidado_{DATA_HOJE}.xlsx")

WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/92a82aea-9b5c-4e3d-9169-8d4753ecef38"
LINK_PASTA = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EvIP3oIiLJRAqcB1SZ_1nmYBXLIYSJkIns5Pf_Xz2OqY_w?e=OEXsJN"

# ==========================================================
# 🏢 BASES VÁLIDAS
# ==========================================================
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
    "F TUR-PA", "F VHL-RO", "F VLP-GO", "F XIG-PA"
]

# ==========================================================
# ⚡️ FUNÇÕES AUXILIARES
# ==========================================================
def cor_percentual(pct: float) -> str:
    """Retorna o emoji correspondente ao SLA."""
    if pct < 0.95:
        return "🔴"
    elif pct < 0.97:
        return "🟡"
    else:
        return "🟢"


def ler_planilha_rapido(caminho):
    """Lê planilha com Polars (detecta formato automaticamente)."""
    try:
        if caminho.endswith(".csv"):
            return pl.read_csv(caminho)
        return pl.read_excel(caminho)
    except Exception:
        return pl.DataFrame()  # evita quebra total se um arquivo falhar


def consolidar_planilhas(pasta_entrada: str) -> pl.DataFrame:
    """Une todas as planilhas Excel/CSV da pasta."""
    arquivos = [os.path.join(pasta_entrada, f)
                for f in os.listdir(pasta_entrada)
                if f.endswith((".xlsx", ".xls", ".csv"))]
    if not arquivos:
        raise FileNotFoundError("❌ Nenhum arquivo Excel/CSV encontrado na pasta de entrada.")

    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        dfs = list(executor.map(ler_planilha_rapido, arquivos))

    return pl.concat(dfs, how="vertical_relaxed")


def detectar_coluna_entregue(df: pl.DataFrame) -> str:
    """Detecta automaticamente a coluna que indica se foi entregue."""
    for col in df.columns:
        if "entregue" in col.lower():
            return col
    raise KeyError("❌ Coluna de status de entrega não encontrada.")


def salvar_backup_excel(df: pd.DataFrame):
    """Salva o consolidado em Excel como backup."""
    try:
        os.makedirs(os.path.dirname(ARQUIVO_SAIDA), exist_ok=True)
        with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Consolidado")
        print(f"💾 Backup salvo em: {ARQUIVO_SAIDA}")
    except Exception as e:
        print(f"⚠️ Falha ao salvar backup Excel: {e}")


def enviar_card_feishu(resumo_df: pd.DataFrame):
    """Monta e envia o card consolidado para o Feishu."""
    data_geracao = datetime.now().strftime("%d/%m/%Y %H:%M")
    total_bases = resumo_df["Base De Entrega"].nunique()
    media_geral = resumo_df["% Entregues"].mean()

    # Ordena ranking
    piores = resumo_df.sort_values(by="% Entregues", ascending=True).head(7)
    melhores = resumo_df.sort_values(by="% Entregues", ascending=False).head(3)

    # Formatação das linhas
    linhas_piores = [
        f"{i}. {cor_percentual(row['% Entregues'])} **{row['Base De Entrega']}** — {row['% Entregues']:.2%} "
        f"({row['Nao_Entregues']} não entregues de {row['Total']})"
        for i, row in enumerate(piores.to_dict('records'), 1)
    ]
    medalhas = ["🥇", "🥈", "🥉"]
    linhas_melhores = [
        f"{medalhas[i-1]} {cor_percentual(row['% Entregues'])} **{row['Base De Entrega']}** — {row['% Entregues']:.2%} "
        f"({row['Nao_Entregues']} não entregues de {row['Total']})"
        for i, row in enumerate(melhores.to_dict('records'), 1)
    ]

    conteudo = (
        f"📅 **Data de Geração:** {data_geracao}\n"
        f"🏢 **Bases Avaliadas:** {total_bases}\n\n"
        f"🔻 **7 Piores SLAs:**\n" + "\n".join(linhas_piores) +
        "\n\n🏆 **Top 3 Melhores SLAs:**\n" + "\n".join(linhas_melhores) +
        f"\n\n📊 **Média Geral:** {media_geral:.2%}"
    )

    card_payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"template": "turquoise", "title": {"tag": "plain_text", "content": "📊 Relatório Consolidado de SLA"}},
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": conteudo}},
                {"tag": "hr"},
                {"tag": "action", "actions": [
                    {"tag": "button",
                     "text": {"tag": "plain_text", "content": "📂 Abrir Pasta no OneDrive"},
                     "url": LINK_PASTA,
                     "type": "default"}
                ]}
            ]
        }
    }

    try:
        resp = requests.post(WEBHOOK_URL, json=card_payload, timeout=10)
        if resp.status_code == 200:
            print("✅ Card enviado com sucesso ao Feishu!")
        else:
            print(f"⚠️ Erro ao enviar card: {resp.status_code} - {resp.text}")
    except requests.RequestException as e:
        print(f"🚨 Falha de conexão com Feishu: {e}")


# ==========================================================
# 🚀 EXECUÇÃO PRINCIPAL
# ==========================================================
if __name__ == "__main__":
    print("🚀 Iniciando processamento turbo...\n")

    df = consolidar_planilhas(PASTA_ENTRADA)
    df = df.rename({c: c.strip().upper() for c in df.columns})
    col_entregue = detectar_coluna_entregue(df)

    # === FILTROS ===
    if "DATA PREVISTA DE ENTREGA" in df.columns:
        tipo_coluna = df["DATA PREVISTA DE ENTREGA"].dtype
        if tipo_coluna == pl.Utf8:
            df = df.with_columns(pl.col("DATA PREVISTA DE ENTREGA").str.strptime(pl.Date, "%Y-%m-%d", strict=False))
        elif tipo_coluna == pl.Datetime:
            df = df.with_columns(pl.col("DATA PREVISTA DE ENTREGA").dt.date().alias("DATA PREVISTA DE ENTREGA"))
        df = df.filter(pl.col("DATA PREVISTA DE ENTREGA") == datetime.now().date())

    if "BASE DE ENTREGA" in df.columns:
        df = df.filter(pl.col("BASE DE ENTREGA").is_in([b.upper() for b in BASES_VALIDAS]))

    # === GERA RESUMO DE SLA ===
    resumo = (
        df.group_by("BASE DE ENTREGA")
        .agg([
            pl.count("BASE DE ENTREGA").alias("Total"),
            (pl.col(col_entregue) == "Y").sum().alias("Entregues"),
            ((pl.col(col_entregue) != "Y") | pl.col(col_entregue).is_null()).sum().alias("Nao_Entregues"),
        ])
        .with_columns((pl.col("Entregues") / pl.col("Total").cast(pl.Float64)).alias("% Entregues"))
        .sort("% Entregues", descending=False)
    )

    resumo_pd = resumo.to_pandas()
    resumo_pd.rename(columns={"BASE DE ENTREGA": "Base De Entrega"}, inplace=True)

    # 🔒 Backup seguro + card final
    salvar_backup_excel(resumo_pd)
    enviar_card_feishu(resumo_pd)
    print("\n🏁 Processo concluído com sucesso!")
