# -*- coding: utf-8 -*-
# 🚀 Custo e Arbitragem - versão Polars Lazy ⚡ (corrigida e definitiva)

import polars as pl
import os
import requests
import json
from datetime import datetime

pl.Config.set_tbl_rows(10)  # evita prints gigantes no terminal

# ============================================================
# ⚙️ Funções auxiliares
# ============================================================

def format_currency(value: float) -> str:
    """Formata número em formato BRL"""
    try:
        formatted_value = f"{value:,.2f}"
        return formatted_value.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "0,00"


def create_feishu_card_payload(title: str, body: str) -> dict:
    """Monta o card interativo do Feishu"""
    return {
        "msg_type": "interactive",
        "card": {
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "template": "blue"
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": body}},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "📎 Abrir Relatório Completo"},
                            "url": LINK_RELATORIO,
                            "type": "default"
                        }
                    ]
                },
                {"tag": "note", "elements": [{"tag": "plain_text", "content": "Resumo automático gerado por script."}]}
            ]
        }
    }


def get_latest_file(folder: str):
    """Retorna o arquivo mais recente de uma pasta"""
    files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith(('.csv', '.xls', '.xlsx'))
    ]
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def read_file_auto(path: str) -> pl.LazyFrame:
    """Lê automaticamente CSV, XLS ou XLSX (com fallback inteligente)"""
    import pandas as pd

    ext = os.path.splitext(path)[1].lower()
    print(f"📖 Lendo arquivo detectado como: {ext.upper()}")

    try:
        if ext == ".csv":
            return pl.read_csv(path, ignore_errors=True).lazy()

        elif ext == ".xlsx":
            return pl.read_excel(path, infer_schema_length=1000).lazy()

        elif ext == ".xls":
            try:
                # tenta com xlrd (para arquivos .xls reais)
                df = pd.read_excel(path, engine="xlrd")
            except Exception:
                # fallback para openpyxl se for um .xlsx disfarçado
                df = pd.read_excel(path, engine="openpyxl")
            return pl.from_pandas(df).lazy()

        else:
            raise ValueError(f"❌ Formato de arquivo não suportado: {ext}")

    except Exception as e:
        raise RuntimeError(f"Erro ao ler o arquivo {path}: {e}")


# ============================================================
# 🧩 CONFIGURAÇÕES
# ============================================================

COORDENADOR_WEBHOOKS = {
    "Franquias": "https://open.feishu.cn/open-apis/bot/v2/hook/328a86ed-6c6f-4b61-acc4-aa33bd1b8254"
}

LINK_RELATORIO = (
    "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/IgBldPNr5MwPQaiozw6vEIihAdlI_1TZb8xYQ3V04eKiLuM?e=G4g9Q9"
)

BASE_DIR = r"C:\Users\mathe_70oz1qs\OneDrive\Desktop\Testes\02 - Custo - Coordenador"
OUTPUT_FILE = r"C:\Users\mathe_70oz1qs\OneDrive - Speed Rabbit Express Ltda\Custo - Franquia\Minha_responsabilidade_atualizada.xlsx"

BASES_PERMITIDAS = [
    "F AGB 02-MT", "F AGB-MT", "F AGL-GO", "F ALV-AM", "F AMB-MS", "F ANA-PA",
    "F ANP-GO", "F APG - GO", "F ARQ - RO", "F BAO-PA", "F BSB-DF", "F BSL-AC",
    "F BVB-RR", "F CAC-RO", "F CCR-MT", "F CDN-AM", "F CEI-DF", "F CGR - MS",
    "F CGR 02-MS", "F CHR-AM", "F CMV-MT", "F CNC-PA", "F CNF-MT", "F CRX-GO",
    "F CTL-GO", "F DOM -PA", "F DOU-MS", "F ELD-PA", "F EMA-DF", "F FMA-GO",
    "F GAI-TO", "F GFN-PA", "F GNS-PA", "F GRP-TO", "F GUA-DF", "F GYN - GO",
    "F GYN 02-GO", "F GYN 03-GO", "F IGA-PA", "F IPX-PA", "F ITI-PA", "F JAU-RO",
    "F JCD-PA", "F JPN 02-RO", "F JPN-RO", "F JRG-GO", "F MAC-AP", "F MCP 02-AP",
    "F MCP-AP", "F MDR-PA", "F MRL-AM", "F MTB-PA", "F NDI-MS", "F OCD-GO",
    "F ORL-PA", "F PAZ-AM", "F PCA-PA", "F PDP-PA", "F PDR-GO", "F PDT-TO",
    "F PGM-PA", "F PLA-GO", "F PLN-DF", "F PNA-TO", "F PON-GO", "F POS-GO",
    "F PPA-MS", "F PTD-MT", "F PVH 02-RO", "F PVH-RO", "F PVL 02-MT", "F PVL-MT",
    "F RBR-AC", "F RDC -PA", "F RFI-DF", "F ROO-MT", "F RVD - GO", "F SAM-DF",
    "F SBN-DF", "F SBS-DF", "F SEN-GO", "F SFX-PA", "F SJA-GO", "F STM-PA",
    "F SVC-RR", "F TGA-MT", "F TGT-DF", "F TLA-PA", "F TRD-GO", "F TUR-PA",
    "F VHL-RO", "F VLP-GO", "F XIG-PA",
]

# ============================================================
# 🚀 PROCESSAMENTO PRINCIPAL
# ============================================================

latest_file = get_latest_file(BASE_DIR)

if not latest_file:
    print("⚠️ Nenhum arquivo encontrado na pasta de entrada.")
else:
    try:
        print(f"📂 Lendo arquivo: {os.path.basename(latest_file)}")

        # 🧠 Lê o Excel/CSV automaticamente em modo Lazy
        lazy_df = read_file_auto(latest_file)

        # 🔹 Normalização e filtragem
        if "Base responsável" in lazy_df.columns:
            lazy_df = (
                lazy_df
                .with_columns([
                    pl.col("Remessa").cast(pl.Utf8).alias("Remessa"),
                    pl.col("Base responsável").cast(pl.Utf8).str.strip_chars().alias("Base responsável"),
                    pl.col("Regional responsável").cast(pl.Utf8).alias("Regional responsável"),
                    pl.col("Valor a pagar (yuan)").cast(pl.Float64).alias("Valor a pagar (yuan)")
                ])
                .filter(~pl.col("Remessa").str.contains("-"))
                .with_columns(
                    pl.when(pl.col("Base responsável") == "VHL -RO")
                    .then(pl.lit("F VHL-RO"))
                    .otherwise(pl.col("Base responsável"))
                    .alias("Base responsável")
                )
                .filter(pl.col("Regional responsável") == "GP")
                .filter(pl.col("Base responsável").is_in(BASES_PERMITIDAS))
            )
        else:
            print("⚠️ Coluna 'Base responsável' não encontrada. Pulando normalização.")

        # 📊 Agrupamento Lazy
        resumo_bases = (
            lazy_df
            .group_by("Base responsável")
            .agg([
                pl.count("Remessa").alias("Qtd_Pedidos"),
                pl.col("Valor a pagar (yuan)").sum().alias("Valor_Total")
            ])
            .sort("Valor_Total", descending=True)
        )

        resumo_bases = resumo_bases.collect()

        valor_total_geral = resumo_bases["Valor_Total"].sum()
        top5 = resumo_bases.head(5)

        # ============================================================
        # 💬 MENSAGEM PARA FEISHU
        # ============================================================
        data_geracao = datetime.now().strftime("%d/%m/%Y %H:%M")
        mensagem = f"📊 **Relatório de Ressarcimento - TOP 5 Piores Bases**\n📅 {data_geracao}\n\n"
        for row in top5.iter_rows(named=True):
            mensagem += f"🔴 {row['Base responsável']} - {row['Qtd_Pedidos']} pedidos - R$ {format_currency(row['Valor_Total'])}\n"
        mensagem += f"\n💰 **Total Geral:** R$ {format_currency(valor_total_geral)}"

        # ============================================================
        # 📤 ENVIAR CARD FEISHU
        # ============================================================
        payload = create_feishu_card_payload("📊 Relatório de Ressarcimento - Franquias", mensagem)
        webhook_url = COORDENADOR_WEBHOOKS.get("Franquias")
        if webhook_url:
            resp = requests.post(webhook_url, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
            print("✅ Card enviado com sucesso!" if resp.status_code == 200 else f"⚠️ Erro {resp.status_code}: {resp.text}")

        # ============================================================
        # 💾 SALVAR RESULTADO FINAL
        # ============================================================
        df_final = lazy_df.collect()
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        df_final.write_excel(OUTPUT_FILE)
        print(f"📎 Arquivo salvo com sucesso: {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ Erro ao processar: {e}")
