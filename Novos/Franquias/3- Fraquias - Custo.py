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
