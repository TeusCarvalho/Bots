# -*- coding: utf-8 -*-
import pandas as pd
import os
import requests
from datetime import datetime

# ================== CONFIGURAÇÕES ==================
# Pasta de entrada (onde ficam os arquivos originais)
PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Local de Teste\Entrega Realizada - Dia"

# Pasta de saída (novo local no OneDrive Franquias)
ARQUIVO_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Franquias\Entrega Realizada\Resumo_Completo.xlsx"

# Webhook Feishu
WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/92a82aea-9b5c-4e3d-9169-8d4753ecef38"

# Novo link da pasta no OneDrive
LINK_PASTA = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EvIP3oIiLJRAqcB1SZ_1nmYBXLIYSJkIns5Pf_Xz2OqY_w?e=OEXsJN"

# Lista de bases válidas (edite aqui para acrescentar/remover)
BASES_VALIDAS = [
    "F AGL-GO","F ALV-AM","F ALX-AM","F AMB-MS","F ANP-GO","F APG - GO","F ARQ - RO",
    "F BAO-PA","F BSB - DF","F BSB-DF","F BSL-AC","F CDN-AM","F CGR - MS","F CGR 02-MS",
    "F CHR-AM","F CMV-MT","F CNC-PA","F CNF-MT","F DOM -PA","F DOU-MS","F ELD-PA",
    "F FMA-GO","F GAI-TO","F GRP-TO","F GYN - GO","F GYN 02-GO","F GYN 03-GO",
    "F IGA-PA","F ITI -PA","F ITI-PA","F JCD-PA","F MCP 02-AP","F MCP-AP","F OCD - GO",
    "F OCD-GO","F ORL-PA","F PCA-PA","F PDR-GO","F PGM-PA","F PLN-DF","F PON-GO",
    "F PVH-RO","F PVH 02-RO","F PVL-MT","F RDC -PA","F RVD - GO","F SEN-GO",
    "F SFX-PA","F TGT-DF","F TGT -DF","F TLA-PA","F TRD-GO","F TUR-PA",
    "F VHL-RO","F VLP-GO","F XIG-PA","F CEI-DF","F CEI -DF"
]


# ================== FUNÇÕES ==================
def cor_percentual(pct: float) -> str:
    if pct < 0.95:
        return "🔴"
    elif pct < 0.97:
        return "🟡"
    else:
        return "🟢"


def consolidar_planilhas(pasta_entrada: str) -> pd.DataFrame:
    """Lê todos os arquivos da pasta de entrada e consolida em um único DataFrame"""
    arquivos = [f for f in os.listdir(pasta_entrada) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        raise FileNotFoundError("Nenhum arquivo Excel encontrado na pasta de entrada!")

    df_total = pd.DataFrame()
    for arquivo in arquivos:
        caminho = os.path.join(pasta_entrada, arquivo)
        print(f"📄 Lendo: {caminho}")
        df = pd.read_excel(caminho)
        df_total = pd.concat([df_total, df], ignore_index=True)

    return df_total


def salvar_resumo(df: pd.DataFrame):
    """Salva o consolidado filtrado em Excel"""
    os.makedirs(os.path.dirname(ARQUIVO_SAIDA), exist_ok=True)
    df.to_excel(ARQUIVO_SAIDA, index=False)
    print(f"✅ Arquivo salvo em: {ARQUIVO_SAIDA}")


def enviar_card_feishu(df: pd.DataFrame):
    """Envia card no Feishu com 7 piores, 3 melhores e média geral"""
    data_geracao = datetime.now().strftime("%d/%m/%Y %H:%M")
    total_bases = df["Base de entrega"].nunique()

    # 7 piores e 3 melhores
    piores = df.sort_values(by="% Entregues", ascending=True).head(7)
    melhores = df.sort_values(by="% Entregues", ascending=False).head(3)

    linhas_piores = []
    for i, row in enumerate(piores.iterrows(), 1):
        base, total, pct, nao_entregues = (
            row[1]["Base de entrega"],
            row[1]["Total"],
            row[1]["% Entregues"],
            row[1]["Nao_Entregues"]
        )
        linhas_piores.append(f"{i}. {cor_percentual(pct)} {base} - {pct:.2%} ({nao_entregues} não entregues de {total})")

    linhas_melhores = []
    for i, row in enumerate(melhores.iterrows(), 1):
        base, total, pct, nao_entregues = (
            row[1]["Base de entrega"],
            row[1]["Total"],
            row[1]["% Entregues"],
            row[1]["Nao_Entregues"]
        )
        linhas_melhores.append(f"{i}. {cor_percentual(pct)} {base} - {pct:.2%} ({nao_entregues} não entregues de {total})")

    media_geral = df["% Entregues"].mean()

    conteudo = "**7 Piores SLAs:**\n" + "\n".join(linhas_piores)
    conteudo += "\n\n**3 Melhores SLAs:**\n" + "\n".join(linhas_melhores)
    conteudo += f"\n\n📊 **Média geral:** {media_geral:.2%}"

    card_payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": "red",
                "title": {"tag": "plain_text", "content": "📊 Relatório Consolidado - Bases Avaliadas"}
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md",
                                        "content": f"**Data de Geração:** {data_geracao}\n**Bases Avaliadas:** {total_bases}"}},
                {"tag": "hr"},
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

    resp = requests.post(WEBHOOK_URL, json=card_payload)
    print("🔎 Status Code:", resp.status_code)
    print("🔎 Resposta Texto:", resp.text)


# ================== EXECUÇÃO ==================
if __name__ == "__main__":
    # 1. Lê e consolida as planilhas da pasta de entrada
    df = consolidar_planilhas(PASTA_ENTRADA)

    # 2. Filtra apenas as bases válidas
    df = df[df["Base de entrega"].isin(BASES_VALIDAS)]

    # 3. Filtra somente o dia de hoje
    hoje = datetime.now().date()
    df["Data prevista de entrega"] = pd.to_datetime(df["Data prevista de entrega"], errors="coerce")
    df = df[df["Data prevista de entrega"].dt.date == hoje]

    # 4. Gera resumo por base
    resumo = df.groupby("Base de entrega").agg(
        Total=("Base de entrega", "count"),
        Entregues=("Entregue no prazo？", lambda x: (x == "Y").sum()),
        Nao_Entregues=("Entregue no prazo？", lambda x: (x.isna() | (x != "Y")).sum())
    ).reset_index()
    resumo["% Entregues"] = resumo["Entregues"] / resumo["Total"]

    # 5. Salva planilha consolidada (somente de hoje)
    salvar_resumo(df)

    # 6. Envia card no Feishu
    enviar_card_feishu(resumo)