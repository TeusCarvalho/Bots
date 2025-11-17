# -*- coding: utf-8 -*-
import os
import pandas as pd
import requests
from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ======================================================
# ⚙️ CONFIGURAÇÕES GERAIS
# ======================================================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Custo - Coordenador"
COORDENADOR_PATH = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
OUTPUT_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\Custos - Coordenadores"

LINK_PASTA = "https://jtexpressdf-my.sharepoint.com/:f:/g/personal/matheus_carvalho_jtexpressdf_onmicrosoft_com/EvIP3oIiLJRAqcB1SZ_1nmYBXLIYSJkIns5Pf_Xz2OqY_w?e=OEXsJN"

DATA_ATUAL = datetime.now().strftime("%Y%m%d_%H%M%S")
DATA_HUMANA = datetime.now().strftime("%d/%m/%Y %H:%M")
ARQUIVO_SAIDA = os.path.join(OUTPUT_DIR, f"Custos_Consolidado_{DATA_ATUAL}.xlsx")

# ======================================================
# 🔗 WEBHOOKS POR COORDENADOR
# ======================================================
COORDENADOR_WEBHOOKS = {
    "João Melo": "https://open.feishu.cn/open-apis/bot/v2/hook/1f3f48d7-b60c-45c1-87ee-6cc8ab9f6467",
    "Johas Vieira": "https://open.feishu.cn/open-apis/bot/v2/hook/b448a316-f146-49d0-9f0a-90b1f086b8a7",
    "Anderson Matheus": "https://open.feishu.cn/open-apis/bot/v2/hook/fa768680-b4ab-4d87-bf2c-285c91034dad",
    "Marcelo Medina": "https://open.feishu.cn/open-apis/bot/v2/hook/e14d0307-c6d6-472b-bea1-d83a5573dc1b",
    "Odária Fereira": "https://open.feishu.cn/open-apis/bot/v2/hook/4cfd01be-defa-4adb-936e-6bfbee5326a6",
    "Rodrigo Castro": "https://open.feishu.cn/open-apis/bot/v2/hook/e3e31e14-79ab-4a95-8a2d-be99e1fc9b10",
    "Orlean Nascimento": "https://open.feishu.cn/open-apis/bot/v2/hook/9ce83b77-04ad-4558-ab83-39929b30f092",
    "Jose Marlon": "https://open.feishu.cn/open-apis/bot/v2/hook/d624dcc1-73c7-4d36-8f63-5c43d0e5259b",
    "Emerson Silva": "https://open.feishu.cn/open-apis/bot/v2/hook/eb777d25-f454-4db7-9364-edf95ee37063",
    "Marcos Caique": "https://open.feishu.cn/open-apis/bot/v2/hook/99557a7f-ca4e-4ede-b9e5-ccd7ad85b96a"
}

# ======================================================
# 🔧 FUNÇÕES AUXILIARES
# ======================================================
def encontrar_arquivo_entrada(pasta):
    arquivos = [f for f in os.listdir(pasta) if f.lower().endswith((".xls", ".xlsx")) and not f.startswith("~$")]
    if not arquivos:
        raise FileNotFoundError("❌ Nenhum arquivo Excel encontrado na pasta de entrada.")
    arquivos.sort(key=lambda f: os.path.getmtime(os.path.join(pasta, f)), reverse=True)
    return os.path.join(pasta, arquivos[0])

def carregar_excel(path):
    return pd.read_excel(path, dtype=str, engine="openpyxl")

def to_float_safe(series):
    return pd.to_numeric(series.astype(str).str.replace(",", ".").str.extract(r"(\d+\.?\d*)")[0], errors="coerce").fillna(0)

# ======================================================
# 🎨 CARD FEISHU IGUAL AO DA IMAGEM
# ======================================================
def enviar_card_feishu_card(webhook, nome_coord, total_pedidos, custo_total, bases, top5):
    custo_fmt = f"R$ {custo_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    elementos_top5 = []
    for base, custo in top5:
        custo_b_fmt = f"R$ {custo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        elementos_top5.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"🪙 **{base} — {custo_b_fmt}**"
            }
        })

    card = {
        "msg_type": "interactive",
        "card": {
            "header": {
                "template": "green",
                "title": {
                    "tag": "plain_text",
                    "content": f"💰 Custos - {nome_coord}"
                }
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": (
                            f"👤 **Coordenador:** {nome_coord}\n"
                            f"📅 **Atualizado em:** {DATA_HUMANA}\n"
                            f"📦 **Total de pedidos:** {total_pedidos}\n"
                            f"💰 **Custo total:** {custo_fmt}\n"
                            f"🏢 **Bases Avaliadas:** {len(bases)}\n"
                        )
                    }
                },
                {"tag": "hr"},
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": "🔻 **5 Maiores Custos:**"
                    }
                },
                *elementos_top5,
                {"tag": "hr"},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {
                                "tag": "plain_text",
                                "content": "📁 Abrir Pasta no OneDrive"
                            },
                            "url": LINK_PASTA,
                            "type": "primary"
                        }
                    ]
                }
            ]
        }
    }

    requests.post(webhook, json=card)


# ======================================================
# 🚀 PROCESSAMENTO PRINCIPAL
# ======================================================
if __name__ == "__main__":
    print("🚀 Iniciando consolidação de custos por coordenador...\n")

    try:
        FILE_PATH = encontrar_arquivo_entrada(BASE_DIR)
        print(f"📂 Arquivo selecionado: {os.path.basename(FILE_PATH)}")

        df = carregar_excel(FILE_PATH)
        print(f"📄 Planilha carregada ({len(df):,} linhas)".replace(",", "."))

        # 🔥 Remover remessas com sufixo "-001"
        if "Remessa" in df.columns:
            antes = len(df)
            df["Remessa"] = df["Remessa"].astype(str).str.strip()
            df = df[~df["Remessa"].str.contains("-", na=False)]
            print(f"🧹 Removidas {antes - len(df)} remessas com sufixo '-XX'.")

        # 🔎 Filtrar GP / GO / PA
        df["Regional responsável"] = df["Regional responsável"].astype(str).str.upper().str.strip()
        df = df[df["Regional responsável"].isin(["GP", "GO", "PA"])]

        # 🔧 Vincular coordenadores
        df_coord = pd.read_excel(COORDENADOR_PATH)
        col_coord = "Coordenadores" if "Coordenadores" in df_coord.columns else "Coordenador"
        df_coord.rename(columns={col_coord: "Coordenadores"}, inplace=True)

        df["Base responsável"] = df["Base responsável"].astype(str).str.upper().str.strip()
        df_coord["Nome da base"] = df_coord["Nome da base"].astype(str).str.upper().str.strip()

        df = pd.merge(
            df, df_coord[["Nome da base", "Coordenadores"]],
            left_on="Base responsável", right_on="Nome da base", how="left"
        ).drop(columns=["Nome da base"], errors="ignore")

        print("👥 Coordenadores vinculados.")

        # 💰 Converter valor
        df["Custo_R$"] = to_float_safe(df["Valor a pagar (yuan)"]) if "Valor a pagar (yuan)" in df.columns else 0

        # 💾 Salvar Excel
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Base_Processada")

        print(f"💾 Arquivo salvo em:\n{ARQUIVO_SAIDA}\n")

        # ======================================================
        # 📤 Enviar cards no Feishu
        # ======================================================
        print("📤 Enviando cards Feishu...\n")

        coordenadores = sorted(df["Coordenadores"].dropna().unique())

        for coord in coordenadores:
            if coord not in COORDENADOR_WEBHOOKS:
                print(f"⚠️ Sem webhook para: {coord}")
                continue

            df_c = df[df["Coordenadores"] == coord]
            total_pedidos = len(df_c)
            custo_total = df_c["Custo_R$"].sum()
            bases = sorted(df_c["Base responsável"].unique())

            top5 = (
                df_c.groupby("Base responsável")["Custo_R$"]
                .sum()
                .sort_values(ascending=False)
                .head(5)
                .items()
            )

            enviar_card_feishu_card(
                webhook=COORDENADOR_WEBHOOKS[coord],
                nome_coord=coord,
                total_pedidos=total_pedidos,
                custo_total=custo_total,
                bases=bases,
                top5=top5
            )

            print(f"✔ Enviado para {coord} ({total_pedidos} pedidos).")

        print("\n🏁 Processo concluído com sucesso!")

    except Exception as e:
        print(f"\n❌ ERRO:\n{e}")
