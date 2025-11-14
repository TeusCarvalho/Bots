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
# 🧩 FUNÇÕES AUXILIARES
# ======================================================
def format_currency(value):
    try:
        return f"R$ {float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"

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
# 🚀 PROCESSAMENTO PRINCIPAL
# ======================================================
if __name__ == "__main__":
    print("🚀 Iniciando consolidação de custos por coordenador...\n")

    try:
        FILE_PATH = encontrar_arquivo_entrada(BASE_DIR)
        print(f"📂 Arquivo selecionado: {os.path.basename(FILE_PATH)}")

        df = carregar_excel(FILE_PATH)
        print(f"📄 Planilha carregada ({len(df):,} linhas)".replace(",", "."))

        # ======================================================
        # 🔎 FILTRA APENAS AS REGIONAIS GP, GO E PA
        # ======================================================
        if "Regional responsável" in df.columns:

            df["Regional responsável"] = (
                df["Regional responsável"]
                .astype(str)
                .str.strip()
                .str.upper()
            )

            regionais_validas = ["GP", "GO", "PA"]
            antes = len(df)

            df = df[df["Regional responsável"].isin(regionais_validas)]
            depois = len(df)

            print(f"🏢 Regionais filtradas: Mantidas apenas GP, GO e PA ({antes} → {depois} linhas).")

        else:
            print("⚠️ Coluna 'Regional responsável' não encontrada. Filtro ignorado.")

        # 🔧 Normaliza e junta coordenadores
        df_coord = pd.read_excel(COORDENADOR_PATH)
        coord_col = "Coordenadores" if "Coordenadores" in df_coord.columns else "Coordenador"
        df_coord.rename(columns={coord_col: "Coordenadores"}, inplace=True)

        df["Base responsável"] = df["Base responsável"].astype(str).str.strip().str.upper()
        df_coord["Nome da base"] = df_coord["Nome da base"].astype(str).str.strip().str.upper()

        df = pd.merge(
            df, df_coord[["Nome da base", "Coordenadores"]],
            left_on="Base responsável", right_on="Nome da base", how="left"
        ).drop(columns=["Nome da base"], errors="ignore")

        print("👥 Coordenadores vinculados com sucesso.")

        # 💰 Converte 'Valor a pagar (yuan)' em float
        if "Valor a pagar (yuan)" in df.columns:
            df["Custo_R$"] = to_float_safe(df["Valor a pagar (yuan)"])
        else:
            df["Custo_R$"] = 0

        # 🧮 Resumo de custos
        resumo_coord = (
            df.groupby(["Coordenadores", "Base responsável"], dropna=False)
            .agg({
                "Remessa": "count",
                "Custo_R$": "sum"
            })
            .reset_index()
        )

        resumo_coord.rename(columns={
            "Remessa": "Total_Pedidos",
            "Custo_R$": "Custo_Total_R$"
        }, inplace=True)

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with pd.ExcelWriter(ARQUIVO_SAIDA, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Base_Processada")
            resumo_coord.to_excel(writer, index=False, sheet_name="Resumo_por_Coordenador")

        print(f"\n💾 Arquivo salvo com sucesso em:\n{ARQUIVO_SAIDA}\n")

        print("\n🏁 Processo concluído com sucesso!")

    except Exception as e:
        print(f"\n❌ Erro ao processar:\n{e}")
