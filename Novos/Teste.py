# -*- coding: utf-8 -*-

import os
import polars as pl
import pandas as pd
import warnings
import random

# ==========================================================
# 🚫 Desativar avisos e logs
# ==========================================================
warnings.filterwarnings("ignore", category=UserWarning)
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_rows(5)

# ==========================================================
# 📂 Caminhos
# ==========================================================
PASTA_ORIGEM = (
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho"
    r"\Testes\Politicas de Bonificação\03 - Redução Shipping Time"
)
PASTA_DESTINO = (
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho"
    r"\Testes\Politicas de Bonificação\Resultados"
)
os.makedirs(PASTA_DESTINO, exist_ok=True)

# ==========================================================
# 🧮 Funções auxiliares
# ==========================================================
def classificar_reducao(total_min):
    if total_min < 1:
        return "Fora da Meta"
    elif total_min < 480:
        return "Meta"
    else:
        return "Desafio"

def pontuacao(total_min):
    if total_min < 1:
        return 0.0
    elif total_min < 480:
        return 1.0
    else:
        return 1.1

# ==========================================================
# ⚙️ Leitura de planilhas (Polars Lazy)
# ==========================================================
arquivos = [f for f in os.listdir(PASTA_ORIGEM) if f.endswith(".xlsx") and not f.startswith("~$")]

if not arquivos:
    print("⚠️ Nenhum arquivo Excel encontrado na pasta de origem.")
    raise SystemExit

dfs_lazy = []

for arquivo in arquivos:
    caminho = os.path.join(PASTA_ORIGEM, arquivo)
    try:
        df = pl.read_excel(caminho)
        print(f"📄 Lendo: {arquivo} ✅ ({len(df.columns)} colunas)")

        # Detecta coluna de base automaticamente
        col_base = None
        for nome in ["Nome da base", "Código da base de entrega", "Nome da Base Remetente"]:
            if nome in df.columns:
                col_base = nome
                break

        if not col_base:
            print(f"⚠️ Nenhuma coluna de base encontrada em {arquivo}, pulando.")
            continue

        # Renomear colunas esperadas
        df = df.rename({
            col_base: "Base",
            "Tempo trânsito SC Destino->Base Entrega": "Etapa 6",
            "Tempo médio processamento Base Entrega": "Etapa 7",
            "Tempo médio Saída para Entrega->Entrega": "Etapa 8"
        })

        # Forçar tipos numéricos
        for col in ["Etapa 6", "Etapa 7", "Etapa 8"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64).fill_null(0))
            else:
                df = df.with_columns(pl.lit(0).alias(col))

        # Adiciona nome do arquivo
        df = df.with_columns(pl.lit(arquivo).alias("Arquivo Origem"))
        dfs_lazy.append(df.lazy())

    except Exception as e:
        print(f"❌ Erro ao ler {arquivo}: {e}")

if not dfs_lazy:
    print("⚠️ Nenhum dado válido encontrado.")
    raise SystemExit

# ==========================================================
# 🧩 Consolidação e Cálculos
# ==========================================================
df_total = pl.concat(dfs_lazy).collect()

df_total = df_total.with_columns(
    (pl.col("Etapa 6") + pl.col("Etapa 7") + pl.col("Etapa 8")).alias("Soma Total (min)")
)

df_resumo = (
    df_total.group_by("Base")
    .agg([
        pl.mean("Etapa 6").alias("Etapa 6"),
        pl.mean("Etapa 7").alias("Etapa 7"),
        pl.mean("Etapa 8").alias("Etapa 8"),
        pl.mean("Soma Total (min)").alias("Soma Total (min)")
    ])
)

# Converter para Pandas e aplicar regras
df_final = df_resumo.to_pandas()

df_final["Classificação"] = df_final["Soma Total (min)"].apply(classificar_reducao)
df_final["Pontuação Total"] = df_final["Soma Total (min)"].apply(pontuacao)
df_final["Elegibilidade (%)"] = df_final["Pontuação Total"] * 100
df_final = df_final.sort_values(by="Soma Total (min)", ascending=False)

# ==========================================================
# 💾 Exportar resultado final
# ==========================================================
CAMINHO_ARQUIVO = os.path.join(PASTA_DESTINO, "Redução_ShippingTime.xlsx")
df_final.to_excel(CAMINHO_ARQUIVO, index=False)

print("\n✅ Consolidação concluída com sucesso!")
print(f"💾 Arquivo salvo em: {CAMINHO_ARQUIVO}")

# ==========================================================
# 📊 Comparativo de Teste (Variações Aleatórias)
# ==========================================================
print("\n🔍 Gerando aba de comparativo de teste...")

variacoes = [0.10, -0.10, 0.15, -0.15]
df_comp = df_final.copy()

df_comp["Variação (%)"] = [random.choice(variacoes) * 100 for _ in range(len(df_comp))]
df_comp["Novo Valor (min)"] = df_comp["Soma Total (min)"] * (1 + df_comp["Variação (%)"] / 100)
df_comp["Diferença (min)"] = df_comp["Novo Valor (min)"] - df_comp["Soma Total (min)"]
df_comp["Resultado"] = df_comp["Diferença (min)"].apply(lambda x: "Melhorou" if x < 0 else "Piorou")

# Arredondar valores
df_comp[["Soma Total (min)", "Novo Valor (min)", "Diferença (min)"]] = df_comp[
    ["Soma Total (min)", "Novo Valor (min)", "Diferença (min)"]
].round(2)

# Reorganizar colunas
df_comp = df_comp[
    ["Base", "Soma Total (min)", "Variação (%)", "Novo Valor (min)", "Diferença (min)", "Resultado"]
]

# Salvar nova aba
with pd.ExcelWriter(CAMINHO_ARQUIVO, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
    df_comp.to_excel(writer, index=False, sheet_name="Comparativo_Teste")

print("✅ Aba 'Comparativo_Teste' criada com sucesso!")
print(f"💾 Atualizado em: {CAMINHO_ARQUIVO}")
