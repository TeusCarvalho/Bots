# -*- coding: utf-8 -*-
# pip install pandas polars tqdm xlsxwriter openpyxl

import os
import pandas as pd
import polars as pl
from tqdm import tqdm

# ==========================================================
# CONFIG
# ==========================================================

PASTA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Nova pasta\Entregues"
ARQUIVO_SAIDA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Nova pasta\resumo_motoristas.xlsx"

COL_MOTORISTA = "Responsável pela entrega"
COL_PEDIDO = "Número de pedido JMS"
COL_STATUS = "Marca de assinatura"

STATUS_FILTRO = "Recebimento com assinatura normal".lower().strip()

dfs = []

# ==========================================================
# LEITURA SEGURA DE TODAS AS ABAS COM PANDAS
# ==========================================================

print("\n🔍 Lendo planilhas...\n")

arquivos = [f for f in os.listdir(PASTA) if f.endswith((".xlsx", ".xls"))]

for arquivo in tqdm(arquivos):
    caminho = os.path.join(PASTA, arquivo)

    try:
        excel = pd.ExcelFile(caminho)

        for aba in excel.sheet_names:
            try:
                dfp = pd.read_excel(caminho, sheet_name=aba)

                # Apenas adiciona se tiver linhas
                if dfp.shape[0] > 0:
                    dfs.append(dfp)

            except:
                pass  # ignora aba ruim

    except Exception as e:
        print(f"⚠️ Erro lendo {arquivo}: {e}")

# ==========================================================
# CONSOLIDAÇÃO
# ==========================================================

if not dfs:
    print("❌ Nenhuma linha encontrada nos arquivos.")
    quit()

df_total_pd = pd.concat(dfs, ignore_index=True)

# Converte para Polars para acelerar
df = pl.from_pandas(df_total_pd)

# ==========================================================
# LIMPEZA E FILTRO
# ==========================================================

# Garantir que a coluna existe
for col in [COL_MOTORISTA, COL_PEDIDO, COL_STATUS]:
    if col not in df.columns:
        print(f"❌ A coluna '{col}' NÃO existe nos arquivos.")
        quit()

# Normaliza status
df = df.with_columns(
    pl.col(COL_STATUS).cast(str).str.to_lowercase().str.strip()
)

# Filtra entregas com assinatura normal
df_filtrado = df.filter(
    pl.col(COL_STATUS) == STATUS_FILTRO
)

if df_filtrado.height() == 0:
    print("❌ Nenhum registro encontrado com o status 'Recebimento com assinatura normal'.")
    quit()

# ==========================================================
# AGRUPAMENTO / RESUMO FINAL
# ==========================================================

resumo = (
    df_filtrado
    .group_by(COL_MOTORISTA)
    .agg(QuantidadePedidos=pl.count(COL_PEDIDO))
    .sort("QuantidadePedidos", descending=True)
)

# ==========================================================
# SALVAR EXCEL FINAL
# ==========================================================

with pd.ExcelWriter(ARQUIVO_SAIDA, engine="xlsxwriter") as writer:
    resumo.to_pandas().to_excel(writer, sheet_name="Resumo", index=False)

print("\n✅ Arquivo gerado com sucesso!")
print("📁 Local:", ARQUIVO_SAIDA)
