# -*- coding: utf-8 -*-
# 🚀 União de Planilhas com Polars - versão com múltiplas abas Excel
# Autor: bb-assistente 😎

import polars as pl
import os
import sys
import tkinter as tk
from tkinter import filedialog
from datetime import datetime
import pandas as pd  # usado só para exportar Excel com múltiplas abas

def juntar_planilhas_na_pasta(diretorio_entrada=None, nome_saida="planilha_unificada"):
    # === Se o diretório não for passado, abre seletor ===
    if diretorio_entrada is None:
        print("📂 Selecione a pasta com as planilhas...")
        root = tk.Tk()
        root.withdraw()
        diretorio_entrada = filedialog.askdirectory(title="Selecione a pasta com as planilhas")
        root.destroy()

        if not diretorio_entrada:
            print("❌ Nenhuma pasta selecionada. Operação cancelada.")
            return

    print(f"🚀 Iniciando união de planilhas em: {diretorio_entrada}")

    # === Listar arquivos ===
    try:
        arquivos = [f for f in os.listdir(diretorio_entrada)
                    if os.path.isfile(os.path.join(diretorio_entrada, f))]
        arquivos_planilha = [f for f in arquivos if f.lower().endswith(('.csv', '.xlsx', '.xls'))]
    except FileNotFoundError:
        print(f"❌ Diretório '{diretorio_entrada}' não encontrado.")
        return

    if not arquivos_planilha:
        print("⚠️ Nenhum arquivo CSV/XLSX/XLS encontrado.")
        return

    planilhas = []

    # === Ler os arquivos ===
    for arquivo in arquivos_planilha:
        caminho = os.path.join(diretorio_entrada, arquivo)
        try:
            if arquivo.lower().endswith('.csv'):
                df = pl.read_csv(caminho, separator=';', ignore_errors=True)
            elif arquivo.lower().endswith(('.xlsx', '.xls')):
                df = pl.read_excel(caminho)
            else:
                continue

            print(f"📖 {arquivo} -> {df.shape[0]} linhas, {df.shape[1]} colunas")
            planilhas.append(df)

        except Exception as e:
            print(f"❌ Erro ao ler {arquivo}: {e}")

    if not planilhas:
        print("⚠️ Nenhuma planilha foi lida com sucesso.")
        return

    # === Concatenar ===
    print("🧩 Concatenando planilhas...")
    planilha_final = pl.concat(planilhas, how="diagonal_relaxed")
    planilha_final = planilha_final.fill_null('')

    total_linhas, total_colunas = planilha_final.shape
    print(f"✅ Planilha final contém {total_linhas} linhas e {total_colunas} colunas")

    # === Exportar CSV ===
    caminho_csv = os.path.join(diretorio_entrada, f"{nome_saida}.csv")
    planilha_final.write_csv(caminho_csv)
    print(f"💾 CSV salvo em: {caminho_csv}")

    # === Exportar Excel dividido em abas ===
    caminho_xlsx = os.path.join(diretorio_entrada, f"{nome_saida}.xlsx")
    max_linhas = 1_000_000  # limite de linhas por aba

    print("📘 Exportando para Excel dividido em abas...")

    # converte para pandas (somente na exportação)
    df_pandas = planilha_final.to_pandas(use_pyarrow_extension_array=True)

    with pd.ExcelWriter(caminho_xlsx, engine="openpyxl") as writer:
        partes = (total_linhas // max_linhas) + 1
        for i in range(partes):
            inicio = i * max_linhas
            fim = min((i + 1) * max_linhas, total_linhas)
            parte = df_pandas.iloc[inicio:fim]
            sheet_name = f"Parte_{i + 1}"
            parte.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f" -> Aba {sheet_name} com {len(parte)} linhas salva.")

    print(f"✅ Excel salvo com sucesso em: {caminho_xlsx}")
    print(f"✨ Finalizado em {datetime.now().strftime('%H:%M:%S')} ✨")


# === Execução Principal ===
if __name__ == "__main__":
    diretorio = sys.argv[1] if len(sys.argv) > 1 else None
    juntar_planilhas_na_pasta(diretorio)
