# -*- coding: utf-8 -*-
import pandas as pd
import os
import sys
import tkinter as tk
from tkinter import filedialog


def juntar_planilhas_na_pasta(diretorio_entrada=None, nome_saida="planilha_unificada"):
    # --- Seleção de pasta caso não seja passado por argumento ---
    if diretorio_entrada is None:
        print("Abrindo seletor de diretório. Por favor, escolha a pasta com as planilhas.")
        root = tk.Tk()
        root.withdraw()
        diretorio_entrada = filedialog.askdirectory(title="Selecione a pasta com as planilhas")
        root.destroy()

        if not diretorio_entrada:
            print("Nenhuma pasta selecionada. Operação cancelada.")
            return

    print(f"Iniciando o processo de união de planilhas na pasta: {diretorio_entrada}...")

    planilhas_juntas = []

    # --- Lista de arquivos ---
    try:
        arquivos = [f for f in os.listdir(diretorio_entrada) if os.path.isfile(os.path.join(diretorio_entrada, f))]
        arquivos_planilha = [f for f in arquivos if f.endswith(('.csv', '.xlsx', '.xls'))]
    except FileNotFoundError:
        print(f"Erro: O diretório '{diretorio_entrada}' não foi encontrado.")
        return

    if not arquivos_planilha:
        print("Nenhum arquivo de planilha (CSV, XLSX, XLS) encontrado na pasta.")
        return

    # --- Leitura dos arquivos ---
    for arquivo in arquivos_planilha:
        caminho_completo = os.path.join(diretorio_entrada, arquivo)
        try:
            if arquivo.endswith('.csv'):
                # Usa separador fixo e engine rápido
                df = pd.read_csv(caminho_completo, sep=";", engine="c", low_memory=False)
            elif arquivo.endswith(('.xlsx', '.xls')):
                # Excel é mais lento, mas ainda funciona
                df = pd.read_excel(caminho_completo)
            else:
                continue

            print(f"Lendo {arquivo} -> {len(df)} linhas e {len(df.columns)} colunas.")
            planilhas_juntas.append(df)

        except Exception as e:
            print(f"Erro ao ler o arquivo {arquivo}: {e}")

    if not planilhas_juntas:
        print("Não foi possível ler nenhuma planilha com sucesso.")
        return

    # --- Concatena todas ---
    planilha_final = pd.concat(planilhas_juntas, ignore_index=True, sort=False)
    planilha_final = planilha_final.fillna('')

    total_linhas = len(planilha_final)
    total_colunas = len(planilha_final.columns)

    print(f"\nPlanilha final contém {total_colunas} colunas e {total_linhas} linhas.")

    # --- Exportação: CSV mais rápido ---
    caminho_saida_csv = os.path.join(diretorio_entrada, f"{nome_saida}.csv")
    planilha_final.to_csv(caminho_saida_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Arquivo CSV salvo em: {caminho_saida_csv}")

    # --- Exportação para Excel somente se necessário ---
    if total_linhas > 1_000_000:
        print("⚠️ Planilha muito grande, será dividida em múltiplas abas no Excel...")
        caminho_saida_xlsx = os.path.join(diretorio_entrada, f"{nome_saida}.xlsx")
        with pd.ExcelWriter(caminho_saida_xlsx, engine="openpyxl") as writer:
            max_linhas = 1_000_000
            partes = (total_linhas // max_linhas) + 1
            for i in range(partes):
                inicio = i * max_linhas
                fim = min((i + 1) * max_linhas, total_linhas)
                df_parte = planilha_final.iloc[inicio:fim]
                df_parte.to_excel(writer, sheet_name=f"Parte_{i+1}", index=False)
                print(f" -> Exportado Parte_{i+1} com {len(df_parte)} linhas.")
        print(f"✅ Arquivo Excel salvo em: {caminho_saida_xlsx}")
    else:
        print("Planilha não ultrapassa 1 milhão de linhas, exportação Excel desnecessária.")


# --- Execução principal ---
if __name__ == "__main__":
    diretorio = sys.argv[1] if len(sys.argv) > 1 else None
    juntar_planilhas_na_pasta(diretorio)