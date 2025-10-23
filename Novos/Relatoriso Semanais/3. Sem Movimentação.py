# -*- coding: utf-8 -*-
"""
📊 Sem Movimentação - v5.0
Gera resumo de aging por base e mostra Top 5 por tipo de estação
"""

import pandas as pd
import os
from tabulate import tabulate

# ======================================================
# ⚙️ CONFIGURAÇÕES
# ======================================================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal"
SEM_MOV_DIR = os.path.join(BASE_DIR, "3. Sem Movimentação")
BASES_INFO_PATH = os.path.join(BASE_DIR, "Bases_Info.xlsx")
OUTPUT_PATH = os.path.join(BASE_DIR, "Top5_Por_Tipo_Estacao.xlsx")

# ======================================================
# 🚀 PROCESSAMENTO
# ======================================================
try:
    # 📂 Arquivo mais recente
    arquivos = [f for f in os.listdir(SEM_MOV_DIR) if f.lower().endswith((".xlsx", ".xls")) and not f.startswith("~$")]
    if not arquivos:
        raise FileNotFoundError("⚠️ Nenhum arquivo Excel encontrado em '3. Sem Movimentação'.")
    arquivos.sort(key=lambda f: os.path.getmtime(os.path.join(SEM_MOV_DIR, f)), reverse=True)
    file_path = os.path.join(SEM_MOV_DIR, arquivos[0])
    print(f"✅ Arquivo de Sem Movimentação: {os.path.basename(file_path)}")

    # 📖 Leitura
    df = pd.read_excel(file_path, dtype=str)
    df_info = pd.read_excel(BASES_INFO_PATH, dtype=str)

    # 🧼 Limpeza
    df.columns = df.columns.str.replace(r'[\s\u3000\xa0]+', '', regex=True)
    df_info.columns = df_info.columns.str.replace(r'[\s\u3000\xa0]+', '', regex=True)

    # 🔍 Colunas principais
    col_regional = "Regionalresponsável责任所属代理区"
    col_base = "Unidaderesponsável责任机构"
    col_aging = "Aging超时类型"

    # 🔎 Filtro GP
    if col_regional in df.columns:
        df = df[df[col_regional].astype(str).str.strip() == "GP"]
        print(f"✅ Linhas após filtro 'Regional = GP': {len(df):,}".replace(",", "."))
    else:
        raise KeyError(f"❌ Coluna '{col_regional}' não encontrada.")

    # 🔢 Define aging de interesse
    aging_map = {
        "Exceed 6 days with no track": "6dias",
        "Exceed 7 days with no track": "7dias",
        "Exceed 10 days with no track": "10dias",
        "Exceed 14 days with no track": "14dias",
        "Exceed 30 days with no track": "30dias"
    }

    # 🧮 Cria tabela dinâmica
    df["AgingLabel"] = df[col_aging].map(aging_map)
    df = df[df["AgingLabel"].notna()]

    resumo = df.groupby(["Unidaderesponsável责任机构", "AgingLabel"]).size().unstack(fill_value=0)
    for col in aging_map.values():
        if col not in resumo.columns:
            resumo[col] = 0
    resumo["Total"] = resumo[list(aging_map.values())].sum(axis=1)
    resumo.reset_index(inplace=True)

    print(f"📊 {len(resumo):,} bases consolidadas.".replace(",", "."))

    # 🔗 Merge com Bases_Info
    col_nome_base = "Nomedabase"
    col_tipo_estacao = "Tipodeestação"

    df_final = pd.merge(
        resumo,
        df_info[[col_nome_base, col_tipo_estacao]],
        how="left",
        left_on="Unidaderesponsável责任机构",
        right_on=col_nome_base
    )

    # 📈 Resumo geral
    resumo_total = (
        df_final.groupby(col_tipo_estacao)[["6dias", "7dias", "10dias", "14dias", "30dias", "Total"]]
        .sum()
        .reset_index()
        .sort_values("Total", ascending=False)
    )

    # 💾 Excel
    resultados = []
    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        resumo_total.to_excel(writer, index=False, sheet_name="Resumo_Geral")

        for tipo in resumo_total[col_tipo_estacao]:
            df_tipo = df_final[df_final[col_tipo_estacao] == tipo]
            if not df_tipo.empty:
                top5 = df_tipo.sort_values("Total", ascending=False).head(5)
                top5.to_excel(writer, index=False, sheet_name=f"Top5_{tipo[:25]}")
                resultados.append((tipo, top5))

    # 🖥️ Terminal
    print("\n🔥 Top 5 Piores Bases por Tipo de Estação:")
    for tipo, top5 in resultados:
        print(f"\n🏷️ Tipo de Estação: {tipo}")
        print(tabulate(
            top5[["Unidaderesponsável责任机构", "6dias", "7dias", "10dias", "14dias", "30dias", "Total"]],
            headers=["SC (Base)", "6 dias", "7 dias", "10 dias", "14 dias", "30 dias", "Total"],
            tablefmt="pretty",
            showindex=False
        ))

    print("\n" + "="*70)
    print("📊 RELATÓRIO CONCLUÍDO COM SUCESSO")
    print("="*70)
    print(f"💾 Arquivo salvo em: {OUTPUT_PATH}")
    print("="*70 + "\n")

except Exception as e:
    print(f"\n❌ Erro ao processar o arquivo:\n{e}")
