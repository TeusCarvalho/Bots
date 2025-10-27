# -*- coding: utf-8 -*-
"""
📊 Sem Movimentação - v5.6
Lê TODAS as planilhas e TODAS as abas da pasta, consolida e gera:
- Resumo de aging por base
- Rankings geral e por tipo de estação (Top 5 piores/melhores)
- Distribuição por tipo de problema (com TOTAL GERAL) indentada/alinhada
"""

import pandas as pd
import os
import re
import textwrap
from tabulate import tabulate

# ======================================================
# ⚙️ CONFIGURAÇÕES
# ======================================================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal"
SEM_MOV_DIR = os.path.join(BASE_DIR, "3. Sem Movimentação")
BASES_INFO_PATH = os.path.join(BASE_DIR, "Bases_Info.xlsx")
OUTPUT_PATH = os.path.join(BASE_DIR, "Top5_Por_Tipo_Estacao.xlsx")

TOP_N_GERAL = 10  # quantas bases no ranking geral

# ======================================================
# 🔧 Funções utilitárias
# ======================================================
def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.replace(r'[\s\u3000\xa0]+', '', regex=True)
    return df

def _ensure_columns(df: pd.DataFrame, required: list[str], ctx: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"❌ Colunas ausentes em {ctx}: {missing}\nDisponíveis: {list(df.columns)}")

def _safe_top(df_in: pd.DataFrame, n: int, ascending: bool) -> pd.DataFrame:
    if df_in.empty:
        return df_in
    return df_in.sort_values("Total", ascending=ascending).head(n)

def _wrap_text(s: str, width: int = 60, indent: str = "   ") -> str:
    """Quebra texto longo (sem cortar palavras) e indenta as linhas 2+."""
    s = str(s).strip()
    s = re.sub(r'(?<=\w)\.(?=\w)', ' ', s)  # troca A.B por A B
    lines = textwrap.wrap(s, width=width, break_long_words=False, break_on_hyphens=False)
    if not lines:
        return ""
    return ("\n" + indent).join(lines)

def _listar_arquivos_xlsx(pasta: str) -> list[str]:
    return [
        f for f in os.listdir(pasta)
        if f.lower().endswith((".xlsx", ".xls")) and not f.startswith("~$")
    ]

# ======================================================
# 🚀 PROCESSAMENTO
# ======================================================
try:
    # 📂 Todos os arquivos
    arquivos = _listar_arquivos_xlsx(SEM_MOV_DIR)
    if not arquivos:
        raise FileNotFoundError("⚠️ Nenhum arquivo Excel encontrado em '3. Sem Movimentação'.")

    # Ordena do mais novo para o mais antigo (apenas informativo)
    arquivos.sort(key=lambda f: os.path.getmtime(os.path.join(SEM_MOV_DIR, f)), reverse=True)
    print("✅ Arquivos encontrados (mais novo → mais antigo):")
    for i, fn in enumerate(arquivos, 1):
        print(f"  {i:02d}. {fn}")

    # 📖 Leitura de TODAS as abas de TODOS os arquivos
    dfs = []
    total_abas = 0
    lidos_ok = 0
    pulados = 0

    # Colunas esperadas (nomes sem espaço após _clean_cols)
    col_regional = "Regionalresponsável责任所属代理区"
    col_base     = "Unidaderesponsável责任机构"
    col_aging    = "Aging超时类型"
    col_problema = "Nomedepacoteproblemático问题件名称"  # (sem espaços)

    for arq in arquivos:
        path = os.path.join(SEM_MOV_DIR, arq)
        try:
            # sheet_name=None => dict {aba: DataFrame}
            book = pd.read_excel(path, dtype=str, sheet_name=None)
        except Exception as e:
            print(f"⚠️ Falha ao abrir '{arq}': {e}")
            pulados += 1
            continue

        for aba, df_aba in (book or {}).items():
            total_abas += 1
            if df_aba is None or df_aba.empty:
                continue
            df_aba = _clean_cols(df_aba)
            # Checagem mínima para garantir que dá para consolidar
            if all(c in df_aba.columns for c in [col_regional, col_base, col_aging]):
                df_aba["__Arquivo"] = arq
                df_aba["__Aba"] = str(aba)
                dfs.append(df_aba)
                lidos_ok += 1
            else:
                print(f"ℹ️ Pulando aba '{aba}' de '{arq}' por falta de colunas mínimas.")

    if not dfs:
        raise ValueError("⚠️ Nenhuma aba válida encontrada após varrer todos os arquivos.")

    df = pd.concat(dfs, ignore_index=True)
    print(f"\n📚 Consolidação concluída: {len(arquivos)} arquivo(s), {total_abas} aba(s), {lidos_ok} aba(s) válidas.")

    # 📖 Bases_Info
    df_info = pd.read_excel(BASES_INFO_PATH, dtype=str)
    df_info = _clean_cols(df_info)

    # 🔍 Dicionário de bases
    col_nome_base    = "Nomedabase"
    col_tipo_estacao = "Tipodeestação"

    _ensure_columns(df_info, [col_nome_base, col_tipo_estacao], "Bases_Info.xlsx")

    # 🔎 Filtro GP
    df[col_regional] = df[col_regional].astype(str).str.strip()
    df = df[df[col_regional].str.upper() == "GP"]
    print(f"✅ Linhas após filtro 'Regional = GP': {len(df):,}".replace(",", "."))

    if df.empty:
        raise ValueError("⚠️ Após o filtro 'Regional = GP', não há linhas para processar.")

    # 🔢 Faixas de aging
    aging_map = {
        "Exceed 6 days with no track":  "6dias",
        "Exceed 7 days with no track":  "7dias",
        "Exceed 10 days with no track": "10dias",
        "Exceed 14 days with no track": "14dias",
        "Exceed 30 days with no track": "30dias"
    }

    # 🧮 Mapeia aging
    df["AgingLabel"] = df[col_aging].map(aging_map)
    df = df[df["AgingLabel"].notna()]
    if df.empty:
        raise ValueError("⚠️ Não há linhas com categorias de aging mapeadas (6/7/10/14/30 dias).")

    # 🧮 Tabela por base
    resumo = df.groupby([col_base, "AgingLabel"]).size().unstack(fill_value=0)
    for col in aging_map.values():
        if col not in resumo.columns:
            resumo[col] = 0
    resumo["Total"] = resumo[list(aging_map.values())].sum(axis=1)
    resumo.reset_index(inplace=True)

    # 🧮 TOTAL GERAL (pós-filtros)
    qtd_total_pedidos = int(resumo["Total"].sum())
    print(f"\n📦 Quantidade TOTAL de pedidos (já filtrados e consolidados): {qtd_total_pedidos:,}".replace(",", "."))

    print(f"📊 {len(resumo):,} bases consolidadas.".replace(",", "."))

    # 🔗 Tipos de estação
    df_final = pd.merge(
        resumo,
        df_info[[col_nome_base, col_tipo_estacao]],
        how="left",
        left_on=col_base,
        right_on=col_nome_base
    )
    df_final[col_tipo_estacao] = df_final[col_tipo_estacao].fillna("Sem classificação")

    # 📈 Resumo por tipo de estação
    cols_aging = ["6dias", "7dias", "10dias", "14dias", "30dias", "Total"]
    resumo_total = (
        df_final.groupby(col_tipo_estacao)[cols_aging]
        .sum()
        .reset_index()
        .sort_values("Total", ascending=False)
    )
    resumo_total["Total_Geral_Pedidos"] = qtd_total_pedidos

    # 🏆 Top bases geral
    top_bases_geral = (
        df_final[[col_base, "Total"]]
        .groupby(col_base, as_index=False)
        .sum(numeric_only=True)
        .sort_values("Total", ascending=False)
        .head(TOP_N_GERAL)
    )

    # 🧩 Resumo de Problemas
    problemas_df = pd.DataFrame(columns=["Tipo de Problema", "Quantidade", "% do Total"])
    if col_problema in df.columns:
        tmp = (
            df[col_problema]
            .fillna("Sem informação")
            .astype(str)
            .str.strip()
            .replace({"": "Sem informação"})
            .value_counts(dropna=False)
            .reset_index()
        )
        tmp.columns = ["Tipo de Problema", "Quantidade"]
        tmp["Quantidade"] = tmp["Quantidade"].astype(int)
        tmp["% do Total"] = (tmp["Quantidade"] / max(qtd_total_pedidos, 1) * 100).round(2)

        total_row = pd.DataFrame({
            "Tipo de Problema": ["TOTAL GERAL"],
            "Quantidade": [int(tmp["Quantidade"].sum())],
            "% do Total": [100.00]
        })
        problemas_df = pd.concat([tmp, total_row], ignore_index=True)
    else:
        print(f"ℹ️ Coluna de problemas não encontrada: '{col_problema}'. Pulando 'Resumo_Problemas'.")

    # 💾 Excel
    resultados_piores = []
    resultados_melhores = []

    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        resumo_total.to_excel(writer, index=False, sheet_name="Resumo_Geral")
        top_bases_geral.to_excel(writer, index=False, sheet_name=f"Top_{TOP_N_GERAL}_Bases_Geral")
        if not problemas_df.empty:
            problemas_xlsx = problemas_df.copy()
            problemas_xlsx["% do Total"] = problemas_xlsx["% do Total"].astype(float)
            problemas_xlsx.to_excel(writer, index=False, sheet_name="Resumo_Problemas")

        for tipo in resumo_total[col_tipo_estacao]:
            df_tipo = df_final[df_final[col_tipo_estacao] == tipo].copy()
            if df_tipo.empty:
                continue
            top5_piores = _safe_top(df_tipo, 5, ascending=False)
            top5_piores.to_excel(writer, index=False, sheet_name=f"Top5_Piores_{tipo[:22]}")
            resultados_piores.append((tipo, top5_piores))

            df_tipo_demanda = df_tipo[df_tipo["Total"] > 0]
            top5_melhores = _safe_top(df_tipo_demanda, 5, ascending=True)
            top5_melhores.to_excel(writer, index=False, sheet_name=f"Top5_Melhores_{tipo[:21]}")
            resultados_melhores.append((tipo, top5_melhores))

    # 🖥️ Terminal — Resumo por tipo
    print("\n" + "="*70)
    print("📌 RESUMO GERAL POR TIPO DE ESTAÇÃO")
    print("="*70)
    print(tabulate(
        resumo_total[[col_tipo_estacao] + cols_aging + ["Total_Geral_Pedidos"]],
        headers=["Tipo de Estação"] + ["6 dias", "7 dias", "10 dias", "14 dias", "30 dias", "Total", "Total Geral"],
        tablefmt="pretty",
        showindex=False
    ))

    # 🖥️ Terminal — Top bases geral
    print("\n🏆 Top", TOP_N_GERAL, "Bases com MAIS pedidos (Geral):")
    print(tabulate(
        top_bases_geral[[col_base, "Total"]],
        headers=["SC (Base)", "Total"],
        tablefmt="pretty",
        showindex=False
    ))

    # 🖥️ Terminal — Resumo de Problemas (INDENTADO)
    if not problemas_df.empty:
        disp = problemas_df.copy()
        disp["% do Total"] = disp["% do Total"].map(lambda x: f"{x:.2f} %")
        disp["Quantidade"] = disp["Quantidade"].map(lambda x: f"{x:,}".replace(",", "."))
        disp["Tipo de Problema"] = disp["Tipo de Problema"].apply(lambda s: _wrap_text(s, width=60, indent="   "))

        print("\n🧩 Quantidade por tipo de problema:")
        print("─" * 65)
        print(tabulate(
            disp[["Tipo de Problema", "Quantidade", "% do Total"]],
            headers=["Tipo de Problema", "Quantidade", "% do Total"],
            tablefmt="fancy_grid",
            colalign=("left", "right", "right"),
            showindex=False
        ))

    # 🖥️ Terminal — Top 5 por tipo
    print("\n🔥 Top 5 Piores Bases por Tipo de Estação:")
    for tipo, top5 in resultados_piores:
        if top5.empty:
            continue
        print(f"\n🏷️ Tipo de Estação: {tipo}")
        print(tabulate(
            top5[[col_base, "6dias", "7dias", "10dias", "14dias", "30dias", "Total"]],
            headers=["SC (Base)", "6 dias", "7 dias", "10 dias", "14 dias", "30 dias", "Total"],
            tablefmt="pretty",
            showindex=False
        ))

    print("\n💎 Top 5 Melhores Bases por Tipo de Estação (com demanda):")
    for tipo, top5 in resultados_melhores:
        if top5.empty:
            continue
        print(f"\n🏷️ Tipo de Estação: {tipo}")
        print(tabulate(
            top5[[col_base, "6dias", "7dias", "10dias", "14dias", "30dias", "Total"]],
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
