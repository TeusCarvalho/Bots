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
ALTERACOES_PATH = os.path.join(BASE_DIR, "Top5_Por_Tipo_Estacao_Alteracoes.xlsx")

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
    s = str(s).strip()
    s = re.sub(r'(?<=\w)\.(?=\w)', ' ', s)
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

    arquivos.sort(key=lambda f: os.path.getmtime(os.path.join(SEM_MOV_DIR, f)), reverse=True)
    print("✅ Arquivos encontrados (mais novo → mais antigo):")
    for i, fn in enumerate(arquivos, 1):
        print(f"  {i:02d}. {fn}")

    dfs = []
    total_abas = 0
    lidos_ok = 0
    pulados = 0

    col_regional = "Regionalresponsável责任所属代理区"
    col_base     = "Unidaderesponsável责任机构"
    col_aging    = "Aging超时类型"
    col_problema = "Nomedepacoteproblemático问题件名称"

    for arq in arquivos:
        path = os.path.join(SEM_MOV_DIR, arq)
        try:
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
    col_nome_base    = "Nomedabase"
    col_tipo_estacao = "Tipodeestação"
    _ensure_columns(df_info, [col_nome_base, col_tipo_estacao], "Bases_Info.xlsx")

    # 🔎 Filtro GP
    df[col_regional] = df[col_regional].astype(str).str.strip()
    df = df[df[col_regional].str.upper() == "GP"]
    print(f"✅ Linhas após filtro 'Regional = GP': {len(df):,}".replace(",", "."))

    if df.empty:
        raise ValueError("⚠️ Após o filtro 'Regional = GP', não há linhas para processar.")

    aging_map = {
        "Exceed 6 days with no track":  "6dias",
        "Exceed 7 days with no track":  "7dias",
        "Exceed 10 days with no track": "10dias",
        "Exceed 14 days with no track": "14dias",
        "Exceed 30 days with no track": "30dias"
    }

    df["AgingLabel"] = df[col_aging].map(aging_map)
    df = df[df["AgingLabel"].notna()]
    if df.empty:
        raise ValueError("⚠️ Não há linhas com categorias de aging mapeadas (6/7/10/14/30 dias).")

    resumo = df.groupby([col_base, "AgingLabel"]).size().unstack(fill_value=0)
    for col in aging_map.values():
        if col not in resumo.columns:
            resumo[col] = 0
    resumo["Total"] = resumo[list(aging_map.values())].sum(axis=1)
    resumo.reset_index(inplace=True)

    qtd_total_pedidos = int(resumo["Total"].sum())
    print(f"\n📦 Quantidade TOTAL de pedidos: {qtd_total_pedidos:,}".replace(",", "."))

    df_final = pd.merge(
        resumo,
        df_info[[col_nome_base, col_tipo_estacao]],
        how="left",
        left_on=col_base,
        right_on=col_nome_base
    )
    df_final[col_tipo_estacao] = df_final[col_tipo_estacao].fillna("Sem classificação")

    cols_aging = ["6dias", "7dias", "10dias", "14dias", "30dias", "Total"]
    resumo_total = (
        df_final.groupby(col_tipo_estacao)[cols_aging]
        .sum()
        .reset_index()
        .sort_values("Total", ascending=False)
    )
    resumo_total["Total_Geral_Pedidos"] = qtd_total_pedidos

    top_bases_geral = (
        df_final[[col_base, "Total"]]
        .groupby(col_base, as_index=False)
        .sum()
        .sort_values("Total", ascending=False)
        .head(TOP_N_GERAL)
    )

    # ======================================================
    # 🆕 COMPARAÇÃO COM A VERSÃO ANTERIOR
    # ======================================================
    if os.path.exists(OUTPUT_PATH):
        try:
            prev_df = pd.read_excel(OUTPUT_PATH, sheet_name=f"Top_{TOP_N_GERAL}_Bases_Geral")
            df_merge = pd.merge(
                prev_df[[col_base, "Total"]],
                top_bases_geral[[col_base, "Total"]],
                on=col_base,
                how="outer",
                suffixes=("_Anterior", "_Atual")
            )

            df_merge["Total_Anterior"] = df_merge["Total_Anterior"].fillna(0).astype(int)
            df_merge["Total_Atual"] = df_merge["Total_Atual"].fillna(0).astype(int)
            df_merge["Diferença"] = df_merge["Total_Atual"] - df_merge["Total_Anterior"]

            def status(row):
                if row["Total_Anterior"] == 0 and row["Total_Atual"] > 0:
                    return "🟢 Nova Base"
                elif row["Total_Anterior"] > 0 and row["Total_Atual"] == 0:
                    return "🔴 Removida"
                elif row["Diferença"] > 0:
                    return "🔼 Aumento"
                elif row["Diferença"] < 0:
                    return "🔽 Redução"
                else:
                    return "⚪ Sem alteração"

            df_merge["Status"] = df_merge.apply(status, axis=1)
            df_merge = df_merge.sort_values("Diferença", ascending=False)
            df_merge.to_excel(ALTERACOES_PATH, index=False)
            print(f"\n📊 Arquivo de alterações gerado com sucesso em:\n{ALTERACOES_PATH}")
        except Exception as e:
            print(f"⚠️ Não foi possível gerar arquivo de alterações: {e}")

    # ======================================================
    # 💾 SALVAR RESULTADOS PRINCIPAIS
    # ======================================================
    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        resumo_total.to_excel(writer, index=False, sheet_name="Resumo_Geral")
        top_bases_geral.to_excel(writer, index=False, sheet_name=f"Top_{TOP_N_GERAL}_Bases_Geral")
        df_final.to_excel(writer, index=False, sheet_name="Bases_Completas")

    print("\n✅ Relatório concluído e salvo com sucesso!")
    print(f"📁 {OUTPUT_PATH}")

except Exception as e:
    print(f"\n❌ Erro ao processar o arquivo:\n{e}")
