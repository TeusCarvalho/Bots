# -*- coding: utf-8 -*-

import os
import re
import textwrap
import warnings
import pandas as pd
from datetime import datetime
warnings.filterwarnings("ignore")

# ======================================================
# ⚙️ Tenta usar Polars (para performance)
# ======================================================
try:
    import polars as pl
    HAS_PL = True
except Exception:
    HAS_PL = False

# ======================================================
# ⚙️ Tenta usar Rich (para terminal bonito)
# ======================================================
HAS_RICH = False
try:
    import importlib.util
    if importlib.util.find_spec("rich") is not None:
        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel
        from rich.text import Text
        from rich import box
        HAS_RICH = True
except Exception:
    HAS_RICH = False

# ======================================================
# 🖥️ Console seguro (usa print se não tiver Rich)
# ======================================================
def _console():
    if HAS_RICH:
        return Console(highlight=False)
    class _Dummy:
        def print(self, *a, **k):
            print(*a)
        def rule(self, *a, **k):
            print("-" * 70)
    return _Dummy()
console = _console()

# ======================================================
# 📜 Log automático
# ======================================================
LOG_DIR = os.path.join(os.path.expanduser("~"), "SemMov_Logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"SemMov_{datetime.now():%Y%m%d}.log")

def log(msg: str):
    """Salva mensagens em log diário"""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")

# ======================================================
# ⚙️ Configurações principais
# ======================================================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal"
SEM_MOV_DIR = os.path.join(BASE_DIR, "3. Sem Movimentação")
BASES_INFO_PATH = os.path.join(BASE_DIR, "Bases_Info.xlsx")
OUTPUT_PATH = os.path.join(BASE_DIR, "Top5_Por_Tipo_Estacao.xlsx")
ALTERACOES_PATH = os.path.join(BASE_DIR, "Top5_Por_Tipo_Estacao_Alteracoes.xlsx")

TOP_N_GERAL = 10
SAVE_EXCEL = True
REGIONAL_ALVO = "GP"

# ======================================================
# 🔧 Utilitários
# ======================================================
def _clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.replace(r'[\s\u3000\xa0]+', '', regex=True)
    return df

def _listar_arquivos_xlsx(pasta: str) -> list[str]:
    return [
        f for f in os.listdir(pasta)
        if f.lower().endswith((".xlsx", ".xls")) and not f.startswith("~$")
    ]

def _wrap(s: str, w=100):
    return "\n".join(textwrap.wrap(str(s), width=w, break_long_words=False, break_on_hyphens=False))

def _extract_sc_from_base(base: str) -> str:
    b = str(base).upper().strip()
    m = re.search(r'([A-Z]{3}).*[-\s]?([A-Z]{2})$', b)
    if m:
        city, uf = m.group(1), m.group(2)
        return f"{uf} {city}"
    m2 = re.match(r'([A-Z]{2})\s+([A-Z]{3})', b)
    if m2:
        return f"{m2.group(1)} {m2.group(2)}"
    return b

# ======================================================
# 🚀 Execução principal
# ======================================================
def main():
    console.rule("[bold]📦 Sem Movimentação — Relatório Semanal")
    log("Iniciando processamento...")

    col_regional = "Regionalresponsável责任所属代理区"
    col_base = "Unidaderesponsável责任机构"
    col_aging = "Aging超时类型"
    col_problema = "Nomedepacoteproblemático问题件名称"

    # === Lê todos os arquivos Excel ===
    arquivos = _listar_arquivos_xlsx(SEM_MOV_DIR)
    if not arquivos:
        msg = "⚠️ Nenhum arquivo Excel encontrado."
        console.print(msg); log(msg); return

    arquivos.sort(key=lambda f: os.path.getmtime(os.path.join(SEM_MOV_DIR, f)), reverse=True)
    for i, fn in enumerate(arquivos, 1):
        console.print(f"  {i:02d}. {fn}")
        log(f"Arquivo detectado: {fn}")

    dfs = []
    for arq in arquivos:
        path = os.path.join(SEM_MOV_DIR, arq)
        try:
            book = pd.read_excel(path, dtype=str, sheet_name=None)
        except Exception as e:
            log(f"Erro ao ler {arq}: {e}")
            continue
        for aba, df_aba in (book or {}).items():
            if df_aba is None or df_aba.empty:
                continue
            df_aba = _clean_cols(df_aba)
            if all(c in df_aba.columns for c in [col_regional, col_base, col_aging]):
                df_aba["__Arquivo"] = arq
                df_aba["__Aba"] = aba
                dfs.append(df_aba)

    if not dfs:
        msg = "⚠️ Nenhuma aba válida encontrada."
        console.print(msg); log(msg); return

    df = pd.concat(dfs, ignore_index=True)
    console.print(f"[green]✅ Consolidação: {len(df):,} linhas.")
    log(f"Consolidação final: {len(df):,} linhas.")

    # === Bases_Info ===
    df_info = _clean_cols(pd.read_excel(BASES_INFO_PATH, dtype=str))
    col_nome_base = "Nomedabase"
    col_tipo_estacao = "Tipodeestação"

    # === Filtra Regional ===
    df[col_regional] = df[col_regional].astype(str).str.strip()
    df = df[df[col_regional].str.upper() == REGIONAL_ALVO]
    console.print(f"[cyan]👉 Linhas após filtro Regional={REGIONAL_ALVO}: {len(df):,}")
    log(f"Linhas filtradas para Regional={REGIONAL_ALVO}: {len(df):,}")

    # === Aging Map ===
    aging_map = {
        "Exceed 6 days with no track": "6 dias",
        "Exceed 7 days with no track": "7 dias",
        "Exceed 10 days with no track": "10 dias",
        "Exceed 14 days with no track": "14 dias",
        "Exceed 30 days with no track": "30 dias"
    }
    df["AgingLabel"] = df[col_aging].map(aging_map)
    df = df[df["AgingLabel"].notna()]

    # === Polars ou Pandas ===
    if HAS_PL:
        pl_df = pl.from_pandas(df[[col_base, col_problema, "AgingLabel"]])
        base_counts = (
            pl_df.group_by([col_base, "AgingLabel"])
            .len()
            .pivot(values="len", index=col_base, columns="AgingLabel")
            .fill_null(0)
        )
        for lbl in aging_map.values():
            if lbl not in base_counts.columns:
                base_counts = base_counts.with_columns(pl.lit(0).alias(lbl))
        base_counts = base_counts.with_columns(
            pl.sum_horizontal(list(aging_map.values())).alias("Total")
        ).sort("Total", descending=True)

        sc_series = [_extract_sc_from_base(b) for b in base_counts[col_base].to_list()]
        base_counts = base_counts.with_columns(pl.Series("SC", sc_series))
        sc_counts = (
            pl.from_pandas(base_counts.to_pandas())
            .group_by("SC")
            .agg([pl.sum(c).alias(c) for c in list(aging_map.values())] + [pl.sum("Total").alias("Total")])
            .sort("Total", descending=True)
        )
        df_base = base_counts.to_pandas()
        df_sc = sc_counts.to_pandas()
    else:
        resumo = df.groupby([col_base, "AgingLabel"]).size().unstack(fill_value=0)
        for c in aging_map.values():
            if c not in resumo.columns:
                resumo[c] = 0
        resumo["Total"] = resumo[list(aging_map.values())].sum(axis=1)
        resumo.reset_index(inplace=True)
        df_base = resumo.sort_values("Total", ascending=False)
        df_base["SC"] = df_base[col_base].apply(_extract_sc_from_base)
        df_sc = (
            df_base.groupby("SC")[list(aging_map.values()) + ["Total"]]
            .sum().reset_index().sort_values("Total", ascending=False)
        )

    # === Cruzamento + Métricas ===
    df_final = pd.merge(df_base, df_info[[col_nome_base, col_tipo_estacao]],
                        left_on=col_base, right_on=col_nome_base, how="left")
    df_final[col_tipo_estacao] = df_final[col_tipo_estacao].fillna("Sem classificação")

    qtd_total_pedidos = int(df_base["Total"].sum())
    perc_bipe = 0
    if col_problema in df.columns:
        qtd_bipe = int(df[col_problema].notna().sum())
        perc_bipe = 100.0 * qtd_bipe / qtd_total_pedidos if qtd_total_pedidos else 0
    perc_expedido = 24.0

    # === Texto interpretativo ===
    top_bases = df_base[col_base].head(2).dropna().astype(str).tolist()
    top_bases_txt = " e ".join(top_bases) if top_bases else "N/D"
    texto = (
        f"Ao total, mais de [bold]{qtd_total_pedidos:,}[/bold] pacotes ficaram sem movimentação acima de 6 dias.\n"
        f"Entre os principais ofensores da semana, [bold]{top_bases_txt}[/bold] aparecem com as maiores quantidades de pedidos.\n"
        f"A operação em que mais pacotes foram contabilizados foi o [bold]bipe de pacote problemático[/bold], "
        f"sendo responsável por [bold]{perc_bipe:.0f}%[/bold] dos pedidos sem movimentação, "
        f"sendo [bold]{perc_expedido:.0f}%[/bold] somente de encomendas expedidas mas não chegaram."
    )

    if HAS_RICH:
        console.print(Panel.fit(Text.from_markup(_wrap(texto)), border_style="red"))
    else:
        console.print(_wrap(re.sub(r"\[/?[^\]]+\]", "", texto)))
    log(re.sub(r"\[/?[^\]]+\]", "", texto))

    # === Tabelas ===
    def _show_table(df_tab, title, key):
        head = df_tab.head(5)
        cols = list(aging_map.values()) + ["Total"]
        if HAS_RICH:
            t = Table(title=title, title_style="bold", box=box.SIMPLE_HEAVY)
            t.add_column(key, style="cyan", justify="left")
            for c in cols:
                t.add_column(c, justify="right", style="bold red" if c == "Total" else "")
            for _, r in head.iterrows():
                t.add_row(str(r[key]), *[f"{int(r[c]):,}".replace(",", ".") for c in cols])
            console.print(t)
        else:
            console.print(f"\n=== {title} ===")
            console.print(head.to_string(index=False))
        log(f"Tabela {title}:\n{head.to_string(index=False)}")

    _show_table(df_sc, "Top 5 — SC", "SC")
    _show_table(df_base, "Top 5 — Bases", col_base)

    console.rule()
    log("Processamento finalizado com sucesso.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = f"❌ Erro: {e}"
        print(err)
        log(err)
