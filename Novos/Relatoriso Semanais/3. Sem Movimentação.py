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
# ⚙️ Tenta usar Rich (terminal bonito)
# ======================================================
HAS_RICH = False
try:
    import importlib.util
    if importlib.util.find_spec("rich") is not None:
        from rich.console import Console
        from rich.table import Table
        from rich.text import Text
        from rich import box
        HAS_RICH = True
except Exception:
    HAS_RICH = False


# ======================================================
# 🖥️ Console seguro
# ======================================================
def _console():
    if HAS_RICH:
        return Console(highlight=False)

    class _Dummy:
        def print(self, *a, **k): print(*a)
        def rule(self, *a, **k): print("-" * 70)

    return _Dummy()

console = _console()

# ======================================================
# 📜 Log diário
# ======================================================
LOG_DIR = os.path.join(os.path.expanduser("~"), "SemMov_Logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, f"SemMov_{datetime.now():%Y%m%d}.log")

def log(msg: str):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")

# ======================================================
# ⚙️ Configurações
# ======================================================
BASE_DIR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Semanal"
SEM_MOV_DIR = os.path.join(BASE_DIR, "3. Sem Movimentação")

BASES_INFO_PATH = (
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
)

TOP_N_GERAL = 10
SAVE_EXCEL = True
REGIONAL_ALVO = "GP"

# ======================================================
# 🔧 Funções Auxiliares
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
    return "\n".join(
        textwrap.wrap(
            str(s),
            width=w,
            break_long_words=False,
            break_on_hyphens=False
        )
    )

def _safe_pct(num: float, den: float) -> float:
    try:
        return round((num / den) * 100, 2) if den else 0.0
    except Exception:
        return 0.0


# ======================================================
# 🧠 CORREÇÃO SC/DC — VERSÃO FINAL
# ======================================================
def _extract_sc_from_base(base: str) -> str:
    """
    Corrige padrões reais da J&T:
    - DC AGB-MT → MT AGB
    - DC MRB-PA → PA MRB
    - DF BSB → DF BSB
    - GO GYN → GO GYN
    - PA ANA-PA → PA ANA
    - PA MRB-PA → PA MRB
    - PA DEVOLUÇÃO-GO → PA DEVOLUÇÃO
    """
    if not base:
        return "N/D"

    b = str(base).strip().upper()

    # 1) "DC XXXXX-YY"
    if b.startswith("DC "):
        try:
            cidade_uf = b[3:].replace(" ", "")
            cidade, uf = cidade_uf.split("-")
            return f"{uf} {cidade}"
        except:
            return b

    # 2) Se tem hífen → usar só parte antes do hífen
    if "-" in b:
        b = b.split("-")[0].strip()

    # 3) Padrão UF + CIDADE
    parts = b.split()
    if len(parts) == 2 and len(parts[0]) == 2:
        uf, cidade = parts
        return f"{uf} {cidade}"

    return b


# ======================================================
# ✅ NOVO: Monta Top10 enriquecido (pior aging + % problemáticos + top motivo)
# ======================================================
def _build_top10_enriquecido(df_sorted: pd.DataFrame, df_raw: pd.DataFrame, col_problema: str) -> pd.DataFrame:
    """
    df_sorted: agregado por base (tem colunas 6/7/10/14/30/Total e SC)
    df_raw   : granularidade pacote (já filtrado por regional+aging) e com SC
    col_problema: coluna do "nome de pacote problemático"
    """
    aging_cols = [c for c in ["6 dias", "7 dias", "10 dias", "14 dias", "30 dias"] if c in df_sorted.columns]

    top10 = df_sorted.head(10).copy()
    if top10.empty:
        return top10

    # total por SC na base bruta (para % problemático)
    if "SC" in df_raw.columns:
        total_sc = df_raw.groupby("SC").size().to_dict()
    else:
        total_sc = {}

    # problemáticos na base bruta
    df_prob = pd.DataFrame()
    has_prob_col = col_problema in df_raw.columns

    if has_prob_col:
        sprob = (
            df_raw[col_problema]
            .fillna("")
            .astype(str)
            .str.strip()
        )
        mask_prob = sprob.ne("") & (~sprob.str.lower().isin(["nan", "none"]))
        df_prob = df_raw.loc[mask_prob].copy()

    rows = []
    for _, r in top10.iterrows():
        sc = r.get("SC", "N/D")
        total = int(r.get("Total", 0))

        # pior bucket (aging) dentro da base
        bucket_pior = "N/D"
        bucket_qtd = 0
        bucket_pct = 0.0
        if aging_cols:
            # pega coluna com maior valor
            vals = {c: int(r.get(c, 0)) for c in aging_cols}
            bucket_pior = max(vals, key=vals.get) if vals else "N/D"
            bucket_qtd = int(vals.get(bucket_pior, 0))
            bucket_pct = _safe_pct(bucket_qtd, total)

        # problemáticos por SC
        prob_qtd = 0
        prob_pct = 0.0
        top_motivo = ""
        top_motivo_qtd = 0
        top_motivo_pct = 0.0

        if has_prob_col and not df_prob.empty and sc != "N/D":
            df_sc_prob = df_prob[df_prob["SC"] == sc].copy()
            prob_qtd = int(len(df_sc_prob))
            prob_pct = _safe_pct(prob_qtd, total_sc.get(sc, total))

            if prob_qtd:
                vc = (
                    df_sc_prob[col_problema]
                    .fillna("")
                    .astype(str)
                    .str.strip()
                )
                vc = vc[vc.ne("")]
                if len(vc):
                    vc_counts = vc.value_counts()
                    top_motivo = str(vc_counts.index[0])
                    top_motivo_qtd = int(vc_counts.iloc[0])
                    top_motivo_pct = _safe_pct(top_motivo_qtd, prob_qtd)

        rows.append({
            "SC": sc,
            "Total": total,
            "Pior Aging": bucket_pior,
            "Qtd Pior Aging": bucket_qtd,
            "% Pior Aging": bucket_pct,
            "Qtd Problemáticos": prob_qtd,
            "% Problemáticos": prob_pct,
            "Top Motivo (Problemático)": top_motivo,
            "Qtd Top Motivo": top_motivo_qtd,
            "% Top Motivo": top_motivo_pct
        })

    return pd.DataFrame(rows)


# ======================================================
# 🧾 RELATÓRIO FINAL — DINÂMICO + DETALHE PROBLEMÁTICOS (terminal)
# ======================================================
def gerar_relatorio_terminal_e_excel(
    df_final: pd.DataFrame,
    df_raw: pd.DataFrame,
    col_base: str,
    col_problema: str,
    output_excel_path: str
):
    """
    df_final = agregado por base (com colunas 6/7/10/14/30/Total/SC)
    df_raw   = base filtrada (regional + aging), na granularidade pacote
    """

    # Ordenar do pior para o melhor
    df_sorted = df_final.sort_values("Total", ascending=False).reset_index(drop=True)

    # Top 2 reais
    base1 = df_sorted.loc[0, "SC"] if len(df_sorted) > 0 else "N/D"
    base2 = df_sorted.loc[1, "SC"] if len(df_sorted) > 1 else "N/D"
    base1_total = int(df_sorted.loc[0, "Total"]) if len(df_sorted) > 0 else 0
    base2_total = int(df_sorted.loc[1, "Total"]) if len(df_sorted) > 1 else 0

    # totais reais na base bruta
    qtd_total = int(len(df_raw))

    # ========= BIPE (problemáticos): qtd e % =========
    qtd_bipe = 0
    perc_bipe = 0.0

    df_prob = pd.DataFrame()
    if col_problema in df_raw.columns:
        sprob = (
            df_raw[col_problema]
            .fillna("")
            .astype(str)
            .str.strip()
        )
        mask_prob = (
            sprob.ne("") &
            (~sprob.str.lower().isin(["nan", "none"]))
        )

        qtd_bipe = int(mask_prob.sum())
        perc_bipe = round((qtd_bipe / qtd_total) * 100, 2) if qtd_total else 0.0

        df_prob = df_raw.loc[mask_prob].copy()

    # Regra operacional (percentual fixo definido por vocês)
    perc_expedido = 24.0
    qtd_expedido = int(round((qtd_total * perc_expedido) / 100)) if qtd_total else 0

    # ========= TERMINAL (texto executivo) — SEM BARRAS =========
    texto_rich = f"""
Ao todo, mais de [bold]{qtd_total:,}[/bold] pacotes ficaram sem movimentação acima de 6 dias.

Entre os principais ofensores da semana, [bold]{base1}[/bold] ([bold]{base1_total:,}[/bold]) e [bold]{base2}[/bold] ([bold]{base2_total:,}[/bold])
aparecem com as maiores quantidades de pedidos.

A operação em que mais pacotes foram contabilizados foi o [bold]bipe de pacote problemático[/bold]:
[bold]{qtd_bipe:,}[/bold] pacotes ([bold]{perc_bipe}%[/bold] do total sem movimentação).

Desses, [bold]{qtd_expedido:,}[/bold] pacotes ([bold]{perc_expedido}%[/bold]) são somente de encomendas expedidas mas não chegaram.
""".strip()

    texto_plain = re.sub(r"\[/?bold\]", "", texto_rich)

    if HAS_RICH:
        console.rule("[bold]📊 Resumo Executivo")
        console.print(Text.from_markup(_wrap(texto_rich, w=100)))
    else:
        print("\n" + "-" * 70)
        print("📊 RESUMO EXECUTIVO")
        print("-" * 70)
        print(_wrap(texto_plain, w=100))
        print("-" * 70)

    # ========= TABELA TOP 5 =========
    top5 = df_sorted.head(5)
    cols_order = ["SC", "6 dias", "7 dias", "10 dias", "14 dias", "30 dias", "Total"]

    if HAS_RICH:
        table = Table(title="Top 5 — Piores Bases", title_style="bold", box=box.SIMPLE_HEAVY)
        for c in cols_order:
            table.add_column(c, justify="right", style="cyan" if c == "SC" else "")
        for _, r in top5.iterrows():
            table.add_row(
                r.get("SC", "N/D"),
                *(str(int(r.get(c, 0))) for c in cols_order[1:])
            )
        console.print(table)
    else:
        print(top5[cols_order].to_string(index=False))

    # ======================================================
    # ✅ NOVO: TOP 10 PIORES BASES + MOTIVO (ENRIQUECIDO)
    # ======================================================
    top10_enriq = _build_top10_enriquecido(df_sorted, df_raw, col_problema)

    if not top10_enriq.empty:
        if HAS_RICH:
            console.rule("[bold]🚨 Top 10 — Piores Bases + Motivo")
            t10 = Table(title="Top 10 — Piores Bases + Motivo", box=box.SIMPLE)
            t10.add_column("SC", justify="left", style="cyan")
            t10.add_column("Total", justify="right")
            t10.add_column("Pior Aging", justify="right")
            t10.add_column("Qtd", justify="right")
            t10.add_column("%", justify="right")
            t10.add_column("% Prob", justify="right")
            t10.add_column("Top Motivo (Prob.)", justify="left")

            for _, r in top10_enriq.iterrows():
                t10.add_row(
                    str(r["SC"]),
                    f"{int(r['Total']):,}",
                    str(r["Pior Aging"]),
                    f"{int(r['Qtd Pior Aging']):,}",
                    f"{float(r['% Pior Aging']):.1f}%",
                    f"{float(r['% Problemáticos']):.1f}%",
                    _wrap(str(r["Top Motivo (Problemático)"]), w=40) if r["Top Motivo (Problemático)"] else "-"
                )
            console.print(t10)
        else:
            print("\n" + "-" * 70)
            print("🚨 TOP 10 — PIORES BASES + MOTIVO")
            print("-" * 70)
            for i, r in enumerate(top10_enriq.to_dict("records"), 1):
                print(
                    f"{i:02d}. {r['SC']} | Total={r['Total']:,} | "
                    f"Pior={r['Pior Aging']} ({r['Qtd Pior Aging']:,}, {r['% Pior Aging']:.1f}%) | "
                    f"Prob={r['% Problemáticos']:.1f}% | Motivo={r['Top Motivo (Problemático)'] or '-'}"
                )
            print("-" * 70)

    # ========= MOTIVOS PROBLEMÁTICOS (GERAL): Qtd + % =========
    vc_geral = None
    if not df_prob.empty:
        if "SC" not in df_prob.columns:
            df_prob["SC"] = df_prob[col_base].apply(_extract_sc_from_base)

        motivos_geral = (
            df_prob[col_problema]
            .fillna("")
            .astype(str)
            .str.strip()
        )
        motivos_geral = motivos_geral[motivos_geral.ne("")]
        vc_geral = motivos_geral.value_counts().head(10)

        if HAS_RICH:
            console.rule("[bold]🧾 Problemáticos — Motivos (Geral)")
            tgeral = Table(title="Top 10 Motivos (Geral)", box=box.SIMPLE)
            tgeral.add_column("Motivo", justify="left")
            tgeral.add_column("Qtd", justify="right")
            tgeral.add_column("%", justify="right")

            total_prob = int(qtd_bipe) if qtd_bipe else 0
            for motivo, qtd in vc_geral.items():
                pct = (qtd / total_prob * 100) if total_prob else 0.0
                tgeral.add_row(str(motivo), f"{int(qtd):,}", f"{pct:.1f}%")

            console.print(tgeral)
        else:
            print("\n" + "-" * 70)
            print("🧾 PROBLEMÁTICOS — MOTIVOS (GERAL) — TOP 10")
            print("-" * 70)
            total_prob = int(qtd_bipe) if qtd_bipe else 0
            for motivo, qtd in vc_geral.items():
                pct = (qtd / total_prob * 100) if total_prob else 0.0
                print(f"- {motivo}: {int(qtd):,} ({pct:.1f}%)")
            print("-" * 70)

    # ========= DETALHAMENTO PROBLEMÁTICOS POR BASE (TOP 5): Qtd + % =========
    if not df_prob.empty:
        bases_top5 = top5["SC"].tolist()

        if HAS_RICH:
            console.rule("[bold]🧩 Detalhamento dos problemáticos (Top 5 Bases)")
        else:
            print("\n" + "-" * 70)
            print("🧩 DETALHAMENTO DOS PROBLEMÁTICOS (TOP 5 BASES)")
            print("-" * 70)

        for sc in bases_top5:
            df_sc = df_prob[df_prob["SC"] == sc].copy()
            if df_sc.empty:
                if HAS_RICH:
                    console.print(f"[yellow]• {sc}: sem problemáticos nessa semana.")
                else:
                    print(f"• {sc}: sem problemáticos nessa semana.")
                continue

            motivos = (
                df_sc[col_problema]
                .fillna("")
                .astype(str)
                .str.strip()
            )
            motivos = motivos[motivos.ne("")]
            vc_sc = motivos.value_counts().sort_values(ascending=False)

            total_sc = int(vc_sc.sum()) if len(vc_sc) else 0

            if HAS_RICH:
                t = Table(title=f"{sc} — Motivos Problemáticos", box=box.SIMPLE)
                t.add_column("Motivo", justify="left")
                t.add_column("Qtd", justify="right")
                t.add_column("%", justify="right")

                for motivo, qtd in vc_sc.items():
                    pct = (qtd / total_sc * 100) if total_sc else 0.0
                    t.add_row(str(motivo), f"{int(qtd):,}", f"{pct:.1f}%")

                console.print(t)
            else:
                print(f"\n{sc} — Motivos Problemáticos (Total: {total_sc:,})")
                for motivo, qtd in vc_sc.items():
                    pct = (qtd / total_sc * 100) if total_sc else 0.0
                    print(f"- {motivo}: {int(qtd):,} ({pct:.1f}%)")
    else:
        if col_problema not in df_raw.columns:
            if HAS_RICH:
                console.print("[yellow]Coluna de problemáticos não encontrada, pulando detalhamento.")
            else:
                print("Coluna de problemáticos não encontrada, pulando detalhamento.")
        else:
            if HAS_RICH:
                console.print("[yellow]Nenhum pacote problemático encontrado para detalhamento.")
            else:
                print("Nenhum pacote problemático encontrado para detalhamento.")

    # ======================================================
    # ✅ ALTERAÇÃO: GERAR EXCEL COM ABAS (mantém o mesmo arquivo)
    # ======================================================
    if not SAVE_EXCEL:
        if HAS_RICH:
            console.print("[yellow]SAVE_EXCEL=False — Excel não será gerado.")
        else:
            print("SAVE_EXCEL=False — Excel não será gerado.")
        return

    try:
        with pd.ExcelWriter(output_excel_path, engine="xlsxwriter") as writer:
            # Aba principal (tudo)
            df_sorted.to_excel(writer, sheet_name="Resumo Bases", index=False)

            # Top10 enriquecido
            if top10_enriq is not None and not top10_enriq.empty:
                top10_enriq.to_excel(writer, sheet_name="Top10 Bases + Motivo", index=False)

            # Motivos geral
            if vc_geral is not None:
                df_motivos = (
                    vc_geral.reset_index()
                    .rename(columns={"index": "Motivo", 0: "Qtd"})
                )
                total_prob = int(qtd_bipe) if qtd_bipe else 0
                df_motivos["%"] = df_motivos["Qtd"].apply(lambda x: round((x / total_prob) * 100, 2) if total_prob else 0.0)
                df_motivos.to_excel(writer, sheet_name="Motivos Geral", index=False)

        if HAS_RICH:
            console.print(f"[green]📁 Planilha salva em: {output_excel_path}")
        else:
            print(f"\n📁 Planilha salva em: {output_excel_path}")

    except Exception as e:
        if HAS_RICH:
            console.print(f"[red]❌ Erro ao salvar Excel: {e}")
        else:
            print(f"❌ Erro ao salvar Excel: {e}")


# ======================================================
# 🚀 MAIN PRINCIPAL
# ======================================================
def main():
    if HAS_RICH:
        console.rule("[bold]📦 Sem Movimentação — Relatório Semanal")
    else:
        print("-" * 70)
        print("📦 Sem Movimentação — Relatório Semanal")
        print("-" * 70)

    log("Iniciando processamento...")

    col_regional = "Regionalresponsável责任所属代理区"
    col_base = "Unidaderesponsável责任机构"
    col_aging = "Aging超时类型"
    col_problema = "Nomedepacoteproblemático问题件名称"

    arquivos = _listar_arquivos_xlsx(SEM_MOV_DIR)
    if not arquivos:
        if HAS_RICH:
            console.print("[red]Nenhum Excel encontrado.")
        else:
            print("Nenhum Excel encontrado.")
        return

    arquivos.sort(key=lambda f: os.path.getmtime(os.path.join(SEM_MOV_DIR, f)), reverse=True)

    for i, fn in enumerate(arquivos, 1):
        if HAS_RICH:
            console.print(f"{i:02d}. {fn}")
        else:
            print(f"{i:02d}. {fn}")
        log(f"Arquivo: {fn}")

    # LER TODOS
    dfs = []
    for arq in arquivos:
        path = os.path.join(SEM_MOV_DIR, arq)
        try:
            book = pd.read_excel(path, dtype=str, sheet_name=None)
        except Exception as e:
            log(f"Erro lendo {arq}: {e}")
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
        if HAS_RICH:
            console.print("[red]Nenhuma aba válida.")
        else:
            print("Nenhuma aba válida.")
        return

    df = pd.concat(dfs, ignore_index=True)
    if HAS_RICH:
        console.print(f"[green]Consolidação: {len(df):,} linhas.")
    else:
        print(f"Consolidação: {len(df):,} linhas.")

    # Base Atualizada (mantida como estava)
    try:
        df_info = _clean_cols(pd.read_excel(BASES_INFO_PATH, dtype=str))
    except:
        if HAS_RICH:
            console.print("[red]ERRO: Base_Atualizada.xlsx não encontrada.")
        else:
            print("ERRO: Base_Atualizada.xlsx não encontrada.")
        return

    # Filtro regional
    df[col_regional] = df[col_regional].astype(str).str.strip()
    df = df[df[col_regional].str.upper() == REGIONAL_ALVO]
    if HAS_RICH:
        console.print(f"[cyan]Após filtro Regional {REGIONAL_ALVO}: {len(df):,}")
    else:
        print(f"Após filtro Regional {REGIONAL_ALVO}: {len(df):,}")

    # Aging
    aging_map = {
        "Exceed 6 days with no track": "6 dias",
        "Exceed 7 days with no track": "7 dias",
        "Exceed 10 days with no track": "10 dias",
        "Exceed 14 days with no track": "14 dias",
        "Exceed 30 days with no track": "30 dias"
    }

    df["AgingLabel"] = df[col_aging].map(aging_map)
    df = df[df["AgingLabel"].notna()]

    # garante SC na base bruta para detalhamento
    df["SC"] = df[col_base].apply(_extract_sc_from_base)

    # POLARS
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

        df_final = base_counts.to_pandas()

    # PANDAS
    else:
        resumo = df.groupby([col_base, "AgingLabel"]).size().unstack(fill_value=0)
        for l in aging_map.values():
            if l not in resumo.columns:
                resumo[l] = 0

        resumo["Total"] = resumo[list(aging_map.values())].sum(axis=1)
        resumo.reset_index(inplace=True)

        df_final = resumo.sort_values("Total", ascending=False)
        df_final["SC"] = df_final[col_base].apply(_extract_sc_from_base)

    # ===========================
    # GERAR RELATÓRIO FINAL
    # ===========================
    output_excel = os.path.join(BASE_DIR, "SemMov_Final_Unico.xlsx")

    gerar_relatorio_terminal_e_excel(
        df_final=df_final,
        df_raw=df,
        col_base=col_base,
        col_problema=col_problema,
        output_excel_path=output_excel
    )


# ======================================================
# 🟢 RODAR
# ======================================================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        if HAS_RICH:
            console.print(f"[red]❌ Erro fatal: {e}")
        else:
            print(f"❌ Erro fatal: {e}")
        log(f"Erro fatal: {e}")
