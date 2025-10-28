# -*- coding: utf-8 -*-
"""
📊 Política de Bonificação - v2.7.8 FINAL
Autor: bb-assistente 😎

Melhorias:
- ShippingTime Atual lê TODAS as planilhas e consolida médias por base.
- pacotes_sem_mov() restaurado (GP/PA + aging 6/7/10/14/30).
- Joins full e normalizados.
- Custo total / Ressarcimento corrigidos.
- Log de progresso e médias no terminal.
"""

import os
import polars as pl
import pandas as pd
from datetime import datetime
import calendar
from tqdm import tqdm
import warnings
import contextlib
import io

# ==========================================================
# 📂 Caminhos
# ==========================================================
BASE_ROOT = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação"

DIR_COLETA = os.path.join(BASE_ROOT, "00 -  Base de Dados (Coleta + Expedição)")
DIR_T0     = os.path.join(BASE_ROOT, "01 - Taxa de entrega T0")
DIR_RESS   = os.path.join(BASE_ROOT, "02 - Ressarcimento por pacote")
DIR_SHIP   = os.path.join(BASE_ROOT, "03 - Redução Shipping Time")
DIR_ANTIGA = os.path.join(BASE_ROOT, "Base Antiga")
DIR_SEMMOV = os.path.join(BASE_ROOT, "05 - Pacotes Sem Movimentação")
DIR_OUT    = os.path.join(BASE_ROOT, "Resultados")
os.makedirs(DIR_OUT, exist_ok=True)

# ==========================================================
# ⚙️ Utilitários
# ==========================================================
def read_excel_silent(path: str) -> pl.DataFrame:
    with warnings.catch_warnings(), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        warnings.simplefilter("ignore")
        try:
            return pl.read_excel(path)
        except Exception:
            return pl.DataFrame()

def to_float(col):
    return pl.col(col).cast(pl.Float64, strict=False).fill_null(0).fill_nan(0)

def _fix_key_cols(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    cols = df.columns
    key_aliases = [c for c in cols if c.startswith("Nome da base")]
    if not key_aliases:
        return df
    chosen = "Nome da base" if "Nome da base" in key_aliases else (
        "Nome da base_left" if "Nome da base_left" in key_aliases else (
            "Nome da base_right" if "Nome da base_right" in key_aliases else key_aliases[0]
        )
    )
    if chosen != "Nome da base":
        df = df.rename({chosen: "Nome da base"})
    for c in key_aliases:
        if c != "Nome da base" and c in df.columns:
            df = df.drop(c)
    return df

def _normalize_base(df: pl.DataFrame) -> pl.DataFrame:
    df = _fix_key_cols(df)
    if "Nome da base" in df.columns:
        df = df.with_columns(pl.col("Nome da base").cast(pl.Utf8, strict=False).str.strip_chars().alias("Nome da base"))
    return df

def _safe_full_join(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    if left.is_empty() and right.is_empty():
        return pl.DataFrame()
    left = _fix_key_cols(left)
    right = _fix_key_cols(right)
    if "Nome da base" not in left.columns and "Nome da base" in right.columns:
        left, right = right, left
    if "Nome da base" not in left.columns:
        return pl.concat([left, right], how="diagonal_relaxed").unique(maintain_order=True)
    if "Nome da base" not in right.columns:
        out = left
    else:
        out = left.join(right, on="Nome da base", how="full", suffix="_dup")
    out = _fix_key_cols(out)
    dup_cols = [c for c in out.columns if c.endswith("_dup")]
    if dup_cols:
        drop = []
        for c in dup_cols:
            base = c[:-4]
            if base in out.columns:
                drop.append(c)
        if drop:
            out = out.drop(drop)
    out = out.unique(subset=["Nome da base"], keep="first")
    return out

# ==========================================================
# 🟥 Sem Movimentação
# ==========================================================
def pacotes_sem_mov():
    if not os.path.isdir(DIR_SEMMOV):
        return pl.DataFrame()
    arquivos = [f for f in os.listdir(DIR_SEMMOV) if f.lower().endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()

    dfs = []
    for arq in tqdm(arquivos, desc="🟥 Lendo Sem Movimentação", colour="red"):
        df = read_excel_silent(os.path.join(DIR_SEMMOV, arq))
        if not df.is_empty():
            dfs.append(df)
    if not dfs:
        return pl.DataFrame()

    df = pl.concat(dfs, how="diagonal_relaxed")
    rename_map = {}
    for c in df.columns:
        if "责任所属代理区" in c or c == "Regional responsável":
            rename_map[c] = "Regional responsável"
        elif "责任机构" in c or c in ("Unidade responsável", "Unidade responsável责任机构"):
            rename_map[c] = "Nome da base"
        elif "Aging" in c:
            rename_map[c] = "Aging"
        elif "JMS" in c or "运单号" in c or c == "Número de pedido JMS 运单号":
            rename_map[c] = "Remessa"
    df = df.rename(rename_map)

    obrig = ["Regional responsável", "Nome da base", "Aging", "Remessa"]
    if not all(c in df.columns for c in obrig):
        return pl.DataFrame()

    df = df.filter(
        (pl.col("Regional responsável").cast(pl.Utf8, strict=False).str.to_uppercase().is_in(["GP", "PA"])) &
        (pl.col("Aging").is_in([
            "Exceed 6 days with no track",
            "Exceed 7 days with no track",
            "Exceed 10 days with no track",
            "Exceed 14 days with no track",
            "Exceed 30 days with no track",
        ]))
    )
    df = _normalize_base(df)
    df = df.group_by("Nome da base").agg(pl.count("Remessa").alias("Qtd Sem Mov"))
    return df

# ==========================================================
# 🟦 Coleta + Expedição
# ==========================================================
def coleta_expedicao():
    if not os.path.isdir(DIR_COLETA):
        raise SystemExit("⚠️ Pasta 'Coleta + Expedição' inexistente.")
    arquivos = [f for f in os.listdir(DIR_COLETA) if f.lower().endswith((".xlsx", ".xls"))]
    dfs = []
    for arq in tqdm(arquivos, desc="🟦 Lendo Coleta + Expedição", colour="blue"):
        df = read_excel_silent(os.path.join(DIR_COLETA, arq))
        if all(c in df.columns for c in [
            "Nome da base",
            "Quantidade coletada",
            "Quantidade com saída para entrega",
            "Quantidade entregue com assinatura",
        ]):
            df = _normalize_base(df).with_columns([
                to_float("Quantidade coletada"),
                to_float("Quantidade com saída para entrega"),
                to_float("Quantidade entregue com assinatura"),
                (pl.col("Quantidade coletada") + pl.col("Quantidade com saída para entrega")).alias("Total Geral"),
            ])
            dfs.append(df.select(["Nome da base", "Total Geral", "Quantidade entregue com assinatura"]))
    if not dfs:
        raise SystemExit("⚠️ Nenhum arquivo válido encontrado em Coleta + Expedição.")
    df = pl.concat(dfs, how="diagonal_relaxed")
    return (
        df.group_by("Nome da base")
        .agg([
            pl.sum("Total Geral").alias("Total Coleta+Entrega"),
            pl.sum("Quantidade entregue com assinatura").alias("Qtd Entregue Assinatura"),
        ])
    )

# ==========================================================
# 🟨 T0 (SLA)
# ==========================================================
def taxa_t0():
    if not os.path.isdir(DIR_T0):
        return pl.DataFrame()
    arquivos = [f for f in os.listdir(DIR_T0) if f.lower().endswith((".xlsx", ".xls"))]
    dfs = []
    for arq in tqdm(arquivos, desc="🟨 Lendo T0", colour="yellow"):
        df = read_excel_silent(os.path.join(DIR_T0, arq))
        if all(c in df.columns for c in ["Nome da base", "T日签收率-应签收量", "T日签收率-已签收量"]):
            df = _normalize_base(
                df.rename({
                    "T日签收率-应签收量": "Total Recebido",
                    "T日签收率-已签收量": "Entregue",
                }).with_columns([
                    to_float("Total Recebido"),
                    to_float("Entregue"),
                ])
            )
            dfs.append(df)
    if not dfs:
        return pl.DataFrame()
    df_total = pl.concat(dfs, how="diagonal_relaxed")
    return (
        df_total.group_by("Nome da base")
        .agg([
            pl.sum("Total Recebido").alias("Total Recebido"),
            pl.sum("Entregue").alias("Entregue"),
        ])
        .with_columns(
            (pl.when(pl.col("Total Recebido") > 0)
             .then(pl.col("Entregue") / pl.col("Total Recebido"))
             .otherwise(0)).alias("SLA (%)")
        )
        .select(["Nome da base", "SLA (%)"])
    )

# ==========================================================
# 📉 Shipping Time (Horas → Atual/Anterior/Variação)
# ==========================================================
def _prep_shipping(df: pl.DataFrame, col_saida: str) -> pl.DataFrame:
    if df.is_empty():
        return df
    base = "PDD de Entrega" if "PDD de Entrega" in df.columns else "Nome da base"
    etapas = [
        "Tempo trânsito SC Destino->Base Entrega",
        "Tempo médio processamento Base Entrega",
        "Tempo médio Saída para Entrega->Entrega",
    ]
    for e in etapas:
        if e not in df.columns:
            df = df.with_columns(pl.lit(0).alias(e))
    df = df.with_columns([
        to_float(etapas[0]),
        to_float(etapas[1]),
        to_float(etapas[2]),
        (pl.col(etapas[0]) + pl.col(etapas[1]) + pl.col(etapas[2])).alias(col_saida),
    ])
    out = (
        df.group_by(base)
        .agg(pl.mean(col_saida))
        .rename({base: "Nome da base"})
    )
    return _normalize_base(out)

def shippingtime_atual():
    """Lê TODAS as planilhas de Shipping Time e calcula média geral."""
    if not os.path.isdir(DIR_SHIP):
        return pl.DataFrame()
    arquivos = [f for f in os.listdir(DIR_SHIP) if f.lower().endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()

    dfs = []
    for arq in tqdm(arquivos, desc="📦 Lendo Shipping Time Atual", colour="cyan"):
        df = read_excel_silent(os.path.join(DIR_SHIP, arq))
        if not df.is_empty():
            dfs.append(df)

    if not dfs:
        return pl.DataFrame()

    df_all = pl.concat(dfs, how="diagonal_relaxed")
    df_out = _prep_shipping(df_all, "S.T. Atual (h)")

    media_geral = df_out["S.T. Atual (h)"].mean() if not df_out.is_empty() else 0
    print(f"📊 Média geral ShippingTime Atual: {media_geral:.2f}h")

    return df_out

def shippingtime_antiga():
    if not os.path.isdir(DIR_ANTIGA):
        return pl.DataFrame()
    arquivos = [os.path.join(DIR_ANTIGA, f) for f in os.listdir(DIR_ANTIGA) if f.lower().endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()
    dfs = [read_excel_silent(f) for f in tqdm(arquivos, desc="📉 Lendo Base Antiga", colour="cyan")]
    dfs = [d for d in dfs if not d.is_empty()]
    if not dfs:
        return pl.DataFrame()
    df = pl.concat(dfs, how="diagonal_relaxed")
    return _prep_shipping(df, "S.T. Anterior (h)")

# ==========================================================
# 💰 Ressarcimento
# ==========================================================
def ressarcimento_por_pacote(df_coleta: pl.DataFrame):
    if not os.path.isdir(DIR_RESS):
        return pl.DataFrame()
    arquivos = [f for f in os.listdir(DIR_RESS) if f.lower().endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()
    df = read_excel_silent(os.path.join(DIR_RESS, sorted(arquivos)[-1]))
    if df.is_empty() or "Regional responsável" not in df.columns:
        return pl.DataFrame()

    df = df.filter(pl.col("Regional responsável").cast(pl.Utf8, strict=False).str.to_uppercase() == "GP")
    df = df.with_columns(to_float("Valor a pagar (yuan)").alias("Custo total (R$)"))
    df = df.group_by("Base responsável").agg(pl.sum("Custo total (R$)").alias("Custo total (R$)"))
    df = df.rename({"Base responsável": "Nome da base"})
    df = _normalize_base(df)

    if not df_coleta.is_empty():
        df = _safe_full_join(df, df_coleta.select(["Nome da base", "Qtd Entregue Assinatura"]))

    df = df.fill_null(0).with_columns([
        (pl.when(pl.col("Qtd Entregue Assinatura") > 0)
         .then(pl.col("Custo total (R$)") / pl.col("Qtd Entregue Assinatura"))
         .otherwise(pl.col("Custo total (R$)"))).alias("Ressarcimento p/pct (R$)")
    ])
    return df.select(["Nome da base", "Custo total (R$)", "Ressarcimento p/pct (R$)"])

# ==========================================================
# 🧮 Consolidação
# ==========================================================
def consolidar():
    dias = calendar.monthrange(datetime.now().year, datetime.now().month)[1]

    df_coleta = coleta_expedicao()
    df_t0 = taxa_t0()
    df_st_at = shippingtime_atual()
    df_st_ant = shippingtime_antiga()
    df_ress = ressarcimento_por_pacote(df_coleta)
    df_sem = pacotes_sem_mov()

    if not df_st_at.is_empty():
        df_st = _safe_full_join(df_st_at, df_st_ant).with_columns(
            (pl.col("S.T. Atual (h)") - pl.col("S.T. Anterior (h)").fill_null(0)).alias("Variação (h)")
        )
    else:
        df_st = pl.DataFrame()

    df_final = _safe_full_join(df_t0, df_st)
    df_final = _safe_full_join(df_final, df_ress)
    df_final = _safe_full_join(df_final, df_sem)
    df_final = _safe_full_join(df_final, df_coleta)

    df = df_final.fill_null(0).with_columns([
        (pl.when(pl.col("Total Coleta+Entrega") > 0)
         .then(pl.col("Qtd Sem Mov") / dias / pl.col("Total Coleta+Entrega"))
         .otherwise(0)).alias("Taxa Sem Mov.")
    ])

    ordered = [
        "Nome da base", "SLA (%)", "S.T. Atual (h)", "S.T. Anterior (h)", "Variação (h)",
        "Ressarcimento p/pct (R$)", "Custo total (R$)", "Qtd Sem Mov", "Taxa Sem Mov.",
    ]
    for c in ordered:
        if c not in df.columns:
            df = df.with_columns(pl.lit(0).alias(c)) if c != "Nome da base" else df.with_columns(pl.lit("").alias(c))
    return df.select(ordered)

# ==========================================================
# 💾 Exportar
# ==========================================================
def main():
    df = consolidar()
    if df.is_empty():
        print("⚠️ Nenhum dado consolidado.")
        return

    out = os.path.join(DIR_OUT, f"Resumo_Politica_Bonificacao_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
    df_pd = df.to_pandas()

    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        startrow = 6
        df_pd.to_excel(writer, sheet_name="Bonificação", startrow=startrow, startcol=0, header=True, index=False)

        wb, ws = writer.book, writer.sheets["Bonificação"]
        red  = wb.add_format({"bold": True, "font_color": "white", "align": "center", "valign": "vcenter", "bg_color": "#C00000", "border": 1})
        gray = wb.add_format({"bold": True, "font_color": "white", "align": "center", "valign": "vcenter", "bg_color": "#595959", "border": 1})
        center = wb.add_format({"align": "center", "valign": "vcenter"})
        fmt_percent_2 = wb.add_format({"num_format": "0.00%", "align": "center"})
        fmt_money = wb.add_format({"num_format": '"R$"#,##0.00', "align": "center"})
        fmt_number = wb.add_format({"num_format": "#,##0.00", "align": "center"})
        fmt_int = wb.add_format({"num_format": "0", "align": "center"})

        # Cabeçalhos (com mesclagem e cores)
        ws.merge_range("A1:I1", "RESULTADOS DE INDICADORES", red)
        ws.merge_range("A2:I2", f"Data de atualização: {datetime.now():%d/%m}", gray)

        ws.merge_range("A5:A6", "Nome da base", red)
        ws.merge_range("B5:B6", "SLA (%)", red)

        ws.merge_range("C5:E5", "Shipping Time", gray)
        ws.write("C6", "S.T. Atual (h)", red)
        ws.write("D6", "S.T. Anterior (h)", red)
        ws.write("E6", "Variação (h)", red)

        ws.merge_range("F5:G5", "Ressarcimentos", gray)
        ws.write("F6", "Ressarcimento p/pct (R$)", red)
        ws.write("G6", "Custo total (R$)", red)

        ws.merge_range("H5:I5", "Sem Movimentação", gray)
        ws.write("H6", "Qtd Sem Mov", red)
        ws.write("I6", "Taxa Sem Mov.", red)

        # Larguras de coluna e formatações
        ws.set_column("A:A", 22, center)
        ws.set_column("B:B", 12, fmt_percent_2)
        ws.set_column("C:E", 14, fmt_number)
        ws.set_column("F:G", 16, fmt_money)
        ws.set_column("H:H", 14, fmt_int)
        ws.set_column("I:I", 14, fmt_percent_2)

    print(f"\n✅ Relatório final gerado com sucesso!")
    print(f"📂 Caminho: {out}")
    print("--------------------------------------------------")
    print("📈 Seções processadas:")
    print("   🟦 Coleta + Expedição")
    print("   🟨 T0 (SLA)")
    print("   📦 Shipping Time (Atual/Anterior)")
    print("   💰 Ressarcimentos")
    print("   🟥 Sem Movimentação")
    print("--------------------------------------------------")

# ==========================================================
if __name__ == "__main__":
    main()
