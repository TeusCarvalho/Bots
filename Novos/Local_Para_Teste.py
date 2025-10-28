# -*- coding: utf-8 -*-
"""
📊 Política de Bonificação - v2.6.2 FINAL
Autor: bb-assistente 😎

Melhorias:
- Corrige leitura de "Sem Movimentação" (colunas com e sem chinês).
- Corrige fórmulas Excel (PT-BR e decimais corretos).
- Mantém layout completo e bonificação final.
- Exibe Top 5 melhores bases no terminal.
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
def _normalize_base(df: pl.DataFrame) -> pl.DataFrame:
    if "Nome da base" in df.columns:
        df = df.with_columns(
            pl.col("Nome da base").cast(pl.Utf8, strict=False).str.strip_chars().alias("Nome da base")
        )
    return df

def to_float(col):
    return pl.col(col).cast(pl.Float64, strict=False).fill_null(0).fill_nan(0)

def read_excel_silent(path):
    with warnings.catch_warnings(), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        warnings.simplefilter("ignore")
        try:
            return pl.read_excel(path)
        except Exception:
            return pl.DataFrame()

# ==========================================================
# 🟥 Sem Movimentação
# ==========================================================
def pacotes_sem_mov():
    arquivos = [f for f in os.listdir(DIR_SEMMOV) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        print("⚠️ Nenhum arquivo encontrado em 'Sem Movimentação'.")
        return pl.DataFrame()

    dfs = []
    for arq in tqdm(arquivos, desc="🟥 Lendo Sem Movimentação", colour="red"):
        df = read_excel_silent(os.path.join(DIR_SEMMOV, arq))
        if not df.is_empty():
            dfs.append(df)

    if not dfs:
        print("⚠️ Nenhum dado válido em Sem Movimentação.")
        return pl.DataFrame()

    df = pl.concat(dfs, how="diagonal_relaxed")

    possible_cols = {
        "Regional responsável": ["Regional responsável", "Regional responsável责任所属代理区"],
        "Nome da base": ["Unidade responsável", "Unidade responsável责任机构"],
        "Aging": ["Aging", "Aging超时类型"],
        "Remessa": ["Número de pedido JMS 运单号", "Número de pedido JMS", "运单号"]
    }

    rename_map = {}
    for new, opts in possible_cols.items():
        for o in opts:
            if o in df.columns:
                rename_map[o] = new
                break
    df = df.rename(rename_map)

    required = ["Regional responsável", "Nome da base", "Aging", "Remessa"]
    if not all(c in df.columns for c in required):
        print(f"⚠️ Colunas obrigatórias ausentes. Encontradas: {df.columns}")
        return pl.DataFrame()

    antes = len(df)
    df = df.filter(
        (pl.col("Regional responsável").is_in(["GP", "PA"])) &
        (pl.col("Aging").is_in([
            "Exceed 6 days with no track",
            "Exceed 7 days with no track",
            "Exceed 10 days with no track",
            "Exceed 14 days with no track",
            "Exceed 30 days with no track"
        ]))
    )
    depois = len(df)
    print(f"📊 Sem Movimentação: {antes:,} linhas originais → {depois:,} válidas após filtro (GP/PA + aging).")

    if depois == 0:
        print("⚠️ Nenhum dado após filtros de Sem Movimentação.")
        return pl.DataFrame()

    df = _normalize_base(df)
    df = df.group_by("Nome da base").agg(pl.count("Remessa").alias("Qtd Sem Mov"))
    return df

# ==========================================================
# 🟦 Coleta + Expedição
# ==========================================================
def coleta_expedicao():
    arquivos = [f for f in os.listdir(DIR_COLETA) if f.endswith((".xlsx", ".xls"))]
    dfs = []
    for arq in tqdm(arquivos, desc="🟦 Lendo Coleta + Expedição", colour="blue"):
        df = read_excel_silent(os.path.join(DIR_COLETA, arq))
        if all(c in df.columns for c in [
            "Nome da base",
            "Quantidade coletada",
            "Quantidade com saída para entrega",
            "Quantidade entregue com assinatura"
        ]):
            df = _normalize_base(df).with_columns([
                to_float("Quantidade coletada"),
                to_float("Quantidade com saída para entrega"),
                to_float("Quantidade entregue com assinatura"),
                (pl.col("Quantidade coletada") + pl.col("Quantidade com saída para entrega")).alias("Total Geral")
            ])
            dfs.append(df.select(["Nome da base", "Total Geral", "Quantidade entregue com assinatura"]))
    if not dfs:
        raise SystemExit("⚠️ Nenhum arquivo encontrado em Coleta + Expedição.")
    df = pl.concat(dfs, how="diagonal_relaxed")
    return (
        df.group_by("Nome da base")
        .agg([
            pl.sum("Total Geral").alias("Total Coleta+Entrega"),
            pl.sum("Quantidade entregue com assinatura").alias("Qtd Entregue Assinatura")
        ])
    )

# ==========================================================
# 🟨 T0
# ==========================================================
def taxa_t0():
    arquivos = [f for f in os.listdir(DIR_T0) if f.endswith((".xlsx", ".xls"))]
    dfs = []
    for arq in tqdm(arquivos, desc="🟨 Lendo T0", colour="yellow"):
        df = read_excel_silent(os.path.join(DIR_T0, arq))
        if all(c in df.columns for c in ["Nome da base", "T日签收率-应签收量", "T日签收率-已签收量"]):
            df = _normalize_base(
                df.rename({
                    "T日签收率-应签收量": "Total Recebido",
                    "T日签收率-已签收量": "Entregue"
                }).with_columns([
                    to_float("Total Recebido"),
                    to_float("Entregue")
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
            pl.sum("Entregue").alias("Entregue")
        ])
        .with_columns(
            (pl.when(pl.col("Total Recebido") > 0)
             .then(pl.col("Entregue") / pl.col("Total Recebido"))
             .otherwise(0)).alias("SLA (%)")
        )
        .select(["Nome da base", "SLA (%)"])
    )

# ==========================================================
# 📉 Shipping Time
# ==========================================================
MAPA_ETAPAS = {
    "Tempo trânsito SC Destino->Base Entrega": "Etapa 6 (Trânsito)",
    "Tempo médio processamento Base Entrega": "Etapa 7 (Processamento)",
    "Tempo médio Saída para Entrega->Entrega": "Etapa 8 (Saída p/ Entrega)"
}

def _prep_shipping(df: pl.DataFrame, nome_col):
    col_base = "PDD de Entrega" if "PDD de Entrega" in df.columns else "Nome da base"
    for c_antigo, c_padrao in MAPA_ETAPAS.items():
        if c_antigo in df.columns:
            df = df.rename({c_antigo: c_padrao})
        elif c_padrao not in df.columns:
            df = df.with_columns(pl.lit(0).alias(c_padrao))
    for etapa in MAPA_ETAPAS.values():
        df = df.with_columns(to_float(etapa))
    df = df.with_columns(
        (pl.col("Etapa 6 (Trânsito)") + pl.col("Etapa 7 (Processamento)") + pl.col("Etapa 8 (Saída p/ Entrega)")).alias(nome_col)
    )
    return df.group_by(col_base).agg(pl.mean(nome_col)).rename({col_base: "Nome da base"})

def shippingtime_atual():
    arquivos = [f for f in os.listdir(DIR_SHIP) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()
    df = read_excel_silent(os.path.join(DIR_SHIP, sorted(arquivos)[-1]))
    return _prep_shipping(df, "S.T. Atual (h)")

def shippingtime_antiga():
    arquivos = [os.path.join(DIR_ANTIGA, f) for f in os.listdir(DIR_ANTIGA) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()
    dfs = [read_excel_silent(f) for f in tqdm(arquivos, desc="📉 Lendo Base Antiga", colour="cyan")]
    dfs = [df for df in dfs if not df.is_empty()]
    if not dfs:
        return pl.DataFrame()
    df = pl.concat(dfs, how="diagonal_relaxed")
    return _prep_shipping(df, "S.T. Anterior (h)")

# ==========================================================
# 💰 Ressarcimento
# ==========================================================
def ressarcimento_por_pacote(df_coleta_assinatura):
    arquivos = [f for f in os.listdir(DIR_RESS) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()
    df = read_excel_silent(os.path.join(DIR_RESS, sorted(arquivos)[-1]))
    if df.is_empty() or "Regional responsável" not in df.columns:
        return pl.DataFrame()
    df = df.filter(pl.col("Regional responsável").str.to_uppercase() == "GP")
    df = df.with_columns(to_float("Valor a pagar (yuan)").alias("Custo total (R$)"))
    df = df.group_by("Base responsável").agg(pl.sum("Custo total (R$)").alias("Custo total (R$)"))
    df = df.rename({"Base responsável": "Nome da base"})
    df = _normalize_base(df)
    if not df_coleta_assinatura.is_empty():
        df = df.join(df_coleta_assinatura.select(["Nome da base", "Qtd Entregue Assinatura"]), on="Nome da base", how="left")
    df = df.fill_null(0).with_columns([
        (
            pl.when(pl.col("Qtd Entregue Assinatura") > 0)
            .then(pl.col("Custo total (R$)") / pl.col("Qtd Entregue Assinatura"))
            .otherwise(pl.col("Custo total (R$)"))
        ).alias("Ressarcimento p/pct (R$)")
    ])
    return df.select(["Nome da base", "Custo total (R$)", "Ressarcimento p/pct (R$)"])

# ==========================================================
# 🧮 Consolidação e Exportação
# ==========================================================
def consolidar():
    dias_do_mes = calendar.monthrange(datetime.now().year, datetime.now().month)[1]
    df_coleta = coleta_expedicao()
    df_t0 = taxa_t0()
    df_st_atual = shippingtime_atual()
    df_st_ant = shippingtime_antiga()
    df_ress = ressarcimento_por_pacote(df_coleta)
    df_sem = pacotes_sem_mov()

    if not df_st_atual.is_empty() and not df_st_ant.is_empty():
        df_st = (
            df_st_atual.join(df_st_ant, on="Nome da base", how="left")
            .with_columns((pl.col("S.T. Atual (h)") - pl.col("S.T. Anterior (h)").fill_null(0)).alias("Diferença (h)"))
        )
    else:
        df_st = df_st_atual.with_columns(pl.lit(0).alias("Diferença (h)"))

    df = df_t0
    for dfx in [df_st, df_ress, df_sem, df_coleta]:
        if not dfx.is_empty() and "Nome da base" in dfx.columns:
            df = df.join(dfx, on="Nome da base", how="left")
    df = df.fill_null(0)
    df = df.with_columns([
        (
            pl.when(pl.col("Total Coleta+Entrega") > 0)
            .then(pl.col("Qtd Sem Mov") / dias_do_mes / pl.col("Total Coleta+Entrega"))
            .otherwise(0)
        ).alias("Taxa Sem Mov.")
    ])
    return df

def main():
    df = consolidar()
    if df.is_empty():
        print("⚠️ Nenhum dado consolidado.")
        return

    out = os.path.join(DIR_OUT, f"Resumo_Politica_Bonificacao_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
    df_pd = df.to_pandas()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        startrow = 6
        df_pd.to_excel(writer, sheet_name="Bonificação", startrow=startrow, startcol=0, header=False, index=False)
        wb, ws = writer.book, writer.sheets["Bonificação"]

        red = wb.add_format({"bold": True, "font_color": "white", "align": "center", "valign": "vcenter", "bg_color": "#C00000", "border": 1})
        gray = wb.add_format({"bold": True, "font_color": "white", "align": "center", "valign": "vcenter", "bg_color": "#595959", "border": 1})
        center = wb.add_format({"align": "center", "valign": "vcenter"})
        fmt_percent_2 = wb.add_format({"num_format": "0.00%", "align": "center"})
        fmt_percent_4 = wb.add_format({"num_format": "0.0000%", "align": "center"})
        fmt_money = wb.add_format({"num_format": '"R$"#,##0.00', "align": "center"})
        fmt_number = wb.add_format({"num_format": "#,##0.00", "align": "center"})
        fmt_int = wb.add_format({"num_format": "0", "align": "center"})

        ws.merge_range("A1:M1", "RESULTADOS DE INDICADORES", red)
        ws.merge_range("A2:M2", f"Data de atualização: {datetime.now():%d/%m}", gray)
        ws.merge_range("A5:A6", "Nome da base", red)
        ws.merge_range("B5:B6", "Taxa T0 (SLA)", red)
        ws.merge_range("C5:E5", "Shipping Time", gray)
        ws.write("C6", "S.T. Atual (h)", red)
        ws.write("D6", "S.T. Anterior (h)", red)
        ws.write("E6", "Diferença (h)", red)
        ws.merge_range("F5:F6", "Elegibilidade", red)
        ws.merge_range("G5:I5", "Ressarcimentos", gray)
        ws.write("G6", "Custo total (R$)", red)
        ws.write("H6", "Ressarcimento p/pct", red)
        ws.write("I6", "Atingimento", red)
        ws.merge_range("J5:L5", "Sem Movimentação", gray)
        ws.write("J6", "Qtd Sem Mov", red)
        ws.write("K6", "Taxa Sem Mov.", red)
        ws.write("L6", "Atingimento", red)
        ws.merge_range("M5:M6", "Total da bonificação", red)

        ws.set_column("A:A", 22, center)
        ws.set_column("B:B", 12, fmt_percent_2)
        ws.set_column("C:D", 16, fmt_number)
        ws.set_column("E:E", 14, fmt_number)
        ws.set_column("F:F", 16, fmt_percent_2)
        ws.set_column("G:G", 16, fmt_money)
        ws.set_column("H:H", 18, fmt_money)
        ws.set_column("I:I", 14, fmt_percent_2)
        ws.set_column("J:J", 14, fmt_int)
        ws.set_column("K:K", 14, fmt_percent_4)
        ws.set_column("L:L", 14, fmt_percent_2)
        ws.set_column("M:M", 20, fmt_percent_2)

        n_rows = len(df_pd)
        first_row = startrow + 1
        last_row = startrow + n_rows
        for r in range(first_row, last_row + 1):
            ws.write_formula(r - 1, 5, f'=SE(E{r}<=-8;110%;SE(E{r}<=0;100%;SE(B{r}>=0,97;110%;SE(B{r}>=0,95;100%;0))))')
            ws.write_formula(r - 1, 8, f'=SE(H{r}<=0,01;45%;SE(H{r}<=0,09;35%;SE(H{r}<=0,15;5%;0)))')
            ws.write_formula(r - 1, 11, f'=SE(K{r}<=0,0001;45%;SE(K{r}<=0,0005;35%;SE(K{r}<=0,0008;5%;0)))')
            ws.write_formula(r - 1, 12, f'=SE(F{r}<>0;SE(F{r}=110%;10%+I{r}+L{r};I{r}+L{r});0)')

        print(f"✅ Fórmulas aplicadas ({n_rows} linhas).")

    print(f"✅ Relatório final gerado!\n📂 {out}")

    top5 = df_pd.nlargest(5, "SLA (%)")[["Nome da base", "SLA (%)", "Custo total (R$)", "Taxa Sem Mov."]]
    print("\n🏆 Top 5 Melhores Bases:")
    print(top5.to_string(index=False))

# ==========================================================
if __name__ == "__main__":
    main()
