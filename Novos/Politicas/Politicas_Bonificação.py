# -*- coding: utf-8 -*-
"""
 - Política de Bonificação - v6.1
 - Status padronizados (sem ícones)
 - Ressarcimento dividido por dias do mês
 - Diferença do ShippingTime em horas
 - Formatação padronizada no Excel
 - Taxa_SemMov formatada como percentual
"""

import os
import polars as pl
import pandas as pd
from datetime import datetime
import calendar

# ==========================================================
# 📂 Caminhos
# ==========================================================
BASE_ROOT = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação"

DIR_COLETA = os.path.join(BASE_ROOT, "00 -  Base de Dados (Coleta + Expedição)")
DIR_T0 = os.path.join(BASE_ROOT, "01 - Taxa de entrega T0")
DIR_RESS = os.path.join(BASE_ROOT, "02 - Ressarcimento por pacote")
DIR_SHIP = os.path.join(BASE_ROOT, "03 - Redução Shipping Time")
DIR_SEMMOV = os.path.join(BASE_ROOT, "05 - Pacotes Sem Movimentação")
DIR_BASE_ANTIGA = os.path.join(BASE_ROOT, "Base Antiga")

DIR_OUT = os.path.join(BASE_ROOT, "Resultados")
os.makedirs(DIR_OUT, exist_ok=True)
OUT_PATH = os.path.join(DIR_OUT, f"Resumo_Politica_Bonificacao_Por_Base_{datetime.now():%Y%m%d_%H%M%S}.xlsx")

# ==========================================================
# ⚙️ Funções auxiliares
# ==========================================================
def to_float(col):
    return pl.col(col).cast(pl.Float64, strict=False).fill_null(0).fill_nan(0)

def latest_excel(path):
    files = [f for f in os.listdir(path) if f.endswith(".xlsx") and not f.startswith("~$")]
    if not files:
        return None
    files.sort(key=lambda f: os.path.getmtime(os.path.join(path, f)), reverse=True)
    return os.path.join(path, files[0])

# ==========================================================
# 📦 Indicadores individuais
# ==========================================================
def coleta_expedicao():
    arquivos = [f for f in os.listdir(DIR_COLETA) if f.endswith(".xlsx")]
    dfs = []
    for arq in arquivos:
        df = pl.read_excel(os.path.join(DIR_COLETA, arq))
        if all(c in df.columns for c in ["Nome da base", "Quantidade coletada", "Quantidade entregue com assinatura"]):
            df = df.with_columns([
                to_float("Quantidade coletada"),
                to_float("Quantidade entregue com assinatura"),
                (pl.col("Quantidade coletada") + pl.col("Quantidade entregue com assinatura")).alias("Total Geral")
            ])
            dfs.append(df.select(["Nome da base", "Total Geral"]))
    if not dfs:
        raise SystemExit("⚠️ Nenhum arquivo encontrado em Coleta + Expedição.")
    df = pl.concat(dfs)
    return df.group_by("Nome da base").agg(pl.sum("Total Geral").alias("Total Coleta+Entrega"))

def taxa_t0():
    arquivos = [f for f in os.listdir(DIR_T0) if f.endswith(".xlsx")]
    dfs = []
    for arq in arquivos:
        df = pl.read_excel(os.path.join(DIR_T0, arq))
        if all(c in df.columns for c in ["Nome da base", "T日签收率-应签收量", "T日签收率-已签收量"]):
            df = df.rename({
                "T日签收率-应签收量": "Total Recebido",
                "T日签收率-已签收量": "Entregue"
            })
            df = df.with_columns([to_float("Total Recebido"), to_float("Entregue")])
            dfs.append(df)
    if not dfs:
        return pl.DataFrame()
    df_total = pl.concat(dfs)
    df_group = (
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
    )
    return df_group.select(["Nome da base", "SLA (%)"])

def ressarcimento_por_pacote():
    f = latest_excel(DIR_RESS)
    if not f:
        return pl.DataFrame()
    df = pl.read_excel(f)
    df = df.filter(pl.col("Regional responsável").str.to_uppercase() == "GP")
    df = df.with_columns(to_float("Valor a pagar (yuan)").alias("Valor a pagar (R$)"))
    df = df.group_by("Base responsável").agg([
        pl.sum("Valor a pagar (R$)").alias("Valor Total (R$)"),
        pl.count("Remessa").alias("Qtd Pacotes")
    ])
    return df.rename({"Base responsável": "Nome da base"})

def pacotes_sem_mov():
    f = latest_excel(DIR_SEMMOV)
    if not f:
        return pl.DataFrame()
    df = pl.read_excel(f)
    df = df.rename({"Unidade responsável": "Nome da base"})
    return df.group_by("Nome da base").agg(pl.count("Remessa").alias("Qtd Sem Mov"))

# ==========================================================
# 📉 ShippingTime atual e base antiga
# ==========================================================
MAPA_ETAPAS = {
    "Tempo trânsito SC Destino->Base Entrega": "Etapa 6 (Trânsito)",
    "Etapa 6 (Trânsito)": "Etapa 6 (Trânsito)",
    "Tempo médio processamento Base Entrega": "Etapa 7 (Processamento)",
    "Etapa 7 (Processamento)": "Etapa 7 (Processamento)",
    "Tempo médio Saída para Entrega->Entrega": "Etapa 8 (Saída p/ Entrega)",
    "Etapa 8 (Saída p/ Entrega)": "Etapa 8 (Saída p/ Entrega)"
}

def shippingtime_atual():
    f = latest_excel(DIR_SHIP)
    if not f:
        return pl.DataFrame()
    df = pl.read_excel(f)
    col_base = "PDD de Entrega" if "PDD de Entrega" in df.columns else "Nome da base"
    for c_antigo, c_padrao in MAPA_ETAPAS.items():
        if c_antigo in df.columns:
            df = df.rename({c_antigo: c_padrao})
        elif c_padrao not in df.columns:
            df = df.with_columns(pl.lit(0).alias(c_padrao))
    for etapa in MAPA_ETAPAS.values():
        df = df.with_columns(to_float(etapa))
    df = df.with_columns(
        (pl.col("Etapa 6 (Trânsito)") + pl.col("Etapa 7 (Processamento)") + pl.col("Etapa 8 (Saída p/ Entrega)")).alias("Soma Total (min)")
    )
    return df.group_by(col_base).agg(pl.mean("Soma Total (min)").alias("Média Atual (min)")).rename({col_base: "Nome da base"})

def shippingtime_antiga():
    arquivos = [os.path.join(DIR_BASE_ANTIGA, f) for f in os.listdir(DIR_BASE_ANTIGA) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()
    dfs = [pl.read_excel(f) for f in arquivos]
    df = pl.concat(dfs, how="diagonal_relaxed")
    col_base = "PDD de Entrega" if "PDD de Entrega" in df.columns else "Nome da base"
    for c_antigo, c_padrao in MAPA_ETAPAS.items():
        if c_antigo in df.columns:
            df = df.rename({c_antigo: c_padrao})
        elif c_padrao not in df.columns:
            df = df.with_columns(pl.lit(0).alias(c_padrao))
    for etapa in MAPA_ETAPAS.values():
        df = df.with_columns(to_float(etapa))
    df = df.with_columns(
        (pl.col("Etapa 6 (Trânsito)") + pl.col("Etapa 7 (Processamento)") + pl.col("Etapa 8 (Saída p/ Entrega)")).alias("Soma Total (min)")
    )
    return df.group_by(col_base).agg(pl.mean("Soma Total (min)").alias("Média Antiga (min)")).rename({col_base: "Nome da base"})

# ==========================================================
# 🧮 Consolidação geral
# ==========================================================
def consolidar():
    dias_do_mes = calendar.monthrange(datetime.now().year, datetime.now().month)[1]

    df_coleta = coleta_expedicao()
    df_t0 = taxa_t0()
    df_ship_atual = shippingtime_atual()
    df_ship_antiga = shippingtime_antiga()
    df_ress = ressarcimento_por_pacote()
    df_sem = pacotes_sem_mov()

    df_ship = df_ship_atual.join(df_ship_antiga, on="Nome da base", how="inner")
    df_ship = df_ship.with_columns(
        ((pl.col("Média Atual (min)") - pl.col("Média Antiga (min)")) / 60).alias("Diferença (h)")
    )

    df_ship = df_ship.with_columns(
        pl.when(pl.col("Diferença (h)") <= -0.5).then(pl.lit("Desafio"))
        .when(pl.col("Diferença (h)") < 0).then(pl.lit("Meta"))
        .when(pl.col("Diferença (h)") <= 0.5).then(pl.lit("Mínimo"))
        .otherwise(pl.lit("Fora")).alias("Status Shipping")
    )

    df = df_coleta.join(df_t0, on="Nome da base", how="inner")
    df = df.join(df_ship, on="Nome da base", how="inner")
    df = df.join(df_ress, on="Nome da base", how="left")
    df = df.join(df_sem, on="Nome da base", how="left")
    df = df.fill_null(0)

    df = df.with_columns([
        ((pl.when(pl.col("Total Coleta+Entrega") > 0)
         .then(pl.col("Valor Total (R$)") / pl.col("Total Coleta+Entrega") / dias_do_mes)
         .otherwise(0))).alias("Ressarcimento por Pacote (R$)"),
        (pl.when(pl.col("Total Coleta+Entrega") > 0)
         .then(pl.col("Qtd Sem Mov") / pl.col("Total Coleta+Entrega"))
         .otherwise(0)).alias("Taxa_SemMov")
    ])

    def status_t0(x):
        if x >= 0.97: return "Desafio"
        elif x >= 0.95: return "Meta"
        else: return "Fora"

    def status_ress(x):
        if x <= 0.01: return "Desafio"
        elif x <= 0.09: return "Meta"
        elif x <= 0.15: return "Mínimo"
        else: return "Fora"

    def status_sem(x):
        if x <= 0.01: return "Desafio"
        elif x <= 0.05: return "Meta"
        elif x <= 0.08: return "Mínimo"
        else: return "Fora"

    df = df.with_columns([
        pl.col("SLA (%)").map_elements(status_t0).alias("Status T0"),
        pl.col("Ressarcimento por Pacote (R$)").map_elements(status_ress).alias("Status Ressarcimento"),
        pl.col("Taxa_SemMov").map_elements(status_sem).alias("Status SemMov")
    ])

    df = df.with_columns(
        (pl.sum_horizontal([
            pl.when(pl.col("SLA (%)") >= 0.95).then(100).otherwise(0),
            pl.when(pl.col("Média Atual (min)") <= 480).then(100).otherwise(0),
            pl.when(pl.col("Ressarcimento por Pacote (R$)") <= 0.15).then(45).otherwise(0),
            pl.when(pl.col("Taxa_SemMov") <= 0.08).then(45).otherwise(0)
        ])).alias("Pontuação_Total")
    )

    cols_final = [
        "Nome da base", "SLA (%)", "Status T0",
        "Média Antiga (min)", "Média Atual (min)", "Diferença (h)", "Status Shipping",
        "Ressarcimento por Pacote (R$)", "Status Ressarcimento",
        "Taxa_SemMov", "Status SemMov",
        "Pontuação_Total"
    ]
    return df.select(cols_final).sort("Pontuação_Total", descending=True)

# ==========================================================
# 💾 Exportar
# ==========================================================
def main():
    df_bon = consolidar()
    if df_bon.is_empty():
        print("⚠️ Nenhum dado encontrado para gerar relatório.")
        return

    df_pd = df_bon.to_pandas()

    with pd.ExcelWriter(OUT_PATH, engine="xlsxwriter") as writer:
        df_pd.to_excel(writer, index=False, sheet_name="Bonificacao_Por_Base")

        workbook = writer.book
        ws = writer.sheets["Bonificacao_Por_Base"]

        fmt_percent = workbook.add_format({"num_format": "0.00%", "align": "center"})
        fmt_number = workbook.add_format({"num_format": "#,##0.00", "align": "center"})
        fmt_money = workbook.add_format({"num_format": '"R$"#,##0.00', "align": "center"})
        fmt_int = workbook.add_format({"num_format": "0", "align": "center"})
        fmt_text = workbook.add_format({"align": "center"})

        for i, col in enumerate(df_pd.columns):
            width = max(df_pd[col].astype(str).map(len).max(), len(col)) + 2
            ws.set_column(i, i, width)
            if col in ["SLA (%)", "Taxa_SemMov"]:
                ws.set_column(i, i, 14, fmt_percent)
            elif "(R$)" in col:
                ws.set_column(i, i, 14, fmt_money)
            elif "(min)" in col or "(h)" in col:
                ws.set_column(i, i, 14, fmt_number)
            elif "Pontuação" in col:
                ws.set_column(i, i, 12, fmt_int)
            else:
                ws.set_column(i, i, width, fmt_text)

    print(f"✅ Relatório final gerado com sucesso!\n📂 {OUT_PATH}")

# ==========================================================
if __name__ == "__main__":
    main()