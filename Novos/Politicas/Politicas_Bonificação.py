# -*- coding: utf-8 -*-
"""
📊 Política de Bonificação - v2.4.6
Autor: bb-assistente 😎
Descrição:
- Corrige o cálculo de Ressarcimento usando a coluna real
  "Quantidade entregue com assinatura" da Coleta + Expedição.
- Soma automaticamente todas as ocorrências da base.
- Gera relatório consolidado e formatado em Excel.
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
DIR_T0 = os.path.join(BASE_ROOT, "01 - Taxa de entrega T0")
DIR_RESS = os.path.join(BASE_ROOT, "02 - Ressarcimento por pacote")
DIR_SHIP = os.path.join(BASE_ROOT, "03 - Redução Shipping Time")
DIR_ANTIGA = os.path.join(BASE_ROOT, "Base Antiga")
DIR_SEMMOV = os.path.join(BASE_ROOT, "05 - Pacotes Sem Movimentação")
DIR_OUT = os.path.join(BASE_ROOT, "Resultados")
os.makedirs(DIR_OUT, exist_ok=True)

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

def read_excel_silent(path):
    with warnings.catch_warnings(), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        warnings.simplefilter("ignore")
        try:
            return pl.read_excel(path)
        except Exception:
            return pl.DataFrame()

# ==========================================================
# 📦 Indicadores individuais
# ==========================================================
def coleta_expedicao():
    """Soma total de coleta, expedição e entregas com assinatura."""
    arquivos = [f for f in os.listdir(DIR_COLETA) if f.endswith(".xlsx")]
    dfs = []
    for arq in tqdm(arquivos, desc="🟦 Lendo Coleta + Expedição", colour="blue"):
        df = read_excel_silent(os.path.join(DIR_COLETA, arq))
        if all(c in df.columns for c in [
            "Nome da base",
            "Quantidade coletada",
            "Quantidade com saída para entrega",
            "Quantidade entregue com assinatura"
        ]):
            df = df.with_columns([
                to_float("Quantidade coletada"),
                to_float("Quantidade com saída para entrega"),
                to_float("Quantidade entregue com assinatura"),
                (pl.col("Quantidade coletada") + pl.col("Quantidade com saída para entrega")).alias("Total Geral")
            ])
            dfs.append(df.select(["Nome da base", "Total Geral", "Quantidade entregue com assinatura"]))
    if not dfs:
        raise SystemExit("⚠️ Nenhum arquivo encontrado em Coleta + Expedição.")
    df = pl.concat(dfs)
    return (
        df.group_by("Nome da base")
        .agg([
            pl.sum("Total Geral").alias("Total Coleta+Entrega"),
            pl.sum("Quantidade entregue com assinatura").alias("Qtd Entregue Assinatura")
        ])
    )

def taxa_t0():
    arquivos = [f for f in os.listdir(DIR_T0) if f.endswith(".xlsx")]
    dfs = []
    for arq in tqdm(arquivos, desc="🟨 Lendo T0", colour="yellow"):
        df = read_excel_silent(os.path.join(DIR_T0, arq))
        if all(c in df.columns for c in ["Nome da base", "T日签收率-应签收量", "T日签收率-已签收量"]):
            df = df.rename({
                "T日签收率-应签收量": "Total Recebido",
                "T日签收率-已签收量": "Entregue"
            }).with_columns([
                to_float("Total Recebido"),
                to_float("Entregue")
            ])
            dfs.append(df)
    if not dfs:
        return pl.DataFrame()
    df_total = pl.concat(dfs, how="diagonal_relaxed")
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

def ressarcimento_por_pacote(df_coleta_assinatura: pl.DataFrame):
    """Calcula o ressarcimento total com base na quantidade real de entregas com assinatura."""
    f = latest_excel(DIR_RESS)
    if not f:
        return pl.DataFrame()
    df = read_excel_silent(f)
    if df.is_empty() or "Regional responsável" not in df.columns:
        return pl.DataFrame()

    df = df.filter(pl.col("Regional responsável").str.to_uppercase() == "GP")
    df = df.with_columns(to_float("Valor a pagar (yuan)").alias("Valor a pagar (R$)"))
    df = df.group_by("Base responsável").agg(pl.sum("Valor a pagar (R$)").alias("Valor Total (R$)"))
    df = df.rename({"Base responsável": "Nome da base"})

    # Junta com a quantidade real de entregas com assinatura
    if not df_coleta_assinatura.is_empty():
        df = df.join(df_coleta_assinatura.select(["Nome da base", "Qtd Entregue Assinatura"]), on="Nome da base", how="left")

    df = df.fill_null(0)
    return df

def pacotes_sem_mov():
    f = latest_excel(DIR_SEMMOV)
    if not f:
        return pl.DataFrame()
    df = read_excel_silent(f)
    if "Unidade responsável" in df.columns:
        df = df.rename({"Unidade responsável": "Nome da base"})
    if "Remessa" not in df.columns:
        return pl.DataFrame()
    return df.group_by("Nome da base").agg(pl.count("Remessa").alias("Qtd Sem Mov"))

# ==========================================================
# 📉 ShippingTime
# ==========================================================
MAPA_ETAPAS = {
    "Tempo trânsito SC Destino->Base Entrega": "Etapa 6 (Trânsito)",
    "Tempo médio processamento Base Entrega": "Etapa 7 (Processamento)",
    "Tempo médio Saída para Entrega->Entrega": "Etapa 8 (Saída p/ Entrega)"
}

def shippingtime_atual():
    f = latest_excel(DIR_SHIP)
    if not f:
        return pl.DataFrame()
    df = read_excel_silent(f)
    col_base = "PDD de Entrega" if "PDD de Entrega" in df.columns else "Nome da base"
    for c_antigo, c_padrao in MAPA_ETAPAS.items():
        if c_antigo in df.columns:
            df = df.rename({c_antigo: c_padrao})
        elif c_padrao not in df.columns:
            df = df.with_columns(pl.lit(0).alias(c_padrao))
    for etapa in MAPA_ETAPAS.values():
        df = df.with_columns(to_float(etapa))
    df = df.with_columns(
        (pl.col("Etapa 6 (Trânsito)") + pl.col("Etapa 7 (Processamento)") + pl.col("Etapa 8 (Saída p/ Entrega)"))
        .alias("Soma Total (h)")
    )
    return df.group_by(col_base).agg(pl.mean("Soma Total (h)").alias("Média Atual (h)")).rename({col_base: "Nome da base"})

def shippingtime_antiga():
    arquivos = [os.path.join(DIR_ANTIGA, f) for f in os.listdir(DIR_ANTIGA) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()
    dfs = [read_excel_silent(f) for f in tqdm(arquivos, desc="📉 Lendo Base Antiga", colour="cyan")]
    dfs = [df for df in dfs if not df.is_empty()]
    if not dfs:
        return pl.DataFrame()
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
        (pl.col("Etapa 6 (Trânsito)") + pl.col("Etapa 7 (Processamento)") + pl.col("Etapa 8 (Saída p/ Entrega)"))
        .alias("Soma Total (h)")
    )
    return df.group_by(col_base).agg(pl.mean("Soma Total (h)").alias("Média Antiga (h)")).rename({col_base: "Nome da base"})

# ==========================================================
# 🧮 Consolidação geral
# ==========================================================
def consolidar():
    dias_do_mes = calendar.monthrange(datetime.now().year, datetime.now().month)[1]
    df_coleta = coleta_expedicao()
    df_t0 = taxa_t0()
    df_ship_atual = shippingtime_atual()
    df_ship_antiga = shippingtime_antiga()
    df_ress = ressarcimento_por_pacote(df_coleta)
    df_sem = pacotes_sem_mov()

    print("\n🔍 Diagnóstico das bases:")
    for nome, df in {
        "Coleta+Expedição": df_coleta,
        "T0": df_t0,
        "Shipping Atual": df_ship_atual,
        "Shipping Antiga": df_ship_antiga,
        "Ressarcimento": df_ress,
        "Sem Movimentação": df_sem,
    }.items():
        if df.is_empty():
            print(f"⚠️ {nome} está vazia.")
        elif "Nome da base" not in df.columns:
            print(f"⚠️ {nome} sem coluna 'Nome da base' — colunas: {df.columns}")

    if "Nome da base" not in df_coleta.columns or "Nome da base" not in df_t0.columns:
        raise SystemExit("❌ Erro crítico: Coleta+Expedição ou T0 sem 'Nome da base'.")

    # Shipping
    if not df_ship_atual.is_empty() and not df_ship_antiga.is_empty():
        df_ship = df_ship_atual.join(df_ship_antiga, on="Nome da base", how="inner")
        df_ship = df_ship.with_columns(
            (pl.col("Média Atual (h)") - pl.col("Média Antiga (h)")).alias("Diferença (h)")
        )
        df_ship = df_ship.with_columns(
            pl.when(pl.col("Diferença (h)") <= -0.5).then(pl.lit("Desafio"))
            .when(pl.col("Diferença (h)") < 0).then(pl.lit("Meta"))
            .when(pl.col("Diferença (h)") <= 0.5).then(pl.lit("Mínimo"))
            .otherwise(pl.lit("Fora")).alias("Status Shipping")
        )
    else:
        print("⚠️ ShippingTime atual ou antigo vazio — colunas ignoradas.")
        df_ship = pl.DataFrame({"Nome da base": [], "Média Atual (h)": [], "Média Antiga (h)": [], "Diferença (h)": [], "Status Shipping": []})

    dfs = [df_coleta, df_t0, df_ship, df_ress, df_sem]
    df_final = dfs[0]
    for next_df in dfs[1:]:
        if not next_df.is_empty() and "Nome da base" in next_df.columns:
            df_final = df_final.join(next_df, on="Nome da base", how="left")

    df = df_final.fill_null(0)

    # Cálculos finais
    df = df.with_columns([
        ((pl.when(pl.col("Total Coleta+Entrega") > 0)
         .then(pl.col("Valor Total (R$)") / pl.col("Qtd Entregue Assinatura") / dias_do_mes)
         .otherwise(0))).alias("Ressarcimento por Pacote (R$)"),
        (pl.when(pl.col("Total Coleta+Entrega") > 0)
         .then(pl.col("Qtd Sem Mov") / pl.col("Total Coleta+Entrega"))
         .otherwise(0)).alias("Taxa_SemMov")
    ])

    df = df.with_columns(
        (pl.sum_horizontal([
            pl.when(pl.col("SLA (%)") >= 0.95).then(100).otherwise(0),
            pl.when(pl.col("Diferença (h)") <= 0).then(100).otherwise(0),
            pl.when(pl.col("Ressarcimento por Pacote (R$)") <= 0.15).then(45).otherwise(0),
            pl.when(pl.col("Taxa_SemMov") <= 0.08).then(45).otherwise(0)
        ])).alias("Pontuação_Total")
    )

    return df.sort("Pontuação_Total", descending=True)

# ==========================================================
# 💾 Exportar
# ==========================================================
def main():
    for old in os.listdir(DIR_OUT):
        if old.endswith(".xlsx"):
            try:
                os.remove(os.path.join(DIR_OUT, old))
            except:
                pass

    df_bon = consolidar()
    if df_bon.is_empty():
        print("⚠️ Nenhum dado encontrado.")
        return

    out_path = os.path.join(DIR_OUT, f"Resumo_Politica_Bonificacao_Por_Base_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
    df_pd = df_bon.to_pandas()

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
        df_pd.to_excel(writer, index=False, sheet_name="Bonificacao_Por_Base")
        ws = writer.sheets["Bonificacao_Por_Base"]
        workbook = writer.book

        fmt_percent = workbook.add_format({"num_format": "0.00%", "align": "center"})
        fmt_number = workbook.add_format({"num_format": "#,##0.00", "align": "center"})
        fmt_money = workbook.add_format({"num_format": '"R$"#,##0.00', "align": "center"})
        fmt_int = workbook.add_format({"num_format": "0", "align": "center"})
        fmt_text = workbook.add_format({"align": "center"})

        for i, col in enumerate(df_pd.columns):
            width = max(df_pd[col].astype(str).map(len).max(), len(col)) + 2
            if col in ["SLA (%)", "Taxa_SemMov"]:
                ws.set_column(i, i, 14, fmt_percent)
            elif "(R$)" in col:
                ws.set_column(i, i, 14, fmt_money)
            elif "(h)" in col:
                ws.set_column(i, i, 14, fmt_number)
            elif "Pontuação" in col:
                ws.set_column(i, i, 12, fmt_int)
            else:
                ws.set_column(i, i, width, fmt_text)

    print(f"✅ Relatório final gerado com sucesso!\n📂 {out_path}")

# ==========================================================
if __name__ == "__main__":
    main()
