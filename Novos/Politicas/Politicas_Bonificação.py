# -*- coding: utf-8 -*-
"""
📊 Política de Bonificação - v2.6.1
Autor: bb-assistente 😎

Novidades v2.6.1:
- Mantém SLA (T-0) sem contagens de pedidos (só %).
- Shipping Time com Atual, Anterior e Diferença (variação).
- Ressarcimentos com valores e Atingimento calculado por fórmula.
- Sem Movimentação com Taxa e Atingimento por fórmula.
- Coluna "Elegibilidade" por fórmula (usa SLA e Diferença do ST).
- Coluna "Total da bonificação" por fórmula (usa Elegibilidade, Atingimentos).
- Cabeçalho Excel idêntico ao print (vermelho/cinza + data).
- Lê TODOS os arquivos de Sem Movimentação (GP/PA + aging 6–7–10–14–30).
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
# 🟦 Coleta + Expedição (usado p/ taxa sem mov)
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
# 🟨 T0 (SLA %)
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
# 📉 Shipping Time (Atual / Anterior / Diferença)
# ==========================================================
MAPA_ETAPAS = {
    "Tempo trânsito SC Destino->Base Entrega": "Etapa 6 (Trânsito)",
    "Tempo médio processamento Base Entrega": "Etapa 7 (Processamento)",
    "Tempo médio Saída para Entrega->Entrega": "Etapa 8 (Saída p/ Entrega)"
}

def _prep_shipping(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
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
    return (
        df.group_by(col_base)
        .agg(pl.mean("Soma Total (h)").alias("Média (h)"))
        .rename({col_base: "Nome da base"})
    )

def shippingtime_atual():
    arquivos = [f for f in os.listdir(DIR_SHIP) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()
    df = read_excel_silent(os.path.join(DIR_SHIP, sorted(arquivos)[-1]))
    return _prep_shipping(df).rename({"Média (h)": "S.T. Atual (h)"})

def shippingtime_antiga():
    arquivos = [os.path.join(DIR_ANTIGA, f) for f in os.listdir(DIR_ANTIGA) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()
    dfs = [read_excel_silent(f) for f in tqdm(arquivos, desc="📉 Lendo Base Antiga", colour="cyan")]
    dfs = [df for df in dfs if not df.is_empty()]
    if not dfs:
        return pl.DataFrame()
    df = pl.concat(dfs, how="diagonal_relaxed")
    return _prep_shipping(df).rename({"Média (h)": "S.T. Anterior (h)"})

# ==========================================================
# 💰 Ressarcimento por pacote (SEERRO)
# ==========================================================
def ressarcimento_por_pacote(df_coleta_assinatura):
    arquivos = [f for f in os.listdir(DIR_RESS) if f.endswith((".xlsx", ".xls"))]
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

    if not df_coleta_assinatura.is_empty():
        df = df.join(
            df_coleta_assinatura.select(["Nome da base", "Qtd Entregue Assinatura"]),
            on="Nome da base",
            how="left"
        )

    df = df.fill_null(0).with_columns([
        (
            pl.when(pl.col("Qtd Entregue Assinatura") > 0)
            .then(pl.col("Custo total (R$)") / pl.col("Qtd Entregue Assinatura"))
            .otherwise(pl.col("Custo total (R$)"))
        ).alias("Ressarcimento p/pct (R$)")
    ])
    return df.select(["Nome da base", "Custo total (R$)", "Ressarcimento p/pct (R$)"])

# ==========================================================
# 🟥 Sem Movimentação (multi-arquivos, GP/PA + aging)
# ==========================================================
def pacotes_sem_mov():
    arquivos = [f for f in os.listdir(DIR_SEMMOV) if f.endswith((".xlsx", ".xls"))]
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
    col_map = {
        "Regional responsável责任所属代理区": "Regional responsável",
        "Unidade responsável责任机构": "Nome da base",
        "Aging超时类型": "Aging",
        "Número de pedido JMS 运单号": "Remessa"
    }
    for o, n in col_map.items():
        if o in df.columns:
            df = df.rename({o: n})

    obrigatorias = ["Regional responsável", "Nome da base", "Aging", "Remessa"]
    if not all(c in df.columns for c in obrigatorias):
        return pl.DataFrame()

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
    df = _normalize_base(df)
    df = df.group_by("Nome da base").agg(pl.count("Remessa").alias("Qtd Sem Mov"))
    return df

# ==========================================================
# 🧮 Consolidação
# ==========================================================
def consolidar():
    dias_do_mes = calendar.monthrange(datetime.now().year, datetime.now().month)[1]

    df_coleta = coleta_expedicao()
    df_t0 = taxa_t0()
    df_st_atual = shippingtime_atual()
    df_st_ant = shippingtime_antiga()
    df_ress = ressarcimento_por_pacote(df_coleta)
    df_sem = pacotes_sem_mov()

    # Shipping diff
    if not df_st_atual.is_empty():
        if not df_st_ant.is_empty():
            df_st = (
                df_st_atual.join(df_st_ant, on="Nome da base", how="left")
                .with_columns((pl.col("S.T. Atual (h)") - pl.col("S.T. Anterior (h)").fill_null(0)).alias("Diferença (h)"))
            )
        else:
            df_st = df_st_atual.with_columns(pl.lit(0).alias("Diferença (h)"))
    else:
        df_st = pl.DataFrame()

    # Merge principal
    df = df_t0
    for dfx in [df_st, df_ress, df_sem, df_coleta]:
        if not dfx.is_empty() and "Nome da base" in dfx.columns:
            df = df.join(dfx, on="Nome da base", how="left")

    df = df.fill_null(0)

    # Taxa Sem Mov (numérica; elegibilidade e atingimentos virão como fórmulas no Excel)
    df = df.with_columns([
        (
            pl.when(pl.col("Total Coleta+Entrega") > 0)
            .then(pl.col("Qtd Sem Mov") / dias_do_mes / pl.col("Total Coleta+Entrega"))
            .otherwise(0)
        ).alias("Taxa Sem Mov.")
    ])

    # Seleção e ordem das colunas para o Excel (casam com cabeçalho)
    ordered = [
        "Nome da base",
        "SLA (%)",
        "S.T. Atual (h)",
        "S.T. Anterior (h)",
        "Diferença (h)",
        # "Elegibilidade" -> fórmula no Excel (col F)
        "Custo total (R$)",
        "Ressarcimento p/pct (R$)",
        # "Atingimento (Ressarc.)" -> fórmula no Excel (col I)
        "Qtd Sem Mov",
        "Taxa Sem Mov.",        # "Atingimento (Sem Mov.)" -> fórmula no Excel (col L)
        # "Total da bonificação" -> fórmula no Excel (col M)
    ]
    # garante colunas ausentes
    for c in ordered:
        if c not in df.columns:
            df = df.with_columns(pl.lit(0).alias(c)) if c != "Nome da base" else df.with_columns(pl.lit("").alias(c))

    df = df.select(ordered)
    return df

# ==========================================================
# 💾 Exportar com cabeçalho e FÓRMULAS
# ==========================================================
def main():
    df = consolidar()
    if df.is_empty():
        print("⚠️ Nenhum dado consolidado.")
        return

    out = os.path.join(DIR_OUT, f"Resumo_Politica_Bonificacao_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
    df_pd = df.to_pandas()

    # Mapeamento de colunas (A..M) conforme cabeçalho desejado
    # A  Nome da base
    # B  SLA (%)
    # C  S.T. Atual (h)
    # D  S.T. Anterior (h)
    # E  Diferença (h)
    # F  Elegibilidade               (FÓRMULA)
    # G  Custo total (R$)
    # H  Ressarcimento p/pct (R$)
    # I  Atingimento (Ressarc.)      (FÓRMULA)
    # J  Qtd Sem Mov
    # K  Taxa Sem Mov.               (numérico)
    # L  Atingimento (Sem Mov.)      (FÓRMULA)
    # M  Total da bonificação        (FÓRMULA)

    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        # vamos escrever os dados SEM cabeçalho pandas e começar na linha 7 (1-based)
        startrow = 6  # zero-based -> linha 7 do Excel
        df_pd.to_excel(writer, sheet_name="Bonificação", startrow=startrow, startcol=0, header=False, index=False)

        wb = writer.book
        ws = writer.sheets["Bonificação"]

        # Formatos
        red  = wb.add_format({"bold": True, "font_color": "white", "align": "center", "valign": "vcenter", "bg_color": "#C00000", "border": 1})
        gray = wb.add_format({"bold": True, "font_color": "white", "align": "center", "valign": "vcenter", "bg_color": "#595959", "border": 1})
        center = wb.add_format({"align": "center", "valign": "vcenter"})
        fmt_percent_2 = wb.add_format({"num_format": "0.00%", "align": "center"})
        fmt_percent_4 = wb.add_format({"num_format": "0.0000%", "align": "center"})
        fmt_money = wb.add_format({"num_format": '"R$"#,##0.00', "align": "center"})
        fmt_number = wb.add_format({"num_format": "#,##0.00", "align": "center"})
        fmt_int = wb.add_format({"num_format": "0", "align": "center"})

        # Cabeçalho principal
        ws.merge_range("A1:M1", "RESULTADOS DE INDICADORES", red)
        ws.merge_range("A2:M2", f"Data de atualização: {datetime.now():%d/%m}", gray)

        # Cabeçalho duplo (linhas 5-6)
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

        # Largura/formatos de colunas
        ws.set_column("A:A", 22, center)
        ws.set_column("B:B", 12, fmt_percent_2)
        ws.set_column("C:D", 16, fmt_number)
        ws.set_column("E:E", 14, fmt_number)
        ws.set_column("F:F", 16, fmt_percent_2)
        ws.set_column("G:G", 16, fmt_money)
        ws.set_column("H:H", 18, fmt_money)
        ws.set_column("I:I", 14, fmt_percent_2)
        ws.set_column("J:J", 14, fmt_int)
        ws.set_column("K:K", 14, fmt_percent_4)  # 4 casas para Taxa Sem Mov.
        ws.set_column("L:L", 14, fmt_percent_2)
        ws.set_column("M:M", 20, fmt_percent_2)

        # Inserção das FÓRMULAS nas linhas de dados
        n_rows = len(df_pd)
        first_row = startrow + 1  # primeira linha de dados (1-based)
        last_row = startrow + n_rows  # última linha de dados (1-based)

        for r in range(first_row, last_row + 1):
            # Elegibilidade (col F) -> =SE(E5<=-8;110%;SE(E5<=0;100%;SE(B5>=97%;110%;SE(B5>=95%;100%;0%))))
            ws.write_formula(r - 1, 5, f'=SE(E{r}<=-8;110%;SE(E{r}<=0;100%;SE(B{r}>=97%;110%;SE(B{r}>=95%;100%;0%))))')

            # Atingimento (Ressarc.) (col I) -> =SE(H5<=0,01;45%;SE(H5<=0,09;35%;SE(H5<=0,15;5%;0)))
            ws.write_formula(r - 1, 8, f'=SE(H{r}<=0,01;45%;SE(H{r}<=0,09;35%;SE(H{r}<=0,15;5%;0)))')

            # Atingimento (Sem Mov.) (col L) -> =SE(K5<=0,01%;45%;SE(K5<=0,05%;35%;SE(K5<=0,08%;5%;0)))
            ws.write_formula(r - 1, 11, f'=SE(K{r}<=0,01%;45%;SE(K{r}<=0,05%;35%;SE(K{r}<=0,08%;5%;0)))')

            # Total da bonificação (col M)
            # =SE(F5<>0;SE(F5=110%;10%+I5+L5;I5+L5);0)
            ws.write_formula(r - 1, 12, f'=SE(F{r}<>0;SE(F{r}=110%;10%+I{r}+L{r};I{r}+L{r});0)')

        print(f"✅ Fórmulas aplicadas de A{first_row}:M{last_row}")

    print(f"✅ Relatório final gerado!\n📂 {out}")

# ==========================================================
if __name__ == "__main__":
    main()
