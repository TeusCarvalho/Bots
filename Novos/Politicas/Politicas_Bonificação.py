
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
    # garante coluna chave única e trim
    df = _fix_key_cols(df)
    if "Nome da base" in df.columns:
        df = df.with_columns(
            pl.col("Nome da base").cast(pl.Utf8, strict=False).str.strip_chars().alias("Nome da base")
        )
    return df

def _fix_key_cols(df: pl.DataFrame) -> pl.DataFrame:
    """Normaliza qualquer variante da chave para 'Nome da base' e remove duplicatas da chave."""
    if df.is_empty():
        return df
    cols = df.columns
    # candidatos que aparecem pós-join
    key_aliases = [c for c in cols if c.startswith("Nome da base")]
    if not key_aliases:
        return df
    # escolhe prioridade: exata > _left > _right > primeira
    chosen = "Nome da base" if "Nome da base" in key_aliases else (
        "Nome da base_left" if "Nome da base_left" in key_aliases else (
            "Nome da base_right" if "Nome da base_right" in key_aliases else key_aliases[0]
        )
    )
    if chosen != "Nome da base":
        df = df.rename({chosen: "Nome da base"})
    # drop demais variantes da chave
    for c in key_aliases:
        if c != "Nome da base" and c in df.columns:
            df = df.drop(c)
    return df

def _safe_full_join(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    """Join 'full' robusto: normaliza chaves antes/depois e evita duplicações."""
    if left.is_empty() and right.is_empty():
        return pl.DataFrame()
    left = _fix_key_cols(left)
    right = _fix_key_cols(right)
    if "Nome da base" not in left.columns and "Nome da base" in right.columns:
        # se o left não tem a chave mas o right tem, inverte para manter a chave
        left, right = right, left
    if "Nome da base" not in left.columns:
        # sem chave nos dois -> retorna concat (fallback)
        return pl.concat([left, right], how="diagonal_relaxed").unique(maintain_order=True)

    if "Nome da base" not in right.columns:
        # right sem chave: retorna left como está
        out = left
    else:
        out = left.join(right, on="Nome da base", how="full", suffix="_dup")
    # normaliza pós-join
    out = _fix_key_cols(out)
    # remove colunas duplicadas com sufixo "_dup" geradas por overlaps não-chave
    dup_cols = [c for c in out.columns if c.endswith("_dup")]
    if dup_cols:
        # regra simples: se já existe a versão "sem _dup", mantemos a sem _dup
        keep = []
        drop = []
        for c in dup_cols:
            base = c[:-4]
            if base in out.columns:
                drop.append(c)
            else:
                keep.append(c)  # só mantém se não existe base
        if drop:
            out = out.drop(drop)
    # dedup por chave
    out = out.unique(subset=["Nome da base"], keep="first")
    return out

def to_float(col):
    return pl.col(col).cast(pl.Float64, strict=False).fill_null(0).fill_nan(0)

def read_excel_silent(path):
    with warnings.catch_warnings(), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        warnings.simplefilter("ignore")
        try:
            return pl.read_excel(path)
        except Exception:
            return pl.DataFrame()

def pacotes_sem_mov():
    arquivos = [f for f in os.listdir(DIR_SEMMOV) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame(), 0  # <- retorna 0 planilhas

    dfs = []
    for arq in tqdm(arquivos, desc="🟥 Lendo Sem Movimentação", colour="red"):
        df = read_excel_silent(os.path.join(DIR_SEMMOV, arq))
        if not df.is_empty():
            dfs.append(df)

    if not dfs:
        return pl.DataFrame(), 0

    df = pl.concat(dfs, how="diagonal_relaxed")

    # renomeia colunas PT/中文 → padrão
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
        return pl.DataFrame(), 0

    df = df.filter(
        (pl.col("Regional responsável").is_in(["GP", "PA"])) &
        (pl.col("Aging").is_in([
            "Exceed 5 days with no track"
            "Exceed 6 days with no track",
            "Exceed 7 days with no track",
            "Exceed 10 days with no track",
            "Exceed 14 days with no track",
            "Exceed 30 days with no track"
        ]))
    )
    df = _normalize_base(df)

    df = df.group_by("Nome da base").agg(pl.count("Remessa").alias("Qtd Sem Mov"))
    qtd_planilhas = len(arquivos)

    print(f"🟥 {qtd_planilhas} planilhas lidas, total consolidado: {df['Qtd Sem Mov'].sum()} registros")
    return df, qtd_planilhas

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
# 🟨 T0 (SLA)
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
# 📉 Shipping Time (Horas) - cálculo consolidado por base (em horas)
# ==========================================================
def _prep_shipping(df: pl.DataFrame, col_nome: str) -> pl.DataFrame:
    """
    Prepara o cálculo de Shipping Time:
    - Soma as 3 etapas (Trânsito, Processamento e Saída-Entrega)
    - Agrupa por base e calcula a média consolidada
    - Detecta e converte automaticamente unidades (minutos/dias → horas)
    - Não divide pela quantidade de planilhas
    """
    if df.is_empty():
        return df

    base = "PDD de Entrega" if "PDD de Entrega" in df.columns else "Nome da base"
    etapas = [
        "Tempo trânsito SC Destino->Base Entrega",
        "Tempo médio processamento Base Entrega",
        "Tempo médio Saída para Entrega->Entrega"
    ]

    # Garante que todas as colunas das etapas existam
    for e in etapas:
        if e not in df.columns:
            df = df.with_columns(pl.lit(0).alias(e))

    # Converte colunas para float
    df = df.with_columns([to_float(e) for e in etapas])

    # ----------------------------------------------------------------------
    # 🔍 Detecção automática de unidade (minutos / dias)
    # ----------------------------------------------------------------------
    def detectar_unidade(col: str) -> float:
        media_valor = df[col].mean()
        if media_valor is None:
            return 1
        if media_valor > 48 and media_valor < 1500:
            print(f"⚠️  Coluna '{col}' parece estar em minutos → convertendo para horas (÷60)")
            return 1 / 60
        elif media_valor >= 1500:
            print(f"⚠️  Coluna '{col}' parece estar em dias → convertendo para horas (×24)")
            return 24
        return 1

    fatores = {e: detectar_unidade(e) for e in etapas}

    for e in etapas:
        if fatores[e] != 1:
            df = df.with_columns((pl.col(e) * fatores[e]).alias(e))

    # ----------------------------------------------------------------------
    # 🧮 Soma das etapas + média por base (em horas)
    # ----------------------------------------------------------------------
    df = df.with_columns([
        (pl.col(etapas[0]) + pl.col(etapas[1]) + pl.col(etapas[2])).alias(col_nome)
    ])

    out = df.group_by(base).agg(pl.mean(col_nome)).rename({base: "Nome da base"})
    print(f"✅ Shipping Time calculado (média consolidada em horas) — {col_nome}")

    return _normalize_base(out)


def shippingtime_atual():
    """Lê TODAS as planilhas atuais e calcula o Shipping Time Atual (h)."""
    arquivos = [os.path.join(DIR_SHIP, f) for f in os.listdir(DIR_SHIP) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        print("⚠️ Nenhum arquivo encontrado em DIR_SHIP.")
        return pl.DataFrame()

    dfs = [read_excel_silent(f) for f in tqdm(arquivos, desc="📊 Lendo Base Atual", colour="green")]
    dfs = [d for d in dfs if not d.is_empty()]
    if not dfs:
        print("⚠️ Nenhum dado válido encontrado nas planilhas atuais.")
        return pl.DataFrame()

    # Junta todas as planilhas atuais
    df = pl.concat(dfs, how="diagonal_relaxed")

    # Calcula a média consolidada por base (em horas)
    return _prep_shipping(df, "S.T. Atual (h)")


def shippingtime_antiga():
    """Lê TODAS as planilhas antigas e calcula o Shipping Time Anterior (h)."""
    arquivos = [os.path.join(DIR_ANTIGA, f) for f in os.listdir(DIR_ANTIGA) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        print("⚠️ Nenhum arquivo encontrado em DIR_ANTIGA.")
        return pl.DataFrame()

    dfs = [read_excel_silent(f) for f in tqdm(arquivos, desc="📉 Lendo Base Antiga", colour="cyan")]
    dfs = [d for d in dfs if not d.is_empty()]
    if not dfs:
        print("⚠️ Nenhum dado válido encontrado nas planilhas antigas.")
        return pl.DataFrame()

    # Junta todas as planilhas antigas
    df = pl.concat(dfs, how="diagonal_relaxed")

    # Calcula a média consolidada por base (em horas)
    return _prep_shipping(df, "S.T. Anterior (h)")

# ==========================================================
# 💰 Ressarcimento
# ==========================================================
def ressarcimento_por_pacote(df_coleta):
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

    if not df_coleta.is_empty():
        df = _safe_full_join(
            df,
            df_coleta.select(["Nome da base", "Qtd Entregue Assinatura"])
        )

    df = df.fill_null(0).with_columns([
        (pl.when(pl.col("Qtd Entregue Assinatura") > 0)
         .then(pl.col("Custo total (R$)") / pl.col("Qtd Entregue Assinatura"))
         .otherwise(pl.col("Custo total (R$)"))).alias("Ressarcimento p/pct (R$)")
    ])

    # ✅ Corrigido: Custo total primeiro, depois Ressarcimento
    return df.select(["Nome da base", "Custo total (R$)", "Ressarcimento p/pct (R$)"])

# ==========================================================
# 🧮 Consolidação (com Coordenador/Base_Atualizada.xlsx)
# ==========================================================
def consolidar():
    dias = calendar.monthrange(datetime.now().year, datetime.now().month)[1]

    # 🔹 Lê a base de coordenadores
    path_coord = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
    df_coord = read_excel_silent(path_coord)
    if "Nome da base" in df_coord.columns:
        df_coord = df_coord.select(["Nome da base"]).unique()
        df_coord = _normalize_base(df_coord)
    else:
        print("⚠️ 'Nome da base' não encontrada em Base_Atualizada.xlsx")
        df_coord = pl.DataFrame({"Nome da base": []})

    # 🔹 Lê as demais bases
    df_coleta = coleta_expedicao()
    df_t0 = taxa_t0()
    df_st_at = shippingtime_atual()
    df_st_ant = shippingtime_antiga()
    df_ress = ressarcimento_por_pacote(df_coleta)
    df_sem, _ = pacotes_sem_mov()

    # 🔹 Calcula Shipping diff
    if not df_st_at.is_empty():
        df_st = _safe_full_join(df_st_at, df_st_ant).with_columns(
            (pl.col("S.T. Atual (h)") - pl.col("S.T. Anterior (h)").fill_null(0)).alias("Variação (h)")
        )
    else:
        df_st = pl.DataFrame()

    # 🔹 Junta tudo com segurança
    df_final = _safe_full_join(df_t0, df_st)
    df_final = _safe_full_join(df_final, df_ress)
    df_final = _safe_full_join(df_final, df_sem)
    df_final = _safe_full_join(df_final, df_coleta)

    # 🔹 Garante todas as bases da planilha Base_Atualizada
    df_final = _safe_full_join(df_coord, df_final)

    # 🔹 Calcula Taxa Sem Movimentação
    df = df_final.fill_null(0).with_columns([
        (pl.when(pl.col("Total Coleta+Entrega") > 0)
         .then(pl.col("Qtd Sem Mov") / dias / pl.col("Total Coleta+Entrega"))
         .otherwise(0)).alias("Taxa Sem Mov.")
    ])

    ordered = [
        "Nome da base",
        "SLA (%)",
        "S.T. Atual (h)",
        "S.T. Anterior (h)",
        "Variação (h)",
        "Ressarcimento p/pct (R$)",
        "Custo total (R$)",
        "Qtd Sem Mov",
        "Taxa Sem Mov."
    ]

    for c in ordered:
        if c not in df.columns:
            df = (df.with_columns(pl.lit(0).alias(c)) if c != "Nome da base" else df.with_columns(pl.lit("").alias(c)))

    return df.select(ordered)

# ==========================================================
# 💾 Exportar (sem fórmulas)
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

        # Cabeçalhos
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

        # Larguras
        ws.set_column("A:A", 22, center)
        ws.set_column("B:B", 12, fmt_percent_2)
        ws.set_column("C:E", 14, fmt_number)
        ws.set_column("F:G", 16, fmt_money)
        ws.set_column("H:H", 14, fmt_int)
        ws.set_column("I:I", 14, fmt_percent_2)

    print(f"✅ Relatório final gerado!\n📂 {out}")

# ==========================================================
if __name__ == "__main__":
    main()

