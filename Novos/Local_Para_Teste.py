# -*- coding: utf-8 -*-
import os
import polars as pl
from datetime import datetime # <-- Importação adicionada

# ==========================================================
# ⚙️ CONFIGURAÇÕES (Ajuste aqui os parâmetros)
# ==========================================================

PASTA_RETIDOS   = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\06 - Retidos"
PASTA_DEVOLUCAO = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.3 - Base Devolução"
PASTA_CUSTODIA  = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.4 - Base Custodia"
PASTA_SAIDA     = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\Resultados"

# Nomes “preferidos” (usados se existirem)
COL_PEDIDO_RET            = "Número do Pedido JMS 运单号"
COL_DATA_ATUALIZACAO_RET  = "Data da Atualização 更新日期"
COL_REGIONAL_RET          = "Regional 区域"

COL_PEDIDO_DEV            = "Número de pedido JMS"
COL_DATA_SOLICITACAO_DEV  = "Tempo de solicitação"

COL_PEDIDO_CUST           = "Número de pedido JMS"
COL_DATA_REGISTRO_CUST    = "data de registro"

REGIONAIS_DESEJADAS = ["GP", "PA", "GO"]
PRAZO_CUSTODIA_DIAS = 9
NOME_ARQUIVO_FINAL  = "resultado_final_analise_retidos"

EXCEL_ROW_LIMIT = 1_048_000  # margem segura (máx: 1_048_576)

# ==========================================================
# 🧩 Utilidades
# ==========================================================
def converter_datetime(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    """Converte coluna para Datetime testando vários formatos; remove nulos ao final."""
    if coluna not in df.columns:
        return df
    formatos = [
        "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M", "%Y-%m-%d", "%d/%m/%Y"
    ]
    for fmt in formatos:
        try:
            df = df.with_columns(pl.col(coluna).str.strptime(pl.Datetime, format=fmt, strict=False))
            if df[coluna].dtype == pl.Datetime and df[coluna].is_not_null().any():
                break
        except Exception:
            continue
    return df.filter(pl.col(coluna).is_not_null())

def detectar_coluna(df: pl.DataFrame, candidatos: list[str]) -> str | None:
    """Retorna o primeiro nome de coluna que contém qualquer candidato (case-insensitive)."""
    cols_low = {c.lower(): c for c in df.columns}
    for cand in candidatos:
        cand = cand.lower()
        for low, original in cols_low.items():
            if cand in low:
                return original
    return None

def safe_pick(df: pl.DataFrame, preferido: str, candidatos_extra: list[str]) -> str | None:
    """Prefere um nome exato; se não houver, detecta por candidatos."""
    if preferido in df.columns:
        return preferido
    return detectar_coluna(df, candidatos_extra)

def ler_planilhas(pasta: str, nome_base: str) -> pl.DataFrame:
    """Lê e concatena a 1ª aba de todos os Excels da pasta; ignora arquivos ~$. """
    if not os.path.exists(pasta):
        print(f"❌ Pasta '{pasta}' não encontrada.")
        return pl.DataFrame()

    arquivos = [
        os.path.join(pasta, f)
        for f in os.listdir(pasta)
        if f.lower().endswith((".xls", ".xlsx")) and not f.startswith("~$")
    ]
    if not arquivos:
        print(f"⚠️ Nenhum arquivo Excel encontrado em {nome_base}.")
        return pl.DataFrame()

    print(f"📂 {len(arquivos)} arquivo(s) encontrado(s) em {nome_base}:")
    for f in arquivos:
        print(f"   • {os.path.basename(f)}")

    dfs = []
    for arq in arquivos:
        try:
            df_raw = pl.read_excel(arq)
            df = next(iter(df_raw.values())) if isinstance(df_raw, dict) else df_raw
            dfs.append(df)
        except Exception as e:
            print(f"   ❌ Erro ao ler {os.path.basename(arq)}: {e}")

    return pl.concat(dfs, how="diagonal_relaxed") if dfs else pl.DataFrame()

def salvar_resultado(df: pl.DataFrame, caminho_saida: str, nome_base: str):
    if not os.path.exists(caminho_saida):
        os.makedirs(caminho_saida)
        print(f"📁 Pasta de saída criada: {caminho_saida}")

    # Ordena colunas para facilitar leitura no Excel
    col_order = [c for c in [
        COL_PEDIDO_RET,
        COL_DATA_ATUALIZACAO_RET,
        COL_REGIONAL_RET,
        COL_PEDIDO_CUST,
        COL_DATA_REGISTRO_CUST,
        "Prazo_Limite",
        "Status_Custodia"
    ] if c in df.columns]
    if col_order:
        df = df.select(col_order)

    # Adiciona timestamp ao nome do arquivo para não sobrescrever
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_filename = f"{nome_base}_{timestamp}"

    # Decide formato conforme limite do Excel
    if df.height >= EXCEL_ROW_LIMIT:
        out = os.path.join(caminho_saida, f"{base_filename}.csv")
        df.write_csv(out)
        print(f"\n✅ Resultado salvo em CSV (sem limite de linhas): {out}")
    else:
        out = os.path.join(caminho_saida, f"{base_filename}.xlsx")
        df.write_excel(out)
        print(f"\n✅ Resultado salvo em Excel: {out}")

# ==========================================================
# 🚀 Principal
# ==========================================================
def analisar_retidos():
    print("\n==============================")
    print("🚀 INICIANDO ANÁLISE COMPLETA")
    print("==============================")

    # 1) RETIDOS
    print("\n🟢 ETAPA 1 – LENDO BASE RETIDOS...")
    df_ret = ler_planilhas(PASTA_RETIDOS, "Retidos")
    if df_ret.is_empty():
        print("❌ Nenhum dado em Retidos. Abortando análise.")
        return

    # Descoberta de colunas (flexível)
    col_pedido_ret = safe_pick(df_ret, COL_PEDIDO_RET, ["jms", "運單", "运单", "pedido"])
    col_data_ret   = safe_pick(df_ret, COL_DATA_ATUALIZACAO_RET, ["atualiza", "更新", "data"])
    col_regional   = safe_pick(df_ret, COL_REGIONAL_RET, ["regional", "区域"])

    faltando = [n for n, v in {
        "Pedido Retidos": col_pedido_ret,
        "Data Atualização": col_data_ret,
    }.items() if v is None]
    if faltando:
        print(f"❌ Colunas essenciais ausentes em Retidos: {', '.join(faltando)}")
        return

    # Seleciona e normaliza nomes
    rename_map = {
        col_pedido_ret: COL_PEDIDO_RET,
        col_data_ret: COL_DATA_ATUALIZACAO_RET
    }
    if col_regional:
        rename_map[col_regional] = COL_REGIONAL_RET

    df_ret = df_ret.select(list(rename_map.keys())).rename(rename_map)
    df_ret = converter_datetime(df_ret, COL_DATA_ATUALIZACAO_RET)

    if COL_REGIONAL_RET in df_ret.columns:
        df_ret = df_ret.filter(pl.col(COL_REGIONAL_RET).is_in(REGIONAIS_DESEJADAS))

    total_inicial = df_ret.height
    print(f"📊 Total inicial de Retidos ({', '.join(REGIONAIS_DESEJADAS)}): {total_inicial}")

    # 2) DEVOLUÇÃO
    print("\n🟡 ETAPA 2 – COMPARAÇÃO COM BASE DE DEVOLUÇÃO...")
    df_dev = ler_planilhas(PASTA_DEVOLUCAO, "Devolução")
    if not df_dev.is_empty():
        col_pedido_dev = safe_pick(df_dev, COL_PEDIDO_DEV, ["jms", "pedido"])
        col_solic_dev  = safe_pick(df_dev, COL_DATA_SOLICITACAO_DEV, ["solicit", "tempo", "data"])
        if col_pedido_dev and col_solic_dev:
            df_dev = df_dev.select([col_pedido_dev, col_solic_dev]).rename({
                col_pedido_dev: COL_PEDIDO_DEV,
                col_solic_dev:  COL_DATA_SOLICITACAO_DEV
            })
            df_dev = converter_datetime(df_dev, COL_DATA_SOLICITACAO_DEV)
            df_dev = df_dev.group_by(COL_PEDIDO_DEV).agg(pl.col(COL_DATA_SOLICITACAO_DEV).min())

            df_merge = df_ret.join(df_dev, left_on=COL_PEDIDO_RET, right_on=COL_PEDIDO_DEV, how="left")
            df_merge = df_merge.with_columns(
                ((pl.col(COL_DATA_ATUALIZACAO_RET) < pl.col(COL_DATA_SOLICITACAO_DEV)) &
                 pl.col(COL_DATA_SOLICITACAO_DEV).is_not_null()).alias("Remover_Dev")
            )
            removidos_dev = df_merge.filter(pl.col("Remover_Dev")).height
            df_ret = df_merge.filter(~pl.col("Remover_Dev")).drop(["Remover_Dev", COL_PEDIDO_DEV, COL_DATA_SOLICITACAO_DEV], strict=False)
            print(f"📦 Devolução → Removidos: {removidos_dev} | Mantidos: {df_ret.height}")
        else:
            print("⚠️ Colunas de Devolução não detectadas — pulando etapa.")
    else:
        print("⚠️ Base de Devolução não encontrada — pulando etapa.")

    # 3) CUSTÓDIA (+9 dias)
    print("\n🔵 ETAPA 3 – COMPARAÇÃO COM BASE DE CUSTÓDIA (+9 dias)...")
    df_cust = ler_planilhas(PASTA_CUSTODIA, "Custódia")
    df_final = df_ret
    if not df_cust.is_empty():
        col_pedido_c = safe_pick(df_cust, COL_PEDIDO_CUST, ["jms", "pedido"])
        col_reg_c    = safe_pick(df_cust, COL_DATA_REGISTRO_CUST, ["registro", "data"])
        if col_pedido_c and col_reg_c:
            df_cust = df_cust.select([col_pedido_c, col_reg_c]).rename({
                col_pedido_c: COL_PEDIDO_CUST,
                col_reg_c:    COL_DATA_REGISTRO_CUST
            })
            df_cust = converter_datetime(df_cust, COL_DATA_REGISTRO_CUST)
            df_cust = (
                df_cust.group_by(COL_PEDIDO_CUST)
                .agg(pl.col(COL_DATA_REGISTRO_CUST).min())
                .with_columns((pl.col(COL_DATA_REGISTRO_CUST) + pl.duration(days=PRAZO_CUSTODIA_DIAS)).alias("Prazo_Limite"))
            )

            df_join = df_ret.join(df_cust, left_on=COL_PEDIDO_RET, right_on=COL_PEDIDO_CUST, how="left")

            df_join = df_join.with_columns(
                pl.when(
                    (pl.col(COL_DATA_ATUALIZACAO_RET) <= pl.col("Prazo_Limite")) &
                    pl.col("Prazo_Limite").is_not_null()
                )
                .then(pl.lit("Dentro do Prazo"))
                .otherwise(pl.lit("Fora do Prazo"))
                .alias("Status_Custodia")
            )

            removidos_cust = df_join.filter(pl.col("Status_Custodia") == "Dentro do Prazo").height
            df_final = df_join.filter(pl.col("Status_Custodia") == "Fora do Prazo")

            if COL_DATA_REGISTRO_CUST in df_final.columns:
                df_final = df_final.sort(COL_DATA_REGISTRO_CUST)

            print(f"📦 Custódia → Removidos: {removidos_cust} | Mantidos: {df_final.height}")
        else:
            print("⚠️ Colunas de Custódia não detectadas — mantendo base após Devolução.")

    # 4) RESULTADO
    print("\n==============================")
    print("📊 RESULTADO FINAL GERAL")
    print("==============================")
    print(f"📉 Total inicial: {total_inicial}")
    print(f"✅ Total final: {df_final.height}")
    print(f"🗑️ Total removido: {total_inicial - df_final.height}")
    print("==============================")

    print("\n📋 Amostra dos primeiros registros:")
    print(df_final.head(15))

    # 5) SALVAR RESULTADO
    if df_final.is_empty():
        print("\n⚠️ Base final vazia — nada para salvar.")
    else:
        salvar_resultado(df_final, PASTA_SAIDA, NOME_ARQUIVO_FINAL)

# ==========================================================
# ▶️ EXECUÇÃO
# ==========================================================
if __name__ == "__main__":
    analisar_retidos()