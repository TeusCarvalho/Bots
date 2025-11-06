# -*- coding: utf-8 -*-
import os
import polars as pl
from datetime import datetime

# ==========================================================
# ⚙️ CONFIGURAÇÕES
# ==========================================================
PASTA_RETIDOS   = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\06 - Retidos"
PASTA_DEVOLUCAO = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.3 - Base Devolução"
PASTA_PROBLEMATICOS = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.2 - Base de Problematicos (Gestão de Anormalidade)"
PASTA_CUSTODIA  = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.4 - Base Custodia"
PASTA_BASE_LISTA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.1 - Base Retidos(Lista)"
PASTA_SAIDA     = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\Resultados"

COL_PEDIDO_RET           = "Número do Pedido JMS 运单号"
COL_DATA_ATUALIZACAO_RET = "Data da Atualização 更新日期"
COL_REGIONAL_RET          = "Regional 区域"
COL_PEDIDO_DEV            = "Número de pedido JMS"
COL_DATA_SOLICITACAO_DEV  = "Tempo de solicitação"
COL_PEDIDO_CUST           = "Número de pedido JMS"
COL_DATA_REGISTRO_CUST    = "data de registro"

REGIONAIS_DESEJADAS = ["GP", "PA", "GO"]
PRAZO_CUSTODIA_DIAS = 9
NOME_ARQUIVO_FINAL  = "resultado_final_analise_retidos"
EXCEL_ROW_LIMIT = 1_048_000

# ==========================================================
# 🧩 FUNÇÕES AUXILIARES
# ==========================================================
def converter_datetime(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    if coluna not in df.columns:
        return df
    try:
        df = df.with_columns(pl.col(coluna).str.to_datetime(strict=False))
    except Exception:
        for fmt in ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
                    "%Y/%m/%d %H:%M", "%d/%m/%Y", "%Y-%m-%d"]:
            try:
                df = df.with_columns(pl.col(coluna).str.strptime(pl.Datetime, fmt, strict=False))
                break
            except Exception:
                continue
    return df.filter(pl.col(coluna).is_not_null())

def detectar_coluna(df, candidatos):
    cols_low = {c.lower(): c for c in df.columns}
    for cand in candidatos:
        cand = cand.lower()
        for low, original in cols_low.items():
            if cand in low:
                return original
    return None

def safe_pick(df, preferido, candidatos_extra):
    if preferido in df.columns:
        return preferido
    return detectar_coluna(df, candidatos_extra)

def limpar_pedidos(df, coluna):
    if coluna in df.columns:
        df = df.with_columns(pl.col(coluna).cast(pl.Utf8).str.strip_chars())
    return df

def ler_planilhas(pasta, nome_base):
    if not os.path.exists(pasta):
        print(f"\033[91m❌ Pasta '{pasta}' não encontrada.\033[0m")
        return pl.DataFrame()
    arquivos = [os.path.join(pasta, f) for f in os.listdir(pasta)
                if f.lower().endswith((".xls", ".xlsx")) and not f.startswith("~$")]
    if not arquivos:
        print(f"\033[93m⚠️ Nenhum arquivo Excel encontrado em {nome_base}.\033[0m")
        return pl.DataFrame()

    print(f"📂 {len(arquivos)} arquivo(s) encontrado(s) em {nome_base}:")
    dfs = []
    for arq in arquivos:
        try:
            df_raw = pl.read_excel(arq)
            df = next(iter(df_raw.values())) if isinstance(df_raw, dict) else df_raw
            dfs.append(df)
            print(f"   ✅ {os.path.basename(arq)} ({df.height} linhas)")
        except Exception as e:
            print(f"\033[91m   ❌ Erro ao ler {os.path.basename(arq)}: {e}\033[0m")
    return pl.concat(dfs, how="diagonal_relaxed") if dfs else pl.DataFrame()

def salvar_resultado(df, caminho_saida, nome_base):
    if not os.path.exists(caminho_saida):
        os.makedirs(caminho_saida)
        print(f"\033[94m📁 Pasta criada: {caminho_saida}\033[0m")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = os.path.join(caminho_saida,
                       f"{nome_base}_{timestamp}.{'csv' if df.height>=EXCEL_ROW_LIMIT else 'xlsx'}")
    (df.write_csv if out.endswith(".csv") else df.write_excel)(out)
    print(f"\n✅ Resultado salvo em: {out}")
    return out

# ==========================================================
# 🚀 ANÁLISE PRINCIPAL
# ==========================================================
def analisar_retidos():
    print("\n==============================\n🚀 INICIANDO ANÁLISE COMPLETA\n==============================")

    removidos_dev = removidos_cust = removidos_cluster = removidos_prob = 0

    # RETIDOS
    df_ret = ler_planilhas(PASTA_RETIDOS, "Retidos")
    if df_ret.is_empty():
        print("❌ Nenhum dado em Retidos."); return

    # 🔹 Remover Clusters 1–2 dias e 3–9 dias
    col_cluster = safe_pick(df_ret, "Cluster Retidos 分类", ["cluster", "分类", "retidos"])
    if col_cluster and col_cluster in df_ret.columns:
        total_antes = df_ret.height
        df_ret = df_ret.with_columns(
            pl.col(col_cluster)
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_lowercase()
            .alias(col_cluster)
        )
        df_ret = df_ret.filter(
            ~(
                pl.col(col_cluster).str.contains("1 到 2") |
                pl.col(col_cluster).str.contains("3 到 9")
            )
        )
        removidos_cluster = total_antes - df_ret.height
        print(f"\033[95m🧹 Cluster Retidos (1–9 dias) → Removidos: {removidos_cluster} | Mantidos: {df_ret.height}\033[0m")

    # Selecionar colunas relevantes
    col_pedido_ret = safe_pick(df_ret, COL_PEDIDO_RET, ["pedido", "运单", "jms"])
    col_data_ret   = safe_pick(df_ret, COL_DATA_ATUALIZACAO_RET, ["data", "atualiza", "更新"])
    col_regional   = safe_pick(df_ret, COL_REGIONAL_RET, ["regional", "区域"])
    col_base_entrega = safe_pick(df_ret, "Base de Entrega 派件网点", ["base", "网点", "派件"])

    cols = [c for c in [col_pedido_ret, col_data_ret, col_regional, col_base_entrega] if c]
    df_ret = df_ret.select(cols).rename({
        col_pedido_ret: COL_PEDIDO_RET,
        col_data_ret: COL_DATA_ATUALIZACAO_RET,
        col_regional: COL_REGIONAL_RET if col_regional else None,
        col_base_entrega: "Base de Entrega 派件网点" if col_base_entrega else None
    })

    df_ret = limpar_pedidos(df_ret, COL_PEDIDO_RET)
    df_ret = converter_datetime(df_ret, COL_DATA_ATUALIZACAO_RET)

    if COL_REGIONAL_RET in df_ret.columns:
        df_ret = df_ret.filter(pl.col(COL_REGIONAL_RET).is_in(REGIONAIS_DESEJADAS))

    total_inicial = df_ret.height
    print(f"\033[92m🟢 Retidos filtrados ({', '.join(REGIONAIS_DESEJADAS)}): {total_inicial}\033[0m")

    # 🟡 DEVOLUÇÃO
    df_dev = ler_planilhas(PASTA_DEVOLUCAO, "Devolução")
    if not df_dev.is_empty():
        col_pedido_dev = safe_pick(df_dev, COL_PEDIDO_DEV, ["pedido", "jms"])
        col_data_dev   = safe_pick(df_dev, COL_DATA_SOLICITACAO_DEV, ["solicit", "data"])
        if col_pedido_dev and col_data_dev:
            df_dev = df_dev.select([col_pedido_dev, col_data_dev]).rename(
                {col_pedido_dev: COL_PEDIDO_DEV, col_data_dev: COL_DATA_SOLICITACAO_DEV})
            df_dev = limpar_pedidos(df_dev, COL_PEDIDO_DEV)
            df_dev = converter_datetime(df_dev, COL_DATA_SOLICITACAO_DEV)
            df_dev = df_dev.group_by(COL_PEDIDO_DEV).agg(pl.col(COL_DATA_SOLICITACAO_DEV).min())
            df_merge = df_ret.join(df_dev, left_on=COL_PEDIDO_RET, right_on=COL_PEDIDO_DEV, how="left")
            df_merge = df_merge.with_columns(
                ((pl.col(COL_DATA_SOLICITACAO_DEV) > pl.col(COL_DATA_ATUALIZACAO_RET))
                 & pl.col(COL_DATA_SOLICITACAO_DEV).is_not_null()).alias("Remover_Dev"))
            removidos_dev = df_merge.filter(pl.col("Remover_Dev")).height
            df_ret = df_merge.filter(~pl.col("Remover_Dev")).drop(
                ["Remover_Dev", COL_PEDIDO_DEV, COL_DATA_SOLICITACAO_DEV], strict=False)
            print(f"\033[93m🟡 Devolução → Removidos: {removidos_dev} | Mantidos: {df_ret.height}\033[0m")

    # 🟠 PROBLEMÁTICOS
    df_prob = ler_planilhas(PASTA_PROBLEMATICOS, "Problemáticos")
    if not df_prob.is_empty():
        col_pedido_prob = safe_pick(df_prob, "Número de pedido JMS", ["pedido", "jms"])
        col_data_prob   = safe_pick(df_prob, "data de registro", ["data", "registro", "anormal"])
        if col_pedido_prob and col_data_prob:
            df_prob = df_prob.select([col_pedido_prob, col_data_prob]).rename({
                col_pedido_prob: "Número de pedido JMS",
                col_data_prob: "data de registro"
            })
            df_prob = limpar_pedidos(df_prob, "Número de pedido JMS")
            df_prob = converter_datetime(df_prob, "data de registro")
            df_prob = df_prob.group_by("Número de pedido JMS").agg(pl.col("data de registro").min())

            df_merge_prob = df_ret.join(df_prob, left_on=COL_PEDIDO_RET, right_on="Número de pedido JMS", how="left")
            df_merge_prob = df_merge_prob.with_columns(
                ((pl.col("data de registro") >= pl.col(COL_DATA_ATUALIZACAO_RET)) &
                 pl.col("data de registro").is_not_null()).alias("Remover_Prob")
            )

            removidos_prob = df_merge_prob.filter(pl.col("Remover_Prob")).height
            df_ret = df_merge_prob.filter(~pl.col("Remover_Prob")).drop(["Remover_Prob", "Número de pedido JMS", "data de registro"], strict=False)
            print(f"\033[38;5;208m🟠 Problemáticos → Removidos: {removidos_prob} | Mantidos: {df_ret.height}\033[0m")

    # 🔵 CUSTÓDIA
    df_cust = ler_planilhas(PASTA_CUSTODIA, "Custódia")
    df_final = df_ret
    if not df_cust.is_empty():
        col_pedido_c = safe_pick(df_cust, COL_PEDIDO_CUST, ["pedido", "jms"])
        col_data_c   = safe_pick(df_cust, COL_DATA_REGISTRO_CUST, ["data", "registro"])
        if col_pedido_c and col_data_c:
            df_cust = df_cust.select([col_pedido_c, col_data_c]).rename(
                {col_pedido_c: COL_PEDIDO_CUST, col_data_c: COL_DATA_REGISTRO_CUST})
            df_cust = limpar_pedidos(df_cust, COL_PEDIDO_CUST)
            df_cust = converter_datetime(df_cust, COL_DATA_REGISTRO_CUST)
            df_cust = df_cust.group_by(COL_PEDIDO_CUST).agg(
                pl.col(COL_DATA_REGISTRO_CUST).min().alias(COL_DATA_REGISTRO_CUST))
            df_cust = df_cust.with_columns(
                (pl.col(COL_DATA_REGISTRO_CUST) + pl.duration(days=PRAZO_CUSTODIA_DIAS))
                .alias("Prazo_Limite"))
            df_join = df_ret.join(df_cust, left_on=COL_PEDIDO_RET, right_on=COL_PEDIDO_CUST, how="left")
            df_join = df_join.with_columns(
                pl.when(
                    (pl.col(COL_DATA_ATUALIZACAO_RET) <= pl.col("Prazo_Limite"))
                    & pl.col("Prazo_Limite").is_not_null())
                .then(pl.lit("Dentro do Prazo"))
                .otherwise(pl.lit("Fora do Prazo"))
                .alias("Status_Custodia"))
            removidos_cust = df_join.filter(pl.col("Status_Custodia") == "Dentro do Prazo").height
            df_final = df_join.filter(pl.col("Status_Custodia") == "Fora do Prazo")
            print(f"\033[94m🔵 Custódia → Removidos: {removidos_cust} | Mantidos: {df_final.height}\033[0m")

    # 🧾 BASE DE REFERÊNCIA (00.1 - Base Retidos Lista)
    df_lista = ler_planilhas(PASTA_BASE_LISTA, "Base Retidos (Lista)")
    if not df_lista.is_empty():
        col_base_lista = safe_pick(df_lista, "Nome da base de entrega", ["base", "entrega", "网点"])
        col_qtd_lista  = safe_pick(df_lista, "Qtd a entregar há mais de 10 dias", ["qtd", "10", "dias"])
        if col_base_lista and col_qtd_lista:
            df_lista = df_lista.select([col_base_lista, col_qtd_lista]).rename({
                col_base_lista: "Nome da Base de Entrega",
                col_qtd_lista: "Qtd_Entregas_>10d"
            })
            df_lista = df_lista.with_columns(pl.col("Qtd_Entregas_>10d").cast(pl.Int64, strict=False))

            if "Base de Entrega 派件网点" in df_final.columns:
                df_resumo = (
                    df_final.group_by("Base de Entrega 派件网点")
                    .agg(pl.count().alias("Qtd_Retidos"))
                    .rename({"Base de Entrega 派件网点": "Nome da Base de Entrega"})
                )

                df_compara = df_lista.join(df_resumo, on="Nome da Base de Entrega", how="left")
                df_compara = df_compara.with_columns([
                    pl.col("Qtd_Retidos").fill_null(0).cast(pl.Int64).alias("Qtd_Retidos"),
                    ((pl.col("Qtd_Retidos") / pl.col("Qtd_Entregas_>10d")) * 100)
                    .round(2)
                    .alias("Percentual_Retidos")
                ])

                # Formata coluna percentual
                df_compara = df_compara.with_columns(
                    (pl.col("Percentual_Retidos").cast(pl.Utf8) + pl.lit(" %")).alias("Percentual_Retidos")
                )

                df_compara = df_compara.select([
                    "Nome da Base de Entrega",
                    "Qtd_Entregas_>10d",
                    "Qtd_Retidos",
                    "Percentual_Retidos"
                ]).sort("Qtd_Retidos", descending=True)

                out_lista = os.path.join(
                    PASTA_SAIDA,
                    f"Comparativo_Base_Lista_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
                )
                df_compara.write_excel(out_lista)
                print(f"\n📊 Comparativo com Base Lista exportado: {out_lista}")

    # 📦 RESULTADO FINAL
    out_final = salvar_resultado(df_final, PASTA_SAIDA, NOME_ARQUIVO_FINAL)

    print("\n==============================")
    print("📦 RESUMO FINAL DE PROCESSAMENTO")
    print("==============================")
    print(f"📊 Total Retidos iniciais: {df_ret.height + removidos_cluster}")
    print(f"🟣 Removidos por Cluster (1–9 dias): {removidos_cluster}")
    print(f"🟡 Removidos por Devolução: {removidos_dev}")
    print(f"🟠 Removidos por Problemáticos: {removidos_prob}")
    print(f"🔵 Removidos por Custódia: {removidos_cust}")
    print(f"✅ Pedidos restantes (fora do prazo): {df_final.height}")
    print(f"📊 Resultado salvo em: {out_final}")

# ==========================================================
# ▶️ EXECUÇÃO
# ==========================================================
if __name__ == "__main__":
    analisar_retidos()
