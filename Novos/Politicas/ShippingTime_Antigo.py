import os
import polars as pl
from datetime import datetime

# ==========================================================
# 📂 Caminhos
# ==========================================================
BASE_ROOT = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação"
DIR_BASE_ANTIGA = os.path.join(BASE_ROOT, "Base Antiga")
DIR_RESULTADOS = os.path.join(BASE_ROOT, "Resultados")
os.makedirs(DIR_RESULTADOS, exist_ok=True)

# ==========================================================
# 🧩 Funções auxiliares
# ==========================================================
def to_float(col: str) -> pl.Expr:
    """Converte coluna para float, tratando erros."""
    return pl.col(col).cast(pl.Float64, strict=False).fill_null(0).fill_nan(0)


# ==========================================================
# 📈 Função principal — Consolida todas as planilhas da Base Antiga
# ==========================================================
def reducao_shipping_base_antiga():
    arquivos = [os.path.join(DIR_BASE_ANTIGA, f) for f in os.listdir(DIR_BASE_ANTIGA) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        print("⚠️ Nenhum arquivo encontrado em Base Antiga.")
        return pl.DataFrame()

    print(f"📂 {len(arquivos)} planilha(s) encontrada(s) em Base Antiga. Lendo todas...")

    # Lê e concatena todas as planilhas
    lista_dfs = []
    for f in arquivos:
        try:
            print(f"📄 Lendo: {os.path.basename(f)}")
            df = pl.read_excel(f)
            df = df.with_columns(pl.lit(os.path.basename(f)).alias("Arquivo_Origem"))
            lista_dfs.append(df)
        except Exception as e:
            print(f"❌ Erro ao ler {f}: {e}")

    if not lista_dfs:
        print("⚠️ Nenhum arquivo pôde ser lido.")
        return pl.DataFrame()

    df = pl.concat(lista_dfs, how="diagonal_relaxed")

    # Detecta coluna da base
    col_base = "PDD de Entrega" if "PDD de Entrega" in df.columns else "Nome da base"

    # Define nomes das etapas
    etapas = {
        "Tempo trânsito SC Destino->Base Entrega": "Etapa 6 (Trânsito)",
        "Tempo médio processamento Base Entrega": "Etapa 7 (Processamento)",
        "Tempo médio Saída para Entrega->Entrega": "Etapa 8 (Saída p/ Entrega)"
    }

    # Garante que todas existam e converte
    for original_col in etapas.keys():
        if original_col not in df.columns:
            df = df.with_columns(pl.lit(0).alias(original_col))
        else:
            df = df.with_columns(to_float(original_col))

    # Cria soma total
    df = df.with_columns(
        (
            pl.col("Tempo trânsito SC Destino->Base Entrega") +
            pl.col("Tempo médio processamento Base Entrega") +
            pl.col("Tempo médio Saída para Entrega->Entrega")
        ).alias("Soma Total (min)")
    )

    # Agrupa por base e calcula médias
    df_final = (
        df.group_by(col_base)
        .agg([
            pl.mean("Tempo trânsito SC Destino->Base Entrega").alias(etapas["Tempo trânsito SC Destino->Base Entrega"]),
            pl.mean("Tempo médio processamento Base Entrega").alias(etapas["Tempo médio processamento Base Entrega"]),
            pl.mean("Tempo médio Saída para Entrega->Entrega").alias(etapas["Tempo médio Saída para Entrega->Entrega"]),
            pl.mean("Soma Total (min)").alias("Média (min)")
        ])
        .rename({col_base: "Nome da base"})
        .sort("Média (min)")
    )

    # ======================================================
    # 💾 Salva o resultado final em "Resultados"
    # ======================================================
    nome_arquivo = f"Resumo_BaseAntiga_ShippingTime_{datetime.now():%Y%m%d_%H%M}.xlsx"
    path_out = os.path.join(DIR_RESULTADOS, nome_arquivo)
    df_final.write_excel(path_out)

    print(f"✅ Relatório consolidado salvo com sucesso em:\n📂 {path_out}")

    return df_final


# ==========================================================
# 🚀 Execução
# ==========================================================
if __name__ == "__main__":
    df_relatorio = reducao_shipping_base_antiga()
    if not df_relatorio.is_empty():
        print("📊 Consolidado de todas as planilhas da Base Antiga:")
        print(df_relatorio)
