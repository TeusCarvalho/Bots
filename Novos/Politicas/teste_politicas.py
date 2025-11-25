import os
import polars as pl

# ============================================================
# 📂 Caminho da pasta
# ============================================================
PASTA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação\00.1 - Base Retidos(Lista)"

def ler_arquivos():
    arquivos = [os.path.join(PASTA, f) for f in os.listdir(PASTA) if f.endswith(".xlsx")]

    if not arquivos:
        raise Exception("Nenhum arquivo .xlsx encontrado na pasta!")

    dfs = []
    for arq in arquivos:
        try:
            df = pl.read_excel(arq)
            dfs.append(df)
        except Exception as e:
            print(f"Erro ao ler {arq}: {e}")

    return dfs


def processar():
    dfs = ler_arquivos()
    df_total = pl.concat(dfs, how="diagonal_relaxed")

    df_total = df_total.rename({
        "Nome da base de entrega": "base",
        "Qtd a entregar há mais de 10 dias": "qtd_10"
    })

    # Remover linhas sem base
    df_total = df_total.filter(
        pl.col("base").is_not_null() &
        (pl.col("base") != "")
    )

    # ============================
    # ❗ CORRIGIDO: group_by (Polars)
    # ============================
    df_final = (
        df_total
        .group_by("base")
        .agg(
            pl.col("qtd_10").sum().alias("total_acima_10_dias")
        )
        .sort("total_acima_10_dias", descending=True)
    )

    # Mostrar TOP 10
    print("\n===== TOP 10 BASES RETIDOS >10 DIAS =====")
    print(df_final.head(10))
    print("===========================================\n")


if __name__ == "__main__":
    processar()
