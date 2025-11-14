# -*- coding: utf-8 -*-
import os
import polars as pl

# ===========================================
# CONFIGURAÇÕES
# ===========================================
PASTA_ENTRADA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Leozinho das Planilhas"
SAIDA = os.path.join(PASTA_ENTRADA, "Divididas_50000")

LINHAS_POR_ARQUIVO = 50000

# Criar pasta de saída, se não existir
os.makedirs(SAIDA, exist_ok=True)

# ===========================================
# FUNÇÃO PARA LIMPAR COLUNA CEP
# ===========================================
def limpar_cep(df):
    # Só continua se a coluna existir
    colunas = [c.lower() for c in df.columns]

    if "cep" in colunas:
        idx = colunas.index("cep")
        nome_col_cep = df.columns[idx]

        # Limpa tudo que não for número (0-9)
        df = df.with_columns(
            pl.col(nome_col_cep)
            .cast(pl.Utf8, strict=False)
            .str.replace_all(r"[^0-9]", "")   # remove tudo que não é número
            .alias(nome_col_cep)
        )

        print("   ➤ CEP normalizado (somente números).")

    return df

# ===========================================
# FUNÇÃO PARA DIVIDIR PLANILHA
# ===========================================
def dividir_planilha(caminho_arquivo):

    nome_arquivo = os.path.basename(caminho_arquivo)
    nome, ext = os.path.splitext(nome_arquivo)

    print(f"\n📂 Processando: {nome_arquivo}")

    # Ler usando Polars com fallback
    try:
        if ext.lower() in [".xlsx", ".xls"]:
            df = pl.read_excel(caminho_arquivo)
        else:
            df = pl.read_csv(caminho_arquivo)
    except Exception as e:
        print(f"❌ Erro ao ler {nome_arquivo}: {e}")
        return

    # ---- LIMPA CEP AQUI ----
    df = limpar_cep(df)

    total_linhas = df.height
    print(f"   ➤ Linhas totais: {total_linhas}")

    # Quantas partes serão criadas?
    partes = (total_linhas // LINHAS_POR_ARQUIVO) + (1 if total_linhas % LINHAS_POR_ARQUIVO != 0 else 0)

    print(f"   ➤ Separando em {partes} parte(s)...")

    for parte in range(partes):
        inicio = parte * LINHAS_POR_ARQUIVO

        df_parte = df.slice(inicio, LINHAS_POR_ARQUIVO)

        nome_saida = f"{nome}_parte_{parte + 1}{ext}"
        caminho_saida = os.path.join(SAIDA, nome_saida)

        # Salvar mantendo cabeçalho
        try:
            if ext.lower() in [".xlsx", ".xls"]:
                df_parte.write_excel(caminho_saida)
            else:
                df_parte.write_csv(caminho_saida)
        except Exception as e:
            print(f"❌ Erro ao salvar {nome_saida}: {e}")
            continue

        print(f"      ✔ Arquivo gerado: {nome_saida}")

# ===========================================
# EXECUÇÃO PRINCIPAL
# ===========================================
def main():
    print("\n====================")
    print("🚀 DIVISOR 50.000 LINHAS")
    print("====================\n")

    arquivos = [
        f for f in os.listdir(PASTA_ENTRADA)
        if f.lower().endswith((".xlsx", ".xls", ".csv"))
    ]

    if not arquivos:
        print("❌ Nenhum arquivo encontrado na pasta.")
        return

    for arquivo in arquivos:
        caminho = os.path.join(PASTA_ENTRADA, arquivo)
        dividir_planilha(caminho)

    print("\n🎉 Finalizado! Arquivos divididos estão em:")
    print(SAIDA)

if __name__ == "__main__":
    main()
