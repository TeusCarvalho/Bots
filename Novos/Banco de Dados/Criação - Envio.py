# -*- coding: utf-8 -*-
import os
import unicodedata
import polars as pl
import psycopg2
from psycopg2.extras import execute_batch

# ======================================================
# CONFIG BANCO
# ======================================================
DB = {
    "host": "localhost",
    "database": "analytics",
    "user": "postgres",
    "password": "Jt2025"
}


# ======================================================
# FUNÇÃO: normalizar nomes
# ======================================================
def limpar_nome(nome):
    if not nome or not isinstance(nome, str):
        return "col_unk"

    nome = nome.strip().lower()

    nome = "".join(
        c for c in unicodedata.normalize("NFKD", nome)
        if not unicodedata.combining(c)
    )

    for r in [" ", "-", "/", ".", "%", "(", ")", ","]:
        nome = nome.replace(r, "_")

    while "__" in nome:
        nome = nome.replace("__", "_")

    if nome[0].isdigit():
        nome = "col_" + nome

    if nome == "":
        nome = "col_unk"

    return nome


# ======================================================
# CONEXÃO POSTGRES
# ======================================================
def conectar():
    return psycopg2.connect(**DB)


# ======================================================
# 1️⃣ CRIAR TODAS AS TABELAS
# ======================================================
def criar_tabelas(pasta_raiz):

    print("\n📌 Criando tabelas...\n")

    for root, dirs, files in os.walk(pasta_raiz):

        arquivos = [f for f in files if f.lower().endswith((".xlsx", ".xls"))]
        if not arquivos:
            continue

        nome_pasta = os.path.basename(root)
        tabela = limpar_nome("col_" + nome_pasta)

        print(f"\n📁 Pasta: {root}")
        print(f"🛠 Criando tabela: {tabela}")

        for arq in arquivos:
            caminho = os.path.join(root, arq)

            try:
                df = pl.read_excel(caminho, engine="calamine")
            except Exception as e:
                print(f"❌ Erro ao ler Excel: {e}")
                continue

            colunas_orig = df.columns
            colunas_norm = [limpar_nome(c) for c in colunas_orig]

            tipos_pg = []
            for c in colunas_norm:
                tipos_pg.append(f'"{c}" TEXT')

            ddl = f'CREATE TABLE IF NOT EXISTS "{tabela}" (\n    ' + ",\n    ".join(tipos_pg) + "\n);"

            try:
                con = conectar()
                cur = con.cursor()
                cur.execute(ddl)
                con.commit()
                cur.close()
                con.close()

                print(f"✔ Tabela criada: {tabela}")
                break

            except Exception as e:
                print(f"❌ Erro ao criar tabela {tabela}: {e}")
                continue

    print("\n🏁 Tabelas criadas!\n")


# ======================================================
# 2️⃣ CARREGAR DADOS
# ======================================================
def carregar_dados(pasta_raiz):

    print("\n📌 Iniciando carga de dados...\n")

    for root, dirs, files in os.walk(pasta_raiz):

        arquivos = [f for f in files if f.lower().endswith((".xlsx", ".xls"))]
        if not arquivos:
            continue

        nome_pasta = os.path.basename(root)
        tabela = limpar_nome("col_" + nome_pasta)

        print(f"\n📁 Pasta: {root}")
        print(f"🛠 Tabela destino: {tabela}")

        # TRUNCATE
        try:
            con = conectar()
            cur = con.cursor()
            cur.execute(f'TRUNCATE TABLE "{tabela}";')
            con.commit()
            cur.close()
            con.close()
            print("🧹 TRUNCATE OK.")
        except Exception as e:
            print(f"❌ Erro ao limpar {tabela}: {e}")
            continue

        for arq in arquivos:

            print(f"📄 Lendo: {arq}")
            caminho = os.path.join(root, arq)

            try:
                df = pl.read_excel(caminho, engine="calamine")
            except Exception as e:
                print(f"❌ Erro ao ler arquivo {arq}: {e}")
                continue

            # normalizar colunas
            cols_orig = df.columns
            cols_norm = [limpar_nome(c) for c in cols_orig]
            df = df.rename(dict(zip(cols_orig, cols_norm)))
            df = df.with_columns(pl.col("*").cast(pl.Utf8).fill_null(""))

            registros = [tuple(row) for row in df.iter_rows()]

            cols_str = ", ".join([f'"{c}"' for c in cols_norm])
            placeholders = ", ".join(["%s"] * len(cols_norm))
            sql = f'INSERT INTO "{tabela}" ({cols_str}) VALUES ({placeholders})'

            try:
                con = conectar()
                cur = con.cursor()
                execute_batch(cur, sql, registros, page_size=5000)
                con.commit()
                cur.close()
                con.close()

                print(f"✔ {len(registros)} registros inseridos.")

            except Exception as e:
                print(f"❌ Erro ao inserir: {e}")

    print("\n🏁 Carga finalizada!\n")


# ======================================================
# EXECUÇÃO
# ======================================================
if __name__ == "__main__":
    PASTA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\QUALIDADE_ FILIAL GO - BASE DE DADOS"

    criar_tabelas(PASTA)
    carregar_dados(PASTA)
