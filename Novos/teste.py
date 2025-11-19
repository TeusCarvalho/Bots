import psycopg2
import pandas as pd

DB = {
    "host": "localhost",
    "database": "analytics",
    "user": "postgres",
    "password": "Jt2025"
}

def ver_tabela(tabela):
    conn = psycopg2.connect(**DB)

    df = pd.read_sql(f'SELECT * FROM "{tabela}" LIMIT 200', conn)

    conn.close()

    print("\n🔎 COLUNAS:")
    print(df.columns.tolist())

    print("\n📄 PRIMEIRAS LINHAS:")
    print(df.head(20))  # mostra 20 linhas

    return df

ver_tabela("col_1_retidos")
