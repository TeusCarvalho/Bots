# teste_conexao.py
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

from db import get_engine


def main() -> None:
    try:
        print("\n[DEBUG] Criando engine...")
        engine = get_engine()
        print("[DEBUG] Engine criado com sucesso:", engine)

        query = "SELECT datname FROM pg_database;"

        print("[DEBUG] Abrindo conexão...")
        with engine.connect() as conn:
            print("[DEBUG] Conexão aberta, executando query de teste...")
            df = pd.read_sql(query, conn)

        print("\n✅ Conexão OK. Bancos encontrados:")
        print(df)

    except SQLAlchemyError as e:
        print("\n❌ Erro SQLAlchemy ao conectar ou executar a query:")
        print(repr(e))
    except Exception as e:
        print("\n❌ Erro genérico ao conectar no banco:")
        print(repr(e))


if __name__ == "__main__":
    main()
