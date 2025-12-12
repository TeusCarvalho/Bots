# db.py
# -*- coding: utf-8 -*-

from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, URL


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    Engine do SQLAlchemy com parâmetros FIXOS.
    Aqui usamos o usuário postgres com a senha que você definiu (Jt2025).
    """

    host = "127.0.0.1"   # força IPv4, evita (::1) e regras diferentes no pg_hba.conf
    port = 5432
    db = "analytics"
    user = "postgres"
    pwd = "Jt2025"       # a senha que você acabou de alterar com ALTER ROLE

    print("\n[DEBUG] Conectando com:")
    print(f"  host = {host!r}")
    print(f"  port = {port!r}")
    print(f"  db   = {db!r}")
    print(f"  user = {user!r}")
    print(f"  pwd  = {pwd!r}")

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password=pwd,
        host=host,
        port=port,
        database=db,
    )

    engine = create_engine(url, pool_pre_ping=True)
    return engine
