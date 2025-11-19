# -*- coding: utf-8 -*-
"""
Auditoria ETL – Pastas Excel x Tabelas PostgreSQL

- Compara todas as pastas com Excel com as tabelas do banco
- Verifica:
    * Pastas com Excel SEM tabela correspondente
    * Tabelas SEM pasta correspondente
    * Tabelas sem linhas
    * Diferença de colunas (Excel x Banco)
- Gera um JSON: relatorio_auditoria.json
"""

import os
import unicodedata
import json
import logging
from typing import Dict, List, Set, Tuple

import polars as pl
import psycopg2

# ======================================================
# CONFIG LOG
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("auditoria_etl.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ======================================================
# CONFIG BANCO
# ======================================================
DB_CONFIG = {
    "host": "localhost",
    "database": "analytics",
    "user": "postgres",
    "password": "Jt2025"
}

# PASTA RAIZ DOS EXCELS
PASTA_RAIZ = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda\QUALIDADE_ FILIAL GO - BASE DE DADOS"


# ======================================================
# NORMALIZAÇÃO – MESMO PADRÃO DO ETL
# ======================================================
def limpar_nome_basico(nome: str) -> str:
    """Normaliza string (colunas, pedaço de nome etc.)."""
    if not nome or not isinstance(nome, str):
        return "col_unk"

    nome = nome.strip().lower()
    nome = "".join(
        c for c in unicodedata.normalize("NFKD", nome)
        if not unicodedata.combining(c)
    )

    # Substitui caracteres "ruins" por underscore
    for ch in [" ", "-", "/", ".", "%", "(", ")", ","]:
        nome = nome.replace(ch, "_")

    # Remove underscores duplicados
    while "__" in nome:
        nome = nome.replace("__", "_")

    if nome and nome[0].isdigit():
        nome = "col_" + nome

    if nome == "":
        nome = "col_unk"

    return nome


def nome_tabela_a_partir_da_pasta(nome_pasta: str) -> str:
    """
    Gera o nome da tabela a partir do nome da pasta,
    seguindo o mesmo padrão do ETL original:
        tabela = limpar_nome_basico("col_" + nome_pasta)
    """
    return limpar_nome_basico("col_" + nome_pasta)


def normalizar_colunas_excel(colunas: List[str]) -> List[str]:
    """Normaliza as colunas do Excel igual ao ETL original."""
    return [limpar_nome_basico(c) for c in colunas]


# ======================================================
# CONEXÃO BANCO
# ======================================================
def conectar():
    return psycopg2.connect(**DB_CONFIG)


# ======================================================
# FUNÇÕES AUXILIARES DE BANCO
# ======================================================
def obter_tabelas_publico(prefixo: str = "col_") -> Set[str]:
    """Retorna o conjunto de tabelas do schema public (opcionalmente filtradas por prefixo)."""
    conn = conectar()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
        )
        rows = cur.fetchall()
        tabelas = {r[0] for r in rows}
        if prefixo:
            tabelas = {t for t in tabelas if t.startswith(prefixo)}
        return tabelas
    finally:
        conn.close()


def contar_linhas_tabela(tabela: str) -> int:
    conn = conectar()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{tabela}";')
        (qtd,) = cur.fetchone()
        return int(qtd)
    except Exception as e:
        logger.error(f"Erro ao contar linhas da tabela '{tabela}': {e}")
        return -1
    finally:
        conn.close()


def obter_colunas_tabela(tabela: str) -> List[str]:
    conn = conectar()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position;
        """, (tabela,))
        return [r[0] for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"Erro ao obter colunas da tabela '{tabela}': {e}")
        return []
    finally:
        conn.close()


# ======================================================
# ESCANEIO DAS PASTAS
# ======================================================
def escanear_pastas_com_excel(pasta_raiz: str) -> Dict[str, Dict]:
    """
    Retorna dict:
    {
      caminho_pasta: {
         "nome_pasta": str,
         "tabela_esperada": str,
         "arquivos_excel": [..],
         "total_excels": int
      }
    }
    """
    info_pastas: Dict[str, Dict] = {}

    for root, dirs, files in os.walk(pasta_raiz):
        excels = [f for f in files if f.lower().endswith((".xlsx", ".xls"))]
        if not excels:
            continue

        nome_pasta = os.path.basename(root)
        tabela_esperada = nome_tabela_a_partir_da_pasta(nome_pasta)

        info_pastas[root] = {
            "nome_pasta": nome_pasta,
            "tabela_esperada": tabela_esperada,
            "arquivos_excel": excels,
            "total_excels": len(excels),
        }

    return info_pastas


# ======================================================
# DIFERENÇA DE COLUNAS (EXCEL x BANCO)
# ======================================================
def comparar_colunas_pasta_tabela(
    caminho_pasta: str,
    arquivos_excel: List[str],
    tabela: str
) -> Dict[str, List[str]]:
    """
    Lê o primeiro Excel da pasta e compara as colunas normalizadas com as colunas da tabela.
    Retorna dict com listas de diferenças.
    """
    if not arquivos_excel:
        return {}

    primeiro_arquivo = arquivos_excel[0]
    caminho_arquivo = os.path.join(caminho_pasta, primeiro_arquivo)

    try:
        df = pl.read_excel(caminho_arquivo, engine="calamine")
    except Exception as e:
        logger.error(f"Erro ao ler Excel para comparação de colunas "
                     f"('{caminho_arquivo}'): {e}")
        return {}

    cols_excel_orig = df.columns
    cols_excel_norm = normalizar_colunas_excel(cols_excel_orig)

    cols_tabela = obter_colunas_tabela(tabela)

    set_excel = set(cols_excel_norm)
    set_tabela = set(cols_tabela)

    somente_excel = sorted(list(set_excel - set_tabela))
    somente_banco = sorted(list(set_tabela - set_excel))

    if not somente_excel and not somente_banco:
        return {}

    return {
        "somente_excel": somente_excel,
        "somente_banco": somente_banco,
        "arquivo_base": primeiro_arquivo
    }


# ======================================================
# AUDITORIA PRINCIPAL
# ======================================================
def rodar_auditoria():
    logger.info("Iniciando auditoria ETL...")

    # 1) Escanear pastas
    info_pastas = escanear_pastas_com_excel(PASTA_RAIZ)
    total_pastas = len(info_pastas)
    total_excels = sum(p["total_excels"] for p in info_pastas.values())

    # 2) Tabelas no banco (prefixo col_)
    tabelas_banco = obter_tabelas_publico(prefixo="col_")
    total_tabelas = len(tabelas_banco)

    logger.info(f"Pastas com Excel: {total_pastas}")
    logger.info(f"Arquivos Excel encontrados: {total_excels}")
    logger.info(f"Tabelas no banco (prefixo 'col_'): {total_tabelas}")

    # Map para encontrar pasta a partir do nome de tabela esperado
    map_tabela_para_pasta: Dict[str, str] = {}
    for caminho, info in info_pastas.items():
        map_tabela_para_pasta[info["tabela_esperada"]] = caminho

    # 3) Pastas sem tabela correspondente
    pastas_sem_tabela: List[Dict] = []
    for caminho, info in info_pastas.items():
        tabela_esp = info["tabela_esperada"]
        if tabela_esp not in tabelas_banco:
            pastas_sem_tabela.append({
                "caminho_pasta": caminho,
                "nome_pasta": info["nome_pasta"],
                "tabela_esperada": tabela_esp,
                "total_excels": info["total_excels"]
            })

    # 4) Tabelas sem pasta correspondente
    tabelas_sem_pasta: List[str] = []
    for tabela in tabelas_banco:
        if tabela not in map_tabela_para_pasta:
            tabelas_sem_pasta.append(tabela)
    tabelas_sem_pasta.sort()

    # 5) Tabelas sem linhas
    tabelas_sem_linhas: List[str] = []
    linhas_por_tabela: Dict[str, int] = {}

    for tabela in sorted(tabelas_banco):
        qtd = contar_linhas_tabela(tabela)
        linhas_por_tabela[tabela] = qtd
        if qtd == 0:
            tabelas_sem_linhas.append(tabela)

    # 6) Diferença de colunas
    diferencas_colunas: Dict[str, Dict[str, List[str]]] = {}

    for caminho, info in info_pastas.items():
        tabela = info["tabela_esperada"]
        if tabela not in tabelas_banco:
            continue  # já está na lista de "pastas sem tabela"

        diff = comparar_colunas_pasta_tabela(
            caminho_pasta=caminho,
            arquivos_excel=info["arquivos_excel"],
            tabela=tabela
        )
        if diff:
            diferencas_colunas[tabela] = diff

    # 7) Monta relatório final
    problemas_detectados = (
        len(pastas_sem_tabela)
        + len(tabelas_sem_pasta)
        + len(tabelas_sem_linhas)
        + len(diferencas_colunas)
    )

    relatorio = {
        "total_pastas_com_excel": total_pastas,
        "total_excels_encontrados": total_excels,
        "total_tabelas_no_banco": total_tabelas,
        "problemas_detectados": problemas_detectados,
        "pastas_sem_tabela": pastas_sem_tabela,
        "tabelas_sem_pasta": tabelas_sem_pasta,
        "tabelas_sem_linhas": tabelas_sem_linhas,
        "diferencas_colunas": diferencas_colunas,
        "linhas_por_tabela": linhas_por_tabela,
    }

    # 8) Salvar JSON
    with open("relatorio_auditoria.json", "w", encoding="utf-8") as f:
        json.dump(relatorio, f, ensure_ascii=False, indent=4)

    # 9) Imprimir resumo no terminal
    print("\n========== 📊 RELATÓRIO FINAL ===============")
    print(json.dumps(
        {
            "total_pastas_com_excel": total_pastas,
            "total_excels_encontrados": total_excels,
            "total_tabelas_no_banco": total_tabelas,
            "problemas_detectados": problemas_detectados
        },
        ensure_ascii=False,
        indent=4
    ))

    print("\n📁 Pastas SEM tabela correspondente:")
    if pastas_sem_tabela:
        for p in pastas_sem_tabela:
            print(f"- {p['nome_pasta']}  => tabela esperada: {p['tabela_esperada']} "
                  f"(arquivos: {p['total_excels']})")
    else:
        print("Nenhuma. ✅")

    print("\n🧭 Tabelas SEM pasta correspondente:")
    if tabelas_sem_pasta:
        print(tabelas_sem_pasta)
    else:
        print("Nenhuma. ✅")

    print("\n⚠️ Tabelas sem linhas:")
    if tabelas_sem_linhas:
        print(tabelas_sem_linhas)
    else:
        print("Nenhuma. ✅")

    print("\n🧩 Diferença de colunas (Excel x Banco):")
    if diferencas_colunas:
        for tabela, diff in diferencas_colunas.items():
            print(f"\nTabela: {tabela}")
            print(f"  Arquivo base: {diff.get('arquivo_base')}")
            print(f"  Somente no Excel : {diff.get('somente_excel')}")
            print(f"  Somente no Banco : {diff.get('somente_banco')}")
    else:
        print("Nenhuma diferença relevante. ✅")

    print("\n📄 Arquivo 'relatorio_auditoria.json' gerado com sucesso!")
    print("=====================================================\n")


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    rodar_auditoria()
