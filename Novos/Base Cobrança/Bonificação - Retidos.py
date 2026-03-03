# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from pathlib import Path
from datetime import datetime
import unicodedata

import pandas as pd

# ==========================================================
# CONFIGURAÇÃO
# ==========================================================
PASTA_ENTRADA = Path(
    r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Retidos"
)

COLUNA_ALVO = "Dias Retidos 滞留日"
VALOR_MINIMO = 5  # vai manter apenas > 5

ARQUIVO_SAIDA_XLSX = PASTA_ENTRADA / f"Retidos_Maior_Que_5_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
ARQUIVO_SAIDA_CSV = PASTA_ENTRADA / f"Retidos_Maior_Que_5_{datetime.now():%Y%m%d_%H%M%S}.csv"

EXTENSOES_VALIDAS = {".xlsx", ".xls", ".xlsm"}


# ==========================================================
# FUNÇÕES AUXILIARES
# ==========================================================
def normalizar_texto(texto: str) -> str:
    """Remove acentos, espaços duplicados e padroniza texto para comparação."""
    if texto is None:
        return ""
    texto = str(texto).strip()
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = re.sub(r"\s+", " ", texto)
    return texto.lower()


def localizar_coluna(df: pd.DataFrame, nome_coluna: str) -> str | None:
    """
    Procura a coluna pelo nome exato normalizado.
    Se não achar, tenta busca parcial.
    """
    alvo_norm = normalizar_texto(nome_coluna)

    mapa_colunas = {col: normalizar_texto(col) for col in df.columns}

    # 1) tentativa exata
    for col_original, col_norm in mapa_colunas.items():
        if col_norm == alvo_norm:
            return col_original

    # 2) tentativa parcial
    for col_original, col_norm in mapa_colunas.items():
        if "dias retidos" in col_norm and "滞留日" in str(col_original):
            return col_original

    for col_original, col_norm in mapa_colunas.items():
        if "dias retidos" in col_norm:
            return col_original

    return None


def converter_para_numero(serie: pd.Series) -> pd.Series:
    """
    Converte valores da coluna para número.
    Ex.: '6', '6 dias', '6,0', ' 7 ' -> 6, 6, 6.0, 7
    """
    s = serie.astype(str).str.strip()

    # troca vírgula por ponto
    s = s.str.replace(",", ".", regex=False)

    # extrai somente o número
    s = s.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)

    return pd.to_numeric(s, errors="coerce")
def main():
    if not PASTA_ENTRADA.exists():
        print(f"❌ Pasta não encontrada: {PASTA_ENTRADA}")
        return

    arquivos = sorted(
        [
            arq for arq in PASTA_ENTRADA.iterdir()
            if arq.is_file()
            and arq.suffix.lower() in EXTENSOES_VALIDAS
            and not arq.name.startswith("~$")
        ]
    )

    if not arquivos:
        print("❌ Nenhum arquivo Excel encontrado na pasta.")
        return

    print(f"📂 Pasta de entrada: {PASTA_ENTRADA}")
    print(f"📄 Arquivos encontrados: {len(arquivos)}")
    print("-" * 80)

    bases_filtradas = []
    total_linhas_lidas = 0
    total_linhas_filtradas = 0

    for i, arquivo in enumerate(arquivos, start=1):
        print(f"[{i}/{len(arquivos)}] Lendo arquivo: {arquivo.name}")

        try:
            abas = pd.read_excel(arquivo, sheet_name=None, dtype=str)
        except Exception as e:
            print(f"   ⚠️ Erro ao ler {arquivo.name}: {e}")
            continue

        for nome_aba, df in abas.items():
            try:
                if df is None or df.empty:
                    print(f"   - Aba '{nome_aba}' vazia. Ignorada.")
                    continue

                total_linhas_lidas += len(df)

                coluna_real = localizar_coluna(df, COLUNA_ALVO)
                if not coluna_real:
                    print(f"   - Aba '{nome_aba}' sem a coluna '{COLUNA_ALVO}'. Ignorada.")
                    continue

                df = df.copy()
                df["__dias_retidos_num__"] = converter_para_numero(df[coluna_real])

                df_filtrado = df[df["__dias_retidos_num__"] > VALOR_MINIMO].copy()

                if df_filtrado.empty:
                    print(f"   - Aba '{nome_aba}' sem registros > {VALOR_MINIMO}.")
                    continue

                df_filtrado.insert(0, "Arquivo Origem", arquivo.name)
                df_filtrado.insert(1, "Aba Origem", nome_aba)

                total_linhas_filtradas += len(df_filtrado)
                bases_filtradas.append(df_filtrado)

                print(
                    f"   ✅ Aba '{nome_aba}' | "
                    f"coluna encontrada: '{coluna_real}' | "
                    f"linhas filtradas: {len(df_filtrado)}"
                )

            except Exception as e:
                print(f"   ⚠️ Erro na aba '{nome_aba}' do arquivo '{arquivo.name}': {e}")

    print("-" * 80)

    if not bases_filtradas:
        print("❌ Nenhum registro com 'Dias Retidos 滞留日' maior que 5 foi encontrado.")
        return

    df_final = pd.concat(bases_filtradas, ignore_index=True)

    # remove coluna auxiliar
    if "__dias_retidos_num__" in df_final.columns:
        df_final.drop(columns=["__dias_retidos_num__"], inplace=True)

    # salva Excel
    try:
        df_final.to_excel(ARQUIVO_SAIDA_XLSX, index=False)
        print(f"✅ Excel salvo em: {ARQUIVO_SAIDA_XLSX}")
    except Exception as e:
        print(f"⚠️ Erro ao salvar Excel: {e}")

    # salva CSV
    try:
        df_final.to_csv(ARQUIVO_SAIDA_CSV, index=False, encoding="utf-8-sig")
        print(f"✅ CSV salvo em: {ARQUIVO_SAIDA_CSV}")
    except Exception as e:
        print(f"⚠️ Erro ao salvar CSV: {e}")

    print("-" * 80)
    print("📊 RESUMO FINAL")
    print(f"Total de arquivos lidos: {len(arquivos)}")
    print(f"Total de linhas lidas: {total_linhas_lidas}")
    print(f"Total de linhas com Dias Retidos > {VALOR_MINIMO}: {total_linhas_filtradas}")
    print(f"Total consolidado final: {len(df_final)}")


if __name__ == "__main__":
    main()