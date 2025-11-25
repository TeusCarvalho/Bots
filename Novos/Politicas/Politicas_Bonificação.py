# -*- coding: utf-8 -*-

import os
import re
import glob
import polars as pl
import pandas as pd
from datetime import datetime
import calendar
from tqdm import tqdm
import warnings
import contextlib
import io
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# ==========================================================
# 📂 Caminhos
# ==========================================================
BASE_ROOT = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação"

DIR_COLETA = os.path.join(BASE_ROOT, "00 -  Base de Dados (Coleta + Expedição)")
DIR_T0 = os.path.join(BASE_ROOT, "01 - Taxa de entrega T0")
DIR_RESS = os.path.join(BASE_ROOT, "02 - Ressarcimento por pacote")
DIR_SHIP = os.path.join(BASE_ROOT, "03 - Redução Shipping Time")  # (não usado ainda, mantido)
DIR_ANTIGA = os.path.join(BASE_ROOT, "Base Antiga")  # (não usado ainda, mantido)
DIR_SEMMOV = os.path.join(BASE_ROOT, "05 - Pacotes Sem Movimentação")
DIR_RETIDOS = os.path.join(BASE_ROOT, "06 - Retidos")
DIR_DEVOLUCAO = os.path.join(BASE_ROOT, "00.3 - Base Devolução")
DIR_PROBLEMATICOS = os.path.join(BASE_ROOT, "00.2 - Base de Problematicos (Gestão de Anormalidade)")
DIR_CUSTODIA = os.path.join(BASE_ROOT, "00.4 - Base Custodia")
DIR_BASE_LISTA = os.path.join(BASE_ROOT, "00.1 - Base Retidos(Lista)")  # (não usado agora)

# Coordenadores agora por pasta
DIR_COORDENADOR = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador"

DIR_OUT = os.path.join(BASE_ROOT, "Resultados")
os.makedirs(DIR_OUT, exist_ok=True)

# ==========================================================
# ⚙️ Configurações
# ==========================================================
REGIONAIS_DESEJADAS = ["GP", "PA", "GO"]
PRAZO_CUSTODIA_DIAS = 9
EXCEL_ROW_LIMIT = 1_048_000
GERAR_DETALHADO_RETIDOS = True


# ==========================================================
# ⚙️ Utilitários
# ==========================================================

def _normalize_strong(texto: str) -> str:
    """Normalização forte para bases: mantém números e hífens."""
    if not texto:
        return ""
    texto = str(texto).upper()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('ASCII')
    texto = re.sub(r'[^A-Z0-9\s-]', '', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto


def _normalize_base(df: pl.DataFrame) -> pl.DataFrame:
    if "Nome da base" not in df.columns or df.is_empty():
        return df
    return df.with_columns(
        pl.col("Nome da base")
        .map_elements(_normalize_strong, return_dtype=pl.Utf8)
        .alias("Nome da base")
    )


def diagnosticar_normalizacao():
    print("\n==============================")
    print("🔍 DIAGNÓSTICO DE NORMALIZAÇÃO")
    print("==============================")

    arquivos = [f for f in os.listdir(DIR_COLETA) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        print("❌ Nenhum arquivo de Coleta encontrado para diagnosticar.")
        return

    path_arquivo = os.path.join(DIR_COLETA, arquivos[0])
    df_raw = read_excel_silent(path_arquivo)

    if df_raw.is_empty() or "Nome da base" not in df_raw.columns and "Nome da base de entrega" not in df_raw.columns:
        print("❌ O arquivo de Coleta não possui uma coluna de base identificável.")
        return

    col_base = "Nome da base" if "Nome da base" in df_raw.columns else "Nome da base de entrega"
    nomes_originais = sorted([str(n) for n in df_raw[col_base].unique().to_list() if n])

    df_normalizado = df_raw.rename({col_base: "Nome da base"}).pipe(_normalize_base)
    nomes_normalizados = sorted([str(n) for n in df_normalizado["Nome da base"].unique().to_list() if n])

    print(f"📊 Arquivo analisado: {os.path.basename(path_arquivo)}")
    print(f"   - Nomes de base únicos ORIGINAIS: {len(nomes_originais)}")
    print(f"   - Nomes de base únicos NORMALIZADOS: {len(nomes_normalizados)}")
    print(f"\n🔍 Redução de {len(nomes_originais) - len(nomes_normalizados)} nomes após a normalização.")

    mapeamento = defaultdict(list)
    for nome_original in nomes_originais:
        nome_normalizado = _normalize_strong(nome_original)
        mapeamento[nome_normalizado].append(nome_original)

    print("\n📌 Exemplos de nomes que foram mesclados (se houver):")
    mesclados = {k: v for k, v in mapeamento.items() if len(v) > 1}
    if not mesclados:
        print("   ✅ Nenhum nome de base foi mesclado. A normalização parece segura.")
    else:
        count = 0
        for nome_final, lista_originais in mesclados.items():
            if count >= 10:
                print("   ... (e mais)")
                break
            print(f"   - {lista_originais}  --->  '{nome_final}'")
            count += 1

    print("==============================\n")


def _fix_key_cols(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty():
        return df
    cols = df.columns
    key_aliases = [c for c in cols if c.startswith("Nome da base")]
    if not key_aliases:
        return df
    chosen = "Nome da base" if "Nome da base" in key_aliases else (
        "Nome da base_left" if "Nome da base_left" in key_aliases else (
            "Nome da base_right" if "Nome da base_right" in key_aliases else key_aliases[0]
        )
    )
    if chosen != "Nome da base":
        df = df.rename({chosen: "Nome da base"})
    for c in key_aliases:
        if c != "Nome da base" and c in df.columns:
            df = df.drop(c)
    return df


def _safe_full_join(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    if left.is_empty() and right.is_empty():
        return pl.DataFrame()

    left = _fix_key_cols(left)
    right = _fix_key_cols(right)

    if "Nome da base" not in left.columns and "Nome da base" in right.columns:
        left, right = right, left

    if "Nome da base" not in left.columns:
        return pl.concat([left, right], how="diagonal_relaxed").unique(maintain_order=True)

    if "Nome da base" not in right.columns:
        out = left
    else:
        out = left.join(right, on="Nome da base", how="full", suffix="_dup")

    out = _fix_key_cols(out)
    dup_cols = [c for c in out.columns if c.endswith("_dup")]
    if dup_cols:
        drop = []
        for c in dup_cols:
            base = c[:-4]
            if base in out.columns:
                drop.append(c)
        if drop:
            out = out.drop(drop)

    return out.unique(subset=["Nome da base"], keep="first")


def to_float(col):
    return pl.col(col).cast(pl.Float64, strict=False).fill_null(0).fill_nan(0)


def read_excel_silent(path):
    with warnings.catch_warnings(), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
            io.StringIO()):
        warnings.simplefilter("ignore")
        try:
            df = pl.read_excel(path)
            if all("__UNNAMED__" in c or c == "Responsáveis" for c in df.columns):
                df = pl.read_excel(path, has_header=False)
                headers = [str(x) for x in df.row(0)]
                df = df.slice(1)
                df.columns = headers
            return df
        except Exception:
            return pl.DataFrame()


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


def converter_datetime(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    if coluna not in df.columns:
        return df

    formatos = [
        "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y/%m/%d %H:%M", "%d/%m/%Y %H:%M",
        "%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"
    ]

    for fmt in formatos:
        try:
            newdf = df.with_columns(pl.col(coluna).str.strptime(pl.Datetime, fmt, strict=False))
            if newdf[coluna].is_not_null().any():
                return newdf
        except Exception:
            continue

    return df


def ler_planilhas(pasta, nome_base):
    if not os.path.exists(pasta):
        print(f"\033[91m❌ Pasta '{pasta}' não encontrada.\033[0m")
        return pl.DataFrame()

    arquivos = [
        os.path.join(pasta, f)
        for f in os.listdir(pasta)
        if f.lower().endswith((".xls", ".xlsx")) and not f.startswith("~$")
    ]

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


# ==========================================================
# 📥 Funções de Leitura de Dados
# ==========================================================

def pacotes_sem_mov():
    arquivos = [f for f in os.listdir(DIR_SEMMOV) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame(), 0

    dfs = []
    for arq in tqdm(arquivos, desc="🟥 Lendo Sem Movimentação", colour="red"):
        df = read_excel_silent(os.path.join(DIR_SEMMOV, arq))
        if not df.is_empty():
            dfs.append(df)

    if not dfs:
        return pl.DataFrame(), 0

    df = pl.concat(dfs, how="diagonal_relaxed")

    rename_map = {}
    for c in df.columns:
        if "责任所属代理区" in c or c == "Regional responsável":
            rename_map[c] = "Regional responsável"
        elif "责任机构" in c or c in ("Unidade responsável", "Unidade responsável责任机构"):
            rename_map[c] = "Nome da base"
        elif "Aging" in c:
            rename_map[c] = "Aging"
        elif "JMS" in c or "运单号" in c or c == "Número de pedido JMS 运单号":
            rename_map[c] = "Remessa"

    df = df.rename(rename_map)

    obrig = ["Regional responsável", "Nome da base", "Aging", "Remessa"]
    if not all(c in df.columns for c in obrig):
        return pl.DataFrame(), 0

    df = df.filter(
        (pl.col("Regional responsável").is_in(["GP", "PA"])) &
        (pl.col("Aging").is_in([
            "Exceed 5 days with no track", "Exceed 6 days with no track",
            "Exceed 7 days with no track", "Exceed 10 days with no track",
            "Exceed 14 days with no track", "Exceed 30 days with no track"
        ]))
    )

    df = _normalize_base(df)
    df = df.group_by("Nome da base").agg(pl.count("Remessa").alias("Qtd Sem Mov"))

    qtd_planilhas = len(arquivos)
    print(f"🟥 {qtd_planilhas} planilhas lidas, total consolidado: {df['Qtd Sem Mov'].sum()} registros")
    return df, qtd_planilhas


# ==========================================================
# 🔍 VERSÃO CORRIGIDA DA FUNÇÃO PRINCIPAL
# ==========================================================
def coleta_expedicao():
    """
    VERSÃO CORRIGIDA: Lida com o novo formato de relatório de retenção.
    """
    print("\n" + "=" * 50)
    print("🔍 INICIANDO LEITURA DE COLETA + EXPEDIÇÃO (MODO CORRIGIDO)")
    print("=" * 50 + "\n")

    arquivos = [f for f in os.listdir(DIR_COLETA) if f.endswith((".xlsx", ".xls"))]
    dfs = []

    if not arquivos:
        print("❌ Nenhum arquivo .xlsx ou .xls encontrado na pasta de Coleta.")
        return pl.DataFrame()

    for arq in tqdm(arquivos, desc="🟦 Lendo Coleta + Expedição", colour="blue"):
        df = read_excel_silent(os.path.join(DIR_COLETA, arq))
        if df.is_empty():
            continue

        # Verifica se é o NOVO formato de relatório
        if "Nome da base de entrega" in df.columns and "Qtd a entregar há mais de 10 dias" in df.columns:
            print(f"   ✅ Arquivo '{arq}' com novo formato detectado.")

            # Renomear colunas para o padrão do script
            rename_map = {
                "Nome da base de entrega": "Nome da base",
                "Qtd a entregar até 10 dias": "Qtd_ate_10_dias",
                "Qtd a entregar há mais de 10 dias": "Qtd_maior_10_dias"
            }
            df = df.rename(rename_map)

            # Normalizar nome da base
            df = _normalize_base(df)

            # O "Total Geral" será a soma de todas as colunas "Qtd a entregar"
            qtd_cols = [c for c in df.columns if c.startswith("Qtd a entregar")]
            df = df.with_columns(
                pl.sum(qtd_cols).alias("Total Geral")
            )

            # A "Qtd Entregue com assinatura" não existe neste formato. Preenchemos com 0.
            df = df.with_columns(
                pl.lit(0).alias("Quantidade entregue com assinatura")
            )

            cols_sel = ["Nome da base", "Total Geral", "Quantidade entregue com assinatura", "Qtd_ate_10_dias",
                        "Qtd_maior_10_dias"]
            dfs.append(df.select(cols_sel))
        else:
            # Se não for o novo formato, verifica se é o antigo (caso tenha arquivos misturados)
            obrig_antigo = ["Nome da base", "Quantidade coletada", "Quantidade com saída para entrega",
                            "Quantidade entregue com assinatura"]
            if all(c in df.columns for c in obrig_antigo):
                print(f"   ✅ Arquivo '{arq}' com formato antigo detectado.")
                df = _normalize_base(df).with_columns([
                    to_float("Quantidade coletada"),
                    to_float("Quantidade com saída para entrega"),
                    to_float("Quantidade entregue com assinatura"),
                    (pl.col("Quantidade coletada") + pl.col("Quantidade com saída para entrega")).alias("Total Geral")
                ])
                cols_sel = ["Nome da base", "Total Geral", "Quantidade entregue com assinatura"]
                dfs.append(df.select(cols_sel))
            else:
                print(f"   ⚠️ Arquivo '{arq}' com formato não reconhecido. Ignorando.")

    if not dfs:
        print("\n❌ Nenhum arquivo válido foi processado.")
        return pl.DataFrame()

    df = pl.concat(dfs, how="diagonal_relaxed")

    aggs = [
        pl.sum("Total Geral").alias("Total Coleta+Entrega"),
        pl.sum("Quantidade entregue com assinatura").alias("Qtd Entregue Assinatura")
    ]

    if "Qtd_ate_10_dias" in df.columns:
        aggs.append(pl.sum("Qtd_ate_10_dias").alias("Qtd_ate_10_dias"))

    if "Qtd_maior_10_dias" in df.columns:
        aggs.append(pl.sum("Qtd_maior_10_dias").alias("Qtd_maior_10_dias"))

    df_agregado = df.group_by("Nome da base").agg(aggs)
    return _normalize_base(df_agregado)


def ressarcimento_por_pacote(df_coleta):
    arquivos = [f for f in os.listdir(DIR_RESS) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame()

    df = read_excel_silent(os.path.join(DIR_RESS, sorted(arquivos)[-1]))
    if df.is_empty() or "Regional responsável" not in df.columns:
        return pl.DataFrame()

    df = df.filter(pl.col("Regional responsável").str.to_uppercase() == "GP")
    df = df.with_columns(to_float("Valor a pagar (yuan)").alias("Custo total (R$)"))
    df = df.group_by("Base responsável").agg(pl.sum("Custo total (R$)").alias("Custo total (R$)"))
    df = df.rename({"Base responsável": "Nome da base"})
    df = _normalize_base(df)

    if not df_coleta.is_empty():
        df = _safe_full_join(df, df_coleta.select(["Nome da base", "Qtd Entregue Assinatura"]))

    df = df.fill_null(0).with_columns([
        (pl.when(pl.col("Qtd Entregue Assinatura") > 0)
         .then(pl.col("Custo total (R$)") / pl.col("Qtd Entregue Assinatura"))
         .otherwise(0)).alias("Ressarcimento p/pct (R$)")
    ])

    return df.select(["Nome da base", "Custo total (R$)", "Ressarcimento p/pct (R$)"])


def taxa_t0():
    """
    Taxa T-0 baseada no motor v2.8.
    """
    arquivos = [
        os.path.join(DIR_T0, f)
        for f in os.listdir(DIR_T0)
        if f.lower().endswith((".xls", ".xlsx", ".csv")) and not f.startswith("~$")
    ]

    if not arquivos:
        print("⚠️ Nenhum arquivo T0 encontrado.")
        return pl.DataFrame({"Nome da base": [], "SLA (%)": []})

    with ThreadPoolExecutor(max_workers=min(16, len(arquivos))) as ex:
        dfs = list(ex.map(read_excel_silent, arquivos))

    dfs = [df for df in dfs if not df.is_empty()]
    if not dfs:
        print("⚠️ Falha ao ler arquivos T0.")
        return pl.DataFrame({"Nome da base": [], "SLA (%)": []})

    df = pl.concat(dfs, how="vertical_relaxed")
    df = df.rename({c: c.strip().upper() for c in df.columns})

    possiveis_base = ["BASE DE ENTREGA", "NOME DA BASE", "BASE", "UNIDADE", "UNIDADE RESPONSÁVEL"]
    col_base = next((c for c in df.columns if c.upper() in [p.upper() for p in possiveis_base]), None)
    if not col_base:
        raise KeyError(f"❌ Nenhuma coluna equivalente a Base encontrada.\nColunas: {df.columns}")

    possiveis_prazo = ["ENTREGUE NO PRAZO?", "ENTREGUE NO PRAZO？", "ENTREGUE NO PRAZO"]
    col_prazo = next((c for c in df.columns if c.upper() in [p.upper() for p in possiveis_prazo]), None)
    if not col_prazo:
        raise KeyError(f"❌ Nenhuma coluna ENTREGUE NO PRAZO encontrada.\nColunas: {df.columns}")

    df = df.with_columns(
        pl.when(pl.col(col_prazo).cast(pl.Utf8).str.to_uppercase() == "Y")
        .then(1).otherwise(0).alias("_ENTREGUE_PRAZO")
    )

    df = df.with_columns(
        pl.col(col_base).map_elements(_normalize_strong).alias("Nome da base")
    )

    return (
        df.group_by("Nome da base")
        .agg((pl.col("_ENTREGUE_PRAZO").sum() / pl.len()).alias("SLA (%)"))
        .select(["Nome da base", "SLA (%)"])
    )


# ==========================================================
# 🚀 RETIDOS (MOTOR ANTIGO) + % REAL COM QTD >10 DIAS
# ==========================================================

def analisar_retidos_motor_antigo():
    """
    Motor antigo:
    - filtra >6 dias
    - remove devolução, problemáticos, custódia
    - retorna Qtd Retidos por base
    """
    print(
        "\n==============================\n🚀 INICIANDO ANÁLISE DE RETIDOS (MOTOR ANTIGO)\n==============================")

    removidos_cluster = removidos_dev = removidos_prob = removidos_cust = 0

    df_ret = ler_planilhas(DIR_RETIDOS, "Retidos")
    if df_ret.is_empty():
        print("❌ Nenhum dado em Retidos.")
        return pl.DataFrame({"Nome da base": [], "Qtd Retidos": []})

    # 1) CLUSTER > 6 DIAS
    col_cluster = safe_pick(df_ret, "Dias Retidos 滞留日", ["滞留", "dias", "retidos"])
    if col_cluster and col_cluster in df_ret.columns:
        total_antes = df_ret.height
        df_ret = df_ret.with_columns(pl.col(col_cluster).cast(pl.Utf8, strict=False))

        def extrair_maior_dia(texto: str) -> int:
            if texto is None:
                return 999
            nums = re.findall(r"\d+", str(texto))
            return max(int(n) for n in nums) if nums else 999

        df_ret = df_ret.with_columns(
            pl.col(col_cluster).map_elements(extrair_maior_dia, return_dtype=pl.Int64).alias("dias_max")
        )
        df_ret = df_ret.filter(pl.col("dias_max") > 6).drop("dias_max")
        removidos_cluster = total_antes - df_ret.height
        print(f"\033[95m🧹 Removidos (0–6 dias): {removidos_cluster} | Mantidos: {df_ret.height}\033[0m")

    # 2) COLUNAS PRINCIPAIS
    col_pedido_ret = safe_pick(df_ret, "Número do Pedido JMS 运单号", ["pedido", "运单", "jms"])
    col_data_ret = safe_pick(df_ret, "Data da Atualização 更新日期", ["data", "atualiza", "更新"])
    col_regional = safe_pick(df_ret, "Regional 区域", ["regional", "区域"])
    col_base_entrega = safe_pick(df_ret, "Base de Entrega 派件网点", ["base", "网点", "派件"])

    cols = [c for c in [col_pedido_ret, col_data_ret, col_regional, col_base_entrega] if c]
    df_ret = df_ret.select(cols).rename({
        col_pedido_ret: "PEDIDO",
        col_data_ret: "DATA_ATUALIZACAO",
        col_regional: "REGIONAL" if col_regional else None,
        col_base_entrega: "BASE_ENTREGA" if col_base_entrega else None
    })

    df_ret = limpar_pedidos(df_ret, "PEDIDO")
    df_ret = converter_datetime(df_ret, "DATA_ATUALIZACAO")

    if "REGIONAL" in df_ret.columns:
        df_ret = df_ret.filter(pl.col("REGIONAL").is_in(REGIONAIS_DESEJADAS))

    total_inicial = df_ret.height
    print(f"\033[92m🟢 Retidos filtrados ({', '.join(REGIONAIS_DESEJADAS)}): {total_inicial}\033[0m")

    # 3) DEVOLUÇÃO
    df_dev = ler_planilhas(DIR_DEVOLUCAO, "Devolução")
    if not df_dev.is_empty():
        col_pedido_dev = safe_pick(df_dev, "Número de pedido JMS", ["pedido", "jms"])
        col_data_dev = safe_pick(df_dev, "Tempo de solicitação", ["solicit", "tempo", "data"])

        if col_pedido_dev and col_data_dev:
            df_dev = (
                df_dev.select([col_pedido_dev, col_data_dev])
                .rename({col_pedido_dev: "PEDIDO_DEV", col_data_dev: "DATA_DEV"})
                .pipe(limpar_pedidos, "PEDIDO_DEV")
                .pipe(converter_datetime, "DATA_DEV")
                .group_by("PEDIDO_DEV")
                .agg(pl.col("DATA_DEV").min())
            )

            dfj = df_ret.join(df_dev, left_on="PEDIDO", right_on="PEDIDO_DEV", how="left")

            df_rem = dfj.filter(
                (pl.col("DATA_DEV") > pl.col("DATA_ATUALIZACAO")) &
                pl.col("DATA_DEV").is_not_null()
            )

            removidos_dev = df_rem.height
            df_ret = dfj.filter(~pl.col("PEDIDO").is_in(df_rem["PEDIDO"])).drop(["PEDIDO_DEV", "DATA_DEV"],
                                                                                strict=False)
            print(f"\033[93m🟡 Devolução → Removidos: {removidos_dev} | Mantidos: {df_ret.height}\033[0m")

    # 4) PROBLEMÁTICOS
    df_prob = ler_planilhas(DIR_PROBLEMATICOS, "Problemáticos")
    if not df_prob.is_empty():
        col_pedido_prob = safe_pick(df_prob, "Número de pedido JMS", ["pedido", "jms", "运单"])
        col_data_prob = safe_pick(df_prob, "data de registro", ["registro", "data", "异常"])

        if col_pedido_prob and col_data_prob:
            df_prob = (
                df_prob.select([col_pedido_prob, col_data_prob])
                .rename({col_pedido_prob: "PEDIDO_PROB", col_data_prob: "DATA_PROB"})
                .pipe(limpar_pedidos, "PEDIDO_PROB")
                .pipe(converter_datetime, "DATA_PROB")
                .group_by("PEDIDO_PROB")
                .agg(pl.col("DATA_PROB").min())
            )

            dfj = df_ret.join(df_prob, left_on="PEDIDO", right_on="PEDIDO_PROB", how="left")

            df_rem = dfj.filter(
                (pl.col("DATA_PROB") >= pl.col("DATA_ATUALIZACAO")) &
                pl.col("DATA_PROB").is_not_null()
            )

            removidos_prob = df_rem.height
            df_ret = dfj.filter(~pl.col("PEDIDO").is_in(df_rem["PEDIDO"])).drop(["PEDIDO_PROB", "DATA_PROB"],
                                                                                strict=False)
            print(f"\033[38;5;208m🟠 Problemáticos → Removidos: {removidos_prob} | Mantidos: {df_ret.height}\033[0m")
    # 5) CUSTÓDIA
    df_cust = ler_planilhas(DIR_CUSTODIA, "Custódia")
    if not df_cust.is_empty():
        col_pedido_c = safe_pick(df_cust, "Número de pedido JMS", ["pedido", "jms"])
        col_data_c = safe_pick(df_cust, "data de registro", ["registro", "data"])

        if col_pedido_c and col_data_c:
            df_cust = (
                df_cust.select([col_pedido_c, col_data_c])
                .rename({col_pedido_c: "PEDIDO_CUST", col_data_c: "DATA_CUST"})
                .pipe(limpar_pedidos, "PEDIDO_CUST")
                .pipe(converter_datetime, "DATA_CUST")
                .group_by("PEDIDO_CUST")
                .agg(pl.col("DATA_CUST").min().alias("DATA_CUST"))
                .with_columns((pl.col("DATA_CUST") + pl.duration(days=PRAZO_CUSTODIA_DIAS)).alias("PRAZO_LIMITE"))
            )

            dfj = df_ret.join(df_cust, left_on="PEDIDO", right_on="PEDIDO_CUST", how="left")

            dfj = dfj.with_columns(
                pl.when(
                    (pl.col("DATA_ATUALIZACAO") <= pl.col("PRAZO_LIMITE")) &
                    pl.col("PRAZO_LIMITE").is_not_null()
                ).then(pl.lit("Dentro")).otherwise(pl.lit("Fora")).alias("STATUS_CUSTODIA")
            )

            df_rem = dfj.filter(pl.col("STATUS_CUSTODIA") == "Dentro")
            removidos_cust = df_rem.height

            df_ret = dfj.filter(pl.col("STATUS_CUSTODIA") == "Fora").drop(
                ["PEDIDO_CUST", "DATA_CUST", "PRAZO_LIMITE", "STATUS_CUSTODIA"],
                strict=False
            )

            print(f"\033[94m🔵 Custódia → Removidos: {removidos_cust} | Mantidos: {df_ret.height}\033[0m")

    # 6) AGREGAR POR BASE
    if "BASE_ENTREGA" in df_ret.columns and df_ret.height > 0:
        df_retidos_base = (
            df_ret.with_columns(
                pl.col("BASE_ENTREGA").map_elements(_normalize_strong).alias("Nome da base")
            )
            .group_by("Nome da base")
            .agg(pl.count().alias("Qtd Retidos"))
        )
    else:
        df_retidos_base = pl.DataFrame({"Nome da base": [], "Qtd Retidos": []})

    # Detalhado opcional
    if GERAR_DETALHADO_RETIDOS and df_ret.height > 0:
        out_final = os.path.join(DIR_OUT, f"Resultado_Detalhado_Retidos_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
        df_ret.write_excel(out_final)
        print(f"\n📊 Resultado detalhado exportado: {out_final}")

    return df_retidos_base


def calcular_retidos_reais(df_retidos_motor: pl.DataFrame, df_coleta: pl.DataFrame) -> pl.DataFrame:
    """
    % Retidos Real = Qtd Retidos (motor antigo) / Qtd_maior_10_dias (coleta+expedição)
    CORREÇÃO: O denominador agora é a quantidade de pacotes com mais de 10 dias.

    Retorna:
        Nome da base | Qtd Retidos | Qtd_maior_10_dias | % Retidos Real
    """
    if df_retidos_motor.is_empty():
        return pl.DataFrame({"Nome da base": [], "Qtd Retidos": [], "Qtd_maior_10_dias": [], "% Retidos Real": []})

    df_retidos_motor = df_retidos_motor.with_columns(
        pl.col("Nome da base").map_elements(_normalize_strong, return_dtype=pl.Utf8)
    )

    # CORREÇÃO: Usar Qtd_maior_10_dias como denominador
    if df_coleta.is_empty() or "Qtd_maior_10_dias" not in df_coleta.columns:
        return df_retidos_motor.with_columns(
            pl.lit(0).alias("Qtd_maior_10_dias"),
            pl.lit(0).alias("% Retidos Real")
        )

    df_coleta_maior_10 = df_coleta.select(["Nome da base", "Qtd_maior_10_dias"]).with_columns(
        pl.col("Nome da base").map_elements(_normalize_strong, return_dtype=pl.Utf8)
    )

    df = df_retidos_motor.join(df_coleta_maior_10, on="Nome da base", how="left").fill_null(0)

    df = df.with_columns(
        pl.when(pl.col("Qtd_maior_10_dias") > 0)
        .then((pl.col("Qtd Retidos") / pl.col("Qtd_maior_10_dias")).round(4))
        .otherwise(0)
        .alias("% Retidos Real")
    )

    return df


# ==========================================================
# 📘 COORDENADORES — pasta nova, só Base + Coordenador
# ==========================================================

def carregar_coordenadores():
    """
    Lê Base_Dados_Geral.xlsx dentro de DIR_COORDENADOR.
    Mantém somente Nome da base + Coordenador.
    """
    path_coord = os.path.join(DIR_COORDENADOR, "Base_Dados_Geral.xlsx")
    df_coord = read_excel_silent(path_coord)

    if df_coord.is_empty():
        print("⚠️ Planilha Base_Dados_Geral.xlsx não encontrada ou vazia.")
        return pl.DataFrame({"Nome da base": [], "Coordenador": []})

    col_base = None
    for possible in ["Base", "Nome da base", "Unidade", "Unidade responsável"]:
        if possible in df_coord.columns:
            col_base = possible
            break

    if col_base is None:
        raise SystemExit("❌ Nenhuma coluna identificada como 'Base' ou equivalente em Base_Dados_Geral.xlsx")

    df_coord = df_coord.rename({
        col_base: "Nome da base",
        "Coordenador": "Coordenador"
    })

    df_coord = df_coord.select([c for c in ["Nome da base", "Coordenador"] if c in df_coord.columns])
    df_coord = _normalize_base(df_coord)

    print(f"✅ {df_coord.height} coordenadores carregados e normalizados.")
    return df_coord


# ==========================================================
# 🧮 Consolidação de Dados
# ==========================================================

def consolidar():
    dias = calendar.monthrange(datetime.now().year, datetime.now().month)[1]

    df_coord = carregar_coordenadores()

    df_coleta = coleta_expedicao()
    df_t0 = taxa_t0()

    # RETIDOS: motor antigo + % real usando Qtd_maior_10_dias da coleta
    df_retidos_motor = analisar_retidos_motor_antigo()
    df_retidos = calcular_retidos_reais(df_retidos_motor, df_coleta)

    df_ress = ressarcimento_por_pacote(df_coleta)
    df_sem, _ = pacotes_sem_mov()

    df_final = _safe_full_join(df_t0, df_retidos)
    df_final = _safe_full_join(df_final, df_ress)
    df_final = _safe_full_join(df_final, df_sem)
    df_final = _safe_full_join(df_final, df_coleta)
    df_final = _safe_full_join(df_coord, df_final)

    # Taxa Sem Mov
    if "Total Coleta+Entrega" in df_final.columns:
        df_final = df_final.fill_null(0).with_columns([
            (pl.when(pl.col("Total Coleta+Entrega") > 0)
             .then(pl.col("Qtd Sem Mov") / dias / pl.col("Total Coleta+Entrega"))
             .otherwise(0)).alias("Taxa Sem Mov.")
        ])
    else:
        df_final = df_final.with_columns(pl.lit(0).alias("Taxa Sem Mov."))

    # ORDEM CORRIGIDA: Adiciona as novas colunas
    ordered = [
        "Nome da base", "Coordenador",
        "SLA (%)",
        "Qtd Retidos", "Qtd_maior_10_dias", "% Retidos Real",
        "Ressarcimento p/pct (R$)", "Custo total (R$)",
        "Qtd Sem Mov", "Taxa Sem Mov.",
        "Total Coleta+Entrega", "Qtd_ate_10_dias"  # Coluna extra no final
    ]

    # garantir que todas colunas existam
    for c in ordered:
        if c not in df_final.columns:
            df_final = df_final.with_columns(pl.lit(None).alias(c))

    # Ajuste de tipos
    text_cols = [c for c in df_final.columns if df_final.schema[c] == pl.Utf8]
    numeric_cols = [c for c in df_final.columns if df_final.schema[c] != pl.Utf8]

    if text_cols:
        df_final = df_final.with_columns([pl.col(c).fill_null("") for c in text_cols])

    if numeric_cols:
        for c in numeric_cols:
            df_final = df_final.with_columns(pl.col(c).fill_null(0).fill_nan(0))

    return df_final.select(ordered).unique(subset=["Nome da base"], keep="first")


# ==========================================================
# 💾 Exportar Relatório Formatado
# ==========================================================

def main():
    diagnosticar_normalizacao()

    df = consolidar()
    if df.is_empty():
        print("⚠️ Nenhum dado consolidado.")
        return

    out = os.path.join(DIR_OUT, f"Resumo_Politica_Bonificacao_{datetime.now():%Y%m%d_%H%M%S}.xlsx")

    df_pd = df.to_pandas()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        startrow = 6
        df_pd.to_excel(writer, sheet_name="Bonificação", startrow=startrow, startcol=0, header=True, index=False)

        wb, ws = writer.book, writer.sheets["Bonificação"]

        red = wb.add_format({
            "bold": True, "font_color": "white", "align": "center",
            "valign": "vcenter", "bg_color": "#C00000", "border": 1
        })
        gray = wb.add_format({
            "bold": True, "font_color": "white", "align": "center",
            "valign": "vcenter", "bg_color": "#595959", "border": 1
        })
        center = wb.add_format({"align": "center", "valign": "vcenter"})
        fmt_percent_2 = wb.add_format({"num_format": "0.00%", "align": "center"})
        fmt_money = wb.add_format({"num_format": '"R$"#,##0.00', "align": "center"})
        fmt_int = wb.add_format({"num_format": "0", "align": "center"})

        # Ajuste para 13 colunas
        ws.merge_range("A1:M1", "RESULTADOS DE INDICADORES — POLÍTICA DE BONIFICAÇÃO", red)
        ws.merge_range("A2:M2", f"Data de atualização: {datetime.now():%d/%m/%Y}", gray)

        # Cabeçalhos corrigidos e reordenados
        headers = [
            ("A6", "Nome da base"),
            ("B6", "Coordenador"),
            ("C6", "SLA (%)"),
            ("D6", "Qtd Retidos"),
            ("E6", "Qtd >10 dias (Denominador)"),
            ("F6", "% Retidos Real"),
            ("G6", "Ressarcimento p/pct (R$)"),
            ("H6", "Custo total (R$)"),
            ("I6", "Qtd Sem Mov"),
            ("J6", "Taxa Sem Mov."),
            ("K6", "Total Coleta+Entrega"),
            ("L6", "Qtd até 10 dias")
        ]

        for c, t in headers:
            ws.write(c, t, red)

        # Formatação das colunas
        ws.set_column("A:B", 22, center)
        ws.set_column("C:C", 12, fmt_percent_2)
        ws.set_column("D:D", 14, fmt_int)
        ws.set_column("E:E", 18, fmt_int)
        ws.set_column("F:F", 12, fmt_percent_2)
        ws.set_column("G:H", 16, fmt_money)
        ws.set_column("I:I", 14, fmt_int)
        ws.set_column("J:J", 14, fmt_percent_2)
        ws.set_column("K:K", 16, fmt_int)
        ws.set_column("L:L", 16, fmt_int)

        ws.freeze_panes(7, 0)

    print(f"✅ Relatório final gerado com sucesso!\n📂 {out}")


# ==========================================================
# Execução do Script
# ==========================================================

if __name__ == "__main__":
    main()