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

# ==========================================================
# 📂 Caminhos
# ==========================================================
BASE_ROOT = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Politicas de Bonificação"

DIR_COLETA = os.path.join(BASE_ROOT, "00 -  Base de Dados (Coleta + Expedição)")
DIR_T0 = os.path.join(BASE_ROOT, "01 - Taxa de entrega T0")
DIR_RESS = os.path.join(BASE_ROOT, "02 - Ressarcimento por pacote")
# DIR_SHIP e DIR_ANTIGA não serão mais usados, mas mantidos para referência
DIR_SHIP = os.path.join(BASE_ROOT, "03 - Redução Shipping Time")
DIR_ANTIGA = os.path.join(BASE_ROOT, "Base Antiga")
DIR_SEMMOV = os.path.join(BASE_ROOT, "05 - Pacotes Sem Movimentação")
# Novos diretórios para análise de retidos
DIR_RETIDOS = os.path.join(BASE_ROOT, "06 - Retidos")
DIR_DEVOLUCAO = os.path.join(BASE_ROOT, "00.3 - Base Devolução")
DIR_PROBLEMATICOS = os.path.join(BASE_ROOT, "00.2 - Base de Problematicos (Gestão de Anormalidade)")
DIR_CUSTODIA = os.path.join(BASE_ROOT, "00.4 - Base Custodia")
DIR_BASE_LISTA = os.path.join(BASE_ROOT, "00.1 - Base Retidos(Lista)")
DIR_OUT = os.path.join(BASE_ROOT, "Resultados")
os.makedirs(DIR_OUT, exist_ok=True)

# Configurações para análise de retidos
REGIONAIS_DESEJADAS = ["GP", "PA", "GO"]
PRAZO_CUSTODIA_DIAS = 9
EXCEL_ROW_LIMIT = 1_048_000


# ==========================================================
# ⚙️ Utilitários
# ==========================================================

def _normalize_base(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normaliza o nome das bases, removendo caracteres especiais e padronizando o formato.
    """
    df = _fix_key_cols(df)

    if "Nome da base" not in df.columns or df.is_empty():
        return df

    def limpar_nome(nome: str) -> str:
        if not nome:
            return ""
        nome = str(nome).upper()
        nome = re.sub(r"[^\x00-\x7F]+", "", nome)  # remove caracteres não ASCII
        nome = re.sub(r"[-_]+", " ", nome)  # troca hífens e underscores por espaço
        nome = re.sub(r"\s+", " ", nome).strip()  # remove espaços duplicados
        partes = nome.split(" ")

        # Detecta inversão comum (ex: 'BSB DF' → 'DF BSB')
        if len(partes) == 2 and len(partes[0]) == 3 and len(partes[1]) == 2:
            nome = f"{partes[1]} {partes[0]}"

        return nome

    df = df.with_columns(
        pl.col("Nome da base")
        .cast(pl.Utf8, strict=False)
        .map_elements(limpar_nome, return_dtype=pl.Utf8)
        .alias("Nome da base")
    )

    return df


def _fix_key_cols(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normaliza qualquer variante da chave para 'Nome da base' e remove duplicatas da chave.
    """
    if df.is_empty():
        return df
    cols = df.columns
    # candidatos que aparecem pós-join
    key_aliases = [c for c in cols if c.startswith("Nome da base")]
    if not key_aliases:
        return df
    # escolhe prioridade: exata > _left > _right > primeira
    chosen = "Nome da base" if "Nome da base" in key_aliases else (
        "Nome da base_left" if "Nome da base_left" in key_aliases else (
            "Nome da base_right" if "Nome da base_right" in key_aliases else key_aliases[0]
        )
    )
    if chosen != "Nome da base":
        df = df.rename({chosen: "Nome da base"})
    # drop demais variantes da chave
    for c in key_aliases:
        if c != "Nome da base" and c in df.columns:
            df = df.drop(c)
    return df


def _safe_full_join(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    """
    Join 'full' robusto: normaliza chaves antes/depois e evita duplicações.
    """
    if left.is_empty() and right.is_empty():
        return pl.DataFrame()
    left = _fix_key_cols(left)
    right = _fix_key_cols(right)
    if "Nome da base" not in left.columns and "Nome da base" in right.columns:
        # se o left não tem a chave mas o right tem, inverte para manter a chave
        left, right = right, left
    if "Nome da base" not in left.columns:
        # sem chave nos dois -> retorna concat (fallback)
        return pl.concat([left, right], how="diagonal_relaxed").unique(maintain_order=True)

    if "Nome da base" not in right.columns:
        # right sem chave: retorna left como está
        out = left
    else:
        out = left.join(right, on="Nome da base", how="full", suffix="_dup")
    # normaliza pós-join
    out = _fix_key_cols(out)
    # remove colunas duplicadas com sufixo "_dup" geradas por overlaps não-chave
    dup_cols = [c for c in out.columns if c.endswith("_dup")]
    if dup_cols:
        # regra simples: se já existe a versão "sem _dup", mantemos a sem _dup
        keep = []
        drop = []
        for c in dup_cols:
            base = c[:-4]
            if base in out.columns:
                drop.append(c)
            else:
                keep.append(c)  # só mantém se não existe base
        if drop:
            out = out.drop(drop)
    # dedup por chave
    out = out.unique(subset=["Nome da base"], keep="first")
    return out


def to_float(col):
    """
    Converte uma coluna para float, tratando valores nulos e NaN.
    """
    return pl.col(col).cast(pl.Float64, strict=False).fill_null(0).fill_nan(0)


def read_excel_silent(path):
    """
    Lê um arquivo Excel de forma silenciosa, sem exibir warnings ou erros.
    """
    with warnings.catch_warnings(), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
            io.StringIO()):
        warnings.simplefilter("ignore")
        try:
            # Tenta ler normalmente
            df = pl.read_excel(path)
            # Se só tiver colunas UNNAMED, tenta ler a partir da segunda linha
            if all("__UNNAMED__" in c or c == "Responsáveis" for c in df.columns):
                df = pl.read_excel(path, has_header=False)
                # Primeira linha (índice 0) vira cabeçalho
                headers = [str(x) for x in df.row(0)]
                df = df.slice(1)
                df.columns = headers
            return df
        except Exception:
            return pl.DataFrame()


# Funções auxiliares para análise de retidos
def converter_datetime(df: pl.DataFrame, coluna: str) -> pl.DataFrame:
    if coluna not in df.columns:
        return df
    try:
        df = df.with_columns(pl.col(coluna).str.to_datetime(strict=False))
    except Exception:
        for fmt in ["%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
                    "%Y/%m/%d %H:%M", "%d/%m/%Y", "%Y-%m-%d"]:
            try:
                df = df.with_columns(pl.col(coluna).str.strptime(pl.Datetime, fmt, strict=False))
                break
            except Exception:
                continue
    return df.filter(pl.col(coluna).is_not_null())


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


def ler_planilhas(pasta, nome_base):
    if not os.path.exists(pasta):
        print(f"\033[91m❌ Pasta '{pasta}' não encontrada.\033[0m")
        return pl.DataFrame()
    arquivos = [os.path.join(pasta, f) for f in os.listdir(pasta)
                if f.lower().endswith((".xls", ".xlsx")) and not f.startswith("~$")]
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
    """
    Lê e processa os dados de pacotes sem movimentação.
    """
    arquivos = [f for f in os.listdir(DIR_SEMMOV) if f.endswith((".xlsx", ".xls"))]
    if not arquivos:
        return pl.DataFrame(), 0  # <- retorna 0 planilhas

    dfs = []
    for arq in tqdm(arquivos, desc="🟥 Lendo Sem Movimentação", colour="red"):
        df = read_excel_silent(os.path.join(DIR_SEMMOV, arq))
        if not df.is_empty():
            dfs.append(df)

    if not dfs:
        return pl.DataFrame(), 0

    df = pl.concat(dfs, how="diagonal_relaxed")

    # renomeia colunas PT/中文 → padrão
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
            "Exceed 5 days with no track",
            "Exceed 6 days with no track",
            "Exceed 7 days with no track",
            "Exceed 10 days with no track",
            "Exceed 14 days with no track",
            "Exceed 30 days with no track"
        ]))
    )
    df = _normalize_base(df)

    df = df.group_by("Nome da base").agg(pl.count("Remessa").alias("Qtd Sem Mov"))
    qtd_planilhas = len(arquivos)

    print(f"🟥 {qtd_planilhas} planilhas lidas, total consolidado: {df['Qtd Sem Mov'].sum()} registros")
    return df, qtd_planilhas


def coleta_expedicao():
    """
    Lê e processa os dados de coleta e expedição.
    """
    arquivos = [f for f in os.listdir(DIR_COLETA) if f.endswith((".xlsx", ".xls"))]
    dfs = []
    for arq in tqdm(arquivos, desc="🟦 Lendo Coleta + Expedição", colour="blue"):
        df = read_excel_silent(os.path.join(DIR_COLETA, arq))
        if all(c in df.columns for c in [
            "Nome da base",
            "Quantidade coletada",
            "Quantidade com saída para entrega",
            "Quantidade entregue com assinatura"
        ]):
            df = _normalize_base(df).with_columns([
                to_float("Quantidade coletada"),
                to_float("Quantidade com saída para entrega"),
                to_float("Quantidade entregue com assinatura"),
                (pl.col("Quantidade coletada") + pl.col("Quantidade com saída para entrega")).alias("Total Geral")
            ])
            dfs.append(df.select(["Nome da base", "Total Geral", "Quantidade entregue com assinatura"]))
    if not dfs:
        raise SystemExit("⚠️ Nenhum arquivo encontrado em Coleta + Expedição.")
    df = pl.concat(dfs, how="diagonal_relaxed")
    return (
        df.group_by("Nome da base")
        .agg([
            pl.sum("Total Geral").alias("Total Coleta+Entrega"),
            pl.sum("Quantidade entregue com assinatura").alias("Qtd Entregue Assinatura")
        ])
    )


def taxa_t0():
    """
    Lê e processa os dados de taxa T0 (SLA).
    """
    arquivos = [f for f in os.listdir(DIR_T0) if f.endswith((".xlsx", ".xls"))]
    dfs = []
    for arq in tqdm(arquivos, desc="🟨 Lendo T0", colour="yellow"):
        df = read_excel_silent(os.path.join(DIR_T0, arq))
        if all(c in df.columns for c in ["Nome da base", "T日签收率-应签收量", "T日签收率-已签收量"]):
            df = _normalize_base(
                df.rename({
                    "T日签收率-应签收量": "Total Recebido",
                    "T日签收率-已签收量": "Entregue"
                }).with_columns([
                    to_float("Total Recebido"),
                    to_float("Entregue")
                ])
            )
            dfs.append(df)
    if not dfs:
        return pl.DataFrame()
    df_total = pl.concat(dfs, how="diagonal_relaxed")
    return (
        df_total.group_by("Nome da base")
        .agg([
            pl.sum("Total Recebido").alias("Total Recebido"),
            pl.sum("Entregue").alias("Entregue")
        ])
        .with_columns(
            (pl.when(pl.col("Total Recebido") > 0)
             .then(pl.col("Entregue") / pl.col("Total Recebido"))
             .otherwise(0)).alias("SLA (%)")
        )
        .select(["Nome da base", "SLA (%)"])
    )


def ressarcimento_por_pacote(df_coleta):
    """
    Lê e processa os dados de ressarcimento por pacote.
    """
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
        df = _safe_full_join(
            df,
            df_coleta.select(["Nome da base", "Qtd Entregue Assinatura"])
        )

    df = df.fill_null(0).with_columns([
        (pl.when(pl.col("Qtd Entregue Assinatura") > 0)
         .then(pl.col("Custo total (R$)") / pl.col("Qtd Entregue Assinatura"))
         .otherwise(pl.col("Custo total (R$)"))).alias("Ressarcimento p/pct (R$)")
    ])

    # ✅ Corrigido: Custo total primeiro, depois Ressarcimento
    return df.select(["Nome da base", "Custo total (R$)", "Ressarcimento p/pct (R$)"])


# ==========================================================
# 🚀 ANÁLISE DE RETIDOS (substituindo Shipping Time)
# ==========================================================
def analisar_retidos():
    """
    Realiza a análise completa de retidos, substituindo o cálculo de Shipping Time.
    """
    print("\n==============================\n🚀 INICIANDO ANÁLISE DE RETIDOS\n==============================")

    removidos_dev = removidos_cust = removidos_cluster = removidos_prob = 0

    # RETIDOS
    df_ret = ler_planilhas(DIR_RETIDOS, "Retidos")
    if df_ret.is_empty():
        print("❌ Nenhum dado em Retidos.")
        return pl.DataFrame()

    # 🔹 Remover Retidos até 6 dias — versão AUTOMÁTICA
    col_cluster = safe_pick(df_ret, "Dias Retidos 滞留日", ["dias", "滞留", "retidos"])
    if col_cluster and col_cluster in df_ret.columns:
        total_antes = df_ret.height

        df_ret = df_ret.with_columns(
            pl.col(col_cluster)
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_lowercase()
            .alias(col_cluster)
        )

        # Função Python que extrai O MAIOR número da faixa
        def extrair_maior_dia(texto: str) -> int:
            import re
            if not texto:
                return 999  # segurança: mantém
            nums = re.findall(r"\d+", texto)
            if not nums:
                return 999
            return max(int(n) for n in nums)

        # Cria coluna auxiliar com o maior dia da faixa
        df_ret = df_ret.with_columns(
            pl.col(col_cluster)
            .map_elements(extrair_maior_dia, return_dtype=pl.Int64)
            .alias("dias_max")
        )

        # Remove todos que são <= 6 dias
        df_ret = df_ret.filter(pl.col("dias_max") > 6).drop("dias_max")

        removidos_cluster = total_antes - df_ret.height
        print(f"\033[95m🧹 Removidos (0–6 dias): {removidos_cluster} | Mantidos: {df_ret.height}\033[0m")

    # Selecionar colunas relevantes
    col_pedido_ret = safe_pick(df_ret, "Número do Pedido JMS 运单号", ["pedido", "运单", "jms"])
    col_data_ret = safe_pick(df_ret, "Data da Atualização 更新日期", ["data", "atualiza", "更新"])
    col_regional = safe_pick(df_ret, "Regional 区域", ["regional", "区域"])
    col_base_entrega = safe_pick(df_ret, "Base de Entrega 派件网点", ["base", "网点", "派件"])

    cols = [c for c in [col_pedido_ret, col_data_ret, col_regional, col_base_entrega] if c]
    df_ret = df_ret.select(cols).rename({
        col_pedido_ret: "Número do Pedido JMS 运单号",
        col_data_ret: "Data da Atualização 更新日期",
        col_regional: "Regional 区域" if col_regional else None,
        col_base_entrega: "Base de Entrega 派件网点" if col_base_entrega else None
    })

    df_ret = limpar_pedidos(df_ret, "Número do Pedido JMS 运单号")
    df_ret = converter_datetime(df_ret, "Data da Atualização 更新日期")

    if "Regional 区域" in df_ret.columns:
        df_ret = df_ret.filter(pl.col("Regional 区域").is_in(REGIONAIS_DESEJADAS))

    total_inicial = df_ret.height
    print(f"\033[92m🟢 Retidos filtrados ({', '.join(REGIONAIS_DESEJADAS)}): {total_inicial}\033[0m")

    # 🟡 DEVOLUÇÃO
    df_dev = ler_planilhas(DIR_DEVOLUCAO, "Devolução")
    if not df_dev.is_empty():
        col_pedido_dev = safe_pick(df_dev, "Número de pedido JMS", ["pedido", "jms"])
        col_data_dev = safe_pick(df_dev, "Tempo de solicitação", ["solicit", "data"])
        if col_pedido_dev and col_data_dev:
            df_dev = df_dev.select([col_pedido_dev, col_data_dev]).rename(
                {col_pedido_dev: "Número de pedido JMS", col_data_dev: "Tempo de solicitação"})
            df_dev = limpar_pedidos(df_dev, "Número de pedido JMS")
            df_dev = converter_datetime(df_dev, "Tempo de solicitação")
            df_dev = df_dev.group_by("Número de pedido JMS").agg(pl.col("Tempo de solicitação").min())
            df_merge = df_ret.join(df_dev, left_on="Número do Pedido JMS 运单号", right_on="Número de pedido JMS",
                                   how="left")
            df_merge = df_merge.with_columns(
                ((pl.col("Tempo de solicitação") > pl.col("Data da Atualização 更新日期"))
                 & pl.col("Tempo de solicitação").is_not_null()).alias("Remover_Dev"))
            removidos_dev = df_merge.filter(pl.col("Remover_Dev")).height
            df_ret = df_merge.filter(~pl.col("Remover_Dev")).drop(
                ["Remover_Dev", "Número de pedido JMS", "Tempo de solicitação"], strict=False)
            print(f"\033[93m🟡 Devolução → Removidos: {removidos_dev} | Mantidos: {df_ret.height}\033[0m")

    # 🟠 PROBLEMÁTICOS
    df_prob = ler_planilhas(DIR_PROBLEMATICOS, "Problemáticos")
    if not df_prob.is_empty():
        col_pedido_prob = safe_pick(df_prob, "Número de pedido JMS", ["pedido", "jms"])
        col_data_prob = safe_pick(df_prob, "data de registro", ["data", "registro", "anormal"])
        if col_pedido_prob and col_data_prob:
            df_prob = df_prob.select([col_pedido_prob, col_data_prob]).rename({
                col_pedido_prob: "Número de pedido JMS",
                col_data_prob: "data de registro"
            })
            df_prob = limpar_pedidos(df_prob, "Número de pedido JMS")
            df_prob = converter_datetime(df_prob, "data de registro")
            df_prob = df_prob.group_by("Número de pedido JMS").agg(pl.col("data de registro").min())

            df_merge_prob = df_ret.join(df_prob, left_on="Número do Pedido JMS 运单号", right_on="Número de pedido JMS",
                                        how="left")
            df_merge_prob = df_merge_prob.with_columns(
                ((pl.col("data de registro") >= pl.col("Data da Atualização 更新日期")) &
                 pl.col("data de registro").is_not_null()).alias("Remover_Prob")
            )

            removidos_prob = df_merge_prob.filter(pl.col("Remover_Prob")).height
            df_ret = df_merge_prob.filter(~pl.col("Remover_Prob")).drop(
                ["Remover_Prob", "Número de pedido JMS", "data de registro"], strict=False)
            print(f"\033[38;5;208m🟠 Problemáticos → Removidos: {removidos_prob} | Mantidos: {df_ret.height}\033[0m")

    # 🔵 CUSTÓDIA
    df_cust = ler_planilhas(DIR_CUSTODIA, "Custódia")
    df_final = df_ret
    if not df_cust.is_empty():
        col_pedido_c = safe_pick(df_cust, "Número de pedido JMS", ["pedido", "jms"])
        col_data_c = safe_pick(df_cust, "data de registro", ["data", "registro"])
        if col_pedido_c and col_data_c:
            df_cust = df_cust.select([col_pedido_c, col_data_c]).rename(
                {col_pedido_c: "Número de pedido JMS", col_data_c: "data de registro"})
            df_cust = limpar_pedidos(df_cust, "Número de pedido JMS")
            df_cust = converter_datetime(df_cust, "data de registro")
            df_cust = df_cust.group_by("Número de pedido JMS").agg(
                pl.col("data de registro").min().alias("data de registro"))
            df_cust = df_cust.with_columns(
                (pl.col("data de registro") + pl.duration(days=PRAZO_CUSTODIA_DIAS))
                .alias("Prazo_Limite"))
            df_join = df_ret.join(df_cust, left_on="Número do Pedido JMS 运单号", right_on="Número de pedido JMS",
                                  how="left")
            df_join = df_join.with_columns(
                pl.when(
                    (pl.col("Data da Atualização 更新日期") <= pl.col("Prazo_Limite"))
                    & pl.col("Prazo_Limite").is_not_null())
                .then(pl.lit("Dentro do Prazo"))
                .otherwise(pl.lit("Fora do Prazo"))
                .alias("Status_Custodia"))
            removidos_cust = df_join.filter(pl.col("Status_Custodia") == "Dentro do Prazo").height
            df_final = df_join.filter(pl.col("Status_Custodia") == "Fora do Prazo")
            print(f"\033[94m🔵 Custódia → Removidos: {removidos_cust} | Mantidos: {df_final.height}\033[0m")

    # 🧾 BASE DE REFERÊNCIA (00.1 - Base Retidos Lista)
    df_lista = ler_planilhas(DIR_BASE_LISTA, "Base Retidos (Lista)")
    if not df_lista.is_empty():
        col_base_lista = safe_pick(df_lista, "Nome da base de entrega", ["base", "entrega", "网点"])
        col_qtd_lista = safe_pick(df_lista, "Qtd a entregar há mais de 10 dias", ["qtd", "10", "dias"])
        if col_base_lista and col_qtd_lista:
            df_lista = df_lista.select([col_base_lista, col_qtd_lista]).rename({
                col_base_lista: "Nome da Base de Entrega",
                col_qtd_lista: "Qtd_Entregas_>10d"
            })
            df_lista = df_lista.with_columns(pl.col("Qtd_Entregas_>10d").cast(pl.Int64, strict=False))

            if "Base de Entrega 派件网点" in df_final.columns:
                df_resumo = (
                    df_final.group_by("Base de Entrega 派件网点")
                    .agg(pl.count().alias("Qtd_Retidos"))
                    .rename({"Base de Entrega 派件网点": "Nome da Base de Entrega"})
                )

                df_compara = df_lista.join(df_resumo, on="Nome da Base de Entrega", how="left")
                df_compara = df_compara.with_columns([
                    pl.col("Qtd_Retidos").fill_null(0).cast(pl.Int64).alias("Qtd_Retidos"),
                    ((pl.col("Qtd_Retidos") / pl.col("Qtd_Entregas_>10d")) * 100)
                    .round(2)
                    .alias("Percentual_Retidos")
                ])

                # Formata coluna percentual
                df_compara = df_compara.with_columns(
                    (pl.col("Percentual_Retidos").cast(pl.Utf8) + pl.lit(" %")).alias("Percentual_Retidos")
                )

                df_compara = df_compara.select([
                    "Nome da Base de Entrega",
                    "Qtd_Entregas_>10d",
                    "Qtd_Retidos",
                    "Percentual_Retidos"
                ]).sort("Qtd_Retidos", descending=True)

                out_lista = os.path.join(
                    DIR_OUT,
                    f"Comparativo_Base_Lista_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
                )
                df_compara.write_excel(out_lista)
                print(f"\n📊 Comparativo com Base Lista exportado: {out_lista}")

    # 📦 RESULTADO FINAL - Agregando por base para compatibilidade com o relatório
    if "Base de Entrega 派件网点" in df_final.columns:
        df_retidos_base = (
            df_final.group_by("Base de Entrega 派件网点")
            .agg(pl.count().alias("Qtd Retidos"))
            .rename({"Base de Entrega 派件网点": "Nome da base"})
        )

        # Normaliza nomes das bases
        df_retidos_base = _normalize_base(df_retidos_base)

        # Adiciona coluna de percentual de retidos
        df_retidos_base = df_retidos_base.with_columns(
            (pl.col("Qtd Retidos") / pl.col("Qtd Retidos").sum() * 100).round(2).alias("% Retidos")
        )
    else:
        df_retidos_base = pl.DataFrame({"Nome da base": [], "Qtd Retidos": [], "% Retidos": []})

    # 📊 Salva resultado detalhado
    out_final = os.path.join(
        DIR_OUT,
        f"Resultado_Detalhado_Retidos_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    )
    df_final.write_excel(out_final)
    print(f"\n📊 Resultado detalhado exportado: {out_final}")

    print("\n==============================")
    print("📦 RESUMO FINAL DE PROCESSAMENTO")
    print("==============================")
    print(f"📊 Total Retidos iniciais: {total_inicial + removidos_cluster}")
    print(f"🟣 Removidos por Cluster (1–9 dias): {removidos_cluster}")
    print(f"🟡 Removidos por Devolução: {removidos_dev}")
    print(f"🟠 Removidos por Problemáticos: {removidos_prob}")
    print(f"🔵 Removidos por Custódia: {removidos_cust}")
    print(f"✅ Pedidos restantes (fora do prazo): {df_final.height}")

    return df_retidos_base


# ==========================================================
# 🧮 Consolidação de Dados
# ==========================================================
def consolidar():
    """
    Consolida todos os dados em um único DataFrame.
    """
    dias = calendar.monthrange(datetime.now().year, datetime.now().month)[1]

    # 🔹 Lê a base de coordenadores (Base_Dados_Geral.xlsx)
    path_coord = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Dados_Geral.xlsx"
    df_coord = read_excel_silent(path_coord)

    if df_coord.is_empty():
        print("⚠️ Planilha Base_Dados_Geral.xlsx não encontrada ou vazia.")
        df_coord = pl.DataFrame(
            {"Nome da base": [], "Coordenador": [], "Supervisor": [], "Líder": [], "Assistente": []})
    else:
        # 🔍 Detecta automaticamente a coluna com nome da base
        col_base = None
        for possible in ["Base", "Nome da base", "Unidade", "Unidade responsável"]:
            if possible in df_coord.columns:
                col_base = possible
                break

        if col_base is None:
            raise SystemExit("❌ Nenhuma coluna identificada como 'Base' ou equivalente em Base_Dados_Geral.xlsx")

        rename_cols = {
            col_base: "Nome da base",
            "Coordenador": "Coordenador",
            "Supervisor": "Supervisor",
            "Líder": "Líder",
            "Assistente": "Assistente"
        }

        df_coord = df_coord.rename(rename_cols)
        df_coord = df_coord.select([c for c in rename_cols.values() if c in df_coord.columns])

        # Normaliza nomes das bases (ex: "BSB DF" → "DF BSB")
        def limpar_nome(nome: str) -> str:
            if not nome:
                return ""
            nome = str(nome).upper().strip()
            nome = re.sub(r"[^\x00-\x7F]+", "", nome)
            nome = re.sub(r"[-_]+", " ", nome)
            nome = re.sub(r"\s+", " ", nome)
            partes = nome.split(" ")
            if len(partes) == 2 and len(partes[0]) == 3 and len(partes[1]) == 2:
                nome = f"{partes[1]} {partes[0]}"
            elif len(partes) == 2 and len(partes[0]) == 2 and len(partes[1]) == 3:
                nome = f"{partes[0]} {partes[1]}"
            return nome.strip()

        df_coord = df_coord.with_columns(
            pl.col("Nome da base").map_elements(limpar_nome, return_dtype=pl.Utf8).alias("Nome da base")
        )

        print(f"✅ {df_coord.height} bases carregadas e padronizadas de Base_Dados_Geral.xlsx")

    # 🔹 Lê as demais bases
    df_coleta = coleta_expedicao()
    df_t0 = taxa_t0()
    # Substituímos as funções de shipping time pela análise de retidos
    df_retidos = analisar_retidos()
    df_ress = ressarcimento_por_pacote(df_coleta)
    df_sem, _ = pacotes_sem_mov()

    # 🔹 Junta tudo com segurança
    df_final = _safe_full_join(df_t0, df_retidos)
    df_final = _safe_full_join(df_final, df_ress)
    df_final = _safe_full_join(df_final, df_sem)
    df_final = _safe_full_join(df_final, df_coleta)

    # 🔹 Garante todas as bases da planilha Base_Atualizada
    df_final = _safe_full_join(df_coord, df_final)

    # 🔹 Calcula Taxa Sem Movimentação
    df = df_final.fill_null(0).with_columns([
        (pl.when(pl.col("Total Coleta+Entrega") > 0)
         .then(pl.col("Qtd Sem Mov") / dias / pl.col("Total Coleta+Entrega"))
         .otherwise(0)).alias("Taxa Sem Mov.")
    ])

    # Atualizamos as colunas ordenadas para incluir as novas colunas de retidos
    ordered = [
        "Nome da base",
        "Coordenador",
        "Supervisor",
        "Líder",
        "Assistente",
        "SLA (%)",
        "Qtd Retidos",
        "% Retidos",
        "Ressarcimento p/pct (R$)",
        "Custo total (R$)",
        "Qtd Sem Mov",
        "Taxa Sem Mov."
    ]

    # Garante que todas as colunas necessárias existam
    for c in ordered:
        if c not in df.columns:
            if c == "Nome da base":
                df = df.with_columns(pl.lit("").alias(c))
            else:
                df = df.with_columns(pl.lit(None).alias(c))

    # Correção: Processa apenas colunas válidas (não vazias) que existem no DataFrame
    valid_cols = [c for c in df.columns if c and c.strip() and c in df.schema]

    # Trata colunas de texto separadamente das colunas numéricas
    text_cols = [c for c in valid_cols if df.schema[c] == pl.Utf8]
    numeric_cols = [c for c in valid_cols if df.schema[c] != pl.Utf8]

    # Aplica tratamento de nulos apenas às colunas de texto (sem fill_nan)
    if text_cols:
        df = df.with_columns([
            pl.col(c).fill_null("") for c in text_cols
        ])

    # Aplica tratamento de nulos apenas às colunas numéricas
    if numeric_cols:
        df = df.with_columns([
            pl.col(c).fill_null(0).fill_nan(0) for c in numeric_cols
        ])

    return df.select(ordered).unique(subset=["Nome da base"], keep="first")


# ==========================================================
# 💾 Exportar Relatório Formatado
# ==========================================================
def main():
    """
    Função principal que executa o processo de consolidação e geração do relatório.
    """
    df = consolidar()
    if df.is_empty():
        print("⚠️ Nenhum dado consolidado.")
        return

    # Caminho de saída
    out = os.path.join(DIR_OUT, f"Resumo_Politica_Bonificacao_{datetime.now():%Y%m%d_%H%M%S}.xlsx")
    df_pd = df.to_pandas()

    # Escrita e formatação
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        startrow = 6
        df_pd.to_excel(writer, sheet_name="Bonificação", startrow=startrow, startcol=0, header=True, index=False)

        wb, ws = writer.book, writer.sheets["Bonificação"]

        # Formatos
        red = wb.add_format(
            {"bold": True, "font_color": "white", "align": "center", "valign": "vcenter", "bg_color": "#C00000",
             "border": 1})
        gray = wb.add_format(
            {"bold": True, "font_color": "white", "align": "center", "valign": "vcenter", "bg_color": "#595959",
             "border": 1})
        center = wb.add_format({"align": "center", "valign": "vcenter"})
        fmt_percent_2 = wb.add_format({"num_format": "0.00%", "align": "center"})
        fmt_money = wb.add_format({"num_format": '"R$"#,##0.00', "align": "center"})
        fmt_number = wb.add_format({"num_format": "#,##0.00", "align": "center"})
        fmt_int = wb.add_format({"num_format": "0", "align": "center"})

        # Cabeçalhos
        ws.merge_range("A1:M1", "RESULTADOS DE INDICADORES — POLÍTICA DE BONIFICAÇÃO", red)
        ws.merge_range("A2:M2", f"Data de atualização: {datetime.now():%d/%m/%Y}", gray)
        ws.merge_range("A5:E5", "Equipe de Responsáveis", gray)
        ws.merge_range("F5:M5", "Indicadores de Desempenho", gray)

        # Atualizamos os cabeçalhos para refletir as novas colunas
        headers = [
            ("A6", "Nome da base"),
            ("B6", "Coordenador"),
            ("C6", "Supervisor"),
            ("D6", "Líder"),
            ("E6", "Assistente"),
            ("F6", "SLA (%)"),
            ("G6", "Qtd Retidos"),
            ("H6", "% Retidos"),
            ("I6", "Ressarcimento p/pct (R$)"),
            ("J6", "Custo total (R$)"),
            ("K6", "Qtd Sem Mov"),
            ("L6", "Taxa Sem Mov.")
        ]
        for c, t in headers:
            ws.write(c, t, red)

        # Largura e formatação
        ws.set_column("A:E", 22, center)
        ws.set_column("F:F", 12, fmt_percent_2)
        ws.set_column("G:G", 14, fmt_int)
        ws.set_column("H:H", 12, fmt_percent_2)
        ws.set_column("I:J", 16, fmt_money)
        ws.set_column("K:K", 14, fmt_int)
        ws.set_column("L:L", 14, fmt_percent_2)

        # Congela cabeçalhos
        ws.freeze_panes(7, 0)

    print(f"✅ Relatório final gerado com sucesso!\n📂 {out}")


# ==========================================================
# Execução do Script
# ==========================================================
if __name__ == "__main__":
    main()