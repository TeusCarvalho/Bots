# -*- coding: utf-8 -*-
import os
import logging
import time
from datetime import datetime
from tqdm import tqdm
import polars as pl

# ===========================================================
# 🧾 Configuração de Logs Coloridos
# ===========================================================
class ColorFormatter(logging.Formatter):
    COLORS = {
        logging.INFO: "\033[92m",
        logging.WARNING: "\033[93m",
        logging.ERROR: "\033[91m",
    }

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        reset = "\033[0m"
        message = super().format(record)
        return f"{color}{message}{reset}"

handler = logging.StreamHandler()
handler.setFormatter(ColorFormatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().handlers = [handler]
logging.getLogger().setLevel(logging.INFO)

# ===========================================================
# 📋 Colunas principais
# ===========================================================
COLUNAS = {
    "motorista": "Responsável pela entrega",
    "pedido": "Número de pedido JMS",
    "assinatura": "Marca de assinatura",
    "tempo_entrega": "Tempo de entrega",
    "horario_entrega": "Horário da entrega",
    "base": "Base de entrega",
    "coordenador": "Coordenador"
}

# ===========================================================
# 🧠 Helpers de compatibilidade (Polars antigo)
# ===========================================================
def normalize_text_col(col_name: str) -> pl.Expr:
    """
    Normaliza texto de uma coluna:
    - cast para Utf8
    - strip (compat: strip_chars)
    - upper
    """
    e = pl.col(col_name).cast(pl.Utf8, strict=False)

    if hasattr(e.str, "strip_chars"):
        e = e.str.strip_chars()
    else:
        e = e.str.strip()

    if hasattr(e.str, "to_uppercase"):
        e = e.str.to_uppercase()
    elif hasattr(e.str, "uppercase"):
        e = e.str.uppercase()

    return e.alias(col_name)


def resolve_col(columns, *candidates):
    """
    Resolve nome real da coluna por matching case-insensitive e strip.
    Retorna o nome real ou None.
    """
    norm = {str(c).strip().lower(): c for c in columns}
    for cand in candidates:
        key = str(cand).strip().lower()
        if key in norm:
            return norm[key]
    return None


def first_existing(df: pl.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ===========================================================
# 📂 Funções auxiliares
# ===========================================================
def encontrar_arquivos_por_prefixo(pasta, prefixo):
    arquivos = []
    try:
        logging.info(f"📂 Listando arquivos em: {pasta}")
        todos = [f for f in os.listdir(pasta) if f.lower().endswith(".xlsx")]
        if not todos:
            logging.warning("⚠️ Nenhum arquivo .xlsx encontrado na pasta.")
            return arquivos

        logging.info("📋 Arquivos encontrados:")
        for nome in todos:
            logging.info(f"   • {nome}")

        if prefixo.strip() == "":
            arquivos = [os.path.join(pasta, nome) for nome in todos]
            logging.info("✅ Nenhum prefixo informado — todos os arquivos .xlsx serão processados.")
        else:
            arquivos = [os.path.join(pasta, f) for f in todos if prefixo.lower() in f.lower()]
            if arquivos:
                logging.info(f"✅ {len(arquivos)} arquivo(s) encontrado(s) contendo '{prefixo}'")
            else:
                logging.warning(f"⚠️ Nenhum arquivo encontrado contendo '{prefixo}'")
    except Exception as e:
        logging.error(f"❌ Erro ao procurar arquivos: {e}")
    return arquivos


def ler_excel_polars(caminho):
    nome = os.path.basename(caminho)
    try:
        df = pl.read_excel(caminho)
        logging.info(f"📄 [{nome}] Lido com sucesso ({len(df):,} linhas)")
        return df
    except Exception as e:
        logging.error(f"❌ Erro ao ler {nome}: {e}")
        return None


# ===========================================================
# 📌 Função de Coordenadores (robusta)
# ===========================================================
def adicionar_coordenador(df):
    caminho_ref = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
    if not os.path.exists(caminho_ref):
        logging.error("❌ Base de coordenadores não encontrada.")
        # garante colunas existirem para não quebrar relatórios
        if "UF" not in df.columns:
            df = df.with_columns(pl.lit(None).alias("UF"))
        if "Coordenador" not in df.columns:
            df = df.with_columns(pl.lit(None).alias("Coordenador"))
        return df

    try:
        ref = pl.read_excel(caminho_ref)
        ref = ref.rename({c: c.strip() for c in ref.columns})

        # resolve colunas na referência (tolerante)
        col_base_ref = resolve_col(ref.columns, "Nome da base", "Nome da Base", "NOME DA BASE")
        col_uf_ref = resolve_col(ref.columns, "UF", "Uf", "U F", "Estado", "ESTADO")
        col_coord_ref = resolve_col(ref.columns, "Coordenador", "COORDENADOR")

        if not col_base_ref:
            logging.error("❌ Na Base_Atualizada.xlsx não encontrei a coluna 'Nome da base'.")
            if "UF" not in df.columns:
                df = df.with_columns(pl.lit(None).alias("UF"))
            if "Coordenador" not in df.columns:
                df = df.with_columns(pl.lit(None).alias("Coordenador"))
            return df

        # Normaliza base_ref
        ref = ref.with_columns([
            normalize_text_col(col_base_ref).alias("base_ref"),
            (pl.col(col_uf_ref).cast(pl.Utf8, strict=False).alias("UF") if col_uf_ref else pl.lit(None).alias("UF")),
            (pl.col(col_coord_ref).cast(pl.Utf8, strict=False).alias("Coordenador") if col_coord_ref else pl.lit(None).alias("Coordenador")),
        ])

        if COLUNAS["base"] not in df.columns:
            logging.warning(f"⚠️ '{COLUNAS['base']}' não encontrada no arquivo atual.")
            if "UF" not in df.columns:
                df = df.with_columns(pl.lit(None).alias("UF"))
            if "Coordenador" not in df.columns:
                df = df.with_columns(pl.lit(None).alias("Coordenador"))
            return df

        # Normaliza base no df
        df = df.with_columns([normalize_text_col(COLUNAS["base"])])

        # Join seguro
        df = df.join(
            ref.select(["base_ref", "UF", "Coordenador"]),
            left_on=COLUNAS["base"],
            right_on="base_ref",
            how="left"
        ).drop("base_ref")

        return df

    except Exception as e:
        logging.error(f"❌ Erro ao adicionar coordenador: {e}")
        # garante colunas existirem para não quebrar relatórios
        if "UF" not in df.columns:
            df = df.with_columns(pl.lit(None).alias("UF"))
        if "Coordenador" not in df.columns:
            df = df.with_columns(pl.lit(None).alias("Coordenador"))
        return df


# ===========================================================
# 📦 Processamento dos arquivos
# ===========================================================
def processar_arquivos(lista_arquivos):
    dfs = []
    colunas_totais = set()

    for caminho in tqdm(lista_arquivos, desc="📦 Lendo planilhas", colour="green"):
        df = ler_excel_polars(caminho)
        if df is None:
            continue

        df = df.rename({c: c.strip() for c in df.columns})

        df = df.with_columns([
            pl.col(c).cast(pl.Utf8, strict=False).alias(c)
            for c in df.columns
        ])

        if COLUNAS["base"] in df.columns:
            df = df.with_columns([normalize_text_col(COLUNAS["base"])])

        colunas_totais.update(df.columns)

        if COLUNAS["tempo_entrega"] in df.columns:
            df = df.with_columns(
                pl.col(COLUNAS["tempo_entrega"]).str.strptime(pl.Datetime, strict=False).alias("tempo_entrega_dt")
            )

        if COLUNAS["horario_entrega"] in df.columns:
            df = df.with_columns(
                pl.col(COLUNAS["horario_entrega"]).str.strptime(pl.Datetime, strict=False).alias("horario_entrega_dt")
            )

        if {"tempo_entrega_dt", "horario_entrega_dt", COLUNAS["assinatura"]}.issubset(df.columns):
            df = df.with_columns([
                (
                    (pl.col("tempo_entrega_dt").dt.weekday() == 6)
                    & (pl.col("horario_entrega_dt").is_not_null())
                    & (pl.col(COLUNAS["assinatura"]).str.strip_chars() == "Recebimento com assinatura normal")
                ).alias("Entrega válida no domingo")
            ])

        df = adicionar_coordenador(df)
        dfs.append(df)

    if not dfs:
        return None

    dfs_padronizados = []
    for df in dfs:
        faltantes = colunas_totais - set(df.columns)
        for col in faltantes:
            df = df.with_columns(pl.lit(None).alias(col))

        dfs_padronizados.append(df.select(sorted(colunas_totais)))

    return pl.concat(dfs_padronizados, how="vertical")


# ===========================================================
# 📌 Planilha final: quantidade por Base de entrega
# ===========================================================
def gerar_planilha_quantidade_por_base(df: pl.DataFrame, caminho_saida: str):
    try:
        if df is None or df.is_empty():
            logging.error("❌ DataFrame vazio — não foi possível gerar 'Quantidade por Base'.")
            return

        if COLUNAS["base"] not in df.columns:
            logging.error(f"❌ Coluna '{COLUNAS['base']}' não existe no DataFrame.")
            return

        qtd_por_base = (
            df.with_columns([normalize_text_col(COLUNAS["base"])])
            .group_by(COLUNAS["base"])
            .agg(pl.count().alias("Quantidade"))
            .sort("Quantidade", descending=True)
        )

        with pl.ExcelWriter(caminho_saida) as writer:
            writer.write(qtd_por_base, sheet_name="Qtd por Base")

        logging.info(f"💾 Planilha 'Quantidade por Base' salva em:\n📍 {caminho_saida}")

    except Exception as e:
        logging.error(f"❌ Erro ao gerar planilha por base: {e}")


# ===========================================================
# 📊 Geração do relatório final (SEM depender de UF fixo)
# ===========================================================
def gerar_relatorio(df, caminho_saida):
    try:
        total_pedidos = df.height

        total_motoristas = (
            df.select(pl.col(COLUNAS["motorista"]).n_unique()).item()
            if COLUNAS["motorista"] in df.columns else 0
        )

        total_domingo = (
            df.filter(pl.col("Entrega válida no domingo")).height
            if "Entrega válida no domingo" in df.columns else 0
        )

        total_bases = (
            df.select(pl.col(COLUNAS["base"]).n_unique()).item()
            if COLUNAS["base"] in df.columns else 0
        )

        logging.info(f"""
📊 RESUMO GERAL:
• Total de pedidos: {total_pedidos:,}
• Motoristas únicos: {total_motoristas:,}
• Entregas válidas no domingo: {total_domingo:,}
• Bases distintas: {total_bases:,}
""")

        # ✅ UF: usa "UF" se existir, senão usa "UF Destino"
        uf_col = first_existing(df, ["UF", "UF Destino"])
        if not uf_col:
            logging.warning("⚠️ Nenhuma coluna de UF encontrada (nem 'UF' nem 'UF Destino'). Resumo por UF ficará vazio.")
            resumo_uf = pl.DataFrame({
                "UF": [],
                "Total de Pedidos Recebidos": [],
                "Motoristas Únicos": [],
                "Bases Distintas": [],
                "Entregas válidas no domingo": []
            })
        else:
            resumo_uf = (
                df.group_by(uf_col)
                .agg([
                    pl.count().alias("Total de Pedidos Recebidos"),
                    (pl.col(COLUNAS["motorista"]).n_unique().alias("Motoristas Únicos") if COLUNAS["motorista"] in df.columns else pl.lit(0).alias("Motoristas Únicos")),
                    (pl.col(COLUNAS["base"]).n_unique().alias("Bases Distintas") if COLUNAS["base"] in df.columns else pl.lit(0).alias("Bases Distintas")),
                    (pl.col("Entrega válida no domingo").sum().alias("Entregas válidas no domingo") if "Entrega válida no domingo" in df.columns else pl.lit(0).alias("Entregas válidas no domingo")),
                ])
                .rename({uf_col: "UF"})
                .sort("UF")
            )

        resumo_geral = pl.DataFrame({
            "Métrica": [
                "Total de pedidos recebidos",
                "Motoristas únicos (geral)",
                "Entregas válidas no domingo (geral)",
                "Bases distintas (Base de entrega)"
            ],
            "Quantidade": [total_pedidos, total_motoristas, total_domingo, total_bases]
        })

        lista_motoristas = (
            df.group_by(COLUNAS["motorista"])
            .agg(pl.count().alias("Total de Pedidos"))
            .rename({COLUNAS["motorista"]: "Motorista"})
            .sort("Total de Pedidos", descending=True)
        ) if COLUNAS["motorista"] in df.columns else pl.DataFrame({"Motorista": [], "Total de Pedidos": []})

        qtd_por_base = (
            df.group_by(COLUNAS["base"])
            .agg(pl.count().alias("Quantidade"))
            .rename({COLUNAS["base"]: "Base de Entrega"})
            .sort("Quantidade", descending=True)
        ) if COLUNAS["base"] in df.columns else pl.DataFrame({"Base de Entrega": [], "Quantidade": []})

        with pl.ExcelWriter(caminho_saida) as writer:
            writer.write(df, sheet_name="Detalhes")
            writer.write(resumo_uf, sheet_name="Resumo por UF")
            writer.write(resumo_geral, sheet_name="Resumo Geral")
            writer.write(lista_motoristas, sheet_name="Motoristas")
            writer.write(qtd_por_base, sheet_name="Qtd por Base")

        logging.info(f"💾 Arquivo salvo com sucesso em:\n📍 {caminho_saida}")

    except Exception as e:
        logging.error(f"❌ Erro ao gerar relatório: {e}")


# ===========================================================
# 🚀 Execução Principal
# ===========================================================
if __name__ == "__main__":
    inicio = time.time()

    PASTA = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Motorista"
    PREFIXO = input("🔎 Digite parte do nome dos arquivos a processar (ex: Outubro, Exportar... ou deixe vazio para todos): ").strip()

    arquivos = encontrar_arquivos_por_prefixo(PASTA, PREFIXO)
    if not arquivos:
        logging.error("❌ Nenhum arquivo encontrado.")
    else:
        df = processar_arquivos(arquivos)
        if df is not None:
            agora = datetime.now().strftime("%Y%m%d_%H%M%S")

            caminho_saida = os.path.join(PASTA, f"Analise_Consolidada_{agora}.xlsx")
            gerar_relatorio(df, caminho_saida)

            caminho_saida_base = os.path.join(PASTA, f"Quantidade_por_Base_{agora}.xlsx")
            gerar_planilha_quantidade_por_base(df, caminho_saida_base)

        else:
            logging.error("❌ Nenhum dado processado.")

    fim = time.time()
    minutos, segundos = divmod(fim - inicio, 60)
    logging.info(f"⏱️ Tempo total de execução: {int(minutos)}m {int(segundos)}s")
