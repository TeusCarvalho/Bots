
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
                logging.info(f"✅ {len(arquivos)} arquivo(s) encontrado(s) contendo '{prefixo}' no nome")
            else:
                logging.warning(f"⚠️ Nenhum arquivo encontrado contendo '{prefixo}'")
    except Exception as e:
        logging.error(f"❌ Erro ao procurar arquivos: {e}")
    return arquivos


def ler_excel_polars(caminho):
    """Leitura rápida com Polars, com fallback seguro."""
    nome = os.path.basename(caminho)
    try:
        df = pl.read_excel(caminho)
        logging.info(f"📄 [{nome}] Lido com sucesso ({len(df):,} linhas)")
        return df
    except Exception as e:
        logging.error(f"❌ Erro ao ler {nome}: {e}")
        return None


def adicionar_coordenador(df):
    """Adiciona coordenador e UF a partir da base de referência."""
    caminho_ref = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
    if not os.path.exists(caminho_ref):
        logging.error("❌ Base de coordenadores não encontrada.")
        return df

    try:
        ref = pl.read_excel(caminho_ref)
        col_base_ref = "Nome da base"
        col_uf_ref = "UF"
        col_coord_ref = next((c for c in ref.columns if "coordenador" in c.lower()), None)

        if not all(c in ref.columns for c in [col_base_ref, col_uf_ref]) or not col_coord_ref:
            logging.error("❌ Colunas necessárias ausentes na base de referência.")
            return df

        ref = ref.with_columns([
            pl.col(col_base_ref).cast(pl.Utf8).str.strip_chars().str.to_uppercase()
        ])

        df = df.with_columns([
            pl.col(COLUNAS["base"]).cast(pl.Utf8).str.strip_chars().str.to_uppercase()
        ])

        df = df.join(
            ref.select([
                pl.col(col_base_ref).alias("base_ref"),
                pl.col(col_uf_ref),
                pl.col(col_coord_ref)
            ]),
            left_on=COLUNAS["base"],
            right_on="base_ref",
            how="left"
        )

        df = df.with_columns([
            pl.col(col_uf_ref).fill_null("UF não encontrado").alias("UF"),
            pl.col(col_coord_ref).fill_null("Coordenador não encontrado").alias(COLUNAS["coordenador"])
        ])

        return df.drop("base_ref")
    except Exception as e:
        logging.error(f"❌ Erro ao adicionar coordenador: {e}")
        return df


def processar_arquivos(lista_arquivos):
    dfs = []
    for caminho in tqdm(lista_arquivos, desc="📦 Lendo planilhas", colour="green"):
        df = ler_excel_polars(caminho)
        if df is None:
            continue

        # Normaliza colunas
        df = df.rename({c: c.strip() for c in df.columns})

        # Conversões de data
        df = df.with_columns([
            pl.col(COLUNAS["tempo_entrega"]).str.strptime(pl.Datetime, strict=False).alias("tempo_entrega_dt"),
            pl.col(COLUNAS["horario_entrega"]).str.strptime(pl.Datetime, strict=False).alias("horario_entrega_dt")
        ])

        # Entrega válida no domingo
        df = df.with_columns([
            (
                (pl.col("tempo_entrega_dt").dt.weekday() == 6)
                & (pl.col("horario_entrega_dt").is_not_null())
                & (pl.col(COLUNAS["assinatura"]).cast(pl.Utf8).str.strip_chars() == "Recebimento com assinatura normal")
            ).alias("Entrega válida no domingo")
        ])

        df = adicionar_coordenador(df)
        dfs.append(df)

    if not dfs:
        return None
    return pl.concat(dfs, how="vertical")


# ===========================================================
# 📊 Geração do relatório final
# ===========================================================
def gerar_relatorio(df, caminho_saida):
    try:
        total_pedidos = df.height
        total_motoristas = df.select(pl.col(COLUNAS["motorista"]).n_unique()).item()
        total_domingo = df.filter(pl.col("Entrega válida no domingo")).height
        total_bases = df.select(pl.col(COLUNAS["base"]).n_unique()).item()

        logging.info(f"""
📊 RESUMO GERAL:
• Total de pedidos: {total_pedidos:,}
• Motoristas únicos: {total_motoristas:,}
• Entregas válidas no domingo: {total_domingo:,}
• Bases distintas: {total_bases:,}
""")

        resumo_uf = (
            df.group_by("UF")
            .agg([
                pl.count(COLUNAS["pedido"]).alias("Total de Pedidos Recebidos"),
                pl.col(COLUNAS["motorista"]).n_unique().alias("Motoristas Únicos"),
                pl.col(COLUNAS["base"]).n_unique().alias("Bases Distintas"),
                pl.col("Entrega válida no domingo").sum().alias("Entregas válidas no domingo")
            ])
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
            .agg(pl.count(COLUNAS["pedido"]).alias("Total de Pedidos"))
            .rename({COLUNAS["motorista"]: "Motorista"})
            .sort("Total de Pedidos", descending=True)
        )

        lista_bases = (
            df.group_by(COLUNAS["base"])
            .agg(pl.count(COLUNAS["pedido"]).alias("Total de Pedidos"))
            .rename({COLUNAS["base"]: "Base de Entrega"})
            .sort("Total de Pedidos", descending=True)
        )

        with pl.ExcelWriter(caminho_saida) as writer:
            writer.write(df, sheet_name="Detalhes")
            writer.write(resumo_uf, sheet_name="Resumo por UF")
            writer.write(resumo_geral, sheet_name="Resumo Geral")
            writer.write(lista_motoristas, sheet_name="Motoristas")
            writer.write(lista_bases, sheet_name="Bases")

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
        else:
            logging.error("❌ Nenhum dado processado.")

    fim = time.time()
    minutos, segundos = divmod(fim - inicio, 60)
    logging.info(f"⏱️ Tempo total de execução: {int(minutos)}m {int(segundos)}s")

