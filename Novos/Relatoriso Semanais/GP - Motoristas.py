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
# 📌 Função de Coordenadores – CORRIGIDA
# ===========================================================
def adicionar_coordenador(df):
    caminho_ref = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
    if not os.path.exists(caminho_ref):
        logging.error("❌ Base de coordenadores não encontrada.")
        return df

    try:
        ref = pl.read_excel(caminho_ref)

        # Normaliza base de referência
        ref = ref.with_columns([
            pl.col("Nome da base").cast(pl.Utf8).str.strip().str.to_uppercase().alias("base_ref"),
            pl.col("UF").cast(pl.Utf8).alias("UF"),
            pl.col("Coordenador").cast(pl.Utf8).alias("Coordenador")
        ])

        if "Base de entrega" not in df.columns:
            logging.warning("⚠️ 'Base de entrega' não encontrada no arquivo atual.")
            return df

        # Normaliza coluna de base do arquivo principal
        df = df.with_columns([
            pl.col("Base de entrega").cast(pl.Utf8).str.strip().str.to_uppercase().alias("Base de entrega")
        ])

        # Join seguro
        df = df.join(
            ref.select(["base_ref", "UF", "Coordenador"]),
            left_on="Base de entrega",
            right_on="base_ref",
            how="left"
        )

        df = df.drop("base_ref")
        return df

    except Exception as e:
        logging.error(f"❌ Erro ao adicionar coordenador: {e}")
        return df


# ===========================================================
# 📦 Processamento dos arquivos – CORRIGIDO
# ===========================================================
def processar_arquivos(lista_arquivos):
    dfs = []
    colunas_totais = set()

    for caminho in tqdm(lista_arquivos, desc="📦 Lendo planilhas", colour="green"):
        df = ler_excel_polars(caminho)
        if df is None:
            continue

        # Normaliza nomes das colunas
        df = df.rename({c: c.strip() for c in df.columns})

        # Converte tudo para texto (impede erro de dtype)
        df = df.with_columns([
            pl.col(c).cast(pl.Utf8, strict=False).alias(c)
            for c in df.columns
        ])

        # Atualiza o conjunto total de colunas
        colunas_totais.update(df.columns)

        # Conversões de data
        if COLUNAS["tempo_entrega"] in df.columns:
            df = df.with_columns(
                pl.col(COLUNAS["tempo_entrega"]).str.strptime(pl.Datetime, strict=False).alias("tempo_entrega_dt")
            )

        if COLUNAS["horario_entrega"] in df.columns:
            df = df.with_columns(
                pl.col(COLUNAS["horario_entrega"]).str.strptime(pl.Datetime, strict=False).alias("horario_entrega_dt")
            )

        # Domingos válidos
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

    # Padroniza colunas em todos os arquivos
    dfs_padronizados = []
    for df in dfs:
        faltantes = colunas_totais - set(df.columns)
        for col in faltantes:
            df = df.with_columns(pl.lit(None).alias(col))

        dfs_padronizados.append(df.select(sorted(colunas_totais)))

    # Finalmente concatena SEM ERROS
    return pl.concat(dfs_padronizados, how="vertical")


# ===========================================================
# 📊 Geração do relatório final
# ===========================================================
def gerar_relatorio(df, caminho_saida):
    try:
        total_pedidos = df.height
        total_motoristas = df.select(pl.col(COLUNAS["motorista"]).n_unique()).item()
        total_domingo = df.filter(pl.col("Entrega válida no domingo")).height if "Entrega válida no domingo" in df.columns else 0
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
                pl.col("Entrega válida no domingo").sum().alias("Entregas válidas no domingo") if "Entrega válida no domingo" in df.columns else pl.lit(0)
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