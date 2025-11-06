# -*- coding: utf-8 -*-
import os
import logging
import time
from datetime import datetime
from tqdm import tqdm
import pandas as pd
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
            for nome in todos:
                if prefixo.lower() in nome.lower():
                    arquivos.append(os.path.join(pasta, nome))

            if arquivos:
                logging.info(f"✅ {len(arquivos)} arquivo(s) encontrado(s) contendo '{prefixo}' no nome")
            else:
                logging.warning(f"⚠️ Nenhum arquivo encontrado contendo '{prefixo}'")
    except Exception as e:
        logging.error(f"❌ Erro ao procurar arquivos: {e}")
    return arquivos


def ler_excel_inteligente(caminho):
    """Usa Pandas ou Polars dependendo do tamanho."""
    tamanho_mb = os.path.getsize(caminho) / (1024 ** 2)
    nome = os.path.basename(caminho)
    try:
        if tamanho_mb > 100:
            logging.info(f"⚙️ [{nome}] Usando Polars Lazy Mode ({tamanho_mb:.1f} MB)...")
            df = pl.read_excel(caminho).lazy().collect().to_pandas()
        else:
            logging.info(f"📄 [{nome}] Usando Pandas ({tamanho_mb:.1f} MB)")
            df = pd.read_excel(caminho)
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

    ref = pd.read_excel(caminho_ref)
    col_base_ref = "Nome da base"
    col_uf_ref = "UF"
    col_coord_ref = next((c for c in ref.columns if "coordenador" in c.lower()), None)

    if not all(col in ref.columns for col in [col_base_ref, col_uf_ref]) or not col_coord_ref:
        logging.error("❌ Colunas necessárias ausentes na base de referência.")
        return df

    ref[col_base_ref] = ref[col_base_ref].astype(str).str.strip().str.upper()
    df[COLUNAS["base"]] = df[COLUNAS["base"]].astype(str).str.strip().str.upper()

    df = df.merge(ref[[col_base_ref, col_uf_ref, col_coord_ref]],
                  left_on=COLUNAS["base"], right_on=col_base_ref, how="left")

    df.rename(columns={col_coord_ref: COLUNAS["coordenador"], col_uf_ref: "UF"}, inplace=True)
    df["UF"] = df["UF"].fillna("UF não encontrado")
    df[COLUNAS["coordenador"]] = df[COLUNAS["coordenador"]].fillna("Coordenador não encontrado")
    return df


def processar_arquivos(lista_arquivos):
    """Processa e gera coluna de entrega válida no domingo."""
    dfs = []
    for caminho in tqdm(lista_arquivos, desc="📦 Lendo planilhas", colour="green"):
        df = ler_excel_inteligente(caminho)
        if df is None:
            continue

        df.columns = df.columns.astype(str).str.strip()

        df["tempo_entrega_dt"] = pd.to_datetime(df.get(COLUNAS["tempo_entrega"]), errors="coerce")
        df["horario_entrega_dt"] = pd.to_datetime(df.get(COLUNAS["horario_entrega"]), errors="coerce")

        df["Entrega válida no domingo"] = (
            (df["tempo_entrega_dt"].dt.dayofweek == 6)
            & (df["horario_entrega_dt"].notna())
            & (df[COLUNAS["assinatura"]].astype(str).str.strip() == "Recebimento com assinatura normal")
        )

        df = adicionar_coordenador(df)
        dfs.append(df)

    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)

# ===========================================================
# 📊 Geração do relatório final (atualizada)
# ===========================================================
def gerar_relatorio(df, caminho_saida):
    """Cria resumo geral, por UF, lista de motoristas e bases."""
    try:
        total_pedidos = len(df)
        total_motoristas = df[COLUNAS["motorista"]].nunique()
        total_domingo = df["Entrega válida no domingo"].sum()
        total_bases = df[COLUNAS["base"]].nunique()

        logging.info(f"""
📊 RESUMO GERAL:
• Total de pedidos: {total_pedidos:,}
• Motoristas únicos: {total_motoristas:,}
• Entregas válidas no domingo: {total_domingo:,}
• Bases distintas (geral): {total_bases:,}
""")

        resumo_uf = (
            df.groupby("UF")
            .agg({
                COLUNAS["pedido"]: "count",
                COLUNAS["motorista"]: pd.Series.nunique,
                COLUNAS["base"]: pd.Series.nunique,
                "Entrega válida no domingo": "sum"
            })
            .reset_index()
            .rename(columns={
                COLUNAS["pedido"]: "Total de Pedidos Recebidos",
                COLUNAS["motorista"]: "Motoristas Únicos",
                COLUNAS["base"]: "Bases Distintas (Base de entrega)",
                "Entrega válida no domingo": "Entregas válidas no domingo"
            })
            .sort_values("UF")
        )

        resumo_geral = pd.DataFrame({
            "Métrica": [
                "Total de pedidos recebidos",
                "Motoristas únicos (geral)",
                "Entregas válidas no domingo (geral)",
                "Bases distintas (Base de entrega)"
            ],
            "Quantidade": [total_pedidos, total_motoristas, total_domingo, total_bases]
        })

        # --- NOVO: Listas de motoristas e bases ---
        lista_motoristas = (
            df.groupby(COLUNAS["motorista"])
            .agg({COLUNAS["pedido"]: "count"})
            .reset_index()
            .rename(columns={
                COLUNAS["motorista"]: "Motorista",
                COLUNAS["pedido"]: "Total de Pedidos"
            })
            .sort_values("Total de Pedidos", ascending=False)
        )

        lista_bases = (
            df.groupby(COLUNAS["base"])
            .agg({COLUNAS["pedido"]: "count"})
            .reset_index()
            .rename(columns={
                COLUNAS["base"]: "Base de Entrega",
                COLUNAS["pedido"]: "Total de Pedidos"
            })
            .sort_values("Total de Pedidos", ascending=False)
        )

        with pd.ExcelWriter(caminho_saida, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Detalhes")
            resumo_uf.to_excel(writer, index=False, sheet_name="Resumo por UF")
            resumo_geral.to_excel(writer, index=False, sheet_name="Resumo Geral")
            lista_motoristas.to_excel(writer, index=False, sheet_name="Motoristas")
            lista_bases.to_excel(writer, index=False, sheet_name="Bases")

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
    duracao = fim - inicio
    minutos, segundos = divmod(duracao, 60)
    logging.info(f"⏱️ Tempo total de execução: {int(minutos)}m {int(segundos)}s")
