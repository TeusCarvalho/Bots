# -*- coding: utf-8 -*-
import os
import logging
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import polars as pl

# ===========================================================
# Configuração de Logs Coloridos
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
# Configuração de Colunas
# ===========================================================
COLUNAS = {
    "coordenador": "Coordenador",
    "motorista": "Responsável pela entrega",
    "pedido": "Número de pedido JMS",
    "assinatura": "Marca de assinatura",
    "data": "Tempo de entrega",
    "base": "Base de entrega"
}

# ===========================================================
# Funções Principais
# ===========================================================

def encontrar_arquivos_por_prefixo(pasta, prefixo):
    arquivos = []
    try:
        for nome_arquivo in os.listdir(pasta):
            if nome_arquivo.startswith(prefixo) and nome_arquivo.endswith('.xlsx'):
                arquivos.append(os.path.join(pasta, nome_arquivo))
        if arquivos:
            logging.info(f"✅ {len(arquivos)} arquivo(s) encontrado(s) com prefixo '{prefixo}'")
        else:
            logging.error(f"❌ Nenhum arquivo Excel começando com '{prefixo}' foi encontrado na pasta '{pasta}'.")
        return arquivos
    except FileNotFoundError:
        logging.error(f"❌ O diretório especificado não foi encontrado: {pasta}")
        return []

def ler_arquivo_excel_inteligente(caminho):
    """Decide entre Pandas e Polars conforme o tamanho do arquivo."""
    tamanho_mb = os.path.getsize(caminho) / (1024 ** 2)
    nome = os.path.basename(caminho)

    try:
        if tamanho_mb > 100:
            logging.info(f"⚙️ [{nome}] Usando Polars Lazy Mode ({tamanho_mb:.1f} MB)...")
            df = pl.read_excel(caminho).lazy().collect().to_pandas()
        else:
            logging.info(f"📄 [{nome}] Usando Pandas (arquivo leve: {tamanho_mb:.1f} MB)")
            df = pd.read_excel(caminho)
        return df
    except Exception as e:
        logging.error(f"❌ Erro ao ler {nome}: {e}")
        return None

def adicionar_coordenador(df):
    caminho_ref = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Coordenador\Base_Atualizada.xlsx"
    if not os.path.exists(caminho_ref):
        logging.error("❌ Planilha de referência não encontrada.")
        return df

    df_ref = pd.read_excel(caminho_ref)
    col_base_ref = "Nome da base"
    col_uf_ref = "UF"
    col_coord_ref = next((c for c in df_ref.columns if "coordenador" in c.lower()), None)

    if not col_coord_ref or col_base_ref not in df_ref.columns or col_uf_ref not in df_ref.columns:
        logging.error("❌ Colunas de referência não identificadas.")
        return df

    df_ref[col_base_ref] = df_ref[col_base_ref].astype(str).str.strip().str.upper()
    df[COLUNAS["base"]] = df[COLUNAS["base"]].astype(str).str.strip().str.upper()

    df = df.merge(
        df_ref[[col_base_ref, col_coord_ref, col_uf_ref]],
        left_on=COLUNAS["base"],
        right_on=col_base_ref,
        how="left"
    )

    df.rename(columns={col_coord_ref: COLUNAS["coordenador"], col_uf_ref: "UF"}, inplace=True)
    df[COLUNAS["coordenador"]] = df[COLUNAS["coordenador"]].fillna("Coordenador não encontrado")
    df["UF"] = df["UF"].fillna("UF não encontrado")

    return df

def processar_varios_arquivos(lista_arquivos):
    dfs = []
    logging.info("📊 Iniciando leitura dos arquivos...")

    for caminho in tqdm(lista_arquivos, desc="🔍 Processando", colour="green"):
        df = ler_arquivo_excel_inteligente(caminho)
        if df is None:
            continue

        try:
            df.columns = df.columns.astype(str).str.strip()
            df['data_convertida'] = pd.to_datetime(df[COLUNAS["data"]], errors='coerce')

            df['Entrega no Domingo'] = (
                (df['data_convertida'].dt.dayofweek == 6) &
                (df[COLUNAS["assinatura"]].astype(str).str.strip() == "Recebimento com assinatura normal")
            )

            df = adicionar_coordenador(df)
            dfs.append(df)
        except Exception as e:
            logging.error(f"❌ Erro ao processar {os.path.basename(caminho)}: {e}")

    if not dfs:
        logging.error("❌ Nenhum DataFrame carregado.")
        return None

    df_final = pd.concat(dfs, ignore_index=True)
    logging.info(f"✅ Total de registros consolidados: {len(df_final)}")
    return df_final

def analisar_entregas_consolidado(df, caminho_saida):
    # (igual ao seu código original — sem alteração)
    ...

# ===========================================================
# Execução Principal
# ===========================================================
if __name__ == "__main__":
    PREFIXO_DO_ARQUIVO = "Exportar carta de porte de entrega"
    caminho_da_pasta = r"C:\Users\J&T-099\OneDrive - Speed Rabbit Express Ltda (1)\Área de Trabalho\Testes\Motorista"

    arquivos_encontrados = encontrar_arquivos_por_prefixo(caminho_da_pasta, PREFIXO_DO_ARQUIVO)

    if arquivos_encontrados:
        df_consolidado = processar_varios_arquivos(arquivos_encontrados)

        if df_consolidado is not None:
            agora = datetime.now().strftime("%Y%m%d_%H%M%S")
            caminho_saida = os.path.join(caminho_da_pasta, f"Analise_Consolidada_{agora}.xlsx")

            analisar_entregas_consolidado(df_consolidado, caminho_saida)

            logging.info(f"""
📊 Resumo Final:
• Pedidos: {df_consolidado[COLUNAS["pedido"]].nunique()}
• Coordenadores: {df_consolidado[COLUNAS["coordenador"]].nunique()}
• Bases: {df_consolidado[COLUNAS["base"]].nunique()}
• Motoristas únicos: {df_consolidado[COLUNAS["motorista"]].nunique()}
""")
    else:
        logging.error("❌ Nenhum arquivo válido foi encontrado.")
